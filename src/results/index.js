import { config as defaultConfig } from '../config/env.js';
import { seedCatalog } from './catalog.js';
import { createKeys } from './keys.js';
import { createBackpressure } from './backpressure.js';
import { createFanout, serialize, yieldToLoop } from './fanout.js';
import { createMetrics, readClusterMetrics } from './metrics.js';
import { createResume } from './resume.js';
import { openSseConnection } from './sse.js';
import { createStreamReader } from './stream.js';
import { createTopicRegistry, parseSubscriptions, TOPIC_PATTERN } from './topics.js';
import { openWebSocketConnection } from './websocket.js';

// Results module (RF-4, RF-5, RF-6, RF-9, RF-10 for the public audience):
// global stream -> per-topic fan-out -> SSE (default), WebSocket or polling.
export function createResults({ redis, config = defaultConfig, logger = console }) {
  const keys = createKeys(config.redisKeyPrefix);
  const registry = createTopicRegistry();
  let redisStatus = { redis: 'up', since: Date.now() };
  let polls = 0;
  let backpressure = null;

  const metrics = createMetrics({
    instanceId: config.instanceId,
    mode: config.fanoutMode,
    registry,
    redis,
    keys,
    resolutionMs: config.eventLoopResolutionMs,
    ttlSeconds: config.metricsTtlSeconds,
    live: () => ({ status: redisStatus.redis, slowDegradedNow: backpressure?.degradedCount ?? 0, polls }),
    logger
  });
  const resumer = createResume({ redis, keys, registry, metrics, maxReplay: config.maxReplay });
  backpressure = createBackpressure({
    softBytes: config.slowSoftBytes,
    hardBytes: config.slowHardBytes,
    maxDegradedMs: config.slowMaxDegradedMs,
    metrics,
    // A drained slow client gets one snapshot of its topics instead of the
    // detail it missed.
    onRecover: (conn) => resumer.resume(conn, [...conn.topics].map((topic) => ({ topic, cursor: null })), { kind: 'recovery' }),
    logger
  });
  const fanout = createFanout({
    registry,
    mode: config.fanoutMode,
    flushMs: config.flushMs,
    batchSize: config.batchSize,
    heartbeatMs: config.heartbeatMs,
    backpressure,
    metrics,
    logger
  });

  // RF-10: when the events stream cannot be read (Redis down) every public
  // client is told, so the page shows "last known data" instead of a silent
  // freeze. The stream has memory, so nothing is lost when it comes back.
  async function broadcastStatus() {
    const frame = serialize({ t: 'status', ...redisStatus });
    let inBatch = 0;
    for (const conn of registry.connections) {
      if (!conn.degraded) conn.send(frame);
      inBatch += 1;
      if (inBatch >= config.batchSize) {
        inBatch = 0;
        await yieldToLoop();
      }
    }
  }

  const reader = createStreamReader({
    redis,
    streamKey: keys.stream,
    onEntries: (entries) => fanout.onEntries(entries),
    onStatus: (status) => {
      if (status === redisStatus.redis) return;
      redisStatus = { redis: status, since: Date.now() };
      logger.log(`Results stream: Redis ${status.toUpperCase()}`);
      broadcastStatus().catch((error) => logger.error('Status broadcast failed:', error.message));
    },
    blockMs: config.streamBlockMs,
    count: config.streamReadCount,
    logger
  });
  const timers = [];
  let started = false;

  function attach(conn, subscriptions) {
    registry.add(conn);
    if (redisStatus.redis === 'down') conn.send(serialize({ t: 'status', ...redisStatus }));
    // Snapshot for topics without cursor; replay or snapshot for the others.
    resumer.resume(conn, subscriptions).catch((error) => {
      logger.error('Could not resume a public connection:', error.message);
      conn.send(serialize({ t: 'status', redis: 'down', since: Date.now() }));
      conn.close();
    });
  }

  function detach(conn) {
    conn.closed = true;
    registry.remove(conn);
    backpressure.forget(conn);
  }

  function parse(raw) {
    return parseSubscriptions(raw, { maxTopics: config.maxTopicsPerConnection });
  }

  function handleStream(req, res) {
    let subscriptions;
    try {
      subscriptions = parse(req.query.sub);
    } catch (error) {
      return res.status(400).json({ status: 'rejected', message: error.message });
    }
    const conn = openSseConnection(req, res, config.instanceId);
    req.on('close', () => detach(conn));
    attach(conn, subscriptions);
    return undefined;
  }

  // Same protocol over WebSocket (/ws?sub=...): used to compare transports.
  function handleWebSocket(ws, req) {
    let subscriptions;
    try {
      subscriptions = parse(new URL(req.url, 'http://localhost').searchParams.get('sub'));
    } catch (error) {
      ws.close(4400, error.message.slice(0, 120));
      return;
    }
    const conn = openWebSocketConnection(ws, config.instanceId);
    ws.on('close', () => detach(conn));
    attach(conn, subscriptions);
  }

  // Polling transport: a snapshot of one topic, cached per instance for
  // POLL_CACHE_MS so N pollers cost at most one Redis read per topic per
  // window and instance.
  const pollCache = new Map();
  async function poll(topic) {
    polls += 1;
    const cached = pollCache.get(topic);
    if (cached && Date.now() - cached.at < config.pollCacheMs) return cached.body;
    const [{ upTo, states: [state] }, ranking] = await Promise.all([
      resumer.readSnapshot([topic]),
      redis.zRangeWithScores(keys.rank(topic), 0, -1, { REV: true })
    ]);
    const body = { topic, upTo, state, ranking: ranking.map(({ value, score }) => ({ candidate: value, votes: score })) };
    pollCache.set(topic, { at: Date.now(), body });
    if (pollCache.size > 5000) pollCache.delete(pollCache.keys().next().value);
    return body;
  }

  return {
    instanceId: config.instanceId,
    registry,
    handleStream,
    handleWebSocket,
    poll,
    isTopic: (topic) => TOPIC_PATTERN.test(topic),
    get status() {
      return redisStatus;
    },
    metrics: () => metrics.local(),
    clusterMetrics: () => readClusterMetrics(redis, keys),
    async start() {
      await seedCatalog(redis, keys.catalog);
      await reader.start();
      fanout.start();
      metrics.start(Math.floor(config.flushMs / 2));
      timers.push(setInterval(() => {
        backpressure.recover().catch((error) => logger.error('Slow-client recovery failed:', error.message));
      }, 1000));
      started = true;
    },
    async stop() {
      for (const timer of timers.splice(0)) clearInterval(timer);
      for (const conn of registry.connections) {
        conn.close();
      }
      if (started) {
        started = false;
        fanout.stop();
        metrics.stop();
        await reader.stop();
      }
    }
  };
}
