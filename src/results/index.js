import { config as defaultConfig } from '../config/env.js';
import { seedCatalog } from './catalog.js';
import { createKeys } from './keys.js';
import { createFanout } from './fanout.js';
import { createMetrics, readClusterMetrics } from './metrics.js';
import { createResume } from './resume.js';
import { openSseConnection } from './sse.js';
import { createStreamReader } from './stream.js';
import { createTopicRegistry, parseSubscriptions } from './topics.js';

// Results module (RF-4, RF-5, RF-6): global stream -> per-topic fan-out -> SSE.
export function createResults({ redis, config = defaultConfig, logger = console }) {
  const keys = createKeys(config.redisKeyPrefix);
  const registry = createTopicRegistry();
  const metrics = createMetrics({
    instanceId: config.instanceId,
    mode: config.fanoutMode,
    registry,
    redis,
    keys,
    resolutionMs: config.eventLoopResolutionMs,
    ttlSeconds: config.metricsTtlSeconds,
    logger
  });
  const fanout = createFanout({
    registry,
    mode: config.fanoutMode,
    flushMs: config.flushMs,
    batchSize: config.batchSize,
    heartbeatMs: config.heartbeatMs,
    metrics,
    logger
  });
  const reader = createStreamReader({
    redis,
    streamKey: keys.stream,
    onEntries: (entries) => fanout.onEntries(entries),
    blockMs: config.streamBlockMs,
    count: config.streamReadCount,
    logger
  });
  const resumer = createResume({ redis, keys, registry, metrics, maxReplay: config.maxReplay });
  let started = false;

  function handleStream(req, res) {
    let subscriptions;
    try {
      subscriptions = parseSubscriptions(req.query.sub, { maxTopics: config.maxTopicsPerConnection });
    } catch (error) {
      return res.status(400).json({ status: 'rejected', message: error.message });
    }

    const conn = openSseConnection(req, res, config.instanceId);
    registry.add(conn);
    req.on('close', () => registry.remove(conn));
    // Snapshot for topics without cursor; replay or snapshot for the others.
    resumer.resume(conn, subscriptions).catch((error) => {
      logger.error('Could not resume the SSE connection:', error.message);
      conn.close();
    });
    return undefined;
  }

  return {
    instanceId: config.instanceId,
    registry,
    handleStream,
    metrics: () => metrics.local(),
    clusterMetrics: () => readClusterMetrics(redis, keys),
    async start() {
      await seedCatalog(redis, keys.catalog);
      await reader.start();
      fanout.start();
      metrics.start(Math.floor(config.flushMs / 2));
      started = true;
    },
    async stop() {
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
