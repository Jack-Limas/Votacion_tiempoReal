import { config as defaultConfig } from '../config.js';
import { sembrarCatalogo } from './catalogo.js';
import { crearClaves } from './claves.js';
import { createFanout } from './fanout.js';
import { createMetrics, leerMetricasCluster } from './metrics.js';
import { openSseConnection } from './sse.js';
import { createStreamReader } from './stream.js';
import { createTopicRegistry, parseSubscriptions } from './topics.js';

// Módulo público (RF-4, RF-5, RF-6): stream global -> fan-out por tópico -> SSE.
export function createPublico({ redis, config = defaultConfig, logger = console }) {
  const claves = crearClaves(config.redisKeyPrefix);
  const registry = createTopicRegistry();
  const metrics = createMetrics({
    instanceId: config.instanceId,
    mode: config.fanoutMode,
    registry,
    redis,
    claves,
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
    streamKey: claves.stream,
    onEntries: (entries) => fanout.onEntries(entries),
    blockMs: config.streamBlockMs,
    count: config.streamReadCount,
    logger
  });
  let started = false;

  function handleStream(req, res) {
    let subscriptions;
    try {
      subscriptions = parseSubscriptions(req.query.sub, { maxTopics: config.maxTopicsPorConexion });
    } catch (error) {
      return res.status(400).json({ status: 'rejected', message: error.message });
    }

    const conn = openSseConnection(req, res, config.instanceId);
    registry.add(conn);
    for (const { topic } of subscriptions) {
      registry.subscribe(conn, topic);
    }
    req.on('close', () => registry.remove(conn));
  }

  return {
    instanceId: config.instanceId,
    registry,
    handleStream,
    metrics: () => metrics.local(),
    metricsCluster: () => leerMetricasCluster(redis, claves),
    async start() {
      await sembrarCatalogo(redis, claves.catalogo);
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
