import { config as defaultConfig } from '../config.js';
import { sembrarCatalogo } from './catalogo.js';
import { crearClaves } from './claves.js';
import { createFanout } from './fanout.js';
import { openSseConnection } from './sse.js';
import { createStreamReader } from './stream.js';
import { createTopicRegistry, parseSubscriptions } from './topics.js';

// Módulo público (RF-4, RF-5, RF-6): stream global -> fan-out por tópico -> SSE.
export function createPublico({ redis, config = defaultConfig, logger = console }) {
  const claves = crearClaves(config.redisKeyPrefix);
  const registry = createTopicRegistry();
  const fanout = createFanout({ registry });
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
    metrics() {
      return {
        instancia: config.instanceId,
        conexiones: registry.connections.size,
        topicos: registry.topicCount,
        suscriptores: registry.counts(),
        mensajesEmitidos: fanout.emitidos
      };
    },
    async start() {
      await sembrarCatalogo(redis, claves.catalogo);
      await reader.start();
      started = true;
    },
    async stop() {
      for (const conn of registry.connections) {
        conn.close();
      }
      if (started) {
        started = false;
        await reader.stop();
      }
    }
  };
}
