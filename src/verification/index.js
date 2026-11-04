import { loadVerificationConfig } from './config.js';
import { createVerificationStore } from './store.js';
import { createRedisHealth } from './redisHealth.js';
import { createIngestor } from './ingestor.js';
import { createVerificationGateway } from './gateway.js';
import { createHttpRateLimiter, createVerificationRoutes } from './routes.js';

// Wires the verification module (RF-7, RF-8). All coordination between
// instances goes through Redis (RT-3): the queue and leases via Lua scripts,
// console sync via Pub/Sub plus versioned snapshots, intake via a consumer
// group on the doubtful-actas stream.
export function createVerificationModule({ redis, subscriber, config = loadVerificationConfig(), logger = console }) {
  const store = createVerificationStore(redis, config);
  const health = createRedisHealth(redis, { intervalMs: config.healthIntervalMs, logger });
  const gateway = createVerificationGateway({ config, store, health, subscriber, logger });
  // Blocking XREADGROUP needs its own connection so it never delays other commands.
  const ingestorClient = redis.duplicate();
  ingestorClient.on('error', (error) => logger.error('Verification ingest Redis client error:', error.message));
  const ingestor = createIngestor({
    client: ingestorClient,
    store,
    streamKey: config.streamKey,
    group: config.consumerGroup,
    consumer: config.instanceId,
    logger
  });
  const router = createVerificationRoutes({ config, store, health, gateway });

  // Rate limit for acta transmission, keyed by the jurado credential.
  const actaRateLimiter = createHttpRateLimiter(store, {
    scope: 'actas',
    windowMs: config.rateLimitWindowMs,
    limit: config.rateLimitActas,
    credential: (req) => (typeof req.body?.jurado === 'string' ? `jurado:${req.body.jurado.slice(0, 64)}` : null)
  });

  let started = false;
  return {
    config,
    store,
    health,
    gateway,
    ingestor,
    router,
    actaRateLimiter,
    upgradeRoute: { path: config.wsPath, handleUpgrade: gateway.handleUpgrade },
    async start() {
      if (started) return;
      started = true;
      await health.start();
      await gateway.start();
      ingestor.start();
    },
    async stop() {
      if (!started) return;
      started = false;
      health.stop();
      await gateway.stop();
      await ingestor.stop();
    }
  };
}
