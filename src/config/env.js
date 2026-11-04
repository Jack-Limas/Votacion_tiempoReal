import 'dotenv/config';
import { hostname } from 'node:os';

function positiveInteger(value, fallback, name) {
  const parsed = Number(value ?? fallback);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return parsed;
}

function oneOf(value, allowed, fallback, name) {
  const selected = value || fallback;
  if (!allowed.includes(selected)) {
    throw new Error(`${name} must be one of: ${allowed.join(', ')}.`);
  }
  return selected;
}

export const config = {
  port: positiveInteger(process.env.PORT, 3000, 'PORT'),
  host: process.env.HOST ?? '0.0.0.0',
  listenBacklog: positiveInteger(process.env.LISTEN_BACKLOG, 4096, 'LISTEN_BACKLOG'),
  wsPath: process.env.WS_PATH ?? '/ws',
  corsOrigins: (process.env.CORS_ORIGIN ?? '*')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean),
  // Number of reverse proxies in front of the app (nginx = 1, Caddy + nginx = 2).
  trustProxy: Number(process.env.TRUST_PROXY ?? 0),
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  idempotencyTtlSeconds: positiveInteger(
    process.env.REDIS_IDEMPOTENCY_TTL_SECONDS,
    60 * 60 * 24 * 30,
    'REDIS_IDEMPOTENCY_TTL_SECONDS'
  ),
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}',
  instanceId: process.env.INSTANCE_ID || hostname(),
  // Retention of the events stream (XADD MAXLEN ~ N).
  streamMaxLen: positiveInteger(process.env.MAXLEN, 10000, 'MAXLEN'),
  streamBlockMs: positiveInteger(process.env.STREAM_BLOCK_MS, 5000, 'STREAM_BLOCK_MS'),
  streamReadCount: positiveInteger(process.env.STREAM_READ_COUNT, 200, 'STREAM_READ_COUNT'),
  maxTopicsPerConnection: positiveInteger(process.env.MAX_TOPICS, 10, 'MAX_TOPICS'),
  // RF-4: window aggregation and batched sending.
  fanoutMode: oneOf(process.env.FANOUT_MODE, ['batched', 'naive'], 'batched', 'FANOUT_MODE'),
  flushMs: positiveInteger(process.env.FLUSH_MS, 1000, 'FLUSH_MS'),
  batchSize: positiveInteger(process.env.BATCH_SIZE, 100, 'BATCH_SIZE'),
  // Public heartbeat; clients treat 2.5x this interval of silence as a dead
  // instance (a killed container never sends a FIN or RST).
  heartbeatMs: positiveInteger(process.env.HEARTBEAT_MS, 10000, 'HEARTBEAT_MS'),
  eventLoopResolutionMs: positiveInteger(process.env.EL_RESOLUTION_MS, 10, 'EL_RESOLUTION_MS'),
  metricsTtlSeconds: positiveInteger(process.env.METRICS_TTL_S, 5, 'METRICS_TTL_S'),
  // RF-6: more than MAX_REPLAY pending events are resolved with a snapshot.
  maxReplay: positiveInteger(process.env.MAX_REPLAY, 500, 'MAX_REPLAY'),
  // RF-9: per-connection output buffer thresholds.
  slowSoftBytes: positiveInteger(process.env.SLOW_SOFT_BYTES, 256 * 1024, 'SLOW_SOFT_BYTES'),
  slowHardBytes: positiveInteger(process.env.SLOW_HARD_BYTES, 1024 * 1024, 'SLOW_HARD_BYTES'),
  slowMaxDegradedMs: positiveInteger(process.env.SLOW_MAX_DEGRADED_MS, 30_000, 'SLOW_MAX_DEGRADED_MS'),
  // Polling transport: per-instance cache of a topic snapshot.
  pollCacheMs: positiveInteger(process.env.POLL_CACHE_MS, 1000, 'POLL_CACHE_MS'),
  // New public connections per client IP and window (0 disables it, e.g. for
  // a load generator that opens thousands of connections from one address).
  rateLimitWindowMs: positiveInteger(process.env.RATE_LIMIT_WINDOW_MS, 10_000, 'RATE_LIMIT_WINDOW_MS'),
  rateLimitPublicConnects: Number(process.env.RATE_LIMIT_PUBLIC_CONNECTS ?? 60)
};
