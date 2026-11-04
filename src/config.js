import 'dotenv/config';
import { hostname } from 'node:os';

function positiveInteger(value, fallback, name) {
  const parsed = Number(value ?? fallback);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} debe ser un entero positivo.`);
  }
  return parsed;
}

export const config = {
  port: positiveInteger(process.env.PORT, 3000, 'PORT'),
  host: process.env.HOST ?? '0.0.0.0',
  wsPath: process.env.WS_PATH ?? '/ws',
  corsOrigins: (process.env.CORS_ORIGIN ?? '*')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean),
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  idempotencyTtlSeconds: positiveInteger(
    process.env.REDIS_IDEMPOTENCY_TTL_SECONDS,
    60 * 60 * 24 * 30,
    'REDIS_IDEMPOTENCY_TTL_SECONDS'
  ),
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}',
  instanceId: process.env.INSTANCE_ID || hostname(),
  // Retención del stream st:eventos (XADD MAXLEN ~ N).
  streamMaxLen: positiveInteger(process.env.MAXLEN, 10000, 'MAXLEN'),
  streamBlockMs: positiveInteger(process.env.STREAM_BLOCK_MS, 5000, 'STREAM_BLOCK_MS'),
  streamReadCount: positiveInteger(process.env.STREAM_READ_COUNT, 200, 'STREAM_READ_COUNT'),
  maxTopicsPorConexion: positiveInteger(process.env.MAX_TOPICS, 10, 'MAX_TOPICS')
};
