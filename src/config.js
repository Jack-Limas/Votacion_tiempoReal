import 'dotenv/config';

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
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}'
};
