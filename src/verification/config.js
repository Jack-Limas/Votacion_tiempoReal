import 'dotenv/config';
import { hostname } from 'node:os';
import { randomBytes } from 'node:crypto';
import { config as baseConfig } from '../config.js';

function int(name, fallback) {
  const raw = process.env[name];
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return value;
}

function resolveSecret() {
  const secret = process.env.VERIFICATION_TOKEN_SECRET;
  if (secret && secret.length >= 32) return secret;
  if (secret) throw new Error('VERIFICATION_TOKEN_SECRET must be at least 32 characters long.');
  if (process.env.NODE_ENV === 'production') {
    throw new Error('VERIFICATION_TOKEN_SECRET is required in production.');
  }
  // Development fallback: tokens only validate on this instance. Every
  // instance behind the load balancer must share the same secret.
  return randomBytes(32).toString('hex');
}

export function loadVerificationConfig(overrides = {}) {
  return {
    instanceId: process.env.INSTANCE_ID ?? `${hostname()}-${process.pid}`,
    keyPrefix: process.env.VERIFICATION_KEY_PREFIX ?? 'electoral:{verification}',
    // Stream written by the acta registration script (jurados module).
    streamKey: process.env.VERIFICATION_STREAM_KEY ?? `${baseConfig.redisKeyPrefix}:verification`,
    consumerGroup: process.env.VERIFICATION_CONSUMER_GROUP ?? 'verification-ingest',
    wsPath: process.env.VERIFICATION_WS_PATH ?? '/ws/verification',
    tokenSecret: resolveSecret(),
    tokenTtlSeconds: int('VERIFICATION_TOKEN_TTL_SECONDS', 12 * 60 * 60),
    auditorAccessCode: process.env.VERIFICATION_AUDITOR_CODE ?? '',
    coordinatorAccessCode: process.env.VERIFICATION_COORDINATOR_CODE ?? '',
    leaseTtlMs: int('VERIFICATION_LEASE_TTL_MS', 15_000),
    idleLimitMs: int('VERIFICATION_IDLE_LIMIT_MS', 5 * 60_000),
    heartbeatMs: int('VERIFICATION_HEARTBEAT_MS', 5_000),
    presenceTtlMs: int('VERIFICATION_PRESENCE_TTL_MS', 30_000),
    sweepIntervalMs: int('VERIFICATION_SWEEP_INTERVAL_MS', 2_000),
    healthIntervalMs: int('VERIFICATION_HEALTH_INTERVAL_MS', 2_000),
    rateLimitWindowMs: int('RATE_LIMIT_WINDOW_MS', 10_000),
    rateLimitWsMessages: int('RATE_LIMIT_WS_MESSAGES', 40),
    rateLimitHttpRequests: int('RATE_LIMIT_HTTP_REQUESTS', 20),
    rateLimitActas: int('RATE_LIMIT_ACTAS', 30),
    ...overrides
  };
}
