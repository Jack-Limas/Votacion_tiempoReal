import { createHash } from 'node:crypto';
import { RATE_LIMIT } from '../verification/scripts.js';

const RATE_LIMIT_SHA = createHash('sha1').update(RATE_LIMIT).digest('hex');

// Fixed-window counter per credential in Redis (same script as the
// verification module). limit <= 0 disables it. Fails open when Redis is
// down: the endpoints it protects degrade on their own and report it.
export function createRateLimiter(redis, { keyPrefix, scope, windowMs, limit }) {
  return async function check(credential) {
    if (limit <= 0) return { allowed: true };
    const options = { keys: [`${keyPrefix}:rl:${scope}:${credential}`], arguments: [String(windowMs), String(limit)] };
    let reply;
    try {
      try {
        reply = await redis.evalSha(RATE_LIMIT_SHA, options);
      } catch (error) {
        if (!String(error?.message).includes('NOSCRIPT')) throw error;
        reply = await redis.eval(RATE_LIMIT, options);
      }
    } catch {
      return { allowed: true, degraded: true };
    }
    const [count, ttl] = reply.map(Number);
    return { allowed: count <= limit, count, limit, retryAfterMs: Math.max(ttl, 0) };
  };
}
