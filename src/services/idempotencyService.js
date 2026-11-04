import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { config } from '../config/env.js';
import { createKeys } from '../results/keys.js';

// src/lua/applyActa.lua: idempotency, totals and the XADD of the public event
// in a single atomic operation.
const REGISTER_ACTA_SCRIPT = readFileSync(
  new URL('../lua/applyActa.lua', import.meta.url),
  'utf8'
);

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const entries = Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value);
}

export class RedisUnavailableError extends Error {
  constructor(cause) {
    super('Could not register the acta in Redis.', { cause });
    this.name = 'RedisUnavailableError';
  }
}

export function createIdempotencyService(redis, {
  keyPrefix = config.redisKeyPrefix,
  ttlSeconds = config.idempotencyTtlSeconds,
  streamMaxLen = config.streamMaxLen
} = {}) {
  const keys = createKeys(keyPrefix);
  return {
    async registerActa(acta, validation) {
      const digest = createHash('sha256').update(stableStringify(acta)).digest('hex');
      const accepted = validation.valid;
      const result = accepted
        ? {
            status: 'accepted',
            accepted: true,
            duplicate: false,
            acta: acta.acta,
            idem: acta.idem,
            message: 'Acta received and added to the totals.'
          }
        : {
            status: 'verification_required',
            accepted: false,
            duplicate: false,
            acta: acta.acta,
            idem: acta.idem,
            reason: 'vote_total_mismatch',
            totalVotos: validation.totalVotos,
            sufragantes: validation.sufragantes,
            message: 'Doubtful acta: votes do not match sufragantes. Sent to verification.'
          };

      try {
        const rawResult = await redis.eval(REGISTER_ACTA_SCRIPT, {
          keys: [
            `${keyPrefix}:idem:${acta.idem}`,
            `${keyPrefix}:aggregate`,
            `${keyPrefix}:verification`,
            keys.mesa(acta.mesa),
            keys.total(`pto:${acta.puesto}`),
            keys.total(`mun:${acta.municipio}`),
            keys.total('dep'),
            keys.stream,
            keys.catalog
          ],
          arguments: [
            digest,
            JSON.stringify(result),
            String(ttlSeconds),
            JSON.stringify(acta.votos),
            accepted ? '1' : '0',
            JSON.stringify(acta),
            acta.idem,
            String(Date.now()),
            String(streamMaxLen)
          ]
        });
        return JSON.parse(rawResult);
      } catch (error) {
        throw new RedisUnavailableError(error);
      }
    }
  };
}
