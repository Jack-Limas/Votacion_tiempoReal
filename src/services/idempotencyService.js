import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { config } from '../config/env.js';
import { crearClaves } from '../results/claves.js';

// El script vive en src/lua/aplicarActa.lua: idempotencia, acumulados y XADD
// del evento público en una sola operación atómica.
const REGISTER_ACTA_SCRIPT = readFileSync(
  new URL('../lua/aplicarActa.lua', import.meta.url),
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
    super('No fue posible registrar el acta en Redis.', { cause });
    this.name = 'RedisUnavailableError';
  }
}

export function createIdempotencyService(redis, {
  keyPrefix = config.redisKeyPrefix,
  ttlSeconds = config.idempotencyTtlSeconds,
  streamMaxLen = config.streamMaxLen
} = {}) {
  const claves = crearClaves(keyPrefix);
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
            message: 'Acta recibida y sumada al acumulado.'
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
            message: 'Acta dudosa: los votos no coinciden con los sufragantes. Enviada a verificación.'
          };

      try {
        const rawResult = await redis.eval(REGISTER_ACTA_SCRIPT, {
          keys: [
            `${keyPrefix}:idem:${acta.idem}`,
            `${keyPrefix}:aggregate`,
            `${keyPrefix}:verification`,
            claves.mesa(acta.mesa),
            claves.acumulado(`pto:${acta.puesto}`),
            claves.acumulado(`mun:${acta.municipio}`),
            claves.acumulado('dep'),
            claves.stream,
            claves.catalogo
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
