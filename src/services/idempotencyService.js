import { createHash } from 'node:crypto';
import { config } from '../config.js';

const REGISTER_ACTA_SCRIPT = `
local existing = redis.call('GET', KEYS[1])
if existing then
  local saved = cjson.decode(existing)
  if saved.digest ~= ARGV[1] then
    return cjson.encode({
      status = 'idempotency_conflict',
      accepted = false,
      duplicate = false,
      message = 'La clave idem ya fue utilizada con un contenido diferente.'
    })
  end
  local result = saved.result
  result.duplicate = true
  return cjson.encode(result)
end

local result = cjson.decode(ARGV[2])
if ARGV[5] == '0' then
  local verificationType = redis.call('TYPE', KEYS[3]).ok
  if verificationType ~= 'none' and verificationType ~= 'stream' then
    return redis.error_reply('La clave de verificación Redis no es un stream.')
  end
  redis.call('XADD', KEYS[3], '*', 'idem', ARGV[7], 'acta', ARGV[6])
end

if ARGV[5] == '1' then
  local aggregateType = redis.call('TYPE', KEYS[2]).ok
  if aggregateType ~= 'none' and aggregateType ~= 'hash' then
    return redis.error_reply('La clave de acumulados Redis no es un hash.')
  end

  local votes = cjson.decode(ARGV[4])
  local maxSafeInteger = 9007199254740991
  for candidate, increment in pairs(votes) do
    local field = 'vote:' .. candidate
    local current = tonumber(redis.call('HGET', KEYS[2], field) or '0')
    local amount = tonumber(increment)
    if not current or not amount or current < 0 or current + amount > maxSafeInteger then
      return redis.error_reply('Acumulado de votos fuera del rango seguro.')
    end
  end
  local currentActas = tonumber(redis.call('HGET', KEYS[2], 'actas') or '0')
  if not currentActas or currentActas < 0 or currentActas + 1 > maxSafeInteger then
    return redis.error_reply('Acumulado de actas fuera del rango seguro.')
  end

  for candidate, increment in pairs(votes) do
    redis.call('HINCRBY', KEYS[2], 'vote:' .. candidate, increment)
  end
  redis.call('HINCRBY', KEYS[2], 'actas', 1)
end

-- Redis ejecuta el script sin intercalar otras operaciones. Si Redis lo completa
-- pero el proceso o la red cae antes de responder, el reenvío con el mismo idem
-- obtiene este resultado guardado y no vuelve a sumar los votos. Configure AOF
-- y/o replicación de Redis para reducir el riesgo ante una caída del propio Redis.
redis.call('SET', KEYS[1], cjson.encode({
  digest = ARGV[1],
  result = result,
  acta = cjson.decode(ARGV[6])
}), 'EX', ARGV[3])
return cjson.encode(result)
`;

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
  ttlSeconds = config.idempotencyTtlSeconds
} = {}) {
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
            `${keyPrefix}:verification`
          ],
          arguments: [
            digest,
            JSON.stringify(result),
            String(ttlSeconds),
            JSON.stringify(acta.votos),
            accepted ? '1' : '0',
            JSON.stringify(acta),
            acta.idem
          ]
        });
        return JSON.parse(rawResult);
      } catch (error) {
        throw new RedisUnavailableError(error);
      }
    }
  };
}
