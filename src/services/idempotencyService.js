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
  const idemKey = (idem) => `${keyPrefix}:idem:${idem}`;
  const digestOf = (value) => createHash('sha256').update(stableStringify(value)).digest('hex');

  async function run(acta, { mode, result, requestKey, requestDigest, token = '', decision = '', originalIdem = '' }) {
    try {
      const raw = await redis.eval(REGISTER_ACTA_SCRIPT, {
        keys: [
          requestKey,
          `${keyPrefix}:verification`,
          keys.mesa(acta.mesa),
          keys.total(`pto:${acta.puesto}`),
          keys.total(`mun:${acta.municipio}`),
          keys.total('dep'),
          keys.stream,
          keys.catalog,
          keys.rank(`pto:${acta.puesto}`),
          keys.rank(`mun:${acta.municipio}`),
          keys.rank('dep'),
          keys.decision(decision ? result.case : ''),
          idemKey(originalIdem)
        ],
        arguments: [
          requestDigest,
          JSON.stringify(result),
          String(ttlSeconds),
          JSON.stringify(acta.votos ?? {}),
          mode,
          JSON.stringify(acta),
          acta.idem ?? '',
          String(Date.now()),
          String(streamMaxLen),
          digestOf({ votos: acta.votos, sufragantes: acta.sufragantes }),
          String(token),
          decision
        ]
      });
      return JSON.parse(raw);
    } catch (error) {
      throw new RedisUnavailableError(error);
    }
  }

  return {
    async registerActa(acta, validation) {
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
      return run(acta, {
        mode: accepted ? '1' : '0',
        result,
        requestKey: idemKey(acta.idem),
        requestDigest: digestOf(acta)
      });
    },

    // Applies an auditor's decision on a doubtful acta (RF-7 -> results).
    //   approve: the figures are confirmed and added exactly once, with the
    //            same version rules as any other acta.
    //   reject:  the acta is not counted; the mesa stays unreported until the
    //            jurado transmits a new version.
    //   recount: same as reject, flagged for a physical recount.
    // Idempotent per (case, fencing token); a token lower than the last one
    // seen for the case is refused.
    async applyVerificationDecision({ caseId, acta, decision, token, idem = '' }) {
      const result = {
        status: decision === 'approve' ? 'approved' : decision,
        accepted: decision === 'approve',
        duplicate: false,
        case: caseId,
        acta: acta.acta,
        token: Number(token)
      };
      return run(acta, {
        mode: decision === 'approve' ? '1' : 'R',
        result,
        requestKey: keys.decisionRequest(caseId, token),
        requestDigest: digestOf({ caseId, token, decision }),
        token,
        decision,
        originalIdem: idem
      });
    },

    // Stored outcome of a submission, for the jury app (includes the
    // verification decision once an auditor resolved it).
    async getSubmission(idem) {
      let raw;
      try {
        raw = await redis.get(idemKey(idem));
      } catch (error) {
        throw new RedisUnavailableError(error);
      }
      return raw ? JSON.parse(raw).result : null;
    }
  };
}
