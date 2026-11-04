import { createHash } from 'node:crypto';
import * as scripts from './scripts.js';

export class VerificationUnavailableError extends Error {
  constructor(cause) {
    super('Redis is not responding; verification actions are temporarily disabled.', { cause });
    this.name = 'VerificationUnavailableError';
    this.code = 'redis_unavailable';
  }
}

// Each voter withheld from the count moves the case one second ahead of the
// FIFO order, capped so no case can jump more than ~16 minutes. See
// docs/verification.md ("Priority").
const MAX_BONUS_VOTERS = 1000;
export function priorityBonusMs({ sufragantes = 0, version = 1 }) {
  const impact = Math.min(Math.max(Number(sufragantes) || 0, 0), MAX_BONUS_VOTERS);
  const correctionBonus = Number(version) > 1 ? 120_000 : 0;
  return impact * 1000 + correctionBonus;
}

function toArray(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object') return Object.values(value);
  return [];
}

function createScriptRunner(redis) {
  const shas = new Map();
  return async function run(source, { keys = [], args = [] }) {
    let sha = shas.get(source);
    if (!sha) {
      sha = createHash('sha1').update(source).digest('hex');
      shas.set(source, sha);
    }
    const options = { keys, arguments: args.map(String) };
    try {
      return await redis.evalSha(sha, options);
    } catch (error) {
      if (String(error?.message).includes('NOSCRIPT')) {
        return redis.eval(source, options);
      }
      throw error;
    }
  };
}

export function createVerificationStore(redis, { keyPrefix, leaseTtlMs, presenceTtlMs }) {
  const run = createScriptRunner(redis);
  const P = keyPrefix;
  const RESOLVED_RETENTION_MS = 48 * 60 * 60 * 1000;

  async function call(source, args, keys) {
    let raw;
    try {
      raw = await run(source, { keys, args: [P, ...args] });
    } catch (error) {
      throw new VerificationUnavailableError(error);
    }
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  }

  return {
    keys: {
      events: `${P}:events`,
      queue: `${P}:queue`,
      leases: `${P}:leases`,
      presence: `${P}:presence`
    },

    ingest({ caseId, acta, idem, reason = 'vote_total_mismatch', sufragantes, totalVotos }) {
      return call(scripts.INGEST, [
        caseId,
        priorityBonusMs({ sufragantes, version: acta?.version }),
        JSON.stringify(acta),
        idem ?? '',
        reason,
        sufragantes ?? '',
        totalVotos ?? ''
      ]);
    },

    claim(auditor, auditorName, caseId = '') {
      return call(scripts.CLAIM, [auditor, auditorName, leaseTtlMs, caseId]);
    },

    renew(caseId, auditor, token) {
      return call(scripts.RENEW, [caseId, auditor, token, leaseTtlMs]);
    },

    release(caseId, auditor, token) {
      return call(scripts.RELEASE, [caseId, auditor, token]);
    },

    resolve(caseId, auditor, token, decision, note = '') {
      return call(scripts.RESOLVE, [caseId, auditor, token, decision, note, RESOLVED_RETENTION_MS]);
    },

    sweep(limit = 200) {
      return call(scripts.SWEEP, [limit]);
    },

    async snapshot(queueLimit = 500) {
      const snap = await call(scripts.SNAPSHOT, [queueLimit]);
      snap.queue = toArray(snap.queue);
      snap.claimed = toArray(snap.claimed);
      snap.stats = snap.stats && !Array.isArray(snap.stats) ? snap.stats : {};
      return snap;
    },

    touchPresence(connId, fields) {
      const pairs = Object.entries(fields)
        .filter(([, value]) => value !== undefined && value !== null)
        .flatMap(([key, value]) => [key, String(value)]);
      return call(scripts.PRESENCE_TOUCH, [connId, presenceTtlMs, ...pairs]);
    },

    removePresence(connId) {
      return call(scripts.PRESENCE_REMOVE, [connId]);
    },

    async listPresence() {
      const raw = await call(scripts.PRESENCE_LIST, []);
      const owners = raw.owners && !Array.isArray(raw.owners) ? raw.owners : {};
      const stats = raw.auditorStats && !Array.isArray(raw.auditorStats) ? raw.auditorStats : {};
      const byAuditor = new Map();
      for (const conn of toArray(raw.connections)) {
        if (conn.role !== 'auditor') continue;
        const entry = byAuditor.get(conn.auditorId) ?? {
          auditorId: conn.auditorId,
          name: conn.name,
          connectedAt: Number(conn.connectedAt),
          lastSeen: 0,
          connections: []
        };
        entry.connectedAt = Math.min(entry.connectedAt, Number(conn.connectedAt));
        entry.lastSeen = Math.max(entry.lastSeen, Number(conn.lastSeen));
        entry.lastActivity = Math.max(entry.lastActivity ?? 0, Number(conn.lastActivity ?? 0));
        entry.connections.push({
          connId: conn.connId,
          instance: conn.instance,
          connectedAt: Number(conn.connectedAt),
          lastSeen: Number(conn.lastSeen)
        });
        byAuditor.set(conn.auditorId, entry);
      }
      const auditors = [...byAuditor.values()].map((entry) => {
        const current = owners[entry.auditorId] ?? null;
        return {
          ...entry,
          currentCase: current,
          currentCaseMs: current ? raw.now - current.claimedAt : 0,
          resolved: stats[`${entry.auditorId}:resolved`] ?? 0,
          workedMs: (stats[`${entry.auditorId}:workMs`] ?? 0) + (current ? raw.now - current.claimedAt : 0)
        };
      });
      auditors.sort((a, b) => a.name.localeCompare(b.name));
      return { now: raw.now, auditors };
    },

    async rateLimit(scope, credential, windowMs, limit) {
      let result;
      try {
        result = await run(scripts.RATE_LIMIT, {
          keys: [`${P}:rl:${scope}:${credential}`],
          args: [windowMs, limit]
        });
      } catch (error) {
        throw new VerificationUnavailableError(error);
      }
      const [count, ttl] = result.map(Number);
      return { allowed: count <= limit, count, limit, retryAfterMs: Math.max(ttl, 0) };
    }
  };
}
