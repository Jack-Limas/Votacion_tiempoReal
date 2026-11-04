// Continuity (RF-6). The client cursor is the Redis stream ID, identical on
// every instance: that is why a client can reconnect to any of them.
//   - Topic without cursor: snapshot.
//   - Cursor outside the retention, or more than MAX_REPLAY pending events:
//     snapshot.
//   - Otherwise: XRANGE (cursor +, filtered by topic and reduced to ONE delta.
import { readFileSync } from 'node:fs';
import { deliver, deltaMessage, reduceEntries, serialize } from './fanout.js';
import { compareIds, decodeEntry, parseId } from './stream.js';
import { PENDING } from './topics.js';

const SNAPSHOT_SCRIPT = readFileSync(new URL('../lua/snapshot.lua', import.meta.url), 'utf8');

export function decideResume({ cursor, firstId, lastId, pending, maxReplay }) {
  if (!cursor) return 'snapshot';
  // Empty or deleted stream, or a cursor from a previous stream: no reliable detail.
  if (!lastId || compareIds(cursor, lastId) > 0) return 'snapshot';
  // What followed the cursor may have been trimmed (MAXLEN).
  if (compareIds(cursor, firstId) < 0) return 'snapshot';
  if (pending > maxReplay) return 'snapshot';
  return 'replay';
}

function toState(flat) {
  const state = {};
  for (let i = 0; i < flat.length; i += 2) {
    state[flat[i]] = Number(flat[i + 1]);
  }
  return state;
}

export function createResume({ redis, keys, registry, metrics, maxReplay = 500 }) {
  async function readSnapshot(topics) {
    const [upTo, ...hashes] = await redis.eval(SNAPSHOT_SCRIPT, {
      keys: [keys.stream, ...topics.map((topic) => keys.total(topic))]
    });
    return { upTo, states: hashes.map(toState) };
  }

  async function resume(conn, subscriptions) {
    // The connection joins the registry BEFORE reading Redis, with its cursor
    // set to PENDING: live deltas that arrive meanwhile are kept and filtered
    // by upTo at the end, so nothing is lost between the read and the join.
    conn.pending = [];
    for (const { topic } of subscriptions) {
      registry.subscribe(conn, topic, PENDING);
    }

    const withCursor = subscriptions.filter((s) => s.cursor);
    const toSnapshot = subscriptions.filter((s) => !s.cursor).map((s) => s.topic);
    for (let i = 0; i < toSnapshot.length; i += 1) metrics.reconnection('initial');

    if (withCursor.length > 0) {
      const oldest = withCursor.reduce((min, s) => (compareIds(s.cursor, min) < 0 ? s.cursor : min), withCursor[0].cursor);
      // The three reads run in MULTI: range and bounds belong to the same instant.
      const [entries, first, last] = await redis.multi()
        .xRange(keys.stream, `(${oldest}`, '+', { COUNT: maxReplay + 1 })
        .xRange(keys.stream, '-', '+', { COUNT: 1 })
        .xRevRange(keys.stream, '+', '-', { COUNT: 1 })
        .exec();
      const firstId = first[0]?.id ?? null;
      const lastId = last[0]?.id ?? null;
      const truncated = entries.length > maxReplay;
      const events = entries.map(decodeEntry);

      for (const { topic, cursor } of withCursor) {
        const later = events.filter((e) => compareIds(e.id, cursor) > 0);
        const decision = decideResume({
          cursor, firstId, lastId, pending: truncated ? Infinity : later.length, maxReplay
        });
        metrics.reconnection(decision);
        if (decision === 'snapshot') {
          toSnapshot.push(topic);
          continue;
        }
        conn.cursors.set(topic, parseId(lastId));
        const { changes, reason, events: count } = reduceEntries(later, topic);
        if (count > 0) {
          conn.send(serialize(deltaMessage(topic, lastId, changes, reason)));
          metrics.messagesSent(1);
        }
      }
    }

    if (toSnapshot.length > 0) {
      const { upTo, states } = await readSnapshot(toSnapshot);
      toSnapshot.forEach((topic, i) => {
        conn.cursors.set(topic, parseId(upTo));
        conn.send(serialize({ t: 'snapshot', topic, upTo, state: states[i] }));
      });
      metrics.messagesSent(toSnapshot.length);
    }

    const queued = conn.pending;
    conn.pending = null;
    for (const [topic, upTo, frame] of queued) {
      if (deliver(conn, topic, upTo, frame)) metrics.messagesSent(1);
    }
  }

  return { resume, readSnapshot };
}
