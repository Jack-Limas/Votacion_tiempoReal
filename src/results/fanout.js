// Fan-out of stream events to the local subscribed connections (RF-4).
//
// batched (default): events are NOT sent on arrival. They are folded into a
//   map of dirty topics (last absolute value per field + reason) and every
//   FLUSH_MS ONE message per topic is built, serialized ONCE and sent in
//   batches of BATCH_SIZE, yielding the Event Loop with setImmediate between
//   batches.
// naive: every event is sent to every subscriber in a synchronous loop (only
//   for the load-test comparison).
//
// In both modes the cost is O(k) per topic (k = local subscribers), not
// O(connections of the instance).
import { compareIds, parseId } from './stream.js';
import { PENDING } from './topics.js';

// setImmediate really yields: pending I/O (sockets, timers) runs before the
// loop continues. process.nextTick and resolved promises are microtasks and
// run before returning to the Event Loop, so they would yield nothing.
export const yieldToLoop = () => new Promise((resolve) => setImmediate(resolve));

// A frame is serialized ONCE and shared by every subscriber; each transport
// picks its encoding (SSE text event or plain JSON for WebSocket).
export function serialize(message) {
  const json = JSON.stringify(message);
  return { json, sse: `data: ${json}\n\n` };
}

export function deltaMessage(topic, upTo, changes, reason) {
  const message = { t: 'delta', topic, upTo, changes };
  if (reason) message.reason = reason;
  return message;
}

// Changes that need an explanation travel with it, so the UI never shows a
// number moving without a reason: a correction that lowers a number, or an
// acta added after an auditor approved it.
export function reasonOf(event) {
  if (event.origin === 'approve') {
    return { type: 'verification', acta: event.acta, version: event.version };
  }
  return event.type === 'correction' && event.decreased
    ? { type: 'correction', acta: event.acta, version: event.version }
    : null;
}

// Folds an event into the delta of a topic: last absolute value per field.
export function accumulate(target, event, topic) {
  const changes = event.changes[topic];
  if (!changes) return false;
  Object.assign(target.changes, changes);
  const reason = reasonOf(event);
  if (reason) target.reason = reason;
  return true;
}

// Reduces several stream entries to ONE delta for a topic.
export function reduceEntries(entries, topic) {
  const target = { changes: {}, reason: null, events: 0 };
  for (const { event } of entries) {
    if (accumulate(target, event, topic)) target.events += 1;
  }
  return target;
}

// Delivers an already serialized frame if it advances the connection cursor
// for that topic. upTo <= cursor is dropped: the connection already holds a
// newer state than the instance reader, so nothing ever goes backwards.
// A slow connection (see backpressure.js) receives no detail; its cursor does
// not move, and a snapshot catches it up once it drains.
export function deliver(conn, topic, upTo, frame, backpressure = null) {
  const cursor = conn.cursors.get(topic);
  if (cursor === PENDING) {
    // The connection is still receiving its snapshot/replay: keep it for later.
    conn.pending.push([topic, upTo, frame]);
    return false;
  }
  if (cursor && compareIds(upTo, cursor) <= 0) return false;
  if (backpressure && !backpressure.admit(conn)) return false;
  conn.cursors.set(topic, upTo);
  conn.send(frame);
  return true;
}

const noMetrics = {
  eventProcessed() {},
  messagesSent() {},
  dropped() {},
  publishDelay() {}
};

export function createFanout({
  registry,
  mode = 'batched',
  flushMs = 1000,
  batchSize = 100,
  heartbeatMs = 15000,
  backpressure = null,
  metrics = noMetrics,
  logger = console
}) {
  let dirty = new Map();
  let receivedAts = [];
  let lastId = null;
  let inFlight = null;
  let again = false;
  const timers = [];

  function accumulateEvent(event) {
    let relevant = false;
    for (const topic of Object.keys(event.changes)) {
      // A topic without local subscribers costs one Map.get.
      if (registry.subscribers(topic).size === 0) continue;
      let entry = dirty.get(topic);
      if (!entry) {
        entry = { changes: {}, reason: null };
        dirty.set(topic, entry);
      }
      accumulate(entry, event, topic);
      relevant = true;
    }
    if (relevant) receivedAts.push(event.receivedAt);
  }

  function sendNaive(id, event) {
    const upTo = parseId(id);
    let sent = 0;
    let dropped = 0;
    for (const [topic, changes] of Object.entries(event.changes)) {
      const subscribers = registry.subscribers(topic);
      if (subscribers.size === 0) continue;
      const frame = serialize(deltaMessage(topic, id, changes, reasonOf(event)));
      for (const conn of subscribers) {
        if (deliver(conn, topic, upTo, frame, backpressure)) sent += 1;
        else dropped += 1;
      }
    }
    if (sent > 0) {
      metrics.messagesSent(sent);
      metrics.publishDelay(Date.now() - event.receivedAt);
    }
    metrics.dropped(dropped);
  }

  async function drain() {
    // Take the whole window and leave a fresh map for what arrives meanwhile.
    const window = dirty;
    const windowReceivedAts = receivedAts;
    const upToText = lastId;
    dirty = new Map();
    receivedAts = [];
    if (window.size === 0) return;

    const upTo = parseId(upToText);
    let inBatch = 0;
    let sent = 0;
    let dropped = 0;
    for (const [topic, { changes, reason }] of window) {
      const subscribers = registry.subscribers(topic);
      if (subscribers.size === 0) continue;
      const frame = serialize(deltaMessage(topic, upToText, changes, reason));
      for (const conn of subscribers) {
        if (deliver(conn, topic, upTo, frame, backpressure)) sent += 1;
        else dropped += 1;
        inBatch += 1;
        if (inBatch >= batchSize) {
          inBatch = 0;
          await yieldToLoop();
        }
      }
    }
    metrics.messagesSent(sent);
    metrics.dropped(dropped);
    const now = Date.now();
    for (const ts of windowReceivedAts) metrics.publishDelay(now - ts);
  }

  // Flushes never overlap: a flush requested while one is sending keeps
  // accumulating and is sent, merged, as soon as the previous one ends.
  function flush() {
    if (inFlight) {
      again = true;
      return inFlight;
    }
    inFlight = (async () => {
      try {
        do {
          again = false;
          await drain();
        } while (again);
      } catch (error) {
        logger.error('Fan-out flush failed:', error);
      } finally {
        inFlight = null;
      }
    })();
    return inFlight;
  }

  // Periodic keep-alive so load balancers do not close idle connections (an
  // AWS ALB cuts at 60 s by default): an SSE comment or a WebSocket ping.
  // Batched, and skipped for degraded (slow) connections.
  async function heartbeat() {
    let inBatch = 0;
    for (const conn of registry.connections) {
      if (!conn.degraded) conn.heartbeat();
      inBatch += 1;
      if (inBatch >= batchSize) {
        inBatch = 0;
        await yieldToLoop();
      }
    }
  }

  return {
    mode,
    onEntries(entries) {
      for (const { id, event } of entries) {
        metrics.eventProcessed(registry.connections.size);
        lastId = id;
        if (mode === 'naive') {
          sendNaive(id, event);
        } else {
          accumulateEvent(event);
        }
      }
    },
    flush,
    start() {
      if (mode === 'batched') timers.push(setInterval(flush, flushMs));
      timers.push(setInterval(() => {
        heartbeat().catch((error) => logger.error('Heartbeat failed:', error));
      }, heartbeatMs));
    },
    stop() {
      for (const timer of timers.splice(0)) clearInterval(timer);
    }
  };
}
