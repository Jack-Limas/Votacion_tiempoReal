import test from 'node:test';
import assert from 'node:assert/strict';
import { createBackpressure } from '../../src/results/backpressure.js';
import { deliver, serialize } from '../../src/results/fanout.js';
import { parseId } from '../../src/results/stream.js';
import { createConnection } from '../../src/results/topics.js';

function setup() {
  const counts = { degraded: 0, recovered: 0, disconnected: 0 };
  const recovered = [];
  const bp = createBackpressure({
    softBytes: 1000,
    hardBytes: 5000,
    metrics: { slow: (kind) => { counts[kind] += 1; } },
    onRecover: async (conn) => recovered.push(conn.id)
  });
  let buffered = 0;
  const sent = [];
  let disconnected = 0;
  const conn = createConnection((frame) => sent.push(frame), {
    bufferedBytes: () => buffered,
    disconnectSlow: () => { disconnected += 1; }
  });
  conn.cursors.set('dep', null);
  return {
    bp, conn, counts, recovered, sent,
    setBuffered: (n) => { buffered = n; },
    disconnected: () => disconnected
  };
}

test('below the soft threshold every delta is delivered', () => {
  const s = setup();
  s.setBuffered(999);
  assert.equal(deliver(s.conn, 'dep', parseId('1-0'), serialize({ a: 1 }), s.bp), true);
  assert.equal(s.sent.length, 1);
});

test('above the soft threshold detail stops and the cursor does not move', () => {
  const s = setup();
  s.setBuffered(1000);
  assert.equal(deliver(s.conn, 'dep', parseId('1-0'), serialize({ a: 1 }), s.bp), false);
  assert.equal(s.conn.degraded, true);
  assert.equal(s.conn.cursors.get('dep'), null);
  s.setBuffered(10);
  assert.equal(deliver(s.conn, 'dep', parseId('2-0'), serialize({ a: 2 }), s.bp), false, 'still degraded until recovery');
  assert.equal(s.sent.length, 0);
  assert.equal(s.counts.degraded, 1);
});

test('a drained slow client is recovered with one snapshot', async () => {
  const s = setup();
  s.setBuffered(2000);
  s.bp.admit(s.conn);
  await s.bp.recover();
  assert.deepEqual(s.recovered, [], 'not drained yet (needs <= soft/4)');
  s.setBuffered(250);
  await s.bp.recover();
  assert.deepEqual(s.recovered, [s.conn.id]);
  assert.equal(s.conn.degraded, false);
  assert.equal(s.counts.recovered, 1);
  assert.equal(s.bp.degradedCount, 0);
});

test('above the hard limit the client is disconnected with a reconnect signal', async () => {
  const s = setup();
  s.setBuffered(2000);
  s.bp.admit(s.conn);
  s.setBuffered(6000);
  await s.bp.recover();
  assert.equal(s.disconnected(), 1);
  assert.equal(s.counts.disconnected, 1);

  const t = setup();
  t.setBuffered(5000);
  assert.equal(t.bp.admit(t.conn), false);
  assert.equal(t.disconnected(), 1, 'also on delivery');
});

test('a client that stays degraded past the time limit is disconnected', async () => {
  let buffered = 2000;
  let disconnected = 0;
  const bp = createBackpressure({
    softBytes: 1000, hardBytes: 1e9, maxDegradedMs: 0,
    metrics: { slow() {} }, onRecover: async () => {}
  });
  const conn = createConnection(() => {}, { bufferedBytes: () => buffered, disconnectSlow: () => { disconnected += 1; } });
  bp.admit(conn);
  buffered = 1500;
  await bp.recover();
  assert.equal(disconnected, 1);
});
