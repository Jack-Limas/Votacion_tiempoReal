import test from 'node:test';
import assert from 'node:assert/strict';
import { createFanout, reduceEntries } from '../../src/results/fanout.js';
import { compareIds, parseId } from '../../src/results/stream.js';
import { createConnection, createTopicRegistry } from '../../src/results/topics.js';

function event(changes, extra = {}) {
  return { acta: 'E14-1-0001-03', version: 1, type: 'new', decreased: false, receivedAt: Date.now(), changes, ...extra };
}

function recordingConnection(registry, topic) {
  const received = [];
  const conn = createConnection((frame) => received.push(frame));
  registry.add(conn);
  registry.subscribe(conn, topic);
  return { conn, received, messages: () => received.map((f) => JSON.parse(f.slice(6))) };
}

test('compareIds compares ms and sequence as numbers, not as text', () => {
  assert.equal(compareIds('1700000000000-10', '1700000000000-9'), 1);
  assert.equal(compareIds('9-0', '10-0'), -1);
  assert.equal(compareIds('1700000000001-0', '1700000000000-99'), 1);
  assert.equal(compareIds('5-3', '5-3'), 0);
  assert.equal(compareIds(parseId('5-2'), '5-3'), -1);
  assert.ok('9-0' > '10-0', 'as text the order would be reversed');
});

test('reduceEntries leaves one delta per topic with the last value of each field', () => {
  const entries = [
    { id: '1-0', event: event({ 'mun:11': { 'v:C1': 10, mesas_reported: 1 }, dep: { 'v:C1': 10 } }) },
    { id: '2-0', event: event({ 'mun:12': { 'v:C1': 99 }, dep: { 'v:C1': 109 } }) },
    { id: '3-0', event: event({ 'mun:11': { 'v:C1': 25, 'v:C2': 4, mesas_reported: 2 }, dep: { 'v:C1': 124 } }) }
  ];
  assert.deepEqual(reduceEntries(entries, 'mun:11'), {
    changes: { 'v:C1': 25, 'v:C2': 4, mesas_reported: 2 },
    reason: null,
    events: 2
  });
  assert.equal(reduceEntries(entries, 'dep').changes['v:C1'], 124);
  assert.equal(reduceEntries(entries, 'pto:5').events, 0);
});

test('a correction that lowers a number carries its reason', () => {
  const entries = [
    { id: '1-0', event: event({ 'mun:11': { 'v:C1': 30 } }) },
    { id: '2-0', event: event({ 'mun:11': { 'v:C1': 22, 'v:C2': 18 } }, { type: 'correction', decreased: true, acta: 'E14-9-0002-03', version: 2 }) }
  ];
  assert.deepEqual(reduceEntries(entries, 'mun:11').reason, { type: 'correction', acta: 'E14-9-0002-03', version: 2 });
});

test('batched: several events in the window produce ONE message per topic, serialized once', async () => {
  const registry = createTopicRegistry();
  const a = recordingConnection(registry, 'mun:11');
  const b = recordingConnection(registry, 'mun:11');
  const c = recordingConnection(registry, 'pto:5');
  const fanout = createFanout({ registry, mode: 'batched' });

  fanout.onEntries([
    { id: '100-0', event: event({ 'mun:11': { 'v:C1': 1 }, 'pto:5': { 'v:C1': 1 } }) },
    { id: '100-1', event: event({ 'mun:11': { 'v:C1': 2, 'v:C2': 7 } }) },
    { id: '101-0', event: event({ 'mun:11': { 'v:C1': 3 }, 'pto:9': { 'v:C1': 5 } }) }
  ]);
  assert.equal(a.received.length, 0, 'nothing is sent on arrival');
  await fanout.flush();

  assert.deepEqual(a.messages(), [{ t: 'delta', topic: 'mun:11', upTo: '101-0', changes: { 'v:C1': 3, 'v:C2': 7 } }]);
  assert.strictEqual(a.received[0], b.received[0], 'the same string for every subscriber');
  assert.deepEqual(c.messages(), [{ t: 'delta', topic: 'pto:5', upTo: '101-0', changes: { 'v:C1': 1 } }]);
});

test('batched: drops deltas with upTo <= the connection cursor', async () => {
  const registry = createTopicRegistry();
  const a = recordingConnection(registry, 'dep');
  a.conn.cursors.set('dep', parseId('200-0'));
  const fanout = createFanout({ registry, mode: 'batched' });

  fanout.onEntries([{ id: '150-0', event: event({ dep: { 'v:C1': 1 } }) }]);
  await fanout.flush();
  assert.equal(a.received.length, 0);

  fanout.onEntries([{ id: '250-0', event: event({ dep: { 'v:C1': 2 } }) }]);
  await fanout.flush();
  assert.equal(a.messages()[0].upTo, '250-0');
  assert.equal(compareIds(a.conn.cursors.get('dep'), '250-0'), 0);
});

test('batched: yields the Event Loop between batches of BATCH_SIZE', async () => {
  const registry = createTopicRegistry();
  const connections = Array.from({ length: 10 }, () => recordingConnection(registry, 'dep'));
  const fanout = createFanout({ registry, mode: 'batched', batchSize: 3 });
  fanout.onEntries([{ id: '1-0', event: event({ dep: { 'v:C1': 1 } }) }]);

  const sentAtYield = [];
  const count = () => connections.filter((c) => c.received.length > 0).length;
  setImmediate(() => sentAtYield.push(count()));
  await fanout.flush();

  assert.equal(count(), 10);
  assert.equal(sentAtYield[0], 3, 'another callback ran after the first batch');
});

test('batched: flushes never overlap; what arrives during one is sent when it ends', async () => {
  const registry = createTopicRegistry();
  Array.from({ length: 4 }, () => recordingConnection(registry, 'dep'));
  const a = recordingConnection(registry, 'dep');
  const fanout = createFanout({ registry, mode: 'batched', batchSize: 2 });

  fanout.onEntries([{ id: '1-0', event: event({ dep: { 'v:C1': 1 } }) }]);
  const first = fanout.flush();
  fanout.onEntries([{ id: '2-0', event: event({ dep: { 'v:C1': 2 } }) }]);
  const second = fanout.flush();
  assert.strictEqual(first, second, 'the second flush merges with the one in flight');
  await second;

  assert.deepEqual(a.messages().map((m) => [m.upTo, m.changes['v:C1']]), [['1-0', 1], ['2-0', 2]]);
});

test('naive: every event is sent at once to every subscriber', () => {
  const registry = createTopicRegistry();
  const a = recordingConnection(registry, 'mun:11');
  const fanout = createFanout({ registry, mode: 'naive' });
  fanout.onEntries([
    { id: '1-0', event: event({ 'mun:11': { 'v:C1': 1 } }) },
    { id: '2-0', event: event({ 'mun:11': { 'v:C1': 2 } }) }
  ]);
  assert.deepEqual(a.messages().map((m) => m.upTo), ['1-0', '2-0']);
});

test('an acta added after verification carries a verification reason', () => {
  const entries = [{ id: '1-0', event: event({ dep: { 'v:C1': 5 } }, { origin: 'approve', acta: 'E14-1-0003-03', version: 1 }) }];
  assert.deepEqual(reduceEntries(entries, 'dep').reason, { type: 'verification', acta: 'E14-1-0003-03', version: 1 });
});
