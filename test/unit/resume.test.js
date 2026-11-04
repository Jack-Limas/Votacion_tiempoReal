import test from 'node:test';
import assert from 'node:assert/strict';
import { deliver } from '../../src/results/fanout.js';
import { decideResume } from '../../src/results/resume.js';
import { parseId } from '../../src/results/stream.js';
import { PENDING, createConnection } from '../../src/results/topics.js';

const base = { firstId: '1000-0', lastId: '5000-3', pending: 10, maxReplay: 500 };

test('no cursor: snapshot', () => {
  assert.equal(decideResume({ ...base, cursor: null }), 'snapshot');
});

test('cursor inside retention and few pending events: replay', () => {
  assert.equal(decideResume({ ...base, cursor: '3000-0' }), 'replay');
  assert.equal(decideResume({ ...base, cursor: '1000-0' }), 'replay');
  assert.equal(decideResume({ ...base, cursor: '5000-3', pending: 0 }), 'replay');
});

test('cursor older than the first retained ID: snapshot', () => {
  assert.equal(decideResume({ ...base, cursor: '999-9' }), 'snapshot');
});

test('more than MAX_REPLAY pending: snapshot', () => {
  assert.equal(decideResume({ ...base, cursor: '3000-0', pending: 501 }), 'snapshot');
  assert.equal(decideResume({ ...base, cursor: '3000-0', pending: 500 }), 'replay');
});

test('empty stream or cursor after the last ID: snapshot', () => {
  assert.equal(decideResume({ ...base, cursor: '3000-0', firstId: null, lastId: null }), 'snapshot');
  assert.equal(decideResume({ ...base, cursor: '6000-0' }), 'snapshot');
});

test('the decision compares IDs as numbers', () => {
  const ids = { ...base, firstId: '1700000000000-10', lastId: '1700000000000-20' };
  assert.equal(decideResume({ ...ids, cursor: '1700000000000-9' }), 'snapshot');
  assert.equal(decideResume({ ...ids, cursor: '1700000000000-11' }), 'replay');
});

test('with the cursor PENDING live deltas are kept, not sent', () => {
  const sent = [];
  const conn = createConnection((frame) => sent.push(frame));
  conn.pending = [];
  conn.cursors.set('dep', PENDING);
  assert.equal(deliver(conn, 'dep', parseId('10-0'), 'x'), false);
  assert.deepEqual(sent, []);
  assert.equal(conn.pending.length, 1);
});
