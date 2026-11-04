import test from 'node:test';
import assert from 'node:assert/strict';
import { entregar } from '../../src/publico/fanout.js';
import { decidirReanudacion } from '../../src/publico/resume.js';
import { parseId } from '../../src/publico/stream.js';
import { PENDIENTE, createConnection } from '../../src/publico/topics.js';

const base = { primerId: '1000-0', ultimoId: '5000-3', pendientes: 10, maxReplay: 500 };

test('sin cursor: snapshot', () => {
  assert.equal(decidirReanudacion({ ...base, cursor: null }), 'snapshot');
});

test('cursor dentro de la retención y pocos pendientes: replay', () => {
  assert.equal(decidirReanudacion({ ...base, cursor: '3000-0' }), 'replay');
  assert.equal(decidirReanudacion({ ...base, cursor: '1000-0' }), 'replay');
  assert.equal(decidirReanudacion({ ...base, cursor: '5000-3', pendientes: 0 }), 'replay');
});

test('cursor anterior al primer ID retenido: snapshot', () => {
  assert.equal(decidirReanudacion({ ...base, cursor: '999-9' }), 'snapshot');
});

test('más de MAX_REPLAY pendientes: snapshot', () => {
  assert.equal(decidirReanudacion({ ...base, cursor: '3000-0', pendientes: 501 }), 'snapshot');
  assert.equal(decidirReanudacion({ ...base, cursor: '3000-0', pendientes: 500 }), 'replay');
});

test('stream vacío o cursor posterior al último ID: snapshot', () => {
  assert.equal(decidirReanudacion({ ...base, cursor: '3000-0', primerId: null, ultimoId: null }), 'snapshot');
  assert.equal(decidirReanudacion({ ...base, cursor: '6000-0' }), 'snapshot');
});

test('la decisión compara IDs como números', () => {
  const ids = { ...base, primerId: '1700000000000-10', ultimoId: '1700000000000-20' };
  assert.equal(decidirReanudacion({ ...ids, cursor: '1700000000000-9' }), 'snapshot');
  assert.equal(decidirReanudacion({ ...ids, cursor: '1700000000000-11' }), 'replay');
});

test('con el cursor en PENDIENTE los deltas en vivo se guardan sin enviarse', () => {
  const enviados = [];
  const conn = createConnection((frame) => enviados.push(frame));
  conn.pending = [];
  conn.cursors.set('dep', PENDIENTE);
  assert.equal(entregar(conn, 'dep', parseId('10-0'), 'x'), false);
  assert.deepEqual(enviados, []);
  assert.equal(conn.pending.length, 1);
});
