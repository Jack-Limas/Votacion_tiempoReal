import test from 'node:test';
import assert from 'node:assert/strict';
import { createFanout, reducirEntradas } from '../../src/results/fanout.js';
import { compareIds, parseId } from '../../src/results/stream.js';
import { createConnection, createTopicRegistry } from '../../src/results/topics.js';

function evento(cambios, extra = {}) {
  return { acta: 'E14-1-0001-03', version: 1, tipo: 'nueva', baja: false, ts_recepcion: Date.now(), cambios, ...extra };
}

function conexionQueGuarda(registry, topic) {
  const recibidos = [];
  const conn = createConnection((frame) => recibidos.push(frame));
  registry.add(conn);
  registry.subscribe(conn, topic);
  return { conn, recibidos, mensajes: () => recibidos.map((f) => JSON.parse(f.slice(6))) };
}

test('compareIds compara ms y secuencia como números, no como texto', () => {
  assert.equal(compareIds('1700000000000-10', '1700000000000-9'), 1);
  assert.equal(compareIds('9-0', '10-0'), -1);
  assert.equal(compareIds('1700000000001-0', '1700000000000-99'), 1);
  assert.equal(compareIds('5-3', '5-3'), 0);
  assert.equal(compareIds(parseId('5-2'), '5-3'), -1);
  assert.ok('9-0' > '10-0', 'como texto el orden sería al revés');
});

test('reducirEntradas deja un delta por tópico con el último valor de cada campo', () => {
  const entries = [
    { id: '1-0', evento: evento({ 'mun:11': { 'v:C1': 10, mesas_informadas: 1 }, dep: { 'v:C1': 10 } }) },
    { id: '2-0', evento: evento({ 'mun:12': { 'v:C1': 99 }, dep: { 'v:C1': 109 } }) },
    { id: '3-0', evento: evento({ 'mun:11': { 'v:C1': 25, 'v:C2': 4, mesas_informadas: 2 }, dep: { 'v:C1': 124 } }) }
  ];
  assert.deepEqual(reducirEntradas(entries, 'mun:11'), {
    cambios: { 'v:C1': 25, 'v:C2': 4, mesas_informadas: 2 },
    motivo: null,
    eventos: 2
  });
  assert.equal(reducirEntradas(entries, 'dep').cambios['v:C1'], 124);
  assert.equal(reducirEntradas(entries, 'pto:5').eventos, 0);
});

test('una corrección que baja un número lleva su motivo', () => {
  const entries = [
    { id: '1-0', evento: evento({ 'mun:11': { 'v:C1': 30 } }) },
    { id: '2-0', evento: evento({ 'mun:11': { 'v:C1': 22, 'v:C2': 18 } }, { tipo: 'correccion', baja: true, acta: 'E14-9-0002-03', version: 2 }) }
  ];
  assert.deepEqual(reducirEntradas(entries, 'mun:11').motivo, { tipo: 'correccion', acta: 'E14-9-0002-03', version: 2 });
});

test('batched: varios eventos en la ventana producen UN mensaje por tópico, serializado una vez', async () => {
  const registry = createTopicRegistry();
  const a = conexionQueGuarda(registry, 'mun:11');
  const b = conexionQueGuarda(registry, 'mun:11');
  const c = conexionQueGuarda(registry, 'pto:5');
  const fanout = createFanout({ registry, mode: 'batched' });

  fanout.onEntries([
    { id: '100-0', evento: evento({ 'mun:11': { 'v:C1': 1 }, 'pto:5': { 'v:C1': 1 } }) },
    { id: '100-1', evento: evento({ 'mun:11': { 'v:C1': 2, 'v:C2': 7 } }) },
    { id: '101-0', evento: evento({ 'mun:11': { 'v:C1': 3 }, 'pto:9': { 'v:C1': 5 } }) }
  ]);
  assert.equal(a.recibidos.length, 0, 'nada se envía al llegar');
  await fanout.flush();

  assert.deepEqual(a.mensajes(), [{ t: 'delta', topic: 'mun:11', upTo: '101-0', cambios: { 'v:C1': 3, 'v:C2': 7 } }]);
  assert.strictEqual(a.recibidos[0], b.recibidos[0], 'el mismo string para todos los suscriptores');
  assert.deepEqual(c.mensajes(), [{ t: 'delta', topic: 'pto:5', upTo: '101-0', cambios: { 'v:C1': 1 } }]);
});

test('batched: descarta deltas con upTo <= cursor de la conexión', async () => {
  const registry = createTopicRegistry();
  const a = conexionQueGuarda(registry, 'dep');
  a.conn.cursors.set('dep', parseId('200-0'));
  const fanout = createFanout({ registry, mode: 'batched' });

  fanout.onEntries([{ id: '150-0', evento: evento({ dep: { 'v:C1': 1 } }) }]);
  await fanout.flush();
  assert.equal(a.recibidos.length, 0);

  fanout.onEntries([{ id: '250-0', evento: evento({ dep: { 'v:C1': 2 } }) }]);
  await fanout.flush();
  assert.equal(a.mensajes()[0].upTo, '250-0');
  assert.equal(compareIds(a.conn.cursors.get('dep'), '250-0'), 0);
});

test('batched: cede el Event Loop entre lotes de BATCH_SIZE', async () => {
  const registry = createTopicRegistry();
  const conexiones = Array.from({ length: 10 }, () => conexionQueGuarda(registry, 'dep'));
  const fanout = createFanout({ registry, mode: 'batched', batchSize: 3 });
  fanout.onEntries([{ id: '1-0', evento: evento({ dep: { 'v:C1': 1 } }) }]);

  const enviadosAlCeder = [];
  const contar = () => conexiones.filter((c) => c.recibidos.length > 0).length;
  setImmediate(() => enviadosAlCeder.push(contar()));
  await fanout.flush();

  assert.equal(contar(), 10);
  assert.equal(enviadosAlCeder[0], 3, 'otro callback corrió después del primer lote');
});

test('batched: los flushes no se solapan; lo que llega durante uno se envía al terminar', async () => {
  const registry = createTopicRegistry();
  Array.from({ length: 4 }, () => conexionQueGuarda(registry, 'dep'));
  const a = conexionQueGuarda(registry, 'dep');
  const fanout = createFanout({ registry, mode: 'batched', batchSize: 2 });

  fanout.onEntries([{ id: '1-0', evento: evento({ dep: { 'v:C1': 1 } }) }]);
  const primero = fanout.flush();
  fanout.onEntries([{ id: '2-0', evento: evento({ dep: { 'v:C1': 2 } }) }]);
  const segundo = fanout.flush();
  assert.strictEqual(primero, segundo, 'el segundo flush se fusiona con el que está en curso');
  await segundo;

  assert.deepEqual(a.mensajes().map((m) => [m.upTo, m.cambios['v:C1']]), [['1-0', 1], ['2-0', 2]]);
});

test('naive: cada evento se envía de inmediato a cada suscriptor', () => {
  const registry = createTopicRegistry();
  const a = conexionQueGuarda(registry, 'mun:11');
  const fanout = createFanout({ registry, mode: 'naive' });
  fanout.onEntries([
    { id: '1-0', evento: evento({ 'mun:11': { 'v:C1': 1 } }) },
    { id: '2-0', evento: evento({ 'mun:11': { 'v:C1': 2 } }) }
  ]);
  assert.deepEqual(a.mensajes().map((m) => m.upTo), ['1-0', '2-0']);
});
