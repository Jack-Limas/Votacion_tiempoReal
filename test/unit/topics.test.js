import test from 'node:test';
import assert from 'node:assert/strict';
import { createConnection, createTopicRegistry, parseSubscriptions } from '../../src/publico/topics.js';

test('alta y recorrido: cada tópico solo contiene sus suscriptores', () => {
  const registry = createTopicRegistry();
  const a = createConnection(() => {});
  const b = createConnection(() => {});
  registry.add(a);
  registry.add(b);
  registry.subscribe(a, 'mun:11');
  registry.subscribe(a, 'dep');
  registry.subscribe(b, 'pto:176');

  assert.deepEqual([...registry.subscribers('mun:11')], [a]);
  assert.deepEqual([...registry.subscribers('pto:176')], [b]);
  assert.deepEqual([...registry.subscribers('dep')], [a]);
  assert.equal(registry.subscribers('mun:99').size, 0);
  assert.deepEqual([...a.topics].sort(), ['dep', 'mun:11']);
  assert.deepEqual(registry.counts(), { 'mun:11': 1, dep: 1, 'pto:176': 1 });
});

test('baja: al cerrar la conexión desaparece de todos sus tópicos', () => {
  const registry = createTopicRegistry();
  const a = createConnection(() => {});
  const b = createConnection(() => {});
  registry.add(a);
  registry.add(b);
  registry.subscribe(a, 'mun:11');
  registry.subscribe(a, 'pto:176');
  registry.subscribe(b, 'mun:11');

  registry.remove(a);
  assert.deepEqual(registry.counts(), { 'mun:11': 1 });
  assert.equal(a.topics.size, 0);
  assert.equal(a.cursors.size, 0);
  assert.equal(registry.connections.size, 1);

  registry.remove(b);
  assert.deepEqual(registry.counts(), {});
  assert.equal(registry.topicCount, 0);
});

test('baja de un solo tópico conserva los demás', () => {
  const registry = createTopicRegistry();
  const a = createConnection(() => {});
  registry.subscribe(a, 'mun:11', '1-0');
  registry.subscribe(a, 'dep');
  registry.unsubscribe(a, 'mun:11');
  assert.deepEqual(registry.counts(), { dep: 1 });
  assert.equal(a.cursors.has('mun:11'), false);
});

test('parseSubscriptions acepta tópicos con y sin cursor', () => {
  assert.deepEqual(parseSubscriptions('mun:11@1700000000000-3,dep,pto:176@5-0'), [
    { topic: 'mun:11', cursor: '1700000000000-3' },
    { topic: 'dep', cursor: null },
    { topic: 'pto:176', cursor: '5-0' }
  ]);
});

test('parseSubscriptions rechaza tópicos, cursores y cantidades inválidos', () => {
  assert.throws(() => parseSubscriptions(''), /obligatorio/);
  assert.throws(() => parseSubscriptions('mesa:4201'), /Tópico inválido/);
  assert.throws(() => parseSubscriptions('mun:11@abc'), /Cursor inválido/);
  assert.throws(() => parseSubscriptions('mun:1,mun:2,mun:3', { maxTopics: 2 }), /Máximo 2/);
  assert.throws(() => parseSubscriptions(['dep', 'mun:1']), /obligatorio/);
});
