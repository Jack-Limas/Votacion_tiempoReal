import test from 'node:test';
import assert from 'node:assert/strict';
import { createConnection, createTopicRegistry, parseSubscriptions } from '../../src/results/topics.js';

test('subscribe and walk: each topic only holds its subscribers', () => {
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

test('remove: a closed connection leaves all its topics', () => {
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

test('unsubscribing one topic keeps the others', () => {
  const registry = createTopicRegistry();
  const a = createConnection(() => {});
  registry.subscribe(a, 'mun:11', '1-0');
  registry.subscribe(a, 'dep');
  registry.unsubscribe(a, 'mun:11');
  assert.deepEqual(registry.counts(), { dep: 1 });
  assert.equal(a.cursors.has('mun:11'), false);
});

test('parseSubscriptions accepts topics with and without cursor', () => {
  assert.deepEqual(parseSubscriptions('mun:11@1700000000000-3,dep,pto:176@5-0'), [
    { topic: 'mun:11', cursor: '1700000000000-3' },
    { topic: 'dep', cursor: null },
    { topic: 'pto:176', cursor: '5-0' }
  ]);
});

test('parseSubscriptions rejects invalid topics, cursors and counts', () => {
  assert.throws(() => parseSubscriptions(''), /required/);
  assert.throws(() => parseSubscriptions('mesa:4201'), /Invalid topic/);
  assert.throws(() => parseSubscriptions('mun:11@abc'), /Invalid cursor/);
  assert.throws(() => parseSubscriptions('mun:1,mun:2,mun:3', { maxTopics: 2 }), /At most 2/);
  assert.throws(() => parseSubscriptions(['dep', 'mun:1']), /required/);
});
