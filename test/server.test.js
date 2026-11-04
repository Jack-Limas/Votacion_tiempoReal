import test from 'node:test';
import assert from 'node:assert/strict';
import WebSocket from 'ws';
import {
  redisClient,
  redisPubClient,
  redisSubClient
} from '../src/config/redis.js';
import {
  server,
  stopServer,
  webSocketServer
} from '../src/server.js';

test('exports three independent Redis clients', () => {
  assert.notStrictEqual(redisClient, redisSubClient);
  assert.notStrictEqual(redisClient, redisPubClient);
  assert.notStrictEqual(redisSubClient, redisPubClient);
});

test('the HTTP server upgrades /ws and validates the public subscription', async (t) => {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  t.after(() => stopServer());

  const { port } = server.address();
  const client = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  t.after(() => client.terminate());

  await new Promise((resolve, reject) => {
    client.once('open', resolve);
    client.once('error', reject);
  });
  // Without ?sub= the server closes with an explicit application code.
  const code = await new Promise((resolve) => client.once('close', (closeCode) => resolve(closeCode)));
  assert.equal(code, 4400);
  assert.ok(webSocketServer);
});
