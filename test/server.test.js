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

test('exporta tres clientes Redis independientes', () => {
  assert.notStrictEqual(redisClient, redisSubClient);
  assert.notStrictEqual(redisClient, redisPubClient);
  assert.notStrictEqual(redisSubClient, redisPubClient);
});

test('el servidor HTTP acepta una conexión WebSocket en /ws', async (t) => {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  t.after(() => stopServer());

  const { port } = server.address();
  const client = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  t.after(() => {
    if (client.readyState === WebSocket.OPEN) {
      client.close();
    } else if (client.readyState !== WebSocket.CLOSED) {
      client.terminate();
    }
  });

  await new Promise((resolve, reject) => {
    client.once('open', resolve);
    client.once('error', reject);
  });

  assert.equal(webSocketServer.clients.size, 1);
  client.close();
});
