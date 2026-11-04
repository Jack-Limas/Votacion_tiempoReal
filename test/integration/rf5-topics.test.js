// Requires the docker compose environment (8 replicas + nginx + Valkey).
import test from 'node:test';
import assert from 'node:assert/strict';
import { mulberry32 } from '../../src/results/catalog.js';
import { connect } from '../../scripts/lib/sse-client.js';
import {
  BASE_URL,
  NO_ENVIRONMENT,
  environmentAvailable,
  mesasWhere,
  metricsOf,
  sendRandom,
  sleep,
  waitUntil
} from '../../scripts/lib/environment.js';

const available = await environmentAvailable();
const options = { skip: !available && NO_ENVIRONMENT };

test('RF-5: /whoami answers from different instances (no sticky sessions)', options, async () => {
  const instances = new Set();
  for (let i = 0; i < 16; i += 1) {
    const response = await fetch(new URL('/whoami', BASE_URL));
    instances.add((await response.json()).instance);
  }
  console.log(`  /whoami: ${instances.size} distinct instances in 16 requests`);
  assert.ok(instances.size >= 2, 'Every request landed on the same instance.');
});

test('RF-5: each client receives only its topic and leaving resets the counter to 0', options, async () => {
  const random = mulberry32(Date.now() % 100000);
  const ofMunicipality = mesasWhere((m) => m.municipio === 11);
  const ofPuesto = mesasWhere((m) => m.puesto === 176);
  const others = mesasWhere((m) => m.municipio !== 11 && m.puesto !== 176 && m.municipio !== 1);

  const receivedMun = [];
  const receivedPto = [];
  const clientMun = await connect(BASE_URL, [{ topic: 'mun:11' }], { onMessage: (m) => receivedMun.push(m) });
  const clientPto = await connect(BASE_URL, [{ topic: 'pto:176' }], { onMessage: (m) => receivedPto.push(m) });

  try {
    await sleep(300);
    for (let i = 0; i < 3; i += 1) {
      await sendRandom(ofMunicipality[i], random);
      await sendRandom(ofPuesto[i % ofPuesto.length], random);
      await sendRandom(others[Math.floor(random() * others.length)], random);
    }

    const arrived = await waitUntil(() => receivedMun.length > 0 && receivedPto.length > 0, { timeout: 8000 });
    assert.ok(arrived, 'Clients did not receive messages of their topic.');
    await sleep(2500);

    assert.ok(receivedMun.every((m) => m.topic === 'mun:11'), `mun:11 received other topics: ${[...new Set(receivedMun.map((m) => m.topic))]}`);
    assert.ok(receivedPto.every((m) => m.topic === 'pto:176'), `pto:176 received other topics: ${[...new Set(receivedPto.map((m) => m.topic))]}`);
    console.log(`  mun:11 received ${receivedMun.length} messages, pto:176 received ${receivedPto.length}; none foreign`);

    const before = await metricsOf(clientMun.instance);
    assert.ok(before.subscribers['mun:11'] >= 1);
  } finally {
    clientMun.close();
    clientPto.close();
  }

  const clean = await waitUntil(async () => {
    const mun = await metricsOf(clientMun.instance);
    const pto = await metricsOf(clientPto.instance);
    return !mun.subscribers['mun:11'] && !pto.subscribers['pto:176'];
  }, { timeout: 5000, interval: 200 });
  assert.ok(clean, 'The disconnected client is still in the topic map.');
});
