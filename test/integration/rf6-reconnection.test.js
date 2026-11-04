// RF-6 A/B test. Requires the docker compose environment.
//   AB_DISCONNECT_MS (30000)  time B stays disconnected
//   AB_ACTAS (30)             actas sent during the disconnection
//   AB_EXPECTED               'replay' | 'snapshot': path B must take
// To force the snapshot path: MAXLEN=20 docker compose up -d app and
// AB_ACTAS=250 AB_EXPECTED=snapshot npm run test:integration
import test from 'node:test';
import assert from 'node:assert/strict';
import { mulberry32 } from '../../src/results/catalog.js';
import { correctVotes, randomVotes } from '../../scripts/lib/actas.js';
import { connect, createClientState } from '../../scripts/lib/sse-client.js';
import {
  BASE_URL,
  NO_ENVIRONMENT,
  environmentAvailable,
  mesasWhere,
  sendRandom,
  sendVotes,
  sleep,
  waitUntil
} from '../../scripts/lib/environment.js';

const DISCONNECT_MS = Number(process.env.AB_DISCONNECT_MS ?? 30000);
const ACTAS_DURING = Number(process.env.AB_ACTAS ?? 30);
const EXPECTED = process.env.AB_EXPECTED;
const available = await environmentAvailable();

function client(name) {
  const state = createClientState();
  const messages = [];
  return {
    name,
    state,
    messages,
    onMessage: (message) => {
      messages.push(message);
      state.apply(message);
    }
  };
}

test('RF-6: B disconnects, comes back on ANOTHER instance and ends identical to A', { skip: !available && NO_ENVIRONMENT }, async () => {
  const random = mulberry32(Date.now() % 100000);
  const municipio = 7;
  const mesas = mesasWhere((m) => m.municipio === municipio);
  const topics = ['dep', `mun:${municipio}`, `pto:${mesas[0].puesto}`];
  const subs = topics.map((topic) => ({ topic }));

  const a = client('A');
  const b = client('B');
  const connectionA = await connect(BASE_URL, subs, { onMessage: a.onMessage });
  let connectionB = await connect(BASE_URL, subs, { onMessage: b.onMessage });
  try {
    const hasSnapshot = (c) => topics.every((t) => c.messages.some((m) => m.t === 'snapshot' && m.topic === t));
    assert.ok(await waitUntil(() => hasSnapshot(a) && hasSnapshot(b)), 'A and B must get an initial snapshot');

    for (let i = 0; i < 5; i += 1) await sendRandom(mesas[i % mesas.length], random);
    await sleep(2000);

    // --- B disconnects ---
    connectionB.close();
    const previousInstance = connectionB.instance;
    const cursors = b.state.cursors();
    const start = Date.now();

    // While away: actas of the municipality and, at the end, a correction that
    // LOWERS a candidate in a mesa of the followed puesto (must carry a reason).
    const correctedMesa = mesas[0];
    const originalVotes = randomVotes(random, 300);
    await sendVotes(correctedMesa, originalVotes);
    for (let i = 0; i < ACTAS_DURING; i += 1) {
      await sendRandom(mesas[1 + Math.floor(random() * (mesas.length - 1))], random);
      await sleep(Math.max(0, (DISCONNECT_MS - 8000) / ACTAS_DURING - 20));
    }
    await sleep(1500);
    const { acta: correction } = await sendVotes(correctedMesa, correctVotes(originalVotes, random));
    await sleep(Math.max(0, DISCONNECT_MS - (Date.now() - start)));

    // --- B comes back with its cursors; it must land on another instance ---
    const messagesBefore = b.messages.length;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      connectionB = await connect(BASE_URL, cursors, { onMessage: b.onMessage });
      if (connectionB.instance !== previousInstance) break;
      connectionB.close();
      await sleep(50);
    }
    assert.notEqual(connectionB.instance, previousInstance, 'B had to reconnect on another instance');
    assert.ok(await waitUntil(() => b.messages.length > messagesBefore || EXPECTED === 'replay', { timeout: 3000 }));

    for (let i = 0; i < 3; i += 1) await sendRandom(mesas[1 + i], random);
    await sleep(2500);

    // --- Compare by hash ---
    const resumed = b.messages.slice(messagesBefore);
    const path = topics.map((t) => resumed.find((m) => m.topic === t)?.t === 'snapshot' ? 'snapshot' : 'replay');
    console.log(`  B: ${previousInstance} -> ${connectionB.instance}, away ${Math.round((Date.now() - start) / 1000)} s, path per topic: ${topics.map((t, i) => `${t}=${path[i]}`).join(' ')}`);
    for (const topic of topics) {
      console.log(`  ${topic}: hash A ${a.state.hash(topic).slice(0, 12)} / hash B ${b.state.hash(topic).slice(0, 12)}`);
      assert.equal(b.state.hash(topic), a.state.hash(topic), `state differs on ${topic}`);
    }
    if (EXPECTED) assert.deepEqual(path, topics.map(() => EXPECTED));

    // --- The correction that lowered a number arrived with its reason ---
    const reason = { type: 'correction', acta: correction.acta, version: correction.version };
    const withReason = (m) => m.topic === `pto:${correctedMesa.puesto}` && m.reason?.version === correction.version;
    const deltaA = a.messages.find(withReason);
    assert.deepEqual(deltaA?.reason, reason, 'A had to receive the correction delta with its reason');
    console.log(`  reason received by A: ${JSON.stringify(deltaA.reason)}`);
    if (path[2] === 'replay') {
      assert.deepEqual(resumed.find(withReason)?.reason, reason, 'B replay had to keep the reason');
    }
  } finally {
    connectionA.close();
    connectionB.close();
  }
});
