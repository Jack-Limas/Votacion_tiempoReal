// RF-2 against a real Redis/Valkey (docker compose up -d valkey).
// The final totals must depend only on the SET of actas received, never on
// their arrival order, and a resend can never add twice.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createClient } from 'redis';
import { createIdempotencyService } from '../../src/services/idempotencyService.js';
import { createKeys } from '../../src/results/keys.js';
import { seedCatalog } from '../../src/results/catalog.js';
import { validateActa } from '../../src/validation/acta.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

async function connectOrNull() {
  const client = createClient({ url: REDIS_URL, socket: { connectTimeout: 1000, reconnectStrategy: false } });
  client.on('error', () => {});
  try {
    await client.connect();
    return client;
  } catch {
    return null;
  }
}

const probe = await connectOrNull();
await probe?.quit();
const options = { skip: probe ? false : `Redis not reachable at ${REDIS_URL} (run: docker compose up -d valkey)` };

function acta(version, votos, idem = randomUUID()) {
  const sufragantes = Object.values(votos).reduce((a, b) => a + b, 0);
  return {
    acta: 'E14-176-0042-03', mesa: 4201, puesto: 176, municipio: 11, version,
    jurado: 'JUR-88134', ts_cierre: 1793577600, votos, sufragantes, hash_foto: 'f', idem
  };
}

const V1 = acta(1, { C1: 100, C2: 50 });
const V2 = acta(2, { C1: 90, C2: 60 });
const V2_OTHER = acta(2, { C1: 80, C2: 70 });
const V3 = acta(3, { C1: 95, C2: 55, C3: 1 });

function permutations(items) {
  if (items.length <= 1) return [items];
  return items.flatMap((item, i) => permutations([...items.slice(0, i), ...items.slice(i + 1)]).map((rest) => [item, ...rest]));
}

async function withPrefix(t) {
  const client = await connectOrNull();
  const prefix = `test:{rf2-${randomUUID().slice(0, 8)}}`;
  const keys = createKeys(prefix);
  await seedCatalog(client, keys.catalog);
  t.after(async () => {
    for await (const key of client.scanIterator({ MATCH: `${prefix}*`, COUNT: 500 })) await client.del(key);
    await client.quit();
  });
  return { client, keys, service: createIdempotencyService(client, { keyPrefix: prefix, ttlSeconds: 60 }) };
}

async function levels(client, keys) {
  const read = async (key) => {
    const hash = await client.hGetAll(key);
    delete hash.digest;
    // Field order in a hash depends on insertion order; compare canonically.
    return Object.fromEntries(Object.entries(hash).sort(([a], [b]) => a.localeCompare(b)));
  };
  return {
    mesa: await read(keys.mesa(4201)),
    pto: await read(keys.total('pto:176')),
    mun: await read(keys.total('mun:11')),
    dep: await read(keys.total('dep'))
  };
}

test('RF-2: every arrival order of v1, v2, v2 (other figures) and v3 converges to the same totals', options, async (t) => {
  const orders = permutations([V1, V2, V2_OTHER, V3]);
  const finals = new Set();
  let reference = null;
  for (const order of orders) {
    const { client, keys, service } = await withPrefix(t);
    for (const item of order) await service.registerActa(item, validateActa(item));
    const state = await levels(client, keys);
    finals.add(JSON.stringify(state));
    reference ??= state;
  }
  assert.equal(finals.size, 1, `${orders.length} orders produced ${finals.size} different results`);
  // v3 is the maximum version, so its figures are the applied ones on every level.
  assert.equal(reference.mesa['v:C1'], '95');
  assert.equal(reference.mesa.version, '3');
  assert.deepEqual(
    { C1: reference.dep['v:C1'], C2: reference.dep['v:C2'], C3: reference.dep['v:C3'] },
    { C1: '95', C2: '55', C3: '1' }
  );
  assert.equal(reference.dep.mesas_reported, '1', 'a mesa is reported once, whatever the corrections');
  console.log(`  ${orders.length} arrival orders, 1 final state`);
});

test('the candidate ranking ZSET matches the hash totals in every arrival order', options, async (t) => {
  for (const order of [[V1, V2, V3], [V3, V2_OTHER, V1], [V2, V3, V1, V2_OTHER]]) {
    const { client, keys, service } = await withPrefix(t);
    for (const item of order) await service.registerActa(item, validateActa(item));
    const ranking = await client.zRangeWithScores(keys.rank('dep'), 0, -1, { REV: true });
    assert.deepEqual(ranking.map(({ value, score }) => [value, score]), [['C1', 95], ['C2', 55], ['C3', 1]]);
  }
});

test('RF-2: a late v2 after v3 is reported as superseded and does not move the totals back', options, async (t) => {
  const { client, keys, service } = await withPrefix(t);
  await service.registerActa(V1, validateActa(V1));
  await service.registerActa(V3, validateActa(V3));
  const before = await levels(client, keys);
  const late = await service.registerActa(V2, validateActa(V2));
  assert.equal(late.superseded, true);
  assert.equal(late.appliedVersion, 3);
  assert.deepEqual(await levels(client, keys), before);
});

test('RF-1: a resend after its idem key expired still adds nothing', options, async (t) => {
  const { client, keys, service } = await withPrefix(t);
  const first = await service.registerActa(V1, validateActa(V1));
  assert.equal(first.status, 'accepted');
  const before = await levels(client, keys);
  await client.del(`${keys.stream.replace(':st:events', '')}:idem:${V1.idem}`);
  const resend = await service.registerActa(V1, validateActa(V1));
  assert.equal(resend.duplicate, false, 'the idem record is gone, so it is not recognised as a duplicate');
  assert.equal(resend.superseded, true, '...but the version/content order still blocks a second sum');
  assert.deepEqual(await levels(client, keys), before);
});

test('RF-1: the same figures resent with a new idem do not produce an empty correction', options, async (t) => {
  const { client, keys, service } = await withPrefix(t);
  await service.registerActa(V1, validateActa(V1));
  const length = await client.xLen(keys.stream);
  const again = await service.registerActa({ ...V1, idem: randomUUID() }, validateActa(V1));
  assert.equal(again.superseded, true);
  assert.equal(await client.xLen(keys.stream), length, 'no new public event');
});
