// Load test (RF-4): opens N SSE connections against the load balancer, runs
// the acta simulator and stores, per second and per instance, the Event Loop
// delay and the messages sent, as CSV.
//
// Usage (inside the docker compose network, see README):
//   node scripts/load.js --connections=10000 --processes=4 --speed=30 --from=0 --to=40
//     [--mix=dep:0.5,mun:0.35,pto:0.15] [--url=http://nginx] [--reset=1] [--label=x]
import { fork } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { createClient } from 'redis';
import { buildCatalog, mulberry32, seedCatalog } from '../src/results/catalog.js';
import { createKeys } from '../src/results/keys.js';
import { subParam } from './lib/sse-client.js';
import { simulate } from './simulate-actas.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function readOptions(argv) {
  const values = Object.fromEntries(argv
    .filter((arg) => arg.startsWith('--'))
    .map((arg) => arg.slice(2).split('=')));
  return {
    role: values.role ?? 'main',
    url: values.url ?? process.env.BASE_URL ?? 'http://localhost:8080',
    redisUrl: values.redis ?? process.env.REDIS_URL ?? 'redis://localhost:6379',
    connections: Number(values.connections ?? 10000),
    processes: Number(values.processes ?? 4),
    mix: values.mix ?? 'dep:0.5,mun:0.35,pto:0.15',
    speed: Number(values.speed ?? 30),
    from: Number(values.from ?? 0),
    to: Number(values.to ?? 40),
    reset: values.reset !== '0',
    output: values.output ?? 'results',
    label: values.label ?? '',
    seed: Number(values.seed ?? 1)
  };
}

// Picks the topic of each connection from the mix; municipalities and puestos
// are picked with probability proportional to their mesas (where people are).
function pickTopics(count, mix, seed) {
  const random = mulberry32(seed);
  const { mesas } = buildCatalog();
  const parts = mix.split(',').map((p) => p.split(':')).map(([kind, weight]) => [kind, Number(weight)]);
  const total = parts.reduce((acc, [, weight]) => acc + weight, 0);
  const topics = [];
  for (let i = 0; i < count; i += 1) {
    let r = random() * total;
    const [kind] = parts.find(([, weight]) => (r -= weight) < 0) ?? parts[0];
    const mesa = mesas[Math.floor(random() * mesas.length)];
    topics.push(kind === 'dep' ? 'dep' : kind === 'mun' ? `mun:${mesa.municipio}` : `pto:${mesa.puesto}`);
  }
  return topics;
}

// ---------- Worker process: keeps connections open and counts what arrives ----------
async function worker(opts) {
  const topics = pickTopics(opts.connections, opts.mix, opts.seed);
  const stats = { open: 0, failed: 0, closed: 0, messages: 0, bytes: 0 };
  const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });

  function open(topic) {
    return new Promise((resolve) => {
      const req = http.get(new URL(`/stream?sub=${subParam([{ topic }])}`, opts.url), { agent }, (res) => {
        if (res.statusCode !== 200) {
          stats.failed += 1;
          res.resume();
          resolve();
          return;
        }
        stats.open += 1;
        res.setEncoding('utf8');
        // Counts messages without parsing JSON. Keeps 5 characters in case
        // 'data: ' is split between two chunks.
        let rest = '';
        res.on('data', (chunk) => {
          stats.bytes += chunk.length;
          const text = rest + chunk;
          for (let i = text.indexOf('data: '); i !== -1; i = text.indexOf('data: ', i + 6)) {
            if (!text.startsWith('data: {"t":"hb"}', i)) stats.messages += 1;
          }
          rest = text.slice(-5);
        });
        res.on('close', () => {
          stats.closed += 1;
        });
        resolve();
      });
      req.on('error', () => {
        stats.failed += 1;
        resolve();
      });
    });
  }

  for (let i = 0; i < topics.length; i += 200) {
    await Promise.all(topics.slice(i, i + 200).map(open));
  }
  process.send({ kind: 'ready', stats });
  process.on('message', (message) => {
    if (message === 'stats') process.send({ kind: 'stats', stats });
    if (message === 'exit') process.exit(0);
  });
}

// ---------- Main process ----------
async function readCluster(url) {
  const response = await fetch(new URL('/metrics/all', url));
  return response.json();
}

function percentile(values, p) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function main(opts) {
  if (opts.reset) {
    const redis = createClient({ url: opts.redisUrl });
    await redis.connect();
    await redis.flushDb();
    await seedCatalog(redis, createKeys(process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}').catalog);
    await redis.quit();
  }

  console.log(`Opening ${opts.connections} SSE connections in ${opts.processes} processes against ${opts.url}...`);
  const perProcess = Math.ceil(opts.connections / opts.processes);
  const children = [];
  const ready = [];
  for (let p = 0; p < opts.processes; p += 1) {
    const count = Math.min(perProcess, opts.connections - p * perProcess);
    const child = fork(process.argv[1], [
      '--role=worker', `--url=${opts.url}`, `--connections=${count}`,
      `--mix=${opts.mix}`, `--seed=${opts.seed * 100 + p}`
    ]);
    children.push(child);
    ready.push(new Promise((resolve) => child.once('message', (m) => resolve(m.stats))));
  }
  const opened = (await Promise.all(ready)).reduce((acc, s) => acc + s.open, 0);
  console.log(`Open connections: ${opened}. Settling...`);
  await sleep(4000);

  const base = await readCluster(opts.url);
  const rows = [];
  let second = 0;
  let simulating = true;
  const sampling = (async () => {
    while (simulating) {
      await sleep(1000);
      second += 1;
      const { instances } = await readCluster(opts.url);
      for (const m of instances) {
        rows.push({
          second,
          instance: m.instance,
          mode: m.mode,
          connections: m.connections,
          el_p50_ms: m.eventLoop.p50,
          el_p99_ms: m.eventLoop.p99,
          el_max_ms: m.eventLoop.max,
          messages_per_s: m.messages.perSecond,
          messages_total: m.messages.total,
          naive_total: m.messages.naiveTotal,
          events: m.events,
          publish_delay_p50_ms: m.publishDelay.p50,
          publish_delay_p99_ms: m.publishDelay.p99
        });
      }
    }
  })();

  console.log(`Simulating minutes ${opts.from}-${opts.to} of the day at x${opts.speed}...`);
  const actas = await simulate({
    url: opts.url, speed: opts.speed, from: opts.from, to: opts.to,
    limit: Infinity, seed: 7, concurrency: 64
  });
  await sleep(3000);
  simulating = false;
  await sampling;
  const final = await readCluster(opts.url);

  const received = await Promise.all(children.map((child) => new Promise((resolve) => {
    child.once('message', (m) => resolve(m.stats));
    child.send('stats');
  })));
  for (const child of children) child.send('exit');

  const delta = (fn) => fn(final.totals) - fn(base.totals);
  const active = rows.filter((r) => r.messages_per_s > 0 || r.el_p99_ms > 0);
  const mode = final.instances[0]?.mode;
  const sent = delta((t) => t.messagesSent);
  const naive = delta((t) => t.naiveMessages);
  const summary = {
    mode,
    connections: final.totals.connections,
    instances: final.totals.instances,
    actas,
    eventsPerInstance: final.totals.eventsPerInstance.map((e, i) => e - (base.totals.eventsPerInstance[i] ?? 0)),
    messagesSent: sent,
    naiveMessages: naive,
    reduction: sent > 0 ? Math.round(naive / sent) : null,
    messagesReceivedByClients: received.reduce((acc, s) => acc + s.messages, 0),
    connectionsClosedDuringTest: received.reduce((acc, s) => acc + s.closed, 0),
    eventLoopPerSecond: {
      p50OfP99: percentile(active.map((r) => r.el_p99_ms), 50),
      p99OfP99: percentile(active.map((r) => r.el_p99_ms), 99),
      maxOfP99: Math.max(...active.map((r) => r.el_p99_ms)),
      max: Math.max(...active.map((r) => r.el_max_ms))
    },
    eventLoopSinceStart: final.instances.map((m) => ({ instance: m.instance, ...m.eventLoopTotal })),
    publishDelayMs: final.instances.map((m) => ({ instance: m.instance, ...m.publishDelay.total })),
    seconds: second
  };

  mkdirSync(opts.output, { recursive: true });
  const name = `${opts.output}/load-${mode}-${final.totals.connections}${opts.label ? `-${opts.label}` : ''}`;
  const columns = Object.keys(rows[0] ?? {});
  writeFileSync(`${name}.csv`, [columns.join(','), ...rows.map((r) => columns.map((c) => r[c] ?? '').join(','))].join('\n'));
  writeFileSync(`${name}.json`, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
  console.log(`CSV: ${name}.csv`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const opts = readOptions(process.argv.slice(2));
  const task = opts.role === 'worker' ? worker(opts) : main(opts);
  task.catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
