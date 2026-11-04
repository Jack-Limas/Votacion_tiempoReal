// Load test (RF-4, RT-8): opens N public clients against the load balancer,
// runs the election-day simulator and stores, per second and per instance,
// the Event Loop delay and the messages sent (CSV), plus a JSON summary with
// client-side delivery latency.
//
// Usage (inside the docker compose network, see README):
//   node scripts/load.js --connections=20000 --processes=8 --speed=30 --from=0 --to=40
//     [--transport=sse|ws|poll] [--poll-ms=2000] [--mix=dep:0.5,mun:0.35,pto:0.15]
//     [--url=http://nginx] [--reset=1] [--label=x]
//
// Several machines: run the generator with --simulate=0 --hold=600 on the
// extra machines (they only hold connections and print their counts), and the
// normal command on one machine (it drives the simulator and collects the
// cluster metrics, which include every machine's connections).
import { fork } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { createClient } from 'redis';
import WebSocket from 'ws';
import { buildCatalog, mulberry32, seedCatalog } from '../src/results/catalog.js';
import { createKeys } from '../src/results/keys.js';
import { subParam } from './lib/sse-client.js';
import { simulate } from './simulate-actas.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const BUCKET_MS = 10;
const BUCKETS = 6000; // latency histogram up to 60 s

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
    transport: values.transport ?? 'sse',
    pollMs: Number(values['poll-ms'] ?? 2000),
    mix: values.mix ?? 'dep:0.5,mun:0.35,pto:0.15',
    speed: Number(values.speed ?? 30),
    from: Number(values.from ?? 0),
    to: Number(values.to ?? 40),
    simulate: values.simulate !== '0',
    hold: Number(values.hold ?? 600),
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

// ---------- Worker process: keeps clients open and measures what arrives ----------
async function worker(opts) {
  const topics = pickTopics(opts.connections, opts.mix, opts.seed);
  const stats = { open: 0, failed: 0, closed: 0, messages: 0, bytes: 0, requests: 0, latency: new Array(BUCKETS).fill(0) };
  const agent = new http.Agent({ keepAlive: opts.transport === 'poll', maxSockets: Infinity });

  // Delivery latency: the stream ID carries the Redis time of the XADD, so
  // "now - upTo" is the time from the acta being applied to the client seeing
  // it (same Docker host clock; across machines, clock skew adds to it).
  function recordLatency(upTo) {
    const ms = Date.now() - Number(String(upTo).split('-')[0]);
    if (ms >= 0) stats.latency[Math.min(BUCKETS - 1, Math.floor(ms / BUCKET_MS))] += 1;
  }

  function onFrame(text) {
    if (text === '{"t":"hb"}') return;
    stats.messages += 1;
    if (text.startsWith('{"t":"delta"')) {
      const at = text.indexOf('"upTo":"');
      if (at !== -1) recordLatency(text.slice(at + 8, text.indexOf('"', at + 8)));
    }
  }

  function openSse(topic) {
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
        let buffer = '';
        res.on('data', (chunk) => {
          stats.bytes += chunk.length;
          buffer += chunk;
          let end = buffer.indexOf('\n\n');
          while (end !== -1) {
            const block = buffer.slice(0, end);
            buffer = buffer.slice(end + 2);
            if (block.startsWith('data: ')) onFrame(block.slice(6));
            end = buffer.indexOf('\n\n');
          }
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

  function openWs(topic) {
    return new Promise((resolve) => {
      const ws = new WebSocket(`${opts.url.replace(/^http/, 'ws')}/ws?sub=${encodeURIComponent(topic)}`, { perMessageDeflate: false });
      ws.on('open', () => {
        stats.open += 1;
        resolve();
      });
      ws.on('message', (data) => {
        stats.bytes += data.length;
        onFrame(data.toString());
      });
      ws.on('close', () => {
        stats.closed += 1;
      });
      ws.on('error', () => {
        stats.failed += 1;
        resolve();
      });
    });
  }

  function startPoller(topic) {
    let lastUpTo = null;
    const pollOnce = () => new Promise((resolve) => {
      stats.requests += 1;
      const req = http.get(new URL(`/api/results/${topic}`, opts.url), { agent }, (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          stats.bytes += body.length;
          if (res.statusCode === 200) {
            const upTo = JSON.parse(body).upTo;
            if (lastUpTo && upTo !== lastUpTo) {
              stats.messages += 1;
              recordLatency(upTo);
            }
            lastUpTo = upTo;
          } else {
            stats.failed += 1;
          }
          resolve();
        });
      });
      req.on('error', () => {
        stats.failed += 1;
        resolve();
      });
    });
    stats.open += 1;
    // Random phase so pollers are not synchronised.
    setTimeout(function loop() {
      pollOnce().finally(() => setTimeout(loop, opts.pollMs));
    }, Math.random() * opts.pollMs);
    return Promise.resolve();
  }

  const opener = opts.transport === 'ws' ? openWs : opts.transport === 'poll' ? startPoller : openSse;
  // Gentle ramp: 100 concurrent opens per process, then a short pause.
  for (let i = 0; i < topics.length; i += 100) {
    await Promise.all(topics.slice(i, i + 100).map(opener));
    await sleep(20);
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

function histogramPercentile(buckets, p) {
  const total = buckets.reduce((a, b) => a + b, 0);
  if (total === 0) return null;
  let seen = 0;
  for (let i = 0; i < buckets.length; i += 1) {
    seen += buckets[i];
    if (seen >= (p / 100) * total) return i * BUCKET_MS;
  }
  return null;
}

function collect(children) {
  return Promise.all(children.map((child) => new Promise((resolve) => {
    child.once('message', (m) => resolve(m.stats));
    child.send('stats');
  })));
}

async function main(opts) {
  if (opts.reset && opts.simulate) {
    const redis = createClient({ url: opts.redisUrl });
    await redis.connect();
    await redis.flushDb();
    await seedCatalog(redis, createKeys(process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}').catalog);
    await redis.quit();
    // Let every instance publish metrics again after the flush.
    await sleep(2000);
  }

  const before = await readCluster(opts.url);
  console.log(`Opening ${opts.connections} ${opts.transport} clients in ${opts.processes} processes against ${opts.url}...`);
  const perProcess = Math.ceil(opts.connections / opts.processes);
  const children = [];
  const ready = [];
  const opening = Date.now();
  for (let p = 0; p < opts.processes; p += 1) {
    const count = Math.min(perProcess, opts.connections - p * perProcess);
    const child = fork(process.argv[1], [
      '--role=worker', `--url=${opts.url}`, `--connections=${count}`, `--transport=${opts.transport}`,
      `--poll-ms=${opts.pollMs}`, `--mix=${opts.mix}`, `--seed=${opts.seed * 100 + p}`
    ]);
    children.push(child);
    ready.push(new Promise((resolve) => child.once('message', (m) => resolve(m.stats))));
  }
  const readyStats = await Promise.all(ready);
  const opened = readyStats.reduce((acc, s) => acc + s.open, 0);
  const failedToOpen = readyStats.reduce((acc, s) => acc + s.failed, 0);
  console.log(`Open clients: ${opened} (failed ${failedToOpen}) in ${Math.round((Date.now() - opening) / 1000)} s. Settling...`);
  await sleep(5000);
  const settled = await readCluster(opts.url);

  if (!opts.simulate) {
    const until = Date.now() + opts.hold * 1000;
    while (Date.now() < until) {
      const stats = await collect(children);
      console.log(JSON.stringify({ open: stats.reduce((a, s) => a + s.open - s.closed, 0), messages: stats.reduce((a, s) => a + s.messages, 0) }));
      await sleep(10_000);
    }
    for (const child of children) child.send('exit');
    return;
  }

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
          cpu_percent: m.cpuPercent,
          heap_mb: m.memory.heapMB,
          rss_mb: m.memory.rssMB,
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
  const received = await collect(children);
  for (const child of children) child.send('exit');

  const latency = new Array(BUCKETS).fill(0);
  for (const s of received) s.latency.forEach((n, i) => { latency[i] += n; });
  const delta = (fn) => fn(final.totals) - fn(before.totals);
  const active = rows.filter((r) => r.messages_per_s > 0 || r.el_p99_ms > 0);
  const sum = (cluster, fn) => cluster.instances.reduce((acc, m) => acc + fn(m), 0);
  const mode = final.instances[0]?.mode;
  const sent = delta((t) => t.messagesSent);
  const naive = delta((t) => t.naiveMessages);
  const summary = {
    transport: opts.transport,
    mode,
    clientsRequested: opts.connections,
    clientsOpen: opened,
    clientsFailedToOpen: failedToOpen,
    serverConnections: settled.totals.connections,
    instances: final.totals.instances,
    simulatedMinutes: [opts.from, opts.to],
    speed: opts.speed,
    actas,
    messagesSent: sent,
    naiveMessages: naive,
    reduction: sent > 0 ? Math.round(naive / sent) : null,
    messagesReceivedByClients: received.reduce((acc, s) => acc + s.messages, 0),
    pollRequests: received.reduce((acc, s) => acc + s.requests, 0),
    bytesReceivedPerClient: Math.round(received.reduce((acc, s) => acc + s.bytes, 0) / Math.max(opened, 1)),
    clientsClosedDuringTest: received.reduce((acc, s) => acc + s.closed, 0),
    // Measured from the NEWEST event of each delivered window to its arrival;
    // the per-acta delay is publishDelayMs (receivedAt -> sent, server side).
    latencyFromNewestEventMs: {
      p50: histogramPercentile(latency, 50),
      p95: histogramPercentile(latency, 95),
      p99: histogramPercentile(latency, 99),
      samples: latency.reduce((a, b) => a + b, 0)
    },
    memory: {
      heapMBBefore: sum(before, (m) => m.memory.heapMB),
      heapMBWithClients: sum(settled, (m) => m.memory.heapMB),
      rssMBBefore: sum(before, (m) => m.memory.rssMB),
      rssMBWithClients: sum(settled, (m) => m.memory.rssMB),
      // Heap growth divided by clients. RSS is reported but not divided: the
      // allocator returns memory lazily, so its delta can even be negative.
      heapKBPerClient: opened > 0 ? Math.round(((sum(settled, (m) => m.memory.heapMB) - sum(before, (m) => m.memory.heapMB)) * 1024) / opened * 10) / 10 : null
    },
    cpuPercentPerInstance: {
      mean: Math.round((active.reduce((a, r) => a + (r.cpu_percent ?? 0), 0) / Math.max(active.length, 1)) * 10) / 10,
      p95: percentile(active.map((r) => r.cpu_percent ?? 0), 95)
    },
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
  const name = `${opts.output}/load-${opts.transport}-${mode}-${opts.connections}${opts.label ? `-${opts.label}` : ''}`;
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
