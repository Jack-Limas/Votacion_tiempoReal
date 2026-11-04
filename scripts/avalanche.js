// RF-10, second scenario: an instance dies and its clients reconnect against
// the remaining ones. Opens N SSE clients through the load balancer, kills
// (docker kill) the instance holding the most clients and records, every
// 100 ms, how many reconnection attempts and successful reconnections happen.
//
//   --strategy=immediate  every client retries at once (worst case)
//   --strategy=fixed      every client retries after the same fixed delay
//                         (EventSource-like: 1 s), i.e. no jitter
//   --strategy=jitter     exponential backoff with full jitter, same function
//                         and base as the public page (public/assets/backoff.js)
//
// Usage, inside the compose network with the Docker socket mounted (avoids the
// host port-forwarding limits of Docker Desktop):
//   docker compose run --rm --no-deps -v /var/run/docker.sock:/var/run/docker.sock //     --entrypoint node load scripts/avalanche.js --url=http://nginx --clients=8000 --strategy=fixed
//   ... --strategy=jitter
//   node scripts/avalanche.js --chart   (draws results/avalanche.svg from both CSVs)
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import WebSocket from 'ws';
import { backoffDelay } from '../public/assets/backoff.js';

const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => {
  const [k, v] = a.slice(2).split('=');
  return [k, v ?? 'true'];
}));
const BASE = args.url ?? process.env.BASE_URL ?? 'http://localhost:8080';
const CLIENTS = Number(args.clients ?? 4000);
const STRATEGY = args.strategy ?? 'jitter';
const TRANSPORT = args.transport ?? 'sse';
const FIXED_MS = Number(args.fixed ?? 1000);
const BASE_MS = args.base ? Number(args.base) : null;
const OBSERVE_MS = Number(args.observe ?? 70_000);
// Same rule as the public page: 25 s without any frame (heartbeats arrive
// every 10 s) means the instance is gone even if the socket never closed.
const WATCHDOG_MS = Number(args.watchdog ?? 25_000);
const DOCKER_SOCKET = '/var/run/docker.sock';
const BUCKET_MS = 100;
const OUTPUT = args.output ?? 'results';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function openWs(client) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`${BASE.replace(/^http/, 'ws')}/ws?sub=dep`, { perMessageDeflate: false });
    let settled = false;
    let watchdog = null;
    const feed = () => {
      clearTimeout(watchdog);
      watchdog = setTimeout(() => ws.terminate(), WATCHDOG_MS);
    };
    ws.on('message', (data) => {
      feed();
      const message = JSON.parse(data);
      if (message.t === 'hello' && !settled) {
        settled = true;
        client.instance = message.instance;
        client.res = { destroy: () => ws.terminate(), destroyed: false };
        resolve(true);
      }
    });
    ws.on('close', () => {
      clearTimeout(watchdog);
      if (client.res) client.res.destroyed = true;
      if (!settled) {
        settled = true;
        resolve(false);
      } else {
        client.onClose();
      }
    });
    ws.on('error', () => {});
  });
}

function open(client) {
  if (TRANSPORT === 'ws') return openWs(client);
  return new Promise((resolve) => {
    const req = http.get(new URL('/stream?sub=dep', BASE), { agent: false }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        resolve(false);
        return;
      }
      // The 10 s timeout only guards the connect; a live stream is watched
      // by the heartbeat watchdog below.
      req.setTimeout(0);
      client.instance = res.headers['x-instance-id'];
      client.res = res;
      let watchdog = setTimeout(() => res.destroy(), WATCHDOG_MS);
      res.on('data', () => {
        clearTimeout(watchdog);
        watchdog = setTimeout(() => res.destroy(), WATCHDOG_MS);
      });
      res.on('close', () => {
        clearTimeout(watchdog);
        client.onClose();
      });
      resolve(true);
    });
    req.on('error', () => resolve(false));
    req.setTimeout(10_000, () => req.destroy());
  });
}

// Kills/starts a container through the Docker Engine API socket when it is
// mounted (running inside the compose network), else with the docker CLI.
function docker(action, container) {
  if (!existsSync(DOCKER_SOCKET)) {
    execFileSync('docker', [action, container], { stdio: 'ignore' });
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: DOCKER_SOCKET, path: `/containers/${container}/${action}`, method: 'POST' }, (res) => {
      res.resume();
      res.on('end', () => (res.statusCode < 300 ? resolve() : reject(new Error(`docker ${action}: HTTP ${res.statusCode}`))));
    });
    req.on('error', reject);
    req.end();
  });
}

async function runExperiment() {
  const clients = Array.from({ length: CLIENTS }, (_, id) => ({ id, attempt: 0, instance: null, res: null, onClose: () => {} }));
  for (let i = 0; i < clients.length; i += 200) {
    await Promise.all(clients.slice(i, i + 200).map(open));
  }
  const byInstance = new Map();
  for (const c of clients) if (c.instance) byInstance.set(c.instance, (byInstance.get(c.instance) ?? 0) + 1);
  const [victim, victimClients] = [...byInstance].sort((a, b) => b[1] - a[1])[0];
  console.log(`Connected ${[...byInstance.values()].reduce((a, b) => a + b, 0)}/${CLIENTS} clients on ${byInstance.size} instances; killing ${victim} (${victimClients} clients), strategy=${STRATEGY}`);

  const start = Date.now();
  const buckets = new Map();
  const bump = (field) => {
    const bucket = Math.floor((Date.now() - start) / BUCKET_MS);
    const row = buckets.get(bucket) ?? { attempts: 0, reconnected: 0, failed: 0 };
    row[field] += 1;
    buckets.set(bucket, row);
  };
  let reconnected = 0;
  const landedOn = new Map();

  const affected = clients.filter((c) => c.instance === victim);
  for (const client of affected) {
    client.onClose = async () => {
      client.onClose = () => {};
      client.closedAt = Date.now() - start;
      while (Date.now() - start < OBSERVE_MS) {
        const delay = STRATEGY === 'immediate' ? 0
          : STRATEGY === 'fixed' ? FIXED_MS
            : backoffDelay(client.attempt, BASE_MS ? { base: BASE_MS } : undefined);
        client.attempt += 1;
        await sleep(delay);
        bump('attempts');
        if (await open(client)) {
          bump('reconnected');
          reconnected += 1;
          landedOn.set(client.instance, (landedOn.get(client.instance) ?? 0) + 1);
          return;
        }
        bump('failed');
      }
    };
  }

  // Connections that already dropped before the kill are reported apart: they
  // are not part of the reconnection the experiment measures.
  const droppedBeforeKill = affected.filter((c) => c.res?.destroyed || c.res?.closed);
  await docker('kill', victim);
  while (Date.now() - start < OBSERVE_MS && reconnected < affected.length) await sleep(250);
  const elapsed = Date.now() - start;
  for (const c of clients) c.res?.destroy();

  const last = Math.max(...buckets.keys(), 0);
  const rows = [];
  for (let b = 0; b <= last; b += 1) {
    const row = buckets.get(b) ?? { attempts: 0, reconnected: 0, failed: 0 };
    rows.push({ t_ms: b * BUCKET_MS, ...row });
  }
  const perSecond = new Map();
  for (const r of rows) {
    const s = Math.floor(r.t_ms / 1000);
    perSecond.set(s, (perSecond.get(s) ?? 0) + r.attempts);
  }
  const summary = {
    transport: TRANSPORT,
    strategy: STRATEGY,
    fixedDelayMs: STRATEGY === 'fixed' ? FIXED_MS : null,
    clients: CLIENTS,
    killedInstance: victim,
    affectedClients: affected.length,
    reconnected,
    reconnectedOnInstances: Object.fromEntries(landedOn),
    landedOnKilledInstance: landedOn.get(victim) ?? 0,
    secondsToReconnectAll: reconnected === affected.length ? Math.round(elapsed / 100) / 10 : null,
    peakAttemptsPer100ms: Math.max(...rows.map((r) => r.attempts), 0),
    peakAttemptsPerSecond: Math.max(...perSecond.values(), 0),
    failedAttempts: rows.reduce((sum, r) => sum + r.failed, 0),
    // Clients of the killed instance that never saw their connection end
    // within the observation window (neither a close nor the watchdog).
    droppedBeforeKill: droppedBeforeKill.length,
    neverDetected: affected.filter((c) => c.closedAt === undefined && !droppedBeforeKill.includes(c)).length,
    detectionMs: (() => {
      const times = affected.map((c) => c.closedAt).filter((t) => t !== undefined).sort((a, b) => a - b);
      return times.length ? { first: times[0], median: times[Math.floor(times.length / 2)], last: times.at(-1) } : null;
    })(),
    baseMs: STRATEGY === 'jitter' ? (BASE_MS ?? 4000) : null
  };
  mkdirSync(OUTPUT, { recursive: true });
  const name = TRANSPORT === 'ws' ? `avalanche-ws-${STRATEGY}` : `avalanche-${STRATEGY}`;
  writeFileSync(`${OUTPUT}/${name}.csv`, ['t_ms,attempts,reconnected,failed', ...rows.map((r) => `${r.t_ms},${r.attempts},${r.reconnected},${r.failed}`)].join('\n'));
  writeFileSync(`${OUTPUT}/${name}.json`, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Restarting ${victim}...`);
  await docker('start', victim);
}

// Plain SVG, no dependencies: reconnections per second for both strategies.
function drawChart() {
  const series = ['immediate', 'fixed', 'jitter'].filter((s) => existsSync(`${OUTPUT}/avalanche-${s}.csv`)).map((strategy) => {
    const rows = readFileSync(`${OUTPUT}/avalanche-${strategy}.csv`, 'utf8').trim().split('\n').slice(1).map((l) => l.split(',').map(Number));
    const perSecond = [];
    for (const [t, attempts] of rows) {
      const s = Math.floor(t / 1000);
      perSecond[s] = (perSecond[s] ?? 0) + attempts;
    }
    return { strategy, values: Array.from(perSecond, (v) => v ?? 0) };
  });
  if (series.length === 0) throw new Error('Run both strategies first.');
  const W = 760; const H = 360; const L = 60; const B = 40; const T = 40;
  const maxX = Math.max(...series.map((s) => s.values.length), 2) - 1;
  const maxY = Math.max(...series.flatMap((s) => s.values), 1);
  const x = (i) => L + (i / maxX) * (W - L - 20);
  const y = (v) => H - B - (v / maxY) * (H - B - T);
  const colors = { immediate: '#991b1b', fixed: '#f97316', jitter: '#2563eb' };
  const labels = { immediate: 'no jitter: immediate retry', fixed: 'no jitter: fixed 1 s retry', jitter: 'exponential backoff + full jitter (4 s base)' };
  const parts = [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" font-family="system-ui, sans-serif" font-size="12">`,
    `<rect width="${W}" height="${H}" fill="#fff"/>`,
    `<text x="${L}" y="22" font-size="14" font-weight="600">Reconnection attempts per second after killing one instance</text>`
  ];
  for (let k = 0; k <= 4; k += 1) {
    const v = Math.round((maxY * k) / 4);
    parts.push(`<line x1="${L}" x2="${W - 20}" y1="${y(v)}" y2="${y(v)}" stroke="#e5e7eb"/><text x="${L - 8}" y="${y(v) + 4}" text-anchor="end" fill="#6b7280">${v}</text>`);
  }
  for (let s = 0; s <= maxX; s += Math.max(1, Math.round(maxX / 10))) {
    parts.push(`<text x="${x(s)}" y="${H - B + 16}" text-anchor="middle" fill="#6b7280">${s}s</text>`);
  }
  series.forEach(({ strategy, values }, i) => {
    const points = values.map((v, s) => `${x(s).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
    parts.push(`<polyline fill="none" stroke="${colors[strategy]}" stroke-width="2" points="${points}"/>`);
    parts.push(`<rect x="${L + 10}" y="${T + 6 + i * 18}" width="12" height="3" fill="${colors[strategy]}"/><text x="${L + 28}" y="${T + 11 + i * 18}">${labels[strategy]} (peak ${Math.max(...values)}/s)</text>`);
  });
  parts.push(`<text x="${(W + L) / 2}" y="${H - 6}" text-anchor="middle" fill="#6b7280">seconds since the instance was killed</text></svg>`);
  writeFileSync(`${OUTPUT}/avalanche.svg`, parts.join('\n'));
  console.log(`Chart: ${OUTPUT}/avalanche.svg`);
}

if (args.chart) drawChart();
else await runExperiment();
