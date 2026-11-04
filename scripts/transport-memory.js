// Server memory per open connection for SSE and WebSocket, measured in one
// process with forced garbage collection (the cluster heap figures are too
// noisy for this). Uses the real transport adapters and topic registry, no
// Redis. Polling holds no connection between requests, so it has no
// per-client memory on the server; its cost is CPU per request (see the
// transport load runs in results/).
//
// Usage: node --expose-gc scripts/transport-memory.js [--clients=5000]
import { createServer } from 'node:http';
import { mkdirSync, writeFileSync } from 'node:fs';
import WebSocket, { WebSocketServer } from 'ws';
import { openSseConnection } from '../src/results/sse.js';
import { openWebSocketConnection } from '../src/results/websocket.js';
import { createTopicRegistry } from '../src/results/topics.js';
import { serialize } from '../src/results/fanout.js';

if (typeof globalThis.gc !== 'function') {
  console.error('Run with: node --expose-gc scripts/transport-memory.js');
  process.exit(1);
}
const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => a.slice(2).split('=')));
const CLIENTS = Number(args.clients ?? 5000);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function settle() {
  for (let i = 0; i < 4; i += 1) {
    globalThis.gc();
    await sleep(200);
  }
  const { heapUsed, rss, external, arrayBuffers } = process.memoryUsage();
  return { heapUsed, rss, external, arrayBuffers };
}

async function measure(transport) {
  const registry = createTopicRegistry();
  const server = createServer((req, res) => {
    const conn = openSseConnection(req, res, 'mem');
    registry.add(conn);
    registry.subscribe(conn, 'dep', '1-0');
    conn.send(serialize({ t: 'snapshot', topic: 'dep', upTo: '1-0', state: { 'v:C1': 1 } }));
    req.on('close', () => registry.remove(conn));
  });
  const wss = new WebSocketServer({ server, perMessageDeflate: false });
  wss.on('connection', (ws) => {
    const conn = openWebSocketConnection(ws, 'mem');
    registry.add(conn);
    registry.subscribe(conn, 'dep', '1-0');
    conn.send(serialize({ t: 'snapshot', topic: 'dep', upTo: '1-0', state: { 'v:C1': 1 } }));
    ws.on('close', () => registry.remove(conn));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = server.address().port;

  // Clients live in a child-free loop of this same process, so their memory
  // is measured separately and subtracted: a baseline with clients connected
  // to a plain TCP echo would be needed otherwise. Instead, clients run in a
  // forked process (see below) and this process only holds the server side.
  const before = await settle();
  const { fork } = await import('node:child_process');
  const child = fork(new URL(import.meta.url), ['--role=clients', `--transport=${transport}`, `--port=${port}`, `--clients=${CLIENTS}`]);
  await new Promise((resolve) => child.once('message', resolve));
  await sleep(1000);
  const after = await settle();
  const open = registry.connections.size;
  child.kill();
  await sleep(500);
  server.closeAllConnections?.();
  wss.close();
  await new Promise((resolve) => server.close(resolve));
  const perClient = (field) => Math.round(((after[field] - before[field]) / open / 1024) * 100) / 100;
  return {
    transport,
    open,
    heapKBPerConnection: perClient('heapUsed'),
    externalKBPerConnection: perClient('external'),
    rssKBPerConnection: perClient('rss')
  };
}

async function clients() {
  const transport = args.transport;
  const port = Number(args.port);
  const keep = [];
  for (let i = 0; i < CLIENTS; i += 100) {
    await Promise.all(Array.from({ length: Math.min(100, CLIENTS - i) }, () => new Promise((resolve) => {
      if (transport === 'ws') {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/`, { perMessageDeflate: false });
        ws.on('message', () => {});
        ws.on('open', resolve);
        ws.on('error', resolve);
        keep.push(ws);
      } else {
        import('node:http').then(({ default: http }) => {
          const req = http.get(`http://127.0.0.1:${port}/`, { agent: false }, (res) => {
            res.on('data', () => {});
            resolve();
          });
          req.on('error', resolve);
          keep.push(req);
        });
      }
    })));
  }
  process.send('ready');
  setInterval(() => {}, 1 << 30);
}

if (args.role === 'clients') {
  await clients();
} else {
  const results = [];
  const order = (args.order ?? 'sse,ws').split(',');
  for (const transport of order) results.push(await measure(transport));
  const output = {
    node: process.version,
    clientsPerTransport: CLIENTS,
    note: 'Server process only, after forced GC; clients run in a separate process. Kernel socket buffers are not included in RSS.',
    results
  };
  console.log(JSON.stringify(output, null, 2));
  mkdirSync('results', { recursive: true });
  writeFileSync(`results/transport-memory-${order.join('-')}.json`, JSON.stringify(output, null, 2));
}
