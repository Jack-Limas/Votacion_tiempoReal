# Election-night results transmission (preconteo)

Real-time transmission and broadcast of a department's preliminary count:
6,400 mesas transmit their actas within two hours, up to 400,000 browsers
follow the totals live, and 120 auditors review doubtful actas. Eight
stateless Node.js instances behind a load balancer share nothing but Redis
(Valkey).

| Audience | Page | Transport | Guarantee |
| --- | --- | --- | --- |
| Jurados (6,400) | `/jury/` | HTTP POST + `idem` + offline outbox (IndexedDB) | An acta counts once; every attempt gets an unambiguous answer |
| Public (≤ 400,000) | `/` | WebSocket (SSE fallback), per-topic subscriptions | Numbers never move without a reason; a reconnecting client ends identical to one that never left |
| Auditors (120) + coordinator | `/verification/` | WebSocket `/ws/verification` | Exclusive fenced leases, live locks, presence that expires by itself |
| Operators | `/ops/` | Polling `/metrics/all` | Connections, msg/s, event-loop delay, CPU, memory per connection, actas/min, publish delay |

## Architecture

```
                          ┌───────────── one Ubuntu VM (production) ─────────────┐
 browsers / jury apps ──► │ Caddy :443 (HTTPS, sslip.io) ──► nginx (round robin) │
                          │                                    │  no sticky sessions
                          │            ┌──────┬──────┬─────────┴──┬──────┐      │
                          │          app-1  app-2   …          app-7  app-8     │   stateless Node.js 22
                          │            └──────┴──────┴─────┬──────┴──────┘      │   (Express + ws)
                          │                                 ▼                    │
                          │   Valkey (AOF): idem keys · totals hashes · rank ZSETs│
                          │   st:events stream · verification streams/ZSETs/leases│
                          │   presence · rate-limit counters · metrics · Pub/Sub  │
                          └───────────────────────────────────────────────────────┘

 POST /api/actas ─► applyActa.lua (atomic: idempotency + 4 levels + ranking + XADD st:events)
 every instance ─► XREAD st:events ─► topic registry ─► 1 s window ─► batched sends (setImmediate)
 doubtful acta ─► verification stream ─► consumer group ─► priority ZSET ─► auditor lease (Lua, fencing)
 auditor decision ─► resolutions stream ─► consumer group ─► applyActa.lua (once per case+token)
```

Instances never call each other (RT-3): the stream, consumer groups, Lua
scripts and Pub/Sub in Valkey are the only coordination. Any instance can
serve any client (RT-2): cursors (stream IDs) and leases live in Valkey.

## Stack

Node.js 22 (ES modules), Express 4, `ws` 8, `redis` (node-redis 4) against
Valkey 8 (Redis-compatible), nginx 1.27, Caddy 2, Docker Compose. Tests:
`node:test`; lint: ESLint 9; browser checks and INP: Chrome via
`puppeteer-core`; CI: GitHub Actions with a Valkey service.

## Repository layout

```
src/
  server.js                 composition root: builds modules, HTTP + upgrade routing, graceful shutdown
  app.js                    Express app: security headers, JSON, /health, acta routes, static pages, errors
  config/env.js             base configuration from the environment (validated)
  config/redis.js           command (fail-fast), subscriber and publisher clients
  lua/applyActa.lua         atomic registration: idempotency, convergence, totals, ranking, event, decisions
  lua/snapshot.lua          atomic snapshot (state + last stream ID)
  controllers/, routes/     acta intake (POST, GET by idem) and results routes
  services/                 idempotency service, decision applier, rate limiter
  validation/acta.js        acta contract validation (acta, mesa, puesto, municipio, votos, sufragantes…)
  results/                  public module: stream reader, topics, fan-out, resume, back-pressure, SSE/WS, metrics
  verification/             auditors: Lua scripts, store, ingest, gateway, auth, presence, rate limits
  sockets/index.js          one upgrade router for /ws and /ws/verification
public/                     /, /jury/, /ops/, /verification/ and shared assets (backoff, INP, CSS)
scripts/                    simulator, load generator, avalanche, INP, transport memory, smoke, browser check
test/                       unit tests and integration tests against Valkey and the 8-replica stack
results/                    measurements produced by the scripts (see results/README.md)
docs/                       results design, verification design, deployment, requirements status
nginx/, deploy/             nginx.conf (dev and prod) and the Caddyfile
```

## Redis roles

| Role | Structure | Where |
| --- | --- | --- |
| Broadcast backplane | One global stream `st:events` read by every instance (`XREAD BLOCK`); Pub/Sub only for the 120 consoles, always backed by versioned snapshots | `src/results/stream.js`, `src/verification/gateway.js` |
| History for recovery | Same stream, monotonic IDs as client cursors, `MAXLEN ~ 10000` (≈ 725 B/event, whole day ≈ 4.8 MB) | `src/results/resume.js` |
| Totals | Hash per level (`acc:mesa/pto/mun/dep`) + candidate ranking ZSET per level | `src/lua/applyActa.lua` |
| Atomic composite operation | One Lua script applies, reverts and updates four levels and the event | `src/lua/applyActa.lua` |
| Idempotency | `idem:<key>` with TTL + version/content order, so even an expired key cannot double count | `src/lua/applyActa.lua` |
| Distributed lock | Lease per case with owner, TTL, fencing token (`INCR`), renewal, owner-checked release, sweeper | `src/verification/scripts.js` |
| Presence | Hash per connection with TTL + index ZSET, heartbeat every 5 s | `src/verification/scripts.js` |
| Rate limit | Fixed-window counter per credential: jury actas, console HTTP and messages, public connections per IP | `src/verification/scripts.js`, `src/services/rateLimiter.js` |

Design notes: [docs/results-design.md](docs/results-design.md) (intake,
convergence, backplane, fan-out, continuity, transports, back-pressure,
degradation) and [docs/verification.md](docs/verification.md) (queue, leases,
presence, auth, failures).

## Run locally

Requirements: Docker with Compose, Node.js 20+.

```bash
npm ci
docker compose up -d --build        # Valkey + 8 replicas + nginx on http://localhost:8080
                                    # (HTTP_PORT=8088 docker compose up -d if 8080 is busy)
```

| URL | What |
| --- | --- |
| http://localhost:8080/ | Public results (WebSocket, SSE fallback) |
| http://localhost:8080/jury/ | Jury transmission page (offline outbox) |
| http://localhost:8080/verification/ | Auditor / coordinator consoles (dev codes `dev-auditor`, `dev-coordinator`) |
| http://localhost:8080/ops/ | Operations dashboard |
| `/health`, `/whoami`, `/metrics`, `/metrics/all` | Health, instance id, metrics of one / all instances |
| `/api/actas` (POST), `/api/actas/:idem` (GET) | Acta intake and submission status |
| `/api/results/:topic` | Polling snapshot + ranking (`dep`, `mun:{id}`, `pto:{id}`) |
| `/ws?sub=…`, `/stream?sub=…` | Public WebSocket and SSE (`sub=mun:11@<cursor>,dep`) |

`npm run simulate -- --url=http://localhost:8080 --speed=60` replays the whole
election day (6,400 actas with resends, corrections and doubtful actas) in two
minutes. `node scripts/reset-data.js` empties Valkey and seeds the catalog.

## Tests

```bash
npm run lint
npm test          # unit + integration; Redis-backed tests use REDIS_URL (default localhost:6379),
                  # RF-5/RF-6 use the compose stack at BASE_URL (default http://localhost:8080)
BASE_URL=http://localhost:8080 npm test
node scripts/browser-check.js --url=http://localhost:8080     # every page in Chrome, no console errors
node scripts/smoke.js --url=http://localhost:8080             # routes + full acta flow end to end
```

Tests that need an unavailable dependency are skipped with a message, never
silently passed. Last full run on the development machine: **79/79 passed**
with Valkey and the 8-replica stack. Integration coverage includes RF-2 (all 24
arrival orders), verification decisions, RF-5, RF-6 (A/B on different
instances), RF-9 (deliberately slow TCP client), RF-10 (Redis cut for jurados,
public and auditors at once), concurrent claims, lease expiry, renewal, late
release, two instances, ghost presence and rate limits.

## Load and measurements

```bash
# inside the compose network (avoids Docker Desktop port-forwarding limits)
docker compose run --rm --no-deps load --connections=20000 --processes=8 --transport=ws --speed=30 --from=0 --to=40
docker compose run --rm --no-deps -v /var/run/docker.sock:/var/run/docker.sock \
  --entrypoint node load scripts/avalanche.js --url=http://nginx --clients=8000 --strategy=jitter
node --expose-gc scripts/transport-memory.js --clients=5000
node scripts/measure-inp.js --url=http://localhost:8080 --label=under-load --wait-for-load=1
npm run estimate -- --flush=1000 --mix=dep:0.5,mun:0.35,pto:0.15
```

`scripts/load.js` runs several worker processes; on extra machines use
`--simulate=0 --hold=600` to add connections while one machine drives the
simulator. Results with the machine description: [results/README.md](results/README.md).

Measured on the development machine (one 12-vCPU Docker VM shared by the
system **and** the load generator):

* 20,000 simultaneous clients held through nginx, SSE and WebSocket: 0 failed
  opens, 0 dropped, every message delivered; 22x–42x fewer messages than
  "every acta to every client". Delays were high in those runs because the
  generator competed for the same CPUs (event-loop p99 median 46–81 ms).
* With 10,000 WebSocket clients: event-loop p99 median 21.7 ms.
* Transports, 3,000 clients each: polling ~3x CPU and ~10x latency; WebSocket
  half the heap per connection of SSE (3.4 vs 6.2 KB) with equal or lower CPU.
* INP on a 6x-throttled low-end profile: 72 ms idle, 160 ms during the peak.
* Killing an instance with 1,000 clients: peak reconnections 512/s (immediate),
  576/s (fixed 1 s), 284/s (full jitter, 4 s base); all reconnected elsewhere.

## Deployment

One Ubuntu VM on Azure or AWS with `docker-compose.prod.yml`: Caddy with
automatic HTTPS on `<PUBLIC_IP>.sslip.io`, nginx, 8 replicas and Valkey with
AOF and a password. Only ports 80 and 443 are public; every secret comes from
`.env` (template `.env.production.example`). Step-by-step commands to create
the VM, open ports, install Docker, clone `main`, configure, start, update and
shut down without wasting credit: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

**Why a VM.** The app runs permanent background readers (stream readers,
consumer groups, sweepers, presence) and holds long-lived WebSockets all night;
serverless platforms freeze or cut both. Eight replicas on one VM cost nothing
per instance. **A single VM is a single point of failure**: the replicas
survive a process crash, not a machine crash, and Valkey lives on the same
machine. For the real election run the replicas on several VMs behind a cloud
load balancer with a replicated managed Redis; the code does not change.

## Configuration

Every variable, with defaults, is in [.env.example](.env.example) (local) and
[.env.production.example](.env.production.example) (production). The most
relevant: `REDIS_URL`, `TRUST_PROXY`, `FLUSH_MS`, `BATCH_SIZE`, `MAXLEN`,
`MAX_REPLAY`, `HEARTBEAT_MS`, `SLOW_SOFT_BYTES`, `SLOW_HARD_BYTES`,
`SLOW_MAX_DEGRADED_MS`, `RATE_LIMIT_PUBLIC_CONNECTS`, `RATE_LIMIT_ACTAS`,
`VERIFICATION_TOKEN_SECRET`, `VERIFICATION_AUDITOR_CODE`,
`VERIFICATION_COORDINATOR_CODE`.

## Known limits

* The 400,000-client target was not measured: the largest run was 20,000
  clients on one laptop, where the generator and the system shared the CPU.
  Per-connection heap (≈ 3.4 KB WebSocket) suggests 50,000 clients per instance
  fit in memory, but CPU and latency at that scale are not demonstrated.
* The cloud deployment steps were written and the production compose was run
  locally over HTTPS, but no public URL was deployed from this branch (RT-10).
* Single VM = single point of failure (see above). Valkey runs without
  replication.
* `/ops/` and `/metrics/all` are public; protect them in a real election.
* The catalog of puestos and mesas is synthetic (deterministic seed).
* Auditor sign-in uses shared access codes per role exchanged for signed
  tokens; a real deployment would use the operator's identity provider.

## Requirement status

Per-requirement status, evidence and gaps: [docs/REQUIREMENTS_STATUS.md](docs/REQUIREMENTS_STATUS.md).
