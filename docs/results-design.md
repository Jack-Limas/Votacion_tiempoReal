# Results module design (RF-1 to RF-6, RF-9, RF-10 for the public)

Intake of actas, hierarchical totals, the backplane to the 8 instances, the
public fan-out and its failure modes. Code: `src/lua/applyActa.lua`,
`src/services/`, `src/results/`. Measurements: `results/README.md`.

## 1. Data flow

```
 jurado ─POST /api/actas─► instance k ─EVAL applyActa.lua─► Valkey  (one atomic script)
                                          │ idem record (RF-1)
                                          │ acc:mesa / acc:pto / acc:mun / acc:dep  + rank:* ZSETs (RF-2, RF-3)
                                          │ XADD st:events                          (backplane + history, RF-6)
                                          └ XADD verification (doubtful actas → RF-7)

 Valkey st:events ─XREAD BLOCK (dedicated connection, no group)─► each of the 8 instances
                                                                    │ topic registry Map<topic, Set<conn>>
                                                                    │ window aggregation + batched sends
                                                                    ▼
                                    WebSocket /ws?sub=…  (default) · SSE /stream?sub=…  · GET /api/results/:topic
```

Instances keep only their local connections. They never talk to each other
(RT-3): the stream is the only path between them.

## 2. Atomic registration (RF-1, RF-2, RF-3)

`applyActa.lua` runs as one Redis script, so no other command interleaves:

1. **Idempotency.** `idem:<key>` stores the digest of the request and the
   answer. A resend with the same content gets the stored answer with
   `duplicate: true` (`200`), never a second sum; the same key with other
   content gets `409`. If the server dies after the script but before
   answering, the jurado's outbox resends the same `idem` and receives the
   stored answer.
2. **Convergence.** Per mesa the applied acta is the maximum by
   `(version, content digest)` where the content digest covers `votos` and
   `sufragantes`. That order is total and independent of arrival order, so the
   totals depend only on the **set** of actas received (tested with all 24
   arrival orders of v1, v2, a different v2 and v3). A late v2 after v3 is
   answered `superseded` and changes nothing. A resend whose `idem` key already
   expired is also `superseded` (its version/content is not greater), so the
   TTL of the idem key is not a safety limit.
3. **Hierarchical totals.** A newer acta computes per-candidate differences
   against the previous figures of the mesa and applies them with `HINCRBY` to
   puesto, municipality and department, plus `ZINCRBY` on `rank:{topic}`
   (ranking with `ZREVRANGE`, O(log C + k), without reading every candidate).
   `mesas_reported` and `percent_reported` change only for the first acta of a
   mesa. A client never sees a municipality total that includes an acta its
   puesto does not show yet, because all levels change inside the same script
   and every read (snapshot script, stream event) happens before or after it.
   Several commands sent from Node would interleave with other instances'
   commands and expose half-applied states; `MULTI` cannot branch on values read
   inside the transaction.
4. **Location.** A mesa's puesto and municipio are fixed by its first applied
   acta; a later acta with another location is answered `409
   location_conflict` and changes nothing (otherwise its differences would be
   applied to the new levels while the old figures stayed in the old ones).
5. **Event.** `XADD st:events MAXLEN ~ N` with the **new absolute values** of
   every changed level, in the same script: state and event are atomic.

The old `aggregate` hash of the delivered version was removed: it added every
accepted acta, including superseded versions and corrections.

## 3. Verified doubtful actas

A doubtful acta (sum ≠ `sufragantes`) is not summed; it is queued for RF-7.
The auditor's decision is written by the verification `RESOLVE` script to the
`resolutions` stream with its fencing token. A consumer group in every
instance applies it through the same `applyActa.lua`:

* `approve`: added once with the usual version rules; the event carries
  `origin: 'approve'` and the public receives `reason: {type: 'verification'}`.
* `reject`: not counted; the mesa stays unreported until the jurado sends a
  new version.
* `recount`: as reject, flagged for a physical recount.

Idempotent per `(case, token)`; a token lower than the last one seen for the
case is refused (`stale_token`). The two writes are in different hash slots
(`{verification}`, `{results}`), so they cannot be one Lua script on Redis
Cluster; the stream plus the idempotent apply gives the same exactly-once
effect. The jurado reads the outcome at `GET /api/actas/:idem`.

## 4. Backplane: one global stream

| | One global stream | One stream per municipality (40) | One per puesto (1,842) |
| --- | --- | --- | --- |
| Events written in the day | ≈ 6,600 | ≈ 6,600 × 2 (municipality + department) | ≈ 6,600 × 3 |
| Streams each instance reads | 1 | 40 + dep | 1,842 + 40 + dep |
| Redis → instances deliveries | 6,600 × 8 = 52,800 | same or more | same or more |
| Reconnection cursor | one monotonic ID for every topic | one per stream | one per stream |

Without sticky sessions each instance gets a random sample of the whole
public. With ~50,000 clients per instance spread over 40 municipalities, every
instance has subscribers of every municipality, so per-municipality streams
would be read by everyone anyway; and `dep` needs every event. Splitting saves
no traffic, multiplies memory (each acta in 2–3 streams) and needs one cursor
per stream for RF-6. The write flow is small; what is expensive is the fan-out
to the public, which is already proportional to local subscribers.

## 5. Topic registry (RF-5)

Topics: `dep`, `mun:{id}`, `pto:{id}`; up to `MAX_TOPICS` per connection.

| Operation | Cost |
| --- | --- |
| Subscribe | O(1) (`Map.get` + `Set.add`) |
| Unsubscribe one topic | O(1); the empty set is deleted |
| Remove a connection | O(t), t = its topics |
| Deliver an event | O(k), k = subscribers of the topic; a topic without local subscribers costs one `Map.get` |

Delivering never walks all the connections of the instance.

## 6. Fan-out without blocking the Event Loop (RF-4)

`FANOUT_MODE=batched` (default):

1. Events are folded into a map of dirty topics (last absolute value per
   field, plus the reason of a correction or verification).
2. Every `FLUSH_MS` (1 s) one message per dirty topic is built and serialized
   **once** (`{json, sse}`); every subscriber gets the same string.
3. Sends go in batches of `BATCH_SIZE` (100) with `await setImmediate`
   between batches. `process.nextTick` and resolved promises are microtasks
   and would yield nothing; `setImmediate` lets pending I/O (sockets, the
   jurados' requests) run between batches.
4. Flushes never overlap; a connection drops deltas with `upTo <= cursor`.

`FANOUT_MODE=naive` sends each event to each subscriber synchronously; it
exists only for the comparison. Delivered-version run with 10,000 SSE clients
(`results/archive/`): naive p99 event-loop delay 483–685 ms per instance and 22
failed jurado submissions; batched 1.2–6.8 ms and 0 failures; 19.3 M vs 0.59 M
messages.

**Against the 2,560 million naive messages** (`npm run estimate`, 400,000
clients connected the whole day, subscribers spread by mesas): with 50 % dep /
35 % municipality / 15 % puesto and a 1 s window the day needs ≈ 879 M messages
(34 % of naive); with a 2 s window ≈ 652 M (25 %); with 10 % dep / 60 % mun /
30 % pto and 2 s ≈ 287 M (11 %). Most of the reduction comes from topics; the
window helps little for `dep` and the capital, which change every second at the
peak. In the measured runs the reduction was 42x (SSE 20k), 22x (WS 20k, slower
simulation) and 56x (WS 10k) against "every event to every connection".

## 7. Continuity on reconnection (RF-6)

The client keeps, per topic, the stream ID (`upTo`) of the last message it
applied. The ID is assigned by Redis in `XADD`, is monotonic and identical on
every instance, so the client can reconnect anywhere:
`/ws?sub=mun:11@<upTo>,dep@<upTo>` (or `/stream?sub=…`).

The connection joins the registry before reading Redis with its cursor set to
`PENDING`; live deltas arriving meanwhile are kept and filtered by `upTo`. Then:

* no cursor → **snapshot** (atomic script: hashes + last stream ID);
* cursor older than the first retained ID, newer than the last one (stream
  reset) or more than `MAX_REPLAY` (500) pending events → **snapshot**;
* otherwise **replay**: `XRANGE (cursor +` filtered by topic and reduced to one
  delta (last value per field, keeping the reason).

Replay transfers every pending event (~0.7 KB each) even if the client gets one
delta; a snapshot is one `HGETALL` of ~10 fields. With `MAX_REPLAY = 500`,
replay covers more than 1.5 minutes of the worst burst (300 actas/min), so a 30 s
disconnection is always a replay.

**Retention and memory.** `XADD … MAXLEN ~ 10000` inside the apply script
(`~` trims whole nodes, O(1) amortised). Measured with `MEMORY USAGE`: 3,305
events = 2,396,528 bytes ≈ 725 B/event, so the whole day (~6,600 events)
≈ 4.8 MB and the cap ≈ 7.3 MB: the whole day is retained. Classic Pub/Sub is not
used for RF-6: it has no memory.

Tested (`test/integration/rf6-reconnection.test.js`, through the 8 replicas): B
disconnects 30 s while 30 actas and a lowering correction arrive, reconnects on
another instance and ends with the same state hash as A, which never
disconnected; also with `MAXLEN=20` (snapshot path).

## 8. Transports (RT-5) — chosen by measurement

| Audience | Transport | Why (measured, `results/README.md`) |
| --- | --- | --- |
| Public | **WebSocket**, SSE fallback | Same frames over both. In two runs of 3,000 clients WebSocket had lower latency and equal or lower CPU, and half the server heap per connection (3.4 KB vs 6.2 KB after forced GC). The page falls back to SSE if the upgrade fails twice. Polling cost ~3x CPU per instance with ~10x latency and skipped half the states. |
| Jurados | HTTP POST + `idem` + IndexedDB outbox | Every attempt is independent and survives a flaky mobile network; no connection to keep alive. |
| Auditors | WebSocket (`/ws/verification`) | Bidirectional commands with live locks (see `docs/verification.md`). |
| Operators | Polling `/metrics/all` (1 s) | A few readers of an aggregate that changes once per second. |

## 9. Slow clients and back-pressure (RF-9)

Each delivery checks the connection's pending bytes (`res.writableLength` for
SSE, `ws.bufferedAmount` for WebSocket):

* below `SLOW_SOFT_BYTES` (256 KB): normal;
* above it: the connection is **degraded**: no more detail, its cursors stop;
* once it drains below a quarter of the threshold: **one snapshot** of its
  topics replaces everything it missed;
* above `SLOW_HARD_BYTES` (1 MB), or still degraded after
  `SLOW_MAX_DEGRADED_MS` (30 s): **disconnected** with a reconnect signal
  (`{t:'reconnect'}` on SSE, close code 4008 on WebSocket); it comes back with
  its cursors. The time limit matters: a degraded client gets no more data, so
  a client that never reads would otherwise pin its backlog forever.

Demonstrated with a TCP client that stops reading
(`test/integration/rf9-slow-client.test.js`): only that client is degraded, the
fast one keeps receiving, and after resuming the slow one gets one snapshot
equal to the fast client's state; a client that never drains is disconnected
with the signal. On Windows loopback the kernel absorbed ~64 MB before Node's
buffer grew, so the thresholds act late on a local socket; over a real mobile
network the kernel buffers are far smaller.

## 10. Degradation and dead instances (RF-10)

* **Redis down.** The command client has `disableOfflineQueue`, so calls fail
  at once: jurados get `503 retryable` in tens of milliseconds and their outbox
  keeps the acta; the stream reader reports `down` and every public client gets
  `{t:'status', redis:'down'}` (the page says it shows the last confirmed
  numbers); polling answers `503`; auditor consoles show the degraded banner.
  When Redis returns, the reader resumes from its last ID (nothing lost),
  clients get `status: up`, and the retried acta is applied once. Tested with a
  proxy cut in `test/integration/rf10-degraded.test.js`.
* **Instance death.** A killed container sends no FIN/RST. The server sends a
  heartbeat frame every 10 s, clients reconnect after 25 s of silence, and
  nginx closes `/stream` after 30 s (`/ws` after 60 s) without upstream bytes.
* **Reconnection avalanche.** Clients use exponential backoff with full jitter
  and a 4 s first window (`public/assets/backoff.js`). Measured with 8,000
  clients and one killed instance holding 1,000 of them: peak attempts 512/s
  (immediate retry), 576/s (fixed 1 s), 284/s (jitter); all reconnected to the
  remaining 7 instances; with WebSocket clients all 1,000 reconnected in 4.2 s
  (peak 270/s). A 500 ms first window concentrated the herd (945/s),
  so the first window must scale with the herd size. Chart:
  `results/avalanche.svg`.
* **Connection bursts.** The 20,000-client run exposed Node's default accept
  backlog (511) and nginx's `max_fails=1`, which together emptied the upstream
  pool ("no live upstreams"), and a keep-alive race that returned 502 to some
  POSTs. Fixed with a 4096 backlog, `max_fails=3 fail_timeout=5s`, a 5 s
  connect timeout and a 65 s Node keep-alive.

## 11. Metrics (RT-4, RT-7)

`GET /metrics` (instance) and `GET /metrics/all` (every instance, read from
Redis: each publishes `metrics:{instance}` every second with a 5 s TTL):
connections per transport, messages per second and total vs naive, event-loop
delay (p50/p99/max of the last second and since start, `monitorEventLoopDelay`
with 10 ms resolution subtracted), CPU %, heap and heap per connection, events
applied in the last minute (actas/min), publish delay receivedAt → sent
(p50/p99), reconnections by path (replay/snapshot/initial/recovery),
slow-client counters and Redis status. `/ops/` draws them.
