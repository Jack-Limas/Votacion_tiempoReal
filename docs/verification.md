# Verification desk (RF-7, RF-8) — design notes

This module serves the 120 auditors that review doubtful actas and the
coordinator who watches them. Code: `src/verification/`, pages:
`public/verification/`, tests: `test/verification-*.test.js`.

## 1. Transport: WebSocket on `/ws/verification`

| Need | Why WebSocket |
| --- | --- |
| Auditors send commands (take, release, resolve, activity) and must see other auditors' locks instantly | Bidirectional, one persistent channel; SSE would need a second HTTP path for commands and polling would add seconds of lag to lock visibility |
| 120 long-lived consoles, not 400k | Connection cost is irrelevant at this size; latency and ordering matter more |
| Liveness of an auditor must be detected | Protocol-level ping/pong gives a heartbeat for free |

`ws` cannot run two `WebSocketServer`s with `{ server, path }` on one HTTP
server: each one answers every `upgrade` and aborts the ones whose path does
not match, breaking the other endpoint. Both `/ws` (existing) and
`/ws/verification` now use `noServer: true` and a single upgrade router in
`src/sockets/index.js` dispatches by pathname (unknown paths get `404`).

## 2. Authentication and roles

* `POST /api/verification/login {id, name, role, accessCode}` returns a token
  signed with HMAC-SHA256 (`node:crypto`), `base64url(payload).base64url(sig)`,
  payload `{sub, name, role, exp}`. Roles: `auditor`, `coordinator`.
* Access codes and the secret come from the environment
  (`VERIFICATION_TOKEN_SECRET`, `VERIFICATION_AUDITOR_CODE`,
  `VERIFICATION_COORDINATOR_CODE`). Comparisons use `timingSafeEqual`.
  All instances must share the same secret; without it, production refuses
  to start and development generates a per-process secret (single instance only).
* The browser sends the token as a WebSocket subprotocol
  (`['verification.v1', 'auth.<token>']`), not in the URL, so it never lands in
  access logs. Non-browser clients may use `Authorization: Bearer`.
* The upgrade is rejected with HTTP `401` before a socket is created.
* Every client message is validated against a strict schema
  (`messages.js`): known `type`, allowed role, no extra fields, patterns for ids
  and tokens, max 4 KB. The identity always comes from the signed token, never
  from the message.

## 3. Redis keys

Prefix `P = VERIFICATION_KEY_PREFIX` (default `electoral:{verification}`). The
`{verification}` hash tag keeps all keys in one cluster slot, which is what
allows the Lua scripts to build per-case key names.

| Key | Type | Content | TTL / retention |
| --- | --- | --- | --- |
| `electoral:{results}:verification` | Stream | Doubtful actas written by the jurados Lua script (unchanged) | Owned by the jurados module |
| consumer group `verification-ingest` | Stream group | One consumer per instance (`INSTANCE_ID`) | — |
| `P:queue` | ZSET | case id → priority score (lower first) | Until claimed |
| `P:case:<id>` | Hash | acta JSON, status (`queued`/`claimed`/`resolved`), priority, owner, token, claimedAt, leaseExpiresAt, decision | Resolved cases expire after 48 h |
| `P:leases` | ZSET | case id → lease expiry (ms, Redis clock) | Entry removed on release/resolve/expiry |
| `P:owners` | Hash | auditor → case id (one case per auditor) | Cleared with the lease |
| `P:fence` | String | Fencing token counter (`INCR`) | Permanent, monotonic |
| `P:version` | String | State version, `INCR` on every queue/lease change | Permanent, monotonic |
| `P:events` | Pub/Sub channel | `{type, version, case}` for every change | No memory (see §6) |
| `P:stats` / `P:auditor-stats` | Hash | Counters: ingested, resolved, expired, per-auditor resolved and work time | Permanent |
| `P:resolutions` | Stream | Decisions with the fencing token and the original `idem`; consumed by the results module (group `results-apply`) | `MAXLEN ~ 10000` |
| `P:conn:<connId>` | Hash | Presence: auditorId, name, role, instance, connectedAt, lastSeen, lastActivity | `PEXPIRE` presence TTL (30 s) |
| `P:presence` | ZSET | connId → presence expiry, index used to list without `SCAN`/`KEYS` | Pruned on read and by the sweeper |
| `P:rl:<scope>:<credential>:<window>` | String | Rate-limit counter | `PEXPIRE` = window |

Memory: a case hash is ~1 KB (the acta JSON dominates). With 6–9 % of 6,400
actas (≤ 600 cases) plus corrections, the queue costs well under 1 MB.
Presence is ~200 B per connection (≤ a few hundred connections). Rate-limit
counters live one window.

## 4. RF-7: queue, exclusive claim, lease, fencing

### Intake
Every instance runs `XREADGROUP` on the doubtful-actas stream with the same
consumer group, so each entry is delivered to exactly one instance. The entry
is `XACK`ed only after the `INGEST` script commits. If the instance dies in
between, the entry stays pending and another instance takes it over with
`XAUTOCLAIM` (idle > 30 s). `INGEST` is idempotent per case id
(`<acta>#v<version>`), so a re-delivery or a re-sent doubtful acta with a new
`idem` never creates a second case. Keyspace notifications are not used.

### Priority
`score = enqueuedAt(ms) − bonus`, lowest first, where
`bonus = min(sufragantes, 1000) × 1 s (+ 2 min for corrections, version > 1)`.
Rationale: a doubtful acta is excluded from the preconteo, so the cases that
withhold the most voters hurt the published picture most and go first; the
time component keeps FIFO between similar cases and bounds the jump to ~16
minutes, so a small case can never starve. Released or expired cases keep their
original score and return to their old position, ahead of newer ones.

### Claim (atomic, `CLAIM` script)
1. If the auditor already holds a live lease → `already_holding`.
2. Pick the requested id or the first of `P:queue`.
3. `ZREM P:queue id` — this is the exclusive gate: only one caller in the
   whole cluster can remove the member; everyone else gets `not_available`.
4. `token = INCR P:fence`, `leaseExpiresAt = now + LEASE_TTL` using Redis
   `TIME` (no clock skew between instances), index in `P:leases`, publish
   `case.claimed` with a new version.

Why Lua and not several commands from Node: between a `ZRANGE` and a `ZREM`
issued separately another instance can take the same case; `MULTI` cannot
branch on a value read inside the transaction. A script runs to completion
without any other command interleaved. Restrictions: it must be short (it
blocks Redis while running), deterministic in its writes, and in Redis Cluster
it may only touch keys in one slot (hence the hash tag).

### Lease renewal: working vs. gone to lunch
Two independent limits:

| Limit | Default | Covers |
| --- | --- | --- |
| Lease TTL (`VERIFICATION_LEASE_TTL_MS`) | 15 s | Connection loss: closed browser, dead battery, crashed instance |
| Inactivity limit (`VERIFICATION_IDLE_LIMIT_MS`) | 5 min | Connected but not working |

Every `VERIFICATION_HEARTBEAT_MS` (5 s) the instance pings each socket and, for
each auditor holding a case, renews the lease **only if** the socket answered
the previous ping and the auditor showed activity (the page sends `activity`
on input, at most every 15 s) within the inactivity limit. Consequences:

* Working auditor: renewed every 5 s, the 15 s lease never expires (tolerates
  two lost heartbeats).
* Browser closed / laptop dead: no renewals → the lease expires ≤ 15 s later.
  A short network blip does not lose the case: the console reconnects through
  any instance, which adopts the lease from the snapshot and resumes renewals.
* Idle auditor: one minute before the limit the page shows a warning; after
  it, renewals stop and the case returns to the queue.

### Safe release/resolve (fencing)
`RENEW`, `RELEASE` and `RESOLVE` check in Lua that the case is `claimed`, the
owner is the caller (from the signed token), the token matches and the lease
has not expired by Redis time. An auditor whose lease expired gets
`lease_expired` (before the sweep) or `not_owner` (after reassignment); the new
owner's lease is untouched. The token also travels in `P:resolutions` so a
downstream store can reject writes with a token lower than the last one seen.

### Sweeper
Every instance runs `SWEEP` every 2 s: it requeues leases whose expiry passed,
re-checking inside the script. Running it concurrently on eight instances is
safe: the first script requeues, the others find nothing (tested). It also
prunes dead presence entries.

### What a decision does to the results
`approve` adds the acta to the totals exactly once, `reject` and `recount`
only record the decision (the mesa stays unreported until a new version
arrives). The results module applies each entry of `P:resolutions` with the
acta apply script, idempotent per (case, token) and refusing tokens older than
the last one seen for the case. The jurado sees the outcome at
`GET /api/actas/:idem`. Details in `docs/results-design.md` §3.

## 5. RF-8: presence

* One hash per connection (`P:conn:<connId>`) with `PEXPIRE` 30 s, refreshed by
  the 5 s heartbeat; tabs and devices of the same auditor are separate
  connections and are grouped by `auditorId` for the coordinator.
* Current case and its duration come from `P:owners` + the case hash, so they
  are always consistent with the lease state; accumulated work time comes from
  `P:auditor-stats` plus the running case.
* Ghosts: a socket that misses one ping/pong round is terminated
  (half-open TCP, battery). If the whole instance dies, nothing deletes the
  hash, but its TTL does, and the listing ignores expired index entries.
* The coordinator gets a presence push on every change (debounced) and every
  5 s.

## 6. Console synchronisation without sticky sessions

Instances never talk to each other (RT-3). Every state change is applied by a
script that also `INCR`s `P:version` and `PUBLISH`es the event with that
version; every instance subscribes and forwards events to its consoles.
Pub/Sub has no memory, so it is never the source of truth:

* On connect (to any instance, RT-2) the console receives a full snapshot
  produced atomically by the `SNAPSHOT` script, tagged with its version.
* The client applies an event only if `event.version === version + 1`; a gap
  triggers a new snapshot. Events that arrive before the snapshot are buffered.
* After a Redis outage every instance pushes a fresh snapshot to all consoles.

## 7. Rate limiting

Fixed-window counter per credential in Redis (`RATE_LIMIT` script: `INCR` +
`PEXPIRE`, window index from Redis time so all instances agree).

| Scope | Credential | Default | Response |
| --- | --- | --- | --- |
| `http` (`/api/verification/*`) | token subject, else login id, else IP | 20 / 10 s | `429` + `Retry-After` + JSON `rate_limited` |
| `ws` (console messages) | token subject (shared across tabs/instances) | 40 / 10 s | `{type:'error', code:'rate_limited', retryAfterMs}` |
| `actas` (`POST /api/actas`) | `jurado` field, else IP | 30 / 10 s | `429`; the jurado outbox already treats 429 as retryable |
| `public-connect` (new `/ws` and `/stream` connections) | client IP (from `X-Forwarded-For` behind `TRUST_PROXY` hops) | 60 / 10 s (`RATE_LIMIT_PUBLIC_CONNECTS`, 0 disables) | SSE `429` + `Retry-After`; WebSocket close `4429` |

If Redis is down the HTTP limiter fails open (the endpoints answer 503
anyway) and the WebSocket limiter falls back to a per-connection local window.

## 8. Failure behaviour

| Failure | Behaviour |
| --- | --- |
| Redis down | The command client has `disableOfflineQueue`, so calls fail immediately instead of hanging. A PING monitor (2 s, 1.5 s timeout) flips the state to `down`; every console gets `{type:'system', redis:'down'}` and shows a red “Degraded mode” banner with buttons disabled; any action returns `redis_unavailable`. `GET /api/verification/health` answers 503. Jurados get `503` from `/api/actas` (their outbox keeps the acta and retries). |
| Redis back | Monitor flips to `up`, every console gets a fresh snapshot; renewals resume (a lease that expired meanwhile is requeued by the sweeper); the ingestor resumes from its pending entries, so nothing written to the stream is lost. |
| Instance dies | Its consoles reconnect elsewhere with exponential backoff + full jitter (0…min(30 s, 0.5 s·2ⁿ)); its leases are adopted by the new instance or expire; its pending stream entries are taken by `XAUTOCLAIM`; its presence hashes expire. |
| Instance dies between `INGEST` and `XACK` | Re-delivered; `INGEST` sees the case exists → acked as duplicate. |
| Auditor's lease expires mid-work | Their late `resolve`/`release` is rejected (`lease_expired` / `not_owner`) and the page tells them. |
| Slow console | Events are skipped while the socket buffer exceeds 512 KB; when it drains, a full snapshot is sent. |
| Pub/Sub message lost | Version gap detected by the client → snapshot. |

## 9. Costs

| Operation | Cost |
| --- | --- |
| Claim next / claim by id | O(log N) (`ZRANGE 0 0`, `ZREM`, `ZADD`) |
| Renew / release / resolve | O(log N) |
| Sweep | O(log N + E) for E expired leases |
| Snapshot | O(Q + C) for queue and claimed cases (Q capped at 500 entries) |
| Presence list | O(P) over live connections (≈120) |
| Event fan-out per instance | O(local consoles) |

## 10. How to run and test

```bash
docker compose up -d          # Redis 7 for development
docker compose up -d --build  # 8 replicas + nginx + Valkey; dev access codes: dev-auditor / dev-coordinator
# open http://localhost:8080/verification/
npm test                      # Redis tests are skipped (with a message) if Redis is not reachable
```

Feed a doubtful acta (sum ≠ sufragantes) through `POST /api/actas`; it lands in
the stream and appears in every console within the next read cycle.
