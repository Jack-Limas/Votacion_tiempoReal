# Requirements status

Audit of the case study requirements against the code. The baseline is
`origin/develop` at `aff9bb2` (the delivered version). The "Final" column is
updated at the end of `feature/final-integration`.

Legend: **Done** = implemented and backed by a test or a measurement in this
repository. **Partial** = implemented but missing a part of the requirement or
its evidence. **Missing** = not implemented.

## Functional requirements

| Id | Why it is needed | Delivered (aff9bb2) | Evidence in delivered version | Gap in delivered version | Final |
| --- | --- | --- | --- | --- | --- |
| RF-1 Idempotent intake | Jurados resend 4–11 % of actas on bad networks; a resend must never count twice and must get a clear answer | Done | `aplicarActa.lua` stores `idem` → result atomically; duplicates return `200 duplicate:true`; `409` on same idem with different content | Crash between register and reply only explained in a comment; if the idem key expired, safety depended on nothing (the `aggregate` hash would add again) | pending |
| RF-2 Corrections and convergence | ~180 corrections; late retries can deliver v2 after v3; the result must depend only on the set of actas | Partial | Highest version wins in `acc:mesa`; v ≤ applied is ignored | `aggregate` hash adds every accepted acta, including superseded versions and corrections (wrong totals); equal versions with different content depend on arrival order; no out-of-order test | pending |
| RF-3 Atomic hierarchical totals | A municipality total must never include an acta its puesto does not show yet | Done | One Lua script updates mesa, puesto, municipio, department and `XADD` | Percentage counts only first versions (correct), but no test reads levels concurrently | pending |
| RF-4 Fan-out without blocking the Event Loop | 50k connections per instance; a naive loop freezes jurados | Done | Window aggregation, batches with `setImmediate`, EL delay histograms; 10k-connection run in `results/` | Measured with 10k connections only; message reduction reported | pending |
| RF-5 Topic subscription | Cost must be proportional to topic subscribers | Done | `Map<topic, Set<conn>>`, integration test | — | pending |
| RF-6 Reconnection with recovery | A client away 30 s must end in exactly the same state | Done | Stream IDs as cursors, replay vs snapshot, A/B test | Only an SSE client in scripts; no browser page used it | pending |
| RF-7 Verification queue with exclusive claim | Two auditors must never review the same acta; abandoned cases return | Done | Lua claim, fenced leases, sweeper, 20 integration tests | Resolving a case had no effect on results: an approved doubtful acta was never added | pending |
| RF-8 Presence | Coordinator must see who works on what; ghosts must expire | Done | Per-connection hash with TTL, ping/pong, tests | — | pending |
| RF-9 Slow clients and back-pressure | Saturated mobile clients can make the output buffer grow until the server dies | Missing (public) | Verification consoles skip events above 512 KB buffered | Public SSE never looked at `writableLength`; no slow-client demonstration | pending |
| RF-10 Degradation and reconnection avalanche | Silent failure when Redis dies; 50k clients reconnecting at once kill the rest | Partial | Verification consoles show degraded mode (tested); jurados get `503` | Public page and jury page did not exist; no instance-death test; no avalanche chart with and without jitter | pending |

## Technical requirements

| Id | Why it is needed | Delivered | Evidence | Gap | Final |
| --- | --- | --- | --- | --- | --- |
| RT-1 ≥ 8 instances behind a load balancer, in the cloud | Capacity for 400k clients and no single app process | Partial | `docker-compose.yml` with 8 replicas + nginx | No cloud deployment; the README pointed to Render/Vercel, which cannot run 8 long-lived WebSocket instances | pending |
| RT-2 No sticky sessions | Any instance must serve any client | Done | nginx round robin; `/whoami` test hits 8 instances | — | pending |
| RT-3 Instances only coordinate through Redis | No hidden coupling | Done | No inter-instance calls in code | — | pending |
| RT-4 Batched fan-out with EL delay per instance | Proof that the loop stays bounded at peak | Done | `/metrics`, `/metrics/all` | No visual panel | pending |
| RT-5 Justified transports, ≥ 2 coexisting | Each audience has different guarantees | Partial | SSE (public), HTTP (jury), WebSocket (verification) | Choice justified by opinion; no measurement comparing SSE, WebSocket and polling | pending |
| RT-6 Jury app works offline | Mobile networks drop | Partial | `public/acta-outbox.js` (IndexedDB + retries) | No jury page used it; `public/` was not in the Docker image | pending |
| RT-7 Visible instrumentation | Operators need connections, msg/s, EL delay, memory per connection, actas/min, publish delay | Partial | JSON in `/metrics/all` | No dashboard; no memory per connection; no actas per minute | pending |
| RT-8 Reproducible load test ≥ 20k connections with the day profile | Proof of capacity | Partial | `scripts/carga.js` (10k measured) | 20k never run; single machine only | pending |
| RT-9 INP of the public page under load on a low-end phone | Usability during the peak | Missing | — | No public page and no INP measurement | pending |
| RT-10 Public URL working without the team | Graders open it themselves | Missing | — | Vercel config cannot host WebSockets/long-lived readers; no VM deployment | pending |

## Redis roles (section 5)

| Role | Why it is needed | Delivered | Evidence | Gap | Final |
| --- | --- | --- | --- | --- | --- |
| Broadcast backplane | An acta received on instance 3 must reach clients on the other 7 | Done | Global stream `st:eventos` read by every instance; Pub/Sub for consoles | Channel design argued in `docs/results-design.md` | pending |
| History for recovery | Clients and servers that were away must catch up | Done | Stream with `MAXLEN ~ 10000`, measured ~725 B/event | — | pending |
| Totals | Hot readable/updatable totals and candidate ranking | Partial | Hashes per level | Ranking requires reading the whole hash (no sorted structure); duplicated `aggregate` hash with wrong semantics | pending |
| Atomic composite operation | Apply, revert and update 4 levels as one | Done | `aplicarActa.lua` | Approval of doubtful actas did not reuse it | pending |
| Idempotency | A resend never adds twice | Done | `idem` keys with TTL | What happens if the key expires before the retry was not covered by a mechanism | pending |
| Distributed lock | Exclusive assignment of a case | Done | Fenced lease scripts | — | pending |
| Presence | Who is online, with automatic expiry | Done | Hash + TTL + index ZSET | — | pending |
| Rate limit | A misconfigured client cannot flood | Partial | Per-credential fixed window for jury HTTP and consoles | Public SSE connections not limited | pending |
