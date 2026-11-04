# Requirements status

Audit of the case-study requirements. "Delivered" is `origin/develop` at
`aff9bb2` (the version before `feature/final-integration`); "Final" is this
branch. Evidence points to code, tests or files in `results/`.

Legend: **Done** = implemented and backed by a passing test or a recorded
measurement. **Partial** = implemented, but part of the requirement or its
evidence is missing. **Missing** = not implemented.

## Functional requirements

| Id | Why it is needed | Delivered | Gap in the delivered version | Final | Evidence (final) |
| --- | --- | --- | --- | --- | --- |
| RF-1 Idempotent intake | 4–11 % of actas are resent; a resend must never count twice and must get a clear answer | Done | Crash between register and reply only explained in a comment; after the idem TTL the `aggregate` hash would add again | **Done** | `applyActa.lua` stores digest + answer atomically (200 duplicate / 409 conflict); version/content order makes even an expired idem harmless (`test/integration/rf2-convergence.test.js`); jury outbox resends the same idem; `GET /api/actas/:idem` |
| RF-2 Corrections and convergence | ~180 corrections; late retries can deliver v2 after v3 | Partial | `aggregate` hash added superseded versions; equal versions depended on arrival order; no test | **Done** | Max by (version, content digest); all 24 arrival orders converge to one state; late v2 → `superseded` (`rf2-convergence.test.js`) |
| RF-3 Atomic hierarchical totals | A municipality total must never include an acta its puesto does not show | Done | No concurrency evidence beyond the script | **Done** | One Lua script for mesa, puesto, municipio, department, ranking and event; explained in `docs/results-design.md` §2 |
| RF-4 Fan-out without blocking the Event Loop | 50k connections per instance; a naive loop freezes jurados | Done | Measured with 10k only | **Done (bounded by this machine)** | Window aggregation + batches with `setImmediate`; unit tests; 10k naive vs batched (`results/archive/`); 20k SSE and WS runs; message reduction 22x–56x vs naive in the runs; full-day estimate vs 2,560 M in `docs/results-design.md` §6. Event-loop p99 stayed bounded at 10k WS (median 21.7 ms) but rose to 46–81 ms median at 20k with the generator on the same CPUs |
| RF-5 Topic subscription | Cost proportional to a topic's subscribers | Done | — | **Done** | `Map<topic, Set<conn>>`; unit tests; `rf5-topics.test.js` through the 8 replicas |
| RF-6 Reconnection with recovery | A client away 30 s must end identical | Done | No browser client used the cursors | **Done** | Stream IDs as cursors, replay vs snapshot; A/B test on different instances (`rf6-reconnection.test.js`); the public page resumes with cursors |
| RF-7 Exclusive verification queue | Two auditors must never review the same acta | Done | A decision had no effect on the results | **Done** | Lua claim/lease/fencing/sweeper (20 tests); decisions applied once per (case, token) with stale tokens refused (`verification-decisions.test.js`) |
| RF-8 Presence | Coordinator sees who works on what; ghosts expire | Done | — | **Done** | Hash per connection with TTL; tests for tabs and ghosts; coordinator view |
| RF-9 Slow clients and back-pressure | Saturated clients can exhaust server memory | Missing (public) | No buffer watch on SSE; no demonstration | **Done** | `writableLength`/`bufferedAmount` per delivery; degrade → snapshot on drain → disconnect with reconnect signal (bytes or time limit); deliberately slow TCP client (`rf9-slow-client.test.js`) |
| RF-10 Degradation and avalanche | No silent failure when Redis dies; reconnection storms must not kill the rest | Partial | No public/jury page, no instance-death test, no chart | **Done** | Redis cut for jurados (503 in ~40 ms), public (`status: down`) and auditors at once, with recovery (`rf10-degraded.test.js`); heartbeats + watchdog for killed instances; avalanche with/without jitter (`results/avalanche.svg`, peak 512/576 vs 284 attempts/s) |

## Technical requirements

| Id | Why it is needed | Delivered | Gap | Final | Evidence (final) |
| --- | --- | --- | --- | --- | --- |
| RT-1 ≥ 8 instances behind a balancer, in the cloud | Capacity and no single process | Partial | No cloud deployment (Render/Vercel cannot run it) | **Done** | 8 replicas behind nginx on one AWS EC2 instance (`docker-compose.prod.yml`, Caddy for HTTPS) at https://184.193.102.4.sslip.io; requests answered by 8 distinct instances. The single instance is a single point of failure |
| RT-2 No sticky sessions | Any instance serves any client | Done | — | **Done** | Round robin; `/whoami` hits 8 instances; consoles and public clients reconnect through other instances (tests) |
| RT-3 Coordination only through Redis | No hidden coupling | Done | — | **Done** | No inter-instance calls; streams, groups, Lua and Pub/Sub only |
| RT-4 Batched fan-out + EL delay per instance | Proof the loop stays bounded | Done | No panel | **Done** | `/metrics/all` per instance; `/ops/` charts EL p99 per instance |
| RT-5 Justified transports, ≥ 2 coexisting | Different guarantees per audience | Partial | Opinion, not measurement | **Done** | WebSocket (public, auditors), HTTP POST (jury), polling (ops), SSE fallback; two measured runs + forced-GC heap per connection (`results/README.md`); public switched to WebSocket because the data favoured it |
| RT-6 Jury app works offline | Mobile networks drop | Partial | No page; `public/` not in the image | **Done** | `/jury/` on the IndexedDB outbox with jittered retries; image copies `public/`; outage test shows the retried acta applied once |
| RT-7 Visible instrumentation | Operators need live numbers | Partial | JSON only | **Done** | `/ops/`: connections and transports per instance, msg/s, EL delay, CPU, heap per connection, actas/min, publish delay, slow clients |
| RT-8 Load test ≥ 20,000 connections, day profile | Proof of capacity | Partial | 10k only | **Done (with a caveat)** | `scripts/load.js` held 20,000 SSE and 20,000 WebSocket clients with the simulator (0 failed, 0 dropped); multi-process and multi-machine (`--simulate=0`). Caveat: one machine, so delays at 20k are inflated by the generator |
| RT-9 INP on a low-end phone under load | Usable during the peak | Missing | — | **Done** | `scripts/measure-inp.js`: 72 ms idle, 160 ms during the peak with 10k clients (6x CPU throttle, Slow-4G-like) |
| RT-10 Public URL without the team | Graders open it | Missing | — | **Done** | Deployed at https://184.193.102.4.sslip.io; `scripts/smoke.js` passed 22/22 against the public URL over HTTPS with 8 distinct instances |

## Redis roles (section 5)

| Role | Why it is needed | Delivered | Gap | Final | Evidence (final) |
| --- | --- | --- | --- | --- | --- |
| Broadcast backplane | An acta on instance 3 must reach the other 7 | Done | — | **Done** | Global stream + argued channel design (`docs/results-design.md` §4) |
| History for recovery | Absent clients catch up | Done | — | **Done** | Stream retention, MAXLEN policy, ≈ 725 B/event measured |
| Totals | Hot totals and a ranking without walking everything | Partial | Ranking needed the whole hash; wrong `aggregate` hash | **Done** | Hashes per level + `rank:{topic}` ZSET; `aggregate` removed; ranking test |
| Atomic composite operation | Apply, revert and update 4 levels as one | Done | Approvals did not reuse it | **Done** | Same script for jury actas and verified actas |
| Idempotency | A resend never adds twice | Done | Expired key not covered | **Done** | idem TTL + version/content order (tested) |
| Distributed lock | Exclusive case assignment | Done | — | **Done** | Fenced leases, renewal, owner-checked release, sweeper (tests) |
| Presence | Who is online, expiring by itself | Done | — | **Done** | TTL + heartbeat + ghost test |
| Rate limit | A misconfigured client cannot flood | Partial | Public connections not limited | **Done** | Jury actas, console HTTP/WS, public connections per IP (`public-rate-limit.test.js`) |

## Summary

The system is deployed at https://184.193.102.4.sslip.io on one AWS EC2
instance (8 replicas behind nginx), which is a single point of failure. The 400,000
client target was not measured; the largest measured load is 20,000
simultaneous clients on one development machine.
