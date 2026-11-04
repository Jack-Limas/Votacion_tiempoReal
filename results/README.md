# Measurement results

Every file here was produced by a script in `scripts/` on the development
machine described below. Nothing was edited by hand except removing one
meaningless field noted in its commit (`rssKBPerClient`, negative because of
lazy memory release).

**Machine:** Windows 11 laptop, Docker Desktop VM with 12 vCPU and 3.9 GB RAM.
The 8 replicas, nginx, Valkey **and the load generator** share that VM, so CPU
contention inflates event-loop and publish delays in the big runs. Treat the
large-run latencies as an upper bound for this machine, not as the system's
capacity; repeat them with the generator on another machine
(`scripts/load.js --simulate=0` on the extra machines).

| File | Script | What it shows |
| --- | --- | --- |
| `load-sse-batched-20000-20k.*` | `load.js --connections=20000 --transport=sse` | 20,000 SSE clients held through nginx with the day profile (minutes 0–40 at x30): 0 failed opens, 0 closed, 1,571,837 messages delivered with no loss, 42x fewer than the naive figure. Event loop p99 per second: median 46 ms, max 525 ms; publish delay p50 ≈ 1.1 s, p99 ≈ 5 s (CPU contention). |
| `load-ws-batched-20000-20k.*` | `load.js --connections=20000 --transport=ws` | Same with WebSocket: 0 failed opens, 0 closed, 0 acta errors; the machine saturated (simulation took 334 s instead of 80 s, event loop p99 median 81 ms, max 1.8 s, publish p50 ≈ 1.3 s). |
| `load-ws-batched-10000-inp-background.*` | `load.js --connections=10000 --transport=ws` | Load used while measuring INP: event loop p99 median 21.7 ms, max 301 ms; 56x fewer messages than naive. |
| `load-{sse,ws,poll}-batched-3000-transports*.*` | `load.js --connections=3000 --transport=…` (2 runs) | Transport comparison on the same profile (minutes 0–10 at x30). |
| `transport-memory-*.json` | `node --expose-gc transport-memory.js` | Server heap per open connection after forced GC, both orders: SSE ≈ 6.2 KB, WebSocket ≈ 3.4 KB. |
| `inp-baseline.json`, `inp-under-load-10k-ws.json` | `measure-inp.js` | INP of the public page on a 6x-throttled CPU, 360x640, Slow-4G-like network: 72 ms without load, 160 ms during the peak with 10,000 WebSocket clients. |
| `avalanche-{immediate,fixed,jitter}.*`, `avalanche.svg` | `avalanche.js` | An instance holding 1,000 of 8,000 SSE clients is killed: peak reconnection attempts per second 512 (immediate), 576 (fixed 1 s), 284 (full jitter, 4 s base); every client reconnected to the other 7 instances. |
| `avalanche-jitter-base500ms.*` | `avalanche.js` (earlier run) | Same experiment with a 500 ms jitter base: peak 945/s, higher than without jitter, which is why the base is now 4 s. Recorded before a client-side timeout bug in the script was fixed; all 1,000 clients reconnected in it. |
| `archive/carga-*` | previous `scripts/carga.js` (Spanish field names) | 10,000 SSE clients, naive vs batched fan-out, from the delivered version; kept for the RF-4 comparison described in `docs/results-design.md`. |

## Transport comparison (3,000 clients each, two runs)

| | SSE | WebSocket | Polling (2 s) |
| --- | --- | --- | --- |
| Latency from newest event, p50 / p95 (ms) | 100 / 710 · 230 / 8,660 | 40 / 570 · 60 / 510 | 1,100 / 3,320 · 1,020 / 4,010 |
| Mean CPU per instance | 24.8 % · 23.6 % | 15.6 % · 22.6 % | 62.4 % · 66.4 % |
| Server heap per connection (forced GC) | ≈ 6.2 KB | ≈ 3.4 KB | none held (cost is per request) |
| Updates seen by clients | all | all | ≈ 47 % (intermediate states skipped) |

"Latency from newest event" is measured on the client from the stream ID of
the newest event in each delivered window, so it understates the wait of the
first acta of a window; the per-acta figure is `publishDelayMs` (server side,
receivedAt → sent).
