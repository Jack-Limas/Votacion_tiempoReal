// Reconnection delay with exponential backoff and FULL jitter:
//   delay = random(0, min(cap, base * 2^attempt))
// When an instance dies, its clients all notice at the same instant. Without
// jitter they retry in lock-step and hit the remaining instances in waves;
// with full jitter the retries are spread uniformly over the window.
// Shared by the browser pages and the Node load/avalanche scripts.
//
// The FIRST window must scale with the size of the herd: when an instance with
// 50,000 clients dies they all start retrying within ~2 s of each other, and a
// 500 ms first window concentrates them into a spike higher than a plain
// fixed retry (measured, see results/avalanche-*.json). A 4 s first window
// spreads them; the cursor makes the wait invisible in the data (RF-6).
export const BACKOFF_BASE_MS = 4000;
export const BACKOFF_CAP_MS = 30_000;

export function backoffDelay(attempt, { base = BACKOFF_BASE_MS, cap = BACKOFF_CAP_MS, random = Math.random } = {}) {
  return Math.round(random() * Math.min(cap, base * 2 ** attempt));
}
