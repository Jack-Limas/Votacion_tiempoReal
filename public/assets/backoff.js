// Reconnection delay with exponential backoff and FULL jitter:
//   delay = random(0, min(cap, base * 2^attempt))
// When an instance dies, its clients all notice at the same instant. Without
// jitter they retry in lock-step and hit the remaining instances in waves;
// with full jitter the retries are spread uniformly over the window.
// Shared by the browser pages and the Node load/avalanche scripts.
export const BACKOFF_BASE_MS = 500;
export const BACKOFF_CAP_MS = 30_000;

export function backoffDelay(attempt, { base = BACKOFF_BASE_MS, cap = BACKOFF_CAP_MS, random = Math.random } = {}) {
  return Math.round(random() * Math.min(cap, base * 2 ** attempt));
}
