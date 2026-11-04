import { buildCatalog } from '../../src/results/catalog.js';
import { createActa, randomVotes, sendActa } from './actas.js';

export const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';

export async function environmentAvailable(baseUrl = BASE_URL) {
  try {
    const response = await fetch(new URL('/whoami', baseUrl), { signal: AbortSignal.timeout(2000) });
    return response.ok;
  } catch {
    return false;
  }
}

export const NO_ENVIRONMENT = `Environment not reachable at ${BASE_URL}: start it with "docker compose up -d --build" (see README).`;

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitUntil(condition, { timeout = 10_000, interval = 100 } = {}) {
  const limit = Date.now() + timeout;
  while (Date.now() < limit) {
    if (await condition()) return true;
    await sleep(interval);
  }
  return false;
}

export function mesasWhere(filter) {
  return buildCatalog().mesas.filter(filter);
}

// Tests use increasing versions (clock based) so every submission produces
// an event even if the mesa was already reported in a previous run.
let lastVersion = 0;
export function newVersion() {
  lastVersion = Math.max(lastVersion + 1, Date.now());
  return lastVersion;
}

export async function sendVotes(mesa, votos, baseUrl = BASE_URL) {
  const acta = createActa(mesa, { version: newVersion(), votos });
  const response = await sendActa(baseUrl, acta);
  if (response.status !== 201) {
    throw new Error(`Acta ${acta.acta} answered ${response.status}: ${JSON.stringify(response.body)}`);
  }
  return { acta, response };
}

export function sendRandom(mesa, random, baseUrl = BASE_URL) {
  return sendVotes(mesa, randomVotes(random, 150 + Math.floor(random() * 300)), baseUrl);
}

// Queries /metrics through the load balancer until it reaches the instance.
export async function metricsOf(instance, baseUrl = BASE_URL, attempts = 60) {
  for (let i = 0; i < attempts; i += 1) {
    const response = await fetch(new URL('/metrics', baseUrl));
    const metrics = await response.json();
    if (metrics.instance === instance) return metrics;
  }
  throw new Error(`Instance ${instance} not reached in ${attempts} attempts.`);
}
