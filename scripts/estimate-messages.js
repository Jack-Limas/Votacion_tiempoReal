// Estimates the messages of the full election day (2 h, 400,000 clients) with
// the simulator plan, against the naive figure (every acta to every client).
//
// Usage: node scripts/estimate-messages.js [--clients=400000] [--flush=1000]
//        [--mix=dep:0.5,mun:0.35,pto:0.15]
// Assumes every client connected the whole day and municipality/puesto
// subscribers spread in proportion to the mesas.
import { buildCatalog } from '../src/results/catalog.js';
import { dayPlan } from './simulate-actas.js';

const values = Object.fromEntries(process.argv.slice(2)
  .filter((arg) => arg.startsWith('--'))
  .map((arg) => arg.slice(2).split('=')));
const clients = Number(values.clients ?? 400000);
const flushMs = Number(values.flush ?? 1000);
const mix = Object.fromEntries((values.mix ?? 'dep:0.5,mun:0.35,pto:0.15')
  .split(',').map((p) => p.split(':')).map(([k, v]) => [k, Number(v)]));

const { totals } = buildCatalog();
const subscribers = (topic) => {
  if (topic === 'dep') return clients * mix.dep;
  const kind = topic.split(':')[0];
  return clients * mix[kind] * (totals[topic] / totals.dep);
};

// Only accepted actas and corrections produce events.
const events = dayPlan().filter((e) => e.kind === 'new' || e.kind === 'correction');
const dirtyWindows = new Set();
let naiveWithTopics = 0;
for (const { t, acta } of events) {
  const window = Math.floor(t / flushMs);
  for (const topic of ['dep', `mun:${acta.municipio}`, `pto:${acta.puesto}`]) {
    dirtyWindows.add(`${topic}|${window}`);
    naiveWithTopics += subscribers(topic);
  }
}
let aggregated = 0;
for (const key of dirtyWindows) aggregated += subscribers(key.split('|')[0]);

const naive = 6400 * clients;
const fmt = (n) => Math.round(n).toLocaleString('en-US');
console.log(JSON.stringify({
  clients,
  flushMs,
  mix,
  events: events.length,
  naiveFromCase: fmt(naive),
  perEventWithTopics: fmt(naiveWithTopics),
  windowAggregated: fmt(aggregated),
  reductionVsNaive: `${Math.round(naive / aggregated)}x`,
  percentOfNaive: `${((aggregated / naive) * 100).toFixed(2)}%`
}, null, 2));
