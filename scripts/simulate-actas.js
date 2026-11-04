// Election-day simulator: 6,400 actas in 2 hours, 55 % in the first 40
// minutes, bursts of 300/min, ~180 corrections that lower values, 4-11 %
// resends with the same idem and 6-9 % doubtful actas.
//
// Usage: node scripts/simulate-actas.js [--url=http://localhost:8080] [--speed=60]
//        [--from=0] [--to=150] [--limit=6400] [--seed=7]
// speed = time compression factor (60 => 2 simulated hours in 2 minutes).
import { pathToFileURL } from 'node:url';
import { buildCatalog, mulberry32 } from '../src/results/catalog.js';
import { correctVotes, createActa, randomVotes, sendActa } from './lib/actas.js';

const MINUTE = 60_000;

function options(argv) {
  const values = Object.fromEntries(argv
    .filter((arg) => arg.startsWith('--'))
    .map((arg) => arg.slice(2).split('=')));
  return {
    url: values.url ?? process.env.BASE_URL ?? 'http://localhost:8080',
    speed: Number(values.speed ?? process.env.SPEED ?? 60),
    from: Number(values.from ?? 0),
    to: Number(values.to ?? Infinity),
    limit: Number(values.limit ?? Infinity),
    seed: Number(values.seed ?? 7),
    concurrency: Number(values.concurrency ?? 64)
  };
}

// Returns the submissions ordered by simulated time (ms since polls closed).
export function dayPlan({ seed = 7 } = {}) {
  const random = mulberry32(seed);
  const { mesas } = buildCatalog();
  const order = [...mesas].sort((a, b) => (b.urban - a.urban) || (random() - 0.5));

  // 55 % in the first 40 min. Three burst minutes: 230 extra actas on top of
  // the background (~70/min) give ~300 actas in that minute.
  const atPeak = Math.round(mesas.length * 0.55);
  const bursts = [5, 12, 20];
  const extraPerBurst = 230;
  const times = [];
  for (let i = 0; i < atPeak; i += 1) {
    if (i < bursts.length * extraPerBurst) {
      times.push((bursts[Math.floor(i / extraPerBurst)] + random()) * MINUTE);
    } else {
      times.push(random() * 40 * MINUTE);
    }
  }
  for (let i = atPeak; i < mesas.length; i += 1) {
    times.push((40 + random() * 80) * MINUTE);
  }

  const submissions = [];
  order.forEach((mesa, i) => {
    const t = times[i];
    const sufragantes = 150 + Math.floor(random() * 300);
    const votos = randomVotes(random, sufragantes);
    const doubtful = random() < 0.075;
    const acta = createActa(mesa, { votos, doubtful });
    submissions.push({ t, kind: doubtful ? 'doubtful' : 'new', acta });
    if (random() < 0.075) {
      submissions.push({ t: t + 1000 + random() * 20_000, kind: 'resend', acta });
    }
    if (!doubtful && random() < 180 / (mesas.length * 0.925)) {
      const corrected = createActa(mesa, { version: 2, votos: correctVotes(votos, random) });
      submissions.push({ t: t + (2 + random() * 28) * MINUTE, kind: 'correction', acta: corrected });
    }
  });
  return submissions.sort((a, b) => a.t - b.t);
}

export async function simulate(opts) {
  const plan = dayPlan(opts)
    .filter(({ t }) => t >= opts.from * MINUTE && t < opts.to * MINUTE)
    .slice(0, opts.limit);
  const simulatedStart = opts.from * MINUTE;
  const start = Date.now();
  const summary = { sent: 0, byKind: {}, byStatus: {}, errors: 0 };
  const inFlight = new Set();

  for (const submission of plan) {
    const wait = start + (submission.t - simulatedStart) / opts.speed - Date.now();
    if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
    while (inFlight.size >= opts.concurrency) await Promise.race(inFlight);

    const task = sendActa(opts.url, submission.acta)
      .then(({ status }) => {
        summary.byStatus[status] = (summary.byStatus[status] ?? 0) + 1;
      })
      .catch(() => {
        summary.errors += 1;
      })
      .finally(() => inFlight.delete(task));
    inFlight.add(task);
    summary.sent += 1;
    summary.byKind[submission.kind] = (summary.byKind[submission.kind] ?? 0) + 1;
  }
  await Promise.all(inFlight);
  summary.seconds = Math.round((Date.now() - start) / 100) / 10;
  return summary;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const opts = options(process.argv.slice(2));
  console.log(`Simulating minutes ${opts.from}-${opts.to} at x${opts.speed} against ${opts.url}`);
  simulate(opts).then((summary) => {
    console.log(JSON.stringify(summary, null, 2));
  }).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
