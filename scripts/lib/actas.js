import { randomBytes } from 'node:crypto';
import { CANDIDATES } from '../../src/results/catalog.js';

const TS_CIERRE = 1793577600;

export function randomVotes(random, sufragantes) {
  const weights = CANDIDATES.map((candidate) => (
    candidate === 'NULO' || candidate === 'NO_MARCADO' ? 0.03 : 0.2 + random()
  ));
  const sum = weights.reduce((acc, weight) => acc + weight, 0);
  const votos = {};
  let assigned = 0;
  CANDIDATES.forEach((candidate, i) => {
    votos[candidate] = Math.floor((sufragantes * weights[i]) / sum);
    assigned += votos[candidate];
  });
  votos.C1 += sufragantes - assigned;
  return votos;
}

// Acta in the format of section 3.1 of the case study.
export function createActa(mesa, { version = 1, votos, doubtful = false }) {
  const sufragantes = Object.values(votos).reduce((acc, n) => acc + n, 0);
  return {
    acta: `E14-${mesa.puesto}-${String(mesa.number).padStart(4, '0')}-03`,
    mesa: mesa.id,
    puesto: mesa.puesto,
    municipio: mesa.municipio,
    version,
    jurado: `JUR-${String(mesa.id).padStart(5, '0')}`,
    ts_cierre: TS_CIERRE,
    votos,
    sufragantes: doubtful ? sufragantes + 3 : sufragantes,
    hash_foto: randomBytes(8).toString('hex'),
    idem: randomBytes(8).toString('hex')
  };
}

// Correction that LOWERS a candidate: moves votes from one candidate to another.
export function correctVotes(votos, random) {
  const candidates = Object.keys(votos).filter((c) => votos[c] > 0 && c !== 'NULO' && c !== 'NO_MARCADO');
  const from = candidates[Math.floor(random() * candidates.length)];
  const to = candidates.find((c) => c !== from) ?? 'BLANCO';
  const amount = Math.max(1, Math.floor(votos[from] * (0.05 + random() * 0.2)));
  return { ...votos, [from]: votos[from] - amount, [to]: votos[to] + amount };
}

export async function sendActa(baseUrl, acta) {
  const response = await fetch(new URL('/api/actas', baseUrl), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(acta)
  });
  return { status: response.status, body: await response.json() };
}
