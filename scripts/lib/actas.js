import { randomBytes } from 'node:crypto';
import { CANDIDATOS } from '../../src/publico/catalogo.js';

const TS_CIERRE = 1793577600;

export function votosAleatorios(random, sufragantes) {
  const pesos = CANDIDATOS.map((candidato) => (
    candidato === 'NULO' || candidato === 'NO_MARCADO' ? 0.03 : 0.2 + random()
  ));
  const suma = pesos.reduce((acc, peso) => acc + peso, 0);
  const votos = {};
  let asignados = 0;
  CANDIDATOS.forEach((candidato, i) => {
    votos[candidato] = Math.floor((sufragantes * pesos[i]) / suma);
    asignados += votos[candidato];
  });
  votos.C1 += sufragantes - asignados;
  return votos;
}

// Acta con el formato de la sección 3.1 del caso.
export function crearActa(mesa, { version = 1, votos, dudosa = false }) {
  const sufragantes = Object.values(votos).reduce((acc, n) => acc + n, 0);
  return {
    acta: `E14-${mesa.puesto}-${String(mesa.numero).padStart(4, '0')}-03`,
    mesa: mesa.id,
    puesto: mesa.puesto,
    municipio: mesa.municipio,
    version,
    jurado: `JUR-${String(mesa.id).padStart(5, '0')}`,
    ts_cierre: TS_CIERRE,
    votos,
    sufragantes: dudosa ? sufragantes + 3 : sufragantes,
    hash_foto: randomBytes(8).toString('hex'),
    idem: randomBytes(8).toString('hex')
  };
}

// Corrección que BAJA un candidato: mueve votos de un candidato a otro.
export function corregirVotos(votos, random) {
  const candidatos = Object.keys(votos).filter((c) => votos[c] > 0 && c !== 'NULO' && c !== 'NO_MARCADO');
  const origen = candidatos[Math.floor(random() * candidatos.length)];
  const destino = candidatos.find((c) => c !== origen) ?? 'BLANCO';
  const cantidad = Math.max(1, Math.floor(votos[origen] * (0.05 + random() * 0.2)));
  return { ...votos, [origen]: votos[origen] - cantidad, [destino]: votos[destino] + cantidad };
}

export async function enviarActa(baseUrl, acta) {
  const response = await fetch(new URL('/api/actas', baseUrl), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(acta)
  });
  return { status: response.status, body: await response.json() };
}
