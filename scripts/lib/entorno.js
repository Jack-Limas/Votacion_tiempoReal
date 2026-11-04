import { generarCatalogo } from '../../src/results/catalogo.js';
import { crearActa, enviarActa, votosAleatorios } from './actas.js';

export const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';

export async function entornoDisponible(baseUrl = BASE_URL) {
  try {
    const response = await fetch(new URL('/whoami', baseUrl), { signal: AbortSignal.timeout(2000) });
    return response.ok;
  } catch {
    return false;
  }
}

export const MOTIVO_SIN_ENTORNO = `Entorno no disponible en ${BASE_URL}: levántelo con "docker compose up -d --build" (ver README).`;

export const dormir = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function esperarHasta(condicion, { timeout = 10_000, intervalo = 100 } = {}) {
  const limite = Date.now() + timeout;
  while (Date.now() < limite) {
    if (await condicion()) return true;
    await dormir(intervalo);
  }
  return false;
}

export function mesasDonde(filtro) {
  return generarCatalogo().mesas.filter(filtro);
}

// Las pruebas usan versiones crecientes (basadas en el reloj) para que cada envío
// produzca un evento aunque la mesa ya se haya informado en una corrida anterior.
let ultimaVersion = 0;
export function versionNueva() {
  ultimaVersion = Math.max(ultimaVersion + 1, Date.now());
  return ultimaVersion;
}

export async function enviarVotos(mesa, votos, baseUrl = BASE_URL) {
  const acta = crearActa(mesa, { version: versionNueva(), votos });
  const respuesta = await enviarActa(baseUrl, acta);
  if (respuesta.status !== 201) {
    throw new Error(`El acta ${acta.acta} respondió ${respuesta.status}: ${JSON.stringify(respuesta.body)}`);
  }
  return { acta, respuesta };
}

export function enviarAleatoria(mesa, random, baseUrl = BASE_URL) {
  return enviarVotos(mesa, votosAleatorios(random, 150 + Math.floor(random() * 300)), baseUrl);
}

// Consulta /metrics a través del balanceador hasta dar con la instancia pedida.
export async function metricasDe(instancia, baseUrl = BASE_URL, intentos = 60) {
  for (let i = 0; i < intentos; i += 1) {
    const response = await fetch(new URL('/metrics', baseUrl));
    const metricas = await response.json();
    if (metricas.instancia === instancia) return metricas;
  }
  throw new Error(`No se alcanzó la instancia ${instancia} en ${intentos} intentos.`);
}
