// Simulador de la jornada (stub): 6.400 actas en 2 horas, 55% en los primeros
// 40 minutos, ráfagas de 300/min, ~180 correcciones que bajan valores, 4-11% de
// reenvíos con el mismo idem y 6-9% de actas dudosas.
//
// Uso: node scripts/simular-actas.js [--url=http://localhost:8080] [--velocidad=60]
//      [--desde=0] [--hasta=150] [--limite=6400] [--semilla=7]
// velocidad = factor de compresión del tiempo (60 => 2 h simuladas en 2 min).
import { pathToFileURL } from 'node:url';
import { generarCatalogo, mulberry32 } from '../src/publico/catalogo.js';
import { corregirVotos, crearActa, enviarActa, votosAleatorios } from './lib/actas.js';

const MINUTO = 60_000;

function opciones(argv) {
  const valores = Object.fromEntries(argv
    .filter((arg) => arg.startsWith('--'))
    .map((arg) => arg.slice(2).split('=')));
  return {
    url: valores.url ?? process.env.BASE_URL ?? 'http://localhost:8080',
    velocidad: Number(valores.velocidad ?? process.env.VELOCIDAD ?? 60),
    desde: Number(valores.desde ?? 0),
    hasta: Number(valores.hasta ?? Infinity),
    limite: Number(valores.limite ?? Infinity),
    semilla: Number(valores.semilla ?? 7),
    concurrencia: Number(valores.concurrencia ?? 64)
  };
}

// Devuelve la lista de envíos ordenada por tiempo simulado (ms desde el cierre).
export function planDeJornada({ semilla = 7 } = {}) {
  const random = mulberry32(semilla);
  const { mesas } = generarCatalogo();
  const orden = [...mesas].sort((a, b) => (b.urbano - a.urbano) || (random() - 0.5));

  // 55% en los primeros 40 min. Tres minutos de ráfaga: 230 actas extra sobre
  // el fondo (~70/min) dan ~300 actas en ese minuto.
  const enPico = Math.round(mesas.length * 0.55);
  const rafagas = [5, 12, 20];
  const extraPorRafaga = 230;
  const tiempos = [];
  for (let i = 0; i < enPico; i += 1) {
    if (i < rafagas.length * extraPorRafaga) {
      tiempos.push((rafagas[Math.floor(i / extraPorRafaga)] + random()) * MINUTO);
    } else {
      tiempos.push(random() * 40 * MINUTO);
    }
  }
  for (let i = enPico; i < mesas.length; i += 1) {
    tiempos.push((40 + random() * 80) * MINUTO);
  }

  const envios = [];
  orden.forEach((mesa, i) => {
    const t = tiempos[i];
    const sufragantes = 150 + Math.floor(random() * 300);
    const votos = votosAleatorios(random, sufragantes);
    const dudosa = random() < 0.075;
    const acta = crearActa(mesa, { votos, dudosa });
    envios.push({ t, tipo: dudosa ? 'dudosa' : 'nueva', acta });
    if (random() < 0.075) {
      envios.push({ t: t + 1000 + random() * 20_000, tipo: 'reenvio', acta });
    }
    if (!dudosa && random() < 180 / (mesas.length * 0.925)) {
      const corregida = crearActa(mesa, { version: 2, votos: corregirVotos(votos, random) });
      envios.push({ t: t + (2 + random() * 28) * MINUTO, tipo: 'correccion', acta: corregida });
    }
  });
  return envios.sort((a, b) => a.t - b.t);
}

export async function simular(opts) {
  const plan = planDeJornada(opts)
    .filter(({ t }) => t >= opts.desde * MINUTO && t < opts.hasta * MINUTO)
    .slice(0, opts.limite);
  const inicioSimulado = opts.desde * MINUTO;
  const inicio = Date.now();
  const resumen = { enviados: 0, porTipo: {}, porEstado: {}, errores: 0 };
  const enVuelo = new Set();

  for (const envio of plan) {
    const espera = inicio + (envio.t - inicioSimulado) / opts.velocidad - Date.now();
    if (espera > 0) await new Promise((resolve) => setTimeout(resolve, espera));
    while (enVuelo.size >= opts.concurrencia) await Promise.race(enVuelo);

    const tarea = enviarActa(opts.url, envio.acta)
      .then(({ status }) => {
        resumen.porEstado[status] = (resumen.porEstado[status] ?? 0) + 1;
      })
      .catch(() => {
        resumen.errores += 1;
      })
      .finally(() => enVuelo.delete(tarea));
    enVuelo.add(tarea);
    resumen.enviados += 1;
    resumen.porTipo[envio.tipo] = (resumen.porTipo[envio.tipo] ?? 0) + 1;
  }
  await Promise.all(enVuelo);
  resumen.segundos = Math.round((Date.now() - inicio) / 100) / 10;
  return resumen;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const opts = opciones(process.argv.slice(2));
  console.log(`Simulando minutos ${opts.desde}-${opts.hasta} a x${opts.velocidad} contra ${opts.url}`);
  simular(opts).then((resumen) => {
    console.log(JSON.stringify(resumen, null, 2));
  }).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
