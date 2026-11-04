// Estima los mensajes de la jornada completa (2 h, 400.000 clientes) con el plan
// del simulador, frente a la cifra ingenua (cada acta a cada cliente).
//
// Uso: node scripts/estimar-mensajes.js [--clientes=400000] [--flush=1000]
//      [--mezcla=dep:0.5,mun:0.35,pto:0.15]
// Supone todos los clientes conectados toda la jornada y los suscriptores de
// municipio/puesto repartidos en proporción a las mesas.
import { generarCatalogo } from '../src/publico/catalogo.js';
import { planDeJornada } from './simular-actas.js';

const valores = Object.fromEntries(process.argv.slice(2)
  .filter((arg) => arg.startsWith('--'))
  .map((arg) => arg.slice(2).split('=')));
const clientes = Number(valores.clientes ?? 400000);
const flushMs = Number(valores.flush ?? 1000);
const mezcla = Object.fromEntries((valores.mezcla ?? 'dep:0.5,mun:0.35,pto:0.15')
  .split(',').map((p) => p.split(':')).map(([k, v]) => [k, Number(v)]));

const { totales } = generarCatalogo();
const suscriptores = (topic) => {
  if (topic === 'dep') return clientes * mezcla.dep;
  const tipo = topic.split(':')[0];
  return clientes * mezcla[tipo] * (totales[topic] / totales.dep);
};

// Solo las actas aceptadas y las correcciones producen eventos.
const eventos = planDeJornada().filter((e) => e.tipo === 'nueva' || e.tipo === 'correccion');
const ventanasSucias = new Set();
let ingenuoConTopicos = 0;
for (const { t, acta } of eventos) {
  const ventana = Math.floor(t / flushMs);
  for (const topic of ['dep', `mun:${acta.municipio}`, `pto:${acta.puesto}`]) {
    ventanasSucias.add(`${topic}|${ventana}`);
    ingenuoConTopicos += suscriptores(topic);
  }
}
let agregado = 0;
for (const clave of ventanasSucias) agregado += suscriptores(clave.split('|')[0]);

const ingenuo = 6400 * clientes;
const fmt = (n) => Math.round(n).toLocaleString('es-CO');
console.log(JSON.stringify({
  clientes,
  flushMs,
  mezcla,
  eventos: eventos.length,
  ingenuoDelCaso: fmt(ingenuo),
  porEventoConTopicos: fmt(ingenuoConTopicos),
  agregadoPorVentana: fmt(agregado),
  reduccionFrenteAlIngenuo: `${Math.round(ingenuo / agregado)}x`,
  porcentajeDelIngenuo: `${((agregado / ingenuo) * 100).toFixed(2)}%`
}, null, 2));
