// Continuidad (RF-6). El cursor del cliente es el ID del stream de Redis, igual
// en todas las instancias: por eso puede reconectar en cualquiera.
//   - Tópico sin cursor: snapshot.
//   - Cursor fuera de la retención, o más de MAX_REPLAY eventos pendientes:
//     snapshot.
//   - Si no: XRANGE (cursor +, filtrado por tópico y reducido a UN delta.
import { readFileSync } from 'node:fs';
import { entregar, mensajeDelta, reducirEntradas, serializar } from './fanout.js';
import { compareIds, decodeEntry, parseId } from './stream.js';
import { PENDIENTE } from './topics.js';

const SNAPSHOT_SCRIPT = readFileSync(new URL('../lua/snapshot.lua', import.meta.url), 'utf8');

export function decidirReanudacion({ cursor, primerId, ultimoId, pendientes, maxReplay }) {
  if (!cursor) return 'snapshot';
  // Stream vacío o borrado, o cursor de un stream anterior: no hay detalle fiable.
  if (!ultimoId || compareIds(cursor, ultimoId) > 0) return 'snapshot';
  // Lo que siguió al cursor pudo haberse recortado (MAXLEN).
  if (compareIds(cursor, primerId) < 0) return 'snapshot';
  if (pendientes > maxReplay) return 'snapshot';
  return 'replay';
}

function aEstado(plano) {
  const estado = {};
  for (let i = 0; i < plano.length; i += 2) {
    estado[plano[i]] = Number(plano[i + 1]);
  }
  return estado;
}

export function createResume({ redis, claves, registry, metrics, maxReplay = 500 }) {
  async function leerSnapshot(topics) {
    const [upTo, ...hashes] = await redis.eval(SNAPSHOT_SCRIPT, {
      keys: [claves.stream, ...topics.map((topic) => claves.acumulado(topic))]
    });
    return { upTo, estados: hashes.map(aEstado) };
  }

  async function reanudar(conn, subscriptions) {
    // La conexión entra al registro ANTES de leer Redis, con el cursor en
    // PENDIENTE: los deltas en vivo que lleguen mientras tanto se guardan y se
    // filtran por upTo al final, así no se pierde nada entre lectura y alta.
    conn.pending = [];
    for (const { topic } of subscriptions) {
      registry.subscribe(conn, topic, PENDIENTE);
    }

    const conCursor = subscriptions.filter((s) => s.cursor);
    const aSnapshot = subscriptions.filter((s) => !s.cursor).map((s) => s.topic);
    for (let i = 0; i < aSnapshot.length; i += 1) metrics.reconexion('inicial');

    if (conCursor.length > 0) {
      const minimo = conCursor.reduce((min, s) => (compareIds(s.cursor, min) < 0 ? s.cursor : min), conCursor[0].cursor);
      // Las tres lecturas en MULTI: el rango y los extremos son del mismo instante.
      const [entradas, primero, ultimo] = await redis.multi()
        .xRange(claves.stream, `(${minimo}`, '+', { COUNT: maxReplay + 1 })
        .xRange(claves.stream, '-', '+', { COUNT: 1 })
        .xRevRange(claves.stream, '+', '-', { COUNT: 1 })
        .exec();
      const primerId = primero[0]?.id ?? null;
      const ultimoId = ultimo[0]?.id ?? null;
      const truncado = entradas.length > maxReplay;
      const eventos = entradas.map(decodeEntry);

      for (const { topic, cursor } of conCursor) {
        const posteriores = eventos.filter((e) => compareIds(e.id, cursor) > 0);
        const decision = decidirReanudacion({
          cursor, primerId, ultimoId, pendientes: truncado ? Infinity : posteriores.length, maxReplay
        });
        metrics.reconexion(decision);
        if (decision === 'snapshot') {
          aSnapshot.push(topic);
          continue;
        }
        conn.cursors.set(topic, parseId(ultimoId));
        const { cambios, motivo, eventos: cantidad } = reducirEntradas(posteriores, topic);
        if (cantidad > 0) {
          conn.send(serializar(mensajeDelta(topic, ultimoId, cambios, motivo)));
          metrics.mensajesEmitidos(1);
        }
      }
    }

    if (aSnapshot.length > 0) {
      const { upTo, estados } = await leerSnapshot(aSnapshot);
      aSnapshot.forEach((topic, i) => {
        conn.cursors.set(topic, parseId(upTo));
        conn.send(serializar({ t: 'snapshot', topic, upTo, estado: estados[i] }));
      });
      metrics.mensajesEmitidos(aSnapshot.length);
    }

    const pendientes = conn.pending;
    conn.pending = null;
    for (const [topic, upTo, frame] of pendientes) {
      if (entregar(conn, topic, upTo, frame)) metrics.mensajesEmitidos(1);
    }
  }

  return { reanudar, leerSnapshot };
}
