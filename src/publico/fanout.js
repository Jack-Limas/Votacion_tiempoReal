// Fan-out de eventos del stream hacia las conexiones locales suscritas (RF-4).
//
// batched (por defecto): los eventos NO se envían al llegar. Se acumulan en un
//   mapa de tópicos sucios (último valor absoluto por campo + motivo) y cada
//   FLUSH_MS se arma UN mensaje por tópico, se serializa UNA vez y se envía en
//   lotes de BATCH_SIZE cediendo el Event Loop con setImmediate entre lotes.
// naive: cada evento se envía a cada suscriptor en un bucle síncrono (solo para
//   comparar en la prueba de carga).
//
// En ambos modos el costo es O(k) por tópico (k = suscriptores locales), no
// O(conexiones de la instancia).
import { compareIds, parseId } from './stream.js';

// setImmediate cede de verdad: deja correr I/O (sockets, timers) antes de
// seguir. process.nextTick y las promesas resueltas son microtareas y se
// ejecutan antes de volver al Event Loop, así que no cederían nada.
export const ceder = () => new Promise((resolve) => setImmediate(resolve));

export function serializar(mensaje) {
  return `data: ${JSON.stringify(mensaje)}\n\n`;
}

export function mensajeDelta(topic, upTo, cambios, motivo) {
  const mensaje = { t: 'delta', topic, upTo, cambios };
  if (motivo) mensaje.motivo = motivo;
  return mensaje;
}

// Una corrección que baja algún número viaja con su explicación para la UI.
export function motivoDe(evento) {
  return evento.tipo === 'correccion' && evento.baja
    ? { tipo: 'correccion', acta: evento.acta, version: evento.version }
    : null;
}

// Acumula un evento sobre el delta de un tópico: último valor absoluto por campo.
export function acumular(destino, evento, topic) {
  const cambios = evento.cambios[topic];
  if (!cambios) return false;
  Object.assign(destino.cambios, cambios);
  const motivo = motivoDe(evento);
  if (motivo) destino.motivo = motivo;
  return true;
}

// Reduce varias entradas del stream a UN delta para un tópico.
export function reducirEntradas(entries, topic) {
  const destino = { cambios: {}, motivo: null, eventos: 0 };
  for (const { evento } of entries) {
    if (acumular(destino, evento, topic)) destino.eventos += 1;
  }
  return destino;
}

// Entrega un frame ya serializado si avanza el cursor de la conexión en ese
// tópico. upTo <= cursor se descarta: evita retrocesos cuando la conexión ya
// tiene un estado más nuevo que el lector de la instancia.
export function entregar(conn, topic, upTo, frame) {
  const cursor = conn.cursors.get(topic);
  if (cursor && compareIds(upTo, cursor) <= 0) return false;
  conn.cursors.set(topic, upTo);
  conn.send(frame);
  return true;
}

const metricasNulas = {
  eventoProcesado() {},
  mensajesEmitidos() {},
  descartados() {},
  retraso() {}
};

export function createFanout({
  registry,
  mode = 'batched',
  flushMs = 1000,
  batchSize = 100,
  heartbeatMs = 15000,
  metrics = metricasNulas,
  logger = console
}) {
  let sucios = new Map();
  let recepciones = [];
  let ultimoId = null;
  let enCurso = null;
  let pendiente = false;
  const timers = [];

  function acumularEvento(evento) {
    let interesa = false;
    for (const topic of Object.keys(evento.cambios)) {
      // Un tópico sin suscriptores locales no cuesta más que un Map.get.
      if (registry.subscribers(topic).size === 0) continue;
      let sucio = sucios.get(topic);
      if (!sucio) {
        sucio = { cambios: {}, motivo: null };
        sucios.set(topic, sucio);
      }
      acumular(sucio, evento, topic);
      interesa = true;
    }
    if (interesa) recepciones.push(evento.ts_recepcion);
  }

  function enviarIngenuo(id, evento) {
    const upTo = parseId(id);
    let enviados = 0;
    let descartados = 0;
    for (const [topic, cambios] of Object.entries(evento.cambios)) {
      const subscribers = registry.subscribers(topic);
      if (subscribers.size === 0) continue;
      const frame = serializar(mensajeDelta(topic, id, cambios, motivoDe(evento)));
      for (const conn of subscribers) {
        if (entregar(conn, topic, upTo, frame)) enviados += 1;
        else descartados += 1;
      }
    }
    if (enviados > 0) {
      metrics.mensajesEmitidos(enviados);
      metrics.retraso(Date.now() - evento.ts_recepcion);
    }
    metrics.descartados(descartados);
  }

  async function vaciar() {
    // Se toma la ventana completa y se deja un mapa nuevo para lo que llegue
    // mientras se envía.
    const ventana = sucios;
    const ventanaRecepciones = recepciones;
    const upToTexto = ultimoId;
    sucios = new Map();
    recepciones = [];
    if (ventana.size === 0) return;

    const upTo = parseId(upToTexto);
    let enLote = 0;
    let enviados = 0;
    let descartados = 0;
    for (const [topic, { cambios, motivo }] of ventana) {
      const subscribers = registry.subscribers(topic);
      if (subscribers.size === 0) continue;
      const frame = serializar(mensajeDelta(topic, upToTexto, cambios, motivo));
      for (const conn of subscribers) {
        if (entregar(conn, topic, upTo, frame)) enviados += 1;
        else descartados += 1;
        enLote += 1;
        if (enLote >= batchSize) {
          enLote = 0;
          await ceder();
        }
      }
    }
    metrics.mensajesEmitidos(enviados);
    metrics.descartados(descartados);
    const ahora = Date.now();
    for (const ts of ventanaRecepciones) metrics.retraso(ahora - ts);
  }

  // Los flushes no se solapan: si toca uno mientras el anterior sigue enviando,
  // la ventana nueva sigue acumulando y se envía en cuanto termina el anterior.
  function flush() {
    if (enCurso) {
      pendiente = true;
      return enCurso;
    }
    enCurso = (async () => {
      try {
        do {
          pendiente = false;
          await vaciar();
        } while (pendiente);
      } catch (error) {
        logger.error('Error en el flush del fan-out:', error);
      } finally {
        enCurso = null;
      }
    })();
    return enCurso;
  }

  // Comentario SSE periódico para que el balanceador no cierre conexiones
  // inactivas (el ALB corta a los 60 s por defecto). También va por lotes.
  async function latido() {
    let enLote = 0;
    for (const conn of registry.connections) {
      conn.send(': latido\n\n');
      enLote += 1;
      if (enLote >= batchSize) {
        enLote = 0;
        await ceder();
      }
    }
  }

  return {
    mode,
    onEntries(entries) {
      for (const { id, evento } of entries) {
        metrics.eventoProcesado(registry.connections.size);
        ultimoId = id;
        if (mode === 'naive') {
          enviarIngenuo(id, evento);
        } else {
          acumularEvento(evento);
        }
      }
    },
    flush,
    start() {
      if (mode === 'batched') timers.push(setInterval(flush, flushMs));
      timers.push(setInterval(() => {
        latido().catch((error) => logger.error('Error enviando latidos:', error));
      }, heartbeatMs));
    },
    stop() {
      for (const timer of timers.splice(0)) clearInterval(timer);
    }
  };
}
