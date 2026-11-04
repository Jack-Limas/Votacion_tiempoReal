// Métricas por instancia (RT-4 / RT-7). Se exponen en /metrics y se publican
// en Redis cada segundo con TTL corto, para leerlas de todas las instancias sin
// que estas se comuniquen entre sí.
import { createHistogram, monitorEventLoopDelay } from 'node:perf_hooks';

const NS_POR_MS = 1e6;
const redondear = (valor) => Math.round(valor * 100) / 100;

export function createMetrics({
  instanceId,
  mode,
  registry,
  redis,
  claves,
  resolutionMs = 10,
  publishMs = 1000,
  ttlSeconds = 5,
  logger = console
}) {
  // monitorEventLoopDelay registra el intervalo entre disparos de su timer
  // interno, que incluye la propia resolución: se resta para obtener el retraso.
  // reset() descarta la primera muestra posterior, así que además se mantiene
  // un segundo histograma que nunca se reinicia (retraso desde el arranque).
  const loopSegundo = monitorEventLoopDelay({ resolution: resolutionMs });
  const loopTotal = monitorEventLoopDelay({ resolution: resolutionMs });
  const retrasoSegundo = createHistogram();
  const retrasoTotal = createHistogram();

  const contadores = {
    eventos: 0,
    mensajes: 0,
    ingenuo: 0,
    descartados: 0,
    reconexiones: { replay: 0, snapshot: 0, inicial: 0 }
  };
  let mensajesUltimoSegundo = 0;
  let ultimo = null;
  let timer = null;

  function retrasoLoop(histograma) {
    const ms = (valor) => redondear(Math.max(0, valor / NS_POR_MS - resolutionMs));
    return {
      p50: ms(histograma.percentile(50)),
      p99: ms(histograma.percentile(99)),
      max: ms(histograma.max)
    };
  }

  function percentiles(histograma) {
    return histograma.count === 0
      ? { p50: null, p99: null }
      : { p50: histograma.percentile(50), p99: histograma.percentile(99) };
  }

  function tomar() {
    const memoria = process.memoryUsage();
    const conexiones = registry.connections.size;
    const muestra = {
      instancia: instanceId,
      modo: mode,
      ts: Date.now(),
      conexiones,
      topicos: registry.topicCount,
      suscriptores: registry.counts(),
      eventLoop: retrasoLoop(loopSegundo),
      eventLoopTotal: retrasoLoop(loopTotal),
      mensajes: {
        porSegundo: contadores.mensajes - mensajesUltimoSegundo,
        total: contadores.mensajes,
        ingenuoTotal: contadores.ingenuo,
        descartados: contadores.descartados
      },
      eventos: contadores.eventos,
      retrasoPublicacion: {
        ...percentiles(retrasoSegundo),
        total: percentiles(retrasoTotal),
        muestras: retrasoTotal.count
      },
      reconexiones: { ...contadores.reconexiones },
      memoria: {
        rssMB: redondear(memoria.rss / 2 ** 20),
        heapMB: redondear(memoria.heapUsed / 2 ** 20)
      }
    };
    mensajesUltimoSegundo = contadores.mensajes;
    loopSegundo.reset();
    retrasoSegundo.reset();
    return muestra;
  }

  async function publicar() {
    ultimo = tomar();
    try {
      await redis.set(claves.metricas(instanceId), JSON.stringify(ultimo), { EX: ttlSeconds });
    } catch (error) {
      logger.error('No se pudieron publicar las métricas en Redis:', error.message);
    }
  }

  return {
    eventoProcesado(conexiones) {
      contadores.eventos += 1;
      // Cifra ingenua: cada acta a cada conexión de la instancia.
      contadores.ingenuo += conexiones;
    },
    mensajesEmitidos(n) {
      contadores.mensajes += n;
    },
    descartados(n) {
      contadores.descartados += n;
    },
    retraso(ms) {
      const valor = Math.max(1, Math.round(ms));
      retrasoSegundo.record(valor);
      retrasoTotal.record(valor);
    },
    reconexion(tipo) {
      contadores.reconexiones[tipo] += 1;
    },
    local() {
      return {
        ...(ultimo ?? {}),
        instancia: instanceId,
        modo: mode,
        conexiones: registry.connections.size,
        topicos: registry.topicCount,
        suscriptores: registry.counts(),
        mensajes: { ...ultimo?.mensajes, total: contadores.mensajes, ingenuoTotal: contadores.ingenuo },
        eventos: contadores.eventos,
        reconexiones: { ...contadores.reconexiones }
      };
    },
    // offsetMs desfasa el reinicio por segundo del histograma respecto del
    // flush, para que un flush nunca empiece justo después de un reset().
    start(offsetMs = 0) {
      loopSegundo.enable();
      loopTotal.enable();
      timer = setTimeout(() => {
        timer = setInterval(() => void publicar(), publishMs);
      }, offsetMs);
    },
    stop() {
      clearTimeout(timer);
      clearInterval(timer);
      loopSegundo.disable();
      loopTotal.disable();
    }
  };
}

// Lee las métricas publicadas por todas las instancias (claves con TTL).
export async function leerMetricasCluster(redis, claves) {
  const llaves = [];
  for await (const clave of redis.scanIterator({ MATCH: claves.patronMetricas, COUNT: 100 })) {
    llaves.push(clave);
  }
  const valores = llaves.length > 0 ? await redis.mGet(llaves) : [];
  const instancias = valores.filter(Boolean).map((valor) => JSON.parse(valor))
    .sort((a, b) => a.instancia.localeCompare(b.instancia));
  const suma = (fn) => instancias.reduce((acc, m) => acc + fn(m), 0);
  return {
    instancias,
    totales: {
      instancias: instancias.length,
      conexiones: suma((m) => m.conexiones),
      eventosPorInstancia: instancias.map((m) => m.eventos),
      mensajesEmitidos: suma((m) => m.mensajes.total),
      mensajesIngenuos: suma((m) => m.mensajes.ingenuoTotal),
      mensajesPorSegundo: suma((m) => m.mensajes.porSegundo),
      reconexiones: {
        replay: suma((m) => m.reconexiones.replay),
        snapshot: suma((m) => m.reconexiones.snapshot),
        inicial: suma((m) => m.reconexiones.inicial)
      },
      eventLoopP99Max: Math.max(0, ...instancias.map((m) => m.eventLoop.p99))
    }
  };
}
