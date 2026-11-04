// Prueba de carga (RF-4): abre N conexiones SSE contra el balanceador, corre el
// simulador de actas y guarda en CSV, por segundo y por instancia, el retraso
// del Event Loop y los mensajes emitidos.
//
// Uso (dentro de la red de docker-compose, ver README):
//   node scripts/carga.js --conexiones=10000 --procesos=4 --velocidad=30 --desde=0 --hasta=40
//     [--mezcla=dep:0.5,mun:0.35,pto:0.15] [--url=http://nginx] [--reset=1] [--etiqueta=x]
import { fork } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { pathToFileURL } from 'node:url';
import { createClient } from 'redis';
import { generarCatalogo, mulberry32, sembrarCatalogo } from '../src/results/catalogo.js';
import { crearClaves } from '../src/results/claves.js';
import { subParam } from './lib/cliente-sse.js';
import { simular } from './simular-actas.js';

const dormir = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function leerOpciones(argv) {
  const valores = Object.fromEntries(argv
    .filter((arg) => arg.startsWith('--'))
    .map((arg) => arg.slice(2).split('=')));
  return {
    rol: valores.rol ?? 'principal',
    url: valores.url ?? process.env.BASE_URL ?? 'http://localhost:8080',
    redisUrl: valores.redis ?? process.env.REDIS_URL ?? 'redis://localhost:6379',
    conexiones: Number(valores.conexiones ?? 10000),
    procesos: Number(valores.procesos ?? 4),
    mezcla: valores.mezcla ?? 'dep:0.5,mun:0.35,pto:0.15',
    velocidad: Number(valores.velocidad ?? 30),
    desde: Number(valores.desde ?? 0),
    hasta: Number(valores.hasta ?? 40),
    reset: valores.reset !== '0',
    salida: valores.salida ?? 'resultados',
    etiqueta: valores.etiqueta ?? '',
    semilla: Number(valores.semilla ?? 1)
  };
}

// Elige el tópico de cada conexión según la mezcla; municipios y puestos se
// eligen con probabilidad proporcional a sus mesas (donde está la gente).
function elegirTopicos(cantidad, mezcla, semilla) {
  const random = mulberry32(semilla);
  const { mesas } = generarCatalogo();
  const partes = mezcla.split(',').map((p) => p.split(':')).map(([tipo, peso]) => [tipo, Number(peso)]);
  const total = partes.reduce((acc, [, peso]) => acc + peso, 0);
  const topicos = [];
  for (let i = 0; i < cantidad; i += 1) {
    let r = random() * total;
    const [tipo] = partes.find(([, peso]) => (r -= peso) < 0) ?? partes[0];
    const mesa = mesas[Math.floor(random() * mesas.length)];
    topicos.push(tipo === 'dep' ? 'dep' : tipo === 'mun' ? `mun:${mesa.municipio}` : `pto:${mesa.puesto}`);
  }
  return topicos;
}

// ---------- Proceso trabajador: mantiene conexiones y cuenta lo recibido ----------
async function trabajador(opts) {
  const topicos = elegirTopicos(opts.conexiones, opts.mezcla, opts.semilla);
  const stats = { abiertas: 0, fallidas: 0, cerradas: 0, mensajes: 0, bytes: 0 };
  const agente = new http.Agent({ keepAlive: false, maxSockets: Infinity });

  function abrir(topic) {
    return new Promise((resolve) => {
      const req = http.get(new URL(`/stream?sub=${subParam([{ topic }])}`, opts.url), { agent: agente }, (res) => {
        if (res.statusCode !== 200) {
          stats.fallidas += 1;
          res.resume();
          resolve();
          return;
        }
        stats.abiertas += 1;
        res.setEncoding('utf8');
        // Cuenta mensajes sin parsear JSON. Se arrastran 5 caracteres por si
        // 'data: ' queda partido entre dos fragmentos.
        let resto = '';
        res.on('data', (chunk) => {
          stats.bytes += chunk.length;
          const texto = resto + chunk;
          for (let i = texto.indexOf('data: '); i !== -1; i = texto.indexOf('data: ', i + 6)) {
            stats.mensajes += 1;
          }
          resto = texto.slice(-5);
        });
        res.on('close', () => {
          stats.cerradas += 1;
        });
        resolve();
      });
      req.on('error', () => {
        stats.fallidas += 1;
        resolve();
      });
    });
  }

  for (let i = 0; i < topicos.length; i += 200) {
    await Promise.all(topicos.slice(i, i + 200).map(abrir));
  }
  process.send({ tipo: 'listo', stats });
  process.on('message', (mensaje) => {
    if (mensaje === 'stats') process.send({ tipo: 'stats', stats });
    if (mensaje === 'salir') process.exit(0);
  });
}

// ---------- Proceso principal ----------
async function leerCluster(url) {
  const response = await fetch(new URL('/metrics/all', url));
  return response.json();
}

function percentil(valores, p) {
  if (valores.length === 0) return null;
  const orden = [...valores].sort((a, b) => a - b);
  return orden[Math.min(orden.length - 1, Math.floor((p / 100) * orden.length))];
}

async function principal(opts) {
  if (opts.reset) {
    const redis = createClient({ url: opts.redisUrl });
    await redis.connect();
    await redis.flushDb();
    await sembrarCatalogo(redis, crearClaves(process.env.REDIS_KEY_PREFIX ?? 'electoral:{results}').catalogo);
    await redis.quit();
  }

  console.log(`Abriendo ${opts.conexiones} conexiones SSE en ${opts.procesos} procesos contra ${opts.url}...`);
  const porProceso = Math.ceil(opts.conexiones / opts.procesos);
  const hijos = [];
  const listos = [];
  for (let p = 0; p < opts.procesos; p += 1) {
    const cantidad = Math.min(porProceso, opts.conexiones - p * porProceso);
    const hijo = fork(process.argv[1], [
      '--rol=trabajador', `--url=${opts.url}`, `--conexiones=${cantidad}`,
      `--mezcla=${opts.mezcla}`, `--semilla=${opts.semilla * 100 + p}`
    ]);
    hijos.push(hijo);
    listos.push(new Promise((resolve) => hijo.once('message', (m) => resolve(m.stats))));
  }
  const abiertas = (await Promise.all(listos)).reduce((acc, s) => acc + s.abiertas, 0);
  console.log(`Conexiones abiertas: ${abiertas}. Estabilizando...`);
  await dormir(4000);

  const base = await leerCluster(opts.url);
  const filas = [];
  let segundo = 0;
  let simulando = true;
  const muestreo = (async () => {
    while (simulando) {
      await dormir(1000);
      segundo += 1;
      const { instancias } = await leerCluster(opts.url);
      for (const m of instancias) {
        filas.push({
          segundo,
          instancia: m.instancia,
          modo: m.modo,
          conexiones: m.conexiones,
          el_p50_ms: m.eventLoop.p50,
          el_p99_ms: m.eventLoop.p99,
          el_max_ms: m.eventLoop.max,
          mensajes_por_s: m.mensajes.porSegundo,
          mensajes_total: m.mensajes.total,
          ingenuo_total: m.mensajes.ingenuoTotal,
          eventos: m.eventos,
          retraso_pub_p50_ms: m.retrasoPublicacion.p50,
          retraso_pub_p99_ms: m.retrasoPublicacion.p99
        });
      }
    }
  })();

  console.log(`Simulando minutos ${opts.desde}-${opts.hasta} de la jornada a x${opts.velocidad}...`);
  const actas = await simular({
    url: opts.url, velocidad: opts.velocidad, desde: opts.desde, hasta: opts.hasta,
    limite: Infinity, semilla: 7, concurrencia: 64
  });
  await dormir(3000);
  simulando = false;
  await muestreo;
  const final = await leerCluster(opts.url);

  const recibidos = await Promise.all(hijos.map((hijo) => new Promise((resolve) => {
    hijo.once('message', (m) => resolve(m.stats));
    hijo.send('stats');
  })));
  for (const hijo of hijos) hijo.send('salir');

  const delta = (fn) => fn(final.totales) - fn(base.totales);
  const activas = filas.filter((f) => f.mensajes_por_s > 0 || f.el_p99_ms > 0);
  const modo = final.instancias[0]?.modo;
  const emitidos = delta((t) => t.mensajesEmitidos);
  const ingenuo = delta((t) => t.mensajesIngenuos);
  const resumen = {
    modo,
    conexiones: final.totales.conexiones,
    instancias: final.totales.instancias,
    actas,
    eventosPorInstancia: final.totales.eventosPorInstancia.map((e, i) => e - (base.totales.eventosPorInstancia[i] ?? 0)),
    mensajesEmitidos: emitidos,
    mensajesIngenuos: ingenuo,
    reduccion: emitidos > 0 ? Math.round(ingenuo / emitidos) : null,
    mensajesRecibidosPorClientes: recibidos.reduce((acc, s) => acc + s.mensajes, 0),
    conexionesCerradasDuranteLaPrueba: recibidos.reduce((acc, s) => acc + s.cerradas, 0),
    eventLoopPorSegundo: {
      p50DeLosP99: percentil(activas.map((f) => f.el_p99_ms), 50),
      p99DeLosP99: percentil(activas.map((f) => f.el_p99_ms), 99),
      maxDeLosP99: Math.max(...activas.map((f) => f.el_p99_ms)),
      max: Math.max(...activas.map((f) => f.el_max_ms))
    },
    eventLoopDesdeArranque: final.instancias.map((m) => ({ instancia: m.instancia, ...m.eventLoopTotal })),
    retrasoPublicacionMs: final.instancias.map((m) => ({ instancia: m.instancia, ...m.retrasoPublicacion.total })),
    segundos: segundo
  };

  mkdirSync(opts.salida, { recursive: true });
  const nombre = `${opts.salida}/carga-${modo}-${final.totales.conexiones}${opts.etiqueta ? `-${opts.etiqueta}` : ''}`;
  const columnas = Object.keys(filas[0] ?? {});
  writeFileSync(`${nombre}.csv`, [columnas.join(','), ...filas.map((f) => columnas.map((c) => f[c] ?? '').join(','))].join('\n'));
  writeFileSync(`${nombre}.json`, JSON.stringify(resumen, null, 2));
  console.log(JSON.stringify(resumen, null, 2));
  console.log(`CSV: ${nombre}.csv`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const opts = leerOpciones(process.argv.slice(2));
  const tarea = opts.rol === 'trabajador' ? trabajador(opts) : principal(opts);
  tarea.catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
