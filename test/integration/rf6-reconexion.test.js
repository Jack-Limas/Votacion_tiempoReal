// Prueba A/B de RF-6. Requiere el entorno de docker-compose.
//   AB_DESCONEXION_MS (30000)  tiempo que B pasa desconectado
//   AB_ACTAS (30)              actas enviadas durante la desconexión
//   AB_ESPERADO                'replay' | 'snapshot': camino que debe tomar B
// Para forzar el camino snapshot: MAXLEN=20 docker compose up -d app y
// AB_ACTAS=250 AB_ESPERADO=snapshot npm run test:integration
import test from 'node:test';
import assert from 'node:assert/strict';
import { mulberry32 } from '../../src/results/catalogo.js';
import { corregirVotos, votosAleatorios } from '../../scripts/lib/actas.js';
import { conectar, createEstadoCliente } from '../../scripts/lib/cliente-sse.js';
import {
  BASE_URL,
  MOTIVO_SIN_ENTORNO,
  dormir,
  entornoDisponible,
  enviarAleatoria,
  enviarVotos,
  esperarHasta,
  mesasDonde
} from '../../scripts/lib/entorno.js';

const DESCONEXION_MS = Number(process.env.AB_DESCONEXION_MS ?? 30000);
const ACTAS_DURANTE = Number(process.env.AB_ACTAS ?? 30);
const ESPERADO = process.env.AB_ESPERADO;
const disponible = await entornoDisponible();

function cliente(nombre) {
  const estado = createEstadoCliente();
  const mensajes = [];
  return {
    nombre,
    estado,
    mensajes,
    onMessage: (mensaje) => {
      mensajes.push(mensaje);
      estado.aplicar(mensaje);
    }
  };
}

test('RF-6: B se desconecta, vuelve en OTRA instancia y queda idéntico a A', { skip: !disponible && MOTIVO_SIN_ENTORNO }, async () => {
  const random = mulberry32(Date.now() % 100000);
  const municipio = 7;
  const mesas = mesasDonde((m) => m.municipio === municipio);
  const topics = ['dep', `mun:${municipio}`, `pto:${mesas[0].puesto}`];
  const subs = topics.map((topic) => ({ topic }));

  const a = cliente('A');
  const b = cliente('B');
  const conexionA = await conectar(BASE_URL, subs, { onMessage: a.onMessage });
  let conexionB = await conectar(BASE_URL, subs, { onMessage: b.onMessage });
  try {
    const conSnapshot = (c) => topics.every((t) => c.mensajes.some((m) => m.t === 'snapshot' && m.topic === t));
    assert.ok(await esperarHasta(() => conSnapshot(a) && conSnapshot(b)), 'A y B deben recibir snapshot inicial');

    for (let i = 0; i < 5; i += 1) await enviarAleatoria(mesas[i % mesas.length], random);
    await dormir(2000);

    // --- B se desconecta ---
    conexionB.close();
    const instanciaAnterior = conexionB.instancia;
    const cursores = b.estado.cursores();
    const inicio = Date.now();

    // Durante la ausencia: actas del municipio y, al final, una corrección que
    // BAJA un candidato en una mesa del puesto seguido (debe llegar con motivo).
    const mesaCorregida = mesas[0];
    const votosOriginales = votosAleatorios(random, 300);
    await enviarVotos(mesaCorregida, votosOriginales);
    for (let i = 0; i < ACTAS_DURANTE; i += 1) {
      await enviarAleatoria(mesas[1 + Math.floor(random() * (mesas.length - 1))], random);
      await dormir(Math.max(0, (DESCONEXION_MS - 8000) / ACTAS_DURANTE - 20));
    }
    await dormir(1500);
    const { acta: correccion } = await enviarVotos(mesaCorregida, corregirVotos(votosOriginales, random));
    await dormir(Math.max(0, DESCONEXION_MS - (Date.now() - inicio)));

    // --- B vuelve con sus cursores; debe caer en otra instancia ---
    const mensajesAntes = b.mensajes.length;
    for (let intento = 0; intento < 20; intento += 1) {
      conexionB = await conectar(BASE_URL, cursores, { onMessage: b.onMessage });
      if (conexionB.instancia !== instanciaAnterior) break;
      conexionB.close();
      await dormir(50);
    }
    assert.notEqual(conexionB.instancia, instanciaAnterior, 'B debía reconectar en otra instancia');
    assert.ok(await esperarHasta(() => b.mensajes.length > mensajesAntes || ESPERADO === 'replay', { timeout: 3000 }));

    for (let i = 0; i < 3; i += 1) await enviarAleatoria(mesas[1 + i], random);
    await dormir(2500);

    // --- Comparación por hash ---
    const reanudacion = b.mensajes.slice(mensajesAntes);
    const camino = topics.map((t) => reanudacion.find((m) => m.topic === t)?.t === 'snapshot' ? 'snapshot' : 'replay');
    console.log(`  B: ${instanciaAnterior} -> ${conexionB.instancia}, ausente ${Math.round((Date.now() - inicio) / 1000)} s, camino por tópico: ${topics.map((t, i) => `${t}=${camino[i]}`).join(' ')}`);
    for (const topic of topics) {
      console.log(`  ${topic}: hash A ${a.estado.hash(topic).slice(0, 12)} / hash B ${b.estado.hash(topic).slice(0, 12)}`);
      assert.equal(b.estado.hash(topic), a.estado.hash(topic), `estado distinto en ${topic}`);
    }
    if (ESPERADO) assert.deepEqual(camino, topics.map(() => ESPERADO));

    // --- La corrección que bajó un número llegó con su motivo ---
    const motivo = { tipo: 'correccion', acta: correccion.acta, version: correccion.version };
    const conMotivo = (m) => m.topic === `pto:${mesaCorregida.puesto}` && m.motivo?.version === correccion.version;
    const deltaA = a.mensajes.find(conMotivo);
    assert.deepEqual(deltaA?.motivo, motivo, 'A debía recibir el delta de la corrección con su motivo');
    console.log(`  motivo recibido por A: ${JSON.stringify(deltaA.motivo)}`);
    if (camino[2] === 'replay') {
      assert.deepEqual(reanudacion.find(conMotivo)?.motivo, motivo, 'el replay de B debía conservar el motivo');
    }
  } finally {
    conexionA.close();
    conexionB.close();
  }
});
