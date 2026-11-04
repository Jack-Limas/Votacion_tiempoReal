// Requiere el entorno de docker-compose (8 réplicas + nginx + Valkey).
import test from 'node:test';
import assert from 'node:assert/strict';
import { mulberry32 } from '../../src/results/catalogo.js';
import { conectar } from '../../scripts/lib/cliente-sse.js';
import {
  BASE_URL,
  MOTIVO_SIN_ENTORNO,
  dormir,
  entornoDisponible,
  enviarAleatoria,
  esperarHasta,
  mesasDonde,
  metricasDe
} from '../../scripts/lib/entorno.js';

const disponible = await entornoDisponible();

test('RF-5: /whoami responde instancias distintas (sin sesiones pegajosas)', { skip: !disponible && MOTIVO_SIN_ENTORNO }, async () => {
  const instancias = new Set();
  for (let i = 0; i < 16; i += 1) {
    const response = await fetch(new URL('/whoami', BASE_URL));
    instancias.add((await response.json()).instancia);
  }
  console.log(`  /whoami: ${instancias.size} instancias distintas en 16 peticiones`);
  assert.ok(instancias.size >= 2, 'Todas las peticiones cayeron en la misma instancia.');
});

test('RF-5: cada cliente recibe solo su tópico y al salir deja el contador en 0', { skip: !disponible && MOTIVO_SIN_ENTORNO }, async () => {
  const random = mulberry32(Date.now() % 100000);
  const delMunicipio = mesasDonde((m) => m.municipio === 11);
  const delPuesto = mesasDonde((m) => m.puesto === 176);
  const otras = mesasDonde((m) => m.municipio !== 11 && m.puesto !== 176 && m.municipio !== 1);

  const recibidosMun = [];
  const recibidosPto = [];
  const clienteMun = await conectar(BASE_URL, [{ topic: 'mun:11' }], { onMessage: (m) => recibidosMun.push(m) });
  const clientePto = await conectar(BASE_URL, [{ topic: 'pto:176' }], { onMessage: (m) => recibidosPto.push(m) });

  try {
    await dormir(300);
    for (let i = 0; i < 3; i += 1) {
      await enviarAleatoria(delMunicipio[i], random);
      await enviarAleatoria(delPuesto[i % delPuesto.length], random);
      await enviarAleatoria(otras[Math.floor(random() * otras.length)], random);
    }

    const llegaron = await esperarHasta(() => recibidosMun.length > 0 && recibidosPto.length > 0, { timeout: 8000 });
    assert.ok(llegaron, 'Los clientes no recibieron mensajes de su tópico.');
    await dormir(2500);

    assert.ok(recibidosMun.every((m) => m.topic === 'mun:11'), `mun:11 recibió otros tópicos: ${[...new Set(recibidosMun.map((m) => m.topic))]}`);
    assert.ok(recibidosPto.every((m) => m.topic === 'pto:176'), `pto:176 recibió otros tópicos: ${[...new Set(recibidosPto.map((m) => m.topic))]}`);
    console.log(`  mun:11 recibió ${recibidosMun.length} mensajes, pto:176 recibió ${recibidosPto.length}; ninguno ajeno`);

    const antes = await metricasDe(clienteMun.instancia);
    assert.ok(antes.suscriptores['mun:11'] >= 1);
  } finally {
    clienteMun.close();
    clientePto.close();
  }

  const limpio = await esperarHasta(async () => {
    const mun = await metricasDe(clienteMun.instancia);
    const pto = await metricasDe(clientePto.instancia);
    return !mun.suscriptores['mun:11'] && !pto.suscriptores['pto:176'];
  }, { timeout: 5000, intervalo: 200 });
  assert.ok(limpio, 'El cliente desconectado sigue en el mapa de tópicos.');
});
