// Catálogo sintético y determinista del departamento (stub): 1.842 puestos y
// 6.400 mesas repartidos en municipios. Lo usan el simulador, las pruebas y el
// script aplicarActa (total de mesas por tópico para el porcentaje informado).

export const TOTAL_MUNICIPIOS = 40;
export const TOTAL_PUESTOS = 1842;
export const TOTAL_MESAS = 6400;
export const CANDIDATOS = ['C1', 'C2', 'C3', 'C4', 'BLANCO', 'NULO', 'NO_MARCADO'];

export function mulberry32(seed) {
  let state = seed >>> 0;
  return function random() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Reparte `total` unidades según `pesos`, garantizando `minimo` por posición.
function repartir(total, pesos, minimo) {
  const libre = total - minimo * pesos.length;
  const suma = pesos.reduce((acc, peso) => acc + peso, 0);
  const cuotas = pesos.map((peso) => minimo + Math.floor((libre * peso) / suma));
  let resto = total - cuotas.reduce((acc, cuota) => acc + cuota, 0);
  for (let i = 0; resto > 0; i = (i + 1) % cuotas.length, resto -= 1) {
    cuotas[i] += 1;
  }
  return cuotas;
}

let cache = null;

export function generarCatalogo() {
  if (cache) return cache;
  const random = mulberry32(2026);

  // El municipio 1 es la capital: concentra cerca de un tercio de los puestos.
  const pesosMunicipio = Array.from({ length: TOTAL_MUNICIPIOS }, (_, i) => (
    i === 0 ? 30 : 0.5 + random() * 2.5
  ));
  const puestosPorMunicipio = repartir(TOTAL_PUESTOS, pesosMunicipio, 5);

  const puestos = [];
  let puestoId = 1;
  puestosPorMunicipio.forEach((cantidad, indice) => {
    for (let i = 0; i < cantidad; i += 1) {
      puestos.push({ id: puestoId, municipio: indice + 1, urbano: indice === 0 || random() < 0.3 });
      puestoId += 1;
    }
  });

  const mesasPorPuesto = repartir(
    TOTAL_MESAS,
    puestos.map((puesto) => (puesto.urbano ? 3 : 1) * (0.5 + random())),
    1
  );

  const mesas = [];
  let mesaId = 1;
  puestos.forEach((puesto, indice) => {
    puesto.mesas = [];
    for (let i = 0; i < mesasPorPuesto[indice]; i += 1) {
      mesas.push({
        id: mesaId,
        numero: i + 1,
        puesto: puesto.id,
        municipio: puesto.municipio,
        urbano: puesto.urbano
      });
      puesto.mesas.push(mesaId);
      mesaId += 1;
    }
  });

  // Total de mesas por tópico: dep, mun:{id}, pto:{id}.
  const totales = { dep: TOTAL_MESAS };
  for (const puesto of puestos) {
    totales[`pto:${puesto.id}`] = puesto.mesas.length;
    const claveMunicipio = `mun:${puesto.municipio}`;
    totales[claveMunicipio] = (totales[claveMunicipio] ?? 0) + puesto.mesas.length;
  }

  cache = { puestos, mesas, totales };
  return cache;
}

export async function sembrarCatalogo(redis, clave) {
  const { totales } = generarCatalogo();
  await redis.hSet(clave, totales);
}
