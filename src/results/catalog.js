// Deterministic synthetic catalog of the department: 1,842 puestos and 6,400
// mesas spread over municipalities. Used by the simulator, the tests and the
// apply script (total mesas per topic, for the reported percentage).

export const MUNICIPALITY_COUNT = 40;
export const PUESTO_COUNT = 1842;
export const MESA_COUNT = 6400;
export const CANDIDATES = ['C1', 'C2', 'C3', 'C4', 'BLANCO', 'NULO', 'NO_MARCADO'];

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

// Splits `total` units according to `weights`, guaranteeing `minimum` per slot.
function distribute(total, weights, minimum) {
  const free = total - minimum * weights.length;
  const sum = weights.reduce((acc, weight) => acc + weight, 0);
  const shares = weights.map((weight) => minimum + Math.floor((free * weight) / sum));
  let rest = total - shares.reduce((acc, share) => acc + share, 0);
  for (let i = 0; rest > 0; i = (i + 1) % shares.length, rest -= 1) {
    shares[i] += 1;
  }
  return shares;
}

let cache = null;

export function buildCatalog() {
  if (cache) return cache;
  const random = mulberry32(2026);

  // Municipality 1 is the capital: about a third of the puestos.
  const municipalityWeights = Array.from({ length: MUNICIPALITY_COUNT }, (_, i) => (
    i === 0 ? 30 : 0.5 + random() * 2.5
  ));
  const puestosPerMunicipality = distribute(PUESTO_COUNT, municipalityWeights, 5);

  const puestos = [];
  let puestoId = 1;
  puestosPerMunicipality.forEach((count, index) => {
    for (let i = 0; i < count; i += 1) {
      puestos.push({ id: puestoId, municipio: index + 1, urban: index === 0 || random() < 0.3 });
      puestoId += 1;
    }
  });

  const mesasPerPuesto = distribute(
    MESA_COUNT,
    puestos.map((puesto) => (puesto.urban ? 3 : 1) * (0.5 + random())),
    1
  );

  const mesas = [];
  let mesaId = 1;
  puestos.forEach((puesto, index) => {
    puesto.mesas = [];
    for (let i = 0; i < mesasPerPuesto[index]; i += 1) {
      mesas.push({
        id: mesaId,
        number: i + 1,
        puesto: puesto.id,
        municipio: puesto.municipio,
        urban: puesto.urban
      });
      puesto.mesas.push(mesaId);
      mesaId += 1;
    }
  });

  // Total mesas per topic: dep, mun:{id}, pto:{id}.
  const totals = { dep: MESA_COUNT };
  for (const puesto of puestos) {
    totals[`pto:${puesto.id}`] = puesto.mesas.length;
    const municipalityKey = `mun:${puesto.municipio}`;
    totals[municipalityKey] = (totals[municipalityKey] ?? 0) + puesto.mesas.length;
  }

  cache = { puestos, mesas, totals };
  return cache;
}

export async function seedCatalog(redis, key) {
  const { totals } = buildCatalog();
  await redis.hSet(key, totals);
}
