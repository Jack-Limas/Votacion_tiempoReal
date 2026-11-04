// Nombres de claves Redis del módulo público. Todas llevan el prefijo con hash
// tag ({results}) para que los scripts Lua sigan siendo válidos en Redis Cluster.
export function crearClaves(prefix) {
  return {
    stream: `${prefix}:st:eventos`,
    catalogo: `${prefix}:catalogo`,
    // dep -> acc:dep, mun:11 -> acc:mun:11, pto:176 -> acc:pto:176
    acumulado: (topic) => `${prefix}:acc:${topic}`,
    mesa: (id) => `${prefix}:acc:mesa:${id}`,
    metricas: (instancia) => `${prefix}:metrics:${instancia}`,
    patronMetricas: `${prefix}:metrics:*`
  };
}
