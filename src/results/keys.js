// Redis key names of the results module. All of them carry the hash-tagged
// prefix ({results}) so the Lua scripts stay valid on Redis Cluster.
export function createKeys(prefix) {
  return {
    stream: `${prefix}:st:events`,
    catalog: `${prefix}:catalog`,
    // dep -> acc:dep, mun:11 -> acc:mun:11, pto:176 -> acc:pto:176
    total: (topic) => `${prefix}:acc:${topic}`,
    mesa: (id) => `${prefix}:acc:mesa:${id}`,
    // Candidate ranking per topic (ZSET candidate -> votes).
    rank: (topic) => `${prefix}:rank:${topic}`,
    // Last verification decision applied per case, with its fencing token.
    decision: (caseId) => `${prefix}:decision:${caseId}`,
    decisionRequest: (caseId, token) => `${prefix}:decision-request:${caseId}:${token}`,
    metrics: (instance) => `${prefix}:metrics:${instance}`,
    metricsPattern: `${prefix}:metrics:*`
  };
}
