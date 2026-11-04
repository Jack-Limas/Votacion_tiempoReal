// Lua scripts for the verification queue (RF-7), presence (RF-8) and rate
// limiting. Every script runs atomically inside Redis: no other command is
// interleaved, so two instances can never observe or produce a half-applied
// state. Time always comes from Redis (TIME) so instance clock skew cannot
// make a lease look alive on one server and expired on another.
//
// All keys share the {verification} hash tag (see config.keyPrefix), so they
// live in the same Redis Cluster slot. Scripts build per-case key names from
// the prefix; that is only valid because every name hashes to that one slot.

const PRELUDE = `
local P = ARGV[1]
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local NOW = now_ms()
local function case_key(id) return P .. ':case:' .. id end
local function load_case(id)
  local raw = redis.call('HGETALL', case_key(id))
  if #raw == 0 then return nil end
  local c = {}
  for i = 1, #raw, 2 do c[raw[i]] = raw[i + 1] end
  return c
end
local function emit(kind, c, extra)
  local version = redis.call('INCR', P .. ':version')
  local event = { type = kind, version = version, at = NOW, case = c }
  if extra then for k, v in pairs(extra) do event[k] = v end end
  redis.call('PUBLISH', P .. ':events', cjson.encode(event))
  return version
end
local function reply(t) return cjson.encode(t) end
local function check_owner(id, auditor, token)
  local c = load_case(id)
  if not c then return nil, 'not_found' end
  if c.status ~= 'claimed' then return nil, 'not_claimed' end
  if c.owner ~= auditor or c.token ~= token then return nil, 'not_owner' end
  if tonumber(c.leaseExpiresAt) <= NOW then return nil, 'lease_expired' end
  return c, nil
end
local function requeue(id, c)
  redis.call('HSET', case_key(id), 'status', 'queued', 'requeuedAt', NOW)
  redis.call('HDEL', case_key(id), 'owner', 'ownerName', 'token', 'claimedAt',
    'leaseExpiresAt', 'lastRenewedAt')
  redis.call('ZADD', P .. ':queue', c.priority, id)
  redis.call('ZREM', P .. ':leases', id)
  if redis.call('HGET', P .. ':owners', c.owner) == id then
    redis.call('HDEL', P .. ':owners', c.owner)
  end
end
`;

// ARGV: prefix, caseId, bonusMs, acta(json), idem, reason, sufragantes, totalVotos, retentionMs
export const INGEST = `${PRELUDE}
local id = ARGV[2]
if redis.call('EXISTS', case_key(id)) == 1 then return reply({ ok = false, code = 'duplicate' }) end
local priority = NOW - tonumber(ARGV[3])
redis.call('HSET', case_key(id),
  'id', id, 'status', 'queued', 'priority', priority, 'enqueuedAt', NOW,
  'acta', ARGV[4], 'idem', ARGV[5], 'reason', ARGV[6],
  'sufragantes', ARGV[7], 'totalVotos', ARGV[8])
redis.call('ZADD', P .. ':queue', priority, id)
redis.call('HINCRBY', P .. ':stats', 'ingested', 1)
emit('case.queued', load_case(id))
return reply({ ok = true })
`;

// ARGV: prefix, auditor, auditorName, leaseTtlMs, caseId ('' = highest priority)
export const CLAIM = `${PRELUDE}
local auditor = ARGV[2]
local held = redis.call('HGET', P .. ':owners', auditor)
if held then
  local h = load_case(held)
  if h and h.status == 'claimed' and h.owner == auditor and tonumber(h.leaseExpiresAt) > NOW then
    return reply({ ok = false, code = 'already_holding', case = h })
  end
end
local id = ARGV[5]
if id == '' then
  local first = redis.call('ZRANGE', P .. ':queue', 0, 0)
  if #first == 0 then return reply({ ok = false, code = 'queue_empty' }) end
  id = first[1]
end
-- ZREM is the exclusive gate: only one caller can remove the member.
if redis.call('ZREM', P .. ':queue', id) == 0 then
  return reply({ ok = false, code = 'not_available' })
end
local token = redis.call('INCR', P .. ':fence')
local expires = NOW + tonumber(ARGV[4])
redis.call('HSET', case_key(id), 'status', 'claimed', 'owner', auditor, 'ownerName', ARGV[3],
  'token', token, 'claimedAt', NOW, 'leaseExpiresAt', expires, 'lastRenewedAt', NOW)
redis.call('ZADD', P .. ':leases', expires, id)
redis.call('HSET', P .. ':owners', auditor, id)
local c = load_case(id)
emit('case.claimed', c)
return reply({ ok = true, case = c })
`;

// ARGV: prefix, caseId, auditor, token, leaseTtlMs
export const RENEW = `${PRELUDE}
local c, err = check_owner(ARGV[2], ARGV[3], ARGV[4])
if not c then return reply({ ok = false, code = err }) end
local expires = NOW + tonumber(ARGV[5])
redis.call('HSET', case_key(ARGV[2]), 'leaseExpiresAt', expires, 'lastRenewedAt', NOW)
redis.call('ZADD', P .. ':leases', 'XX', expires, ARGV[2])
return reply({ ok = true, leaseExpiresAt = expires })
`;

// ARGV: prefix, caseId, auditor, token
export const RELEASE = `${PRELUDE}
local id = ARGV[2]
local c, err = check_owner(id, ARGV[3], ARGV[4])
if not c then return reply({ ok = false, code = err }) end
requeue(id, c)
emit('case.released', load_case(id), { previousOwner = c.owner })
return reply({ ok = true })
`;

// ARGV: prefix, caseId, auditor, token, decision, note, resolvedRetentionMs
export const RESOLVE = `${PRELUDE}
local id = ARGV[2]
local c, err = check_owner(id, ARGV[3], ARGV[4])
if not c then return reply({ ok = false, code = err }) end
local worked = NOW - tonumber(c.claimedAt)
redis.call('HSET', case_key(id), 'status', 'resolved', 'decision', ARGV[5], 'note', ARGV[6],
  'resolvedBy', c.owner, 'resolvedAt', NOW, 'workedMs', worked)
redis.call('HDEL', case_key(id), 'leaseExpiresAt', 'lastRenewedAt')
redis.call('PEXPIRE', case_key(id), ARGV[7])
redis.call('ZREM', P .. ':leases', id)
if redis.call('HGET', P .. ':owners', c.owner) == id then
  redis.call('HDEL', P .. ':owners', c.owner)
end
redis.call('HINCRBY', P .. ':stats', 'resolved', 1)
redis.call('HINCRBY', P .. ':stats', 'decision:' .. ARGV[5], 1)
redis.call('HINCRBY', P .. ':auditor-stats', c.owner .. ':resolved', 1)
redis.call('HINCRBY', P .. ':auditor-stats', c.owner .. ':workMs', worked)
-- Downstream consumers (results module) read decisions from this stream.
-- The fencing token travels with the decision so a store can reject writes
-- from a stale owner.
redis.call('XADD', P .. ':resolutions', 'MAXLEN', '~', 10000, '*',
  'case', id, 'acta', c.acta, 'decision', ARGV[5], 'note', ARGV[6],
  'auditor', c.owner, 'token', c.token)
emit('case.resolved', load_case(id))
return reply({ ok = true, workedMs = worked })
`;

// ARGV: prefix, limit
// Returns expired leases to the queue. Safe to run concurrently from every
// instance: each script is atomic and re-checks the lease, so a case is
// requeued exactly once and a lease renewed in between is left alone.
export const SWEEP = `${PRELUDE}
local ids = redis.call('ZRANGEBYSCORE', P .. ':leases', '-inf', NOW, 'LIMIT', 0, tonumber(ARGV[2]))
local expired = 0
for _, id in ipairs(ids) do
  local c = load_case(id)
  if c and c.status == 'claimed' and tonumber(c.leaseExpiresAt) <= NOW then
    requeue(id, c)
    redis.call('HINCRBY', P .. ':stats', 'expired', 1)
    emit('case.expired', load_case(id), { previousOwner = c.owner })
    expired = expired + 1
  elseif not c or c.status ~= 'claimed' then
    redis.call('ZREM', P .. ':leases', id)
  end
end
local dead = redis.call('ZRANGEBYSCORE', P .. ':presence', '-inf', NOW, 'LIMIT', 0, tonumber(ARGV[2]))
for _, conn in ipairs(dead) do
  redis.call('ZREM', P .. ':presence', conn)
  redis.call('DEL', P .. ':conn:' .. conn)
end
if #dead > 0 then
  redis.call('PUBLISH', P .. ':events', cjson.encode({ type = 'presence.changed', at = NOW }))
end
return reply({ expired = expired, ghosts = #dead })
`;

// ARGV: prefix, queueLimit
export const SNAPSHOT = `${PRELUDE}
local version = tonumber(redis.call('GET', P .. ':version') or '0')
local queue = {}
for _, id in ipairs(redis.call('ZRANGE', P .. ':queue', 0, tonumber(ARGV[2]) - 1)) do
  local c = load_case(id)
  if c then table.insert(queue, c) end
end
local claimed = {}
for _, id in ipairs(redis.call('ZRANGE', P .. ':leases', 0, -1)) do
  local c = load_case(id)
  if c and c.status == 'claimed' then table.insert(claimed, c) end
end
local statsRaw = redis.call('HGETALL', P .. ':stats')
local stats = {}
for i = 1, #statsRaw, 2 do stats[statsRaw[i]] = tonumber(statsRaw[i + 1]) end
return reply({
  version = version, now = NOW, queueLength = redis.call('ZCARD', P .. ':queue'),
  queue = queue, claimed = claimed, stats = stats
})
`;

// ARGV: prefix, connId, ttlMs, then field/value pairs
export const PRESENCE_TOUCH = `${PRELUDE}
local key = P .. ':conn:' .. ARGV[2]
local isNew = redis.call('EXISTS', key) == 0
local fields = {}
for i = 4, #ARGV do table.insert(fields, ARGV[i]) end
redis.call('HSET', key, 'lastSeen', NOW, unpack(fields))
redis.call('PEXPIRE', key, ARGV[3])
redis.call('ZADD', P .. ':presence', NOW + tonumber(ARGV[3]), ARGV[2])
if isNew then
  redis.call('PUBLISH', P .. ':events', cjson.encode({ type = 'presence.changed', at = NOW }))
end
return reply({ ok = true, isNew = isNew })
`;

// ARGV: prefix, connId
export const PRESENCE_REMOVE = `${PRELUDE}
redis.call('DEL', P .. ':conn:' .. ARGV[2])
if redis.call('ZREM', P .. ':presence', ARGV[2]) == 1 then
  redis.call('PUBLISH', P .. ':events', cjson.encode({ type = 'presence.changed', at = NOW }))
end
return reply({ ok = true })
`;

// ARGV: prefix
export const PRESENCE_LIST = `${PRELUDE}
redis.call('ZREMRANGEBYSCORE', P .. ':presence', '-inf', NOW)
local conns = {}
for _, id in ipairs(redis.call('ZRANGE', P .. ':presence', 0, -1)) do
  local raw = redis.call('HGETALL', P .. ':conn:' .. id)
  if #raw > 0 then
    local c = {}
    for i = 1, #raw, 2 do c[raw[i]] = raw[i + 1] end
    table.insert(conns, c)
  end
end
local owners = {}
local ownersRaw = redis.call('HGETALL', P .. ':owners')
for i = 1, #ownersRaw, 2 do
  local c = load_case(ownersRaw[i + 1])
  if c and c.status == 'claimed' and c.owner == ownersRaw[i] then
    owners[ownersRaw[i]] = { caseId = c.id, claimedAt = tonumber(c.claimedAt),
      leaseExpiresAt = tonumber(c.leaseExpiresAt), acta = c.acta }
  end
end
local stats = {}
local statsRaw = redis.call('HGETALL', P .. ':auditor-stats')
for i = 1, #statsRaw, 2 do stats[statsRaw[i]] = tonumber(statsRaw[i + 1]) end
return reply({ now = NOW, connections = conns, owners = owners, auditorStats = stats })
`;

// KEYS[1]: counter base key. ARGV: windowMs, limit
// Fixed-window counter. The window index comes from Redis time so every
// instance agrees on the window boundaries.
export const RATE_LIMIT = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local key = KEYS[1] .. ':' .. math.floor(now / window)
local count = redis.call('INCR', key)
if count == 1 then redis.call('PEXPIRE', key, window) end
local ttl = redis.call('PTTL', key)
return { count, ttl }
`;
