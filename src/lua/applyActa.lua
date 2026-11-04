-- Registers an acta: idempotency, hierarchical totals and the public event,
-- as one atomic operation.
--
-- KEYS[1] idem  KEYS[2] verification stream
-- KEYS[3] acc:mesa:{id}  KEYS[4] acc:pto:{id}  KEYS[5] acc:mun:{id}  KEYS[6] acc:dep
-- KEYS[7] events stream  KEYS[8] catalog (total mesas per topic)
-- KEYS[9] rank:pto:{id}  KEYS[10] rank:mun:{id}  KEYS[11] rank:dep (candidate ZSETs)
-- ARGV[1] request digest  ARGV[2] result  ARGV[3] ttl  ARGV[4] votos  ARGV[5] accepted
-- ARGV[6] acta  ARGV[7] idem  ARGV[8] receivedAt (ms)  ARGV[9] stream MAXLEN
-- ARGV[10] content digest (votos + sufragantes)
--
-- Convergence (RF-2): per mesa, the applied version is the maximum of
-- (version, digest) over every acta received. A higher version always wins;
-- for two actas with the same version and different figures the larger
-- content digest wins. That order is total and independent of arrival order, so the final
-- totals depend only on the set of actas received. Resending an acta whose
-- idem key already expired is still safe: its (version, digest) is not greater
-- than the applied one, so it is reported as superseded and adds nothing.
local existing = redis.call('GET', KEYS[1])
if existing then
  local saved = cjson.decode(existing)
  if saved.digest ~= ARGV[1] then
    return cjson.encode({
      status = 'idempotency_conflict',
      accepted = false,
      duplicate = false,
      message = 'The idem key was already used with different content.'
    })
  end
  local result = saved.result
  result.duplicate = true
  return cjson.encode(result)
end

local result = cjson.decode(ARGV[2])
if ARGV[5] == '0' then
  local verificationType = redis.call('TYPE', KEYS[2]).ok
  if verificationType ~= 'none' and verificationType ~= 'stream' then
    return redis.error_reply('The verification Redis key is not a stream.')
  end
  redis.call('XADD', KEYS[2], '*', 'idem', ARGV[7], 'acta', ARGV[6])
end

if ARGV[5] == '1' then
  local votes = cjson.decode(ARGV[4])

  -- Hierarchical totals and stream event. Only an acta that is greater than the
  -- applied one in (version, content digest) order touches the totals; it
  -- reverts the previous figures (per-candidate difference) in puesto,
  -- municipio and department.
  local acta = cjson.decode(ARGV[6])
  local version = tonumber(acta.version)
  local previous = tonumber(redis.call('HGET', KEYS[3], 'version') or '0')
  local previousDigest = redis.call('HGET', KEYS[3], 'digest') or ''
  if version < previous or (version == previous and ARGV[10] <= previousDigest) then
    result.superseded = true
    result.appliedVersion = previous
    result.message = 'Version ' .. version .. ' does not supersede the applied one (' .. previous .. '); totals unchanged.'
  else
    local before = {}
    if previous > 0 then
      local flat = redis.call('HGETALL', KEYS[3])
      for i = 1, #flat, 2 do
        if string.sub(flat[i], 1, 2) == 'v:' then
          before[string.sub(flat[i], 3)] = tonumber(flat[i + 1])
        end
      end
    end

    local differences = {}
    for candidate, amount in pairs(votes) do
      local difference = tonumber(amount) - (before[candidate] or 0)
      if difference ~= 0 then
        differences[candidate] = difference
      end
    end
    for candidate, amount in pairs(before) do
      if votes[candidate] == nil and amount ~= 0 then
        differences[candidate] = -amount
      end
    end

    local kind = 'new'
    if previous > 0 then
      kind = 'correction'
    end
    local decreased = false
    local changes = {}

    -- Mesa: absolute values of the new version.
    local mesa = {}
    for candidate, _ in pairs(differences) do
      mesa['v:' .. candidate] = tonumber(votes[candidate] or 0)
    end
    redis.call('DEL', KEYS[3])
    for candidate, amount in pairs(votes) do
      redis.call('HSET', KEYS[3], 'v:' .. candidate, amount)
    end
    redis.call('HSET', KEYS[3], 'version', version, 'digest', ARGV[10], 'acta', acta.acta)
    changes['mesa:' .. acta.mesa] = mesa

    -- Puesto, municipio and department: HINCRBY returns the new absolute
    -- value, which is what travels in the event.
    local levels = {
      { key = KEYS[4], rank = KEYS[9], topic = 'pto:' .. acta.puesto },
      { key = KEYS[5], rank = KEYS[10], topic = 'mun:' .. acta.municipio },
      { key = KEYS[6], rank = KEYS[11], topic = 'dep' }
    }
    for _, level in ipairs(levels) do
      local delta = {}
      for candidate, difference in pairs(differences) do
        delta['v:' .. candidate] = redis.call('HINCRBY', level.key, 'v:' .. candidate, difference)
        -- Sorted set kept in step with the hash: the ranking is ZREVRANGE,
        -- O(log C + k), without reading every candidate.
        redis.call('ZINCRBY', level.rank, difference, candidate)
        if difference < 0 then
          decreased = true
        end
      end
      if kind == 'new' then
        local reported = redis.call('HINCRBY', level.key, 'mesas_reported', 1)
        delta.mesas_reported = reported
        local total = tonumber(redis.call('HGET', KEYS[8], level.topic) or '0')
        if total and total > 0 then
          -- Stored as text with two decimals so the hash and the event carry
          -- exactly the same number.
          local percent = string.format('%.2f', reported * 100 / total)
          redis.call('HSET', level.key, 'percent_reported', percent)
          delta.percent_reported = tonumber(percent)
        end
      end
      changes[level.topic] = delta
    end

    -- The XADD runs in the same script that updates the totals: state and
    -- event are atomic. MAXLEN ~ trims whole stream nodes.
    local event = {
      acta = acta.acta,
      version = version,
      type = kind,
      receivedAt = tonumber(ARGV[8]),
      mesa = acta.mesa,
      puesto = acta.puesto,
      municipio = acta.municipio,
      decreased = decreased,
      changes = changes
    }
    result.event = redis.call('XADD', KEYS[7], 'MAXLEN', '~', ARGV[9], '*', 'e', cjson.encode(event))
    result.type = kind
  end
end

-- Redis runs the script without interleaving other commands. If Redis
-- completes it but the process or the network dies before answering, the
-- resend with the same idem gets this stored result and never adds the votes
-- again. Use AOF and/or replication to cover a crash of Redis itself.
redis.call('SET', KEYS[1], cjson.encode({
  digest = ARGV[1],
  result = result,
  acta = cjson.decode(ARGV[6])
}), 'EX', ARGV[3])
return cjson.encode(result)
