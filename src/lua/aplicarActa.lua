-- KEYS[1] idem  KEYS[2] aggregate  KEYS[3] verification
-- KEYS[4] acc:mesa:{id}  KEYS[5] acc:pto:{id}  KEYS[6] acc:mun:{id}  KEYS[7] acc:dep
-- KEYS[8] st:eventos  KEYS[9] catalogo (total de mesas por tópico)
-- ARGV[1] digest  ARGV[2] result  ARGV[3] ttl  ARGV[4] votos  ARGV[5] aceptada
-- ARGV[6] acta  ARGV[7] idem  ARGV[8] ts_recepcion (ms)  ARGV[9] MAXLEN del stream
local existing = redis.call('GET', KEYS[1])
if existing then
  local saved = cjson.decode(existing)
  if saved.digest ~= ARGV[1] then
    return cjson.encode({
      status = 'idempotency_conflict',
      accepted = false,
      duplicate = false,
      message = 'La clave idem ya fue utilizada con un contenido diferente.'
    })
  end
  local result = saved.result
  result.duplicate = true
  return cjson.encode(result)
end

local result = cjson.decode(ARGV[2])
if ARGV[5] == '0' then
  local verificationType = redis.call('TYPE', KEYS[3]).ok
  if verificationType ~= 'none' and verificationType ~= 'stream' then
    return redis.error_reply('La clave de verificación Redis no es un stream.')
  end
  redis.call('XADD', KEYS[3], '*', 'idem', ARGV[7], 'acta', ARGV[6])
end

if ARGV[5] == '1' then
  local aggregateType = redis.call('TYPE', KEYS[2]).ok
  if aggregateType ~= 'none' and aggregateType ~= 'hash' then
    return redis.error_reply('La clave de acumulados Redis no es un hash.')
  end

  local votes = cjson.decode(ARGV[4])
  local maxSafeInteger = 9007199254740991
  for candidate, increment in pairs(votes) do
    local field = 'vote:' .. candidate
    local current = tonumber(redis.call('HGET', KEYS[2], field) or '0')
    local amount = tonumber(increment)
    if not current or not amount or current < 0 or current + amount > maxSafeInteger then
      return redis.error_reply('Acumulado de votos fuera del rango seguro.')
    end
  end
  local currentActas = tonumber(redis.call('HGET', KEYS[2], 'actas') or '0')
  if not currentActas or currentActas < 0 or currentActas + 1 > maxSafeInteger then
    return redis.error_reply('Acumulado de actas fuera del rango seguro.')
  end

  for candidate, increment in pairs(votes) do
    redis.call('HINCRBY', KEYS[2], 'vote:' .. candidate, increment)
  end
  redis.call('HINCRBY', KEYS[2], 'actas', 1)

  -- Acumulados jerárquicos y evento del stream (stub mínimo para RF-4/5/6).
  -- La versión mayor gana: si llega una versión igual o anterior a la aplicada
  -- no se toca el acumulado. Una versión nueva revierte la anterior (diferencia
  -- por candidato) en puesto, municipio y departamento.
  local acta = cjson.decode(ARGV[6])
  local version = tonumber(acta.version)
  local previa = tonumber(redis.call('HGET', KEYS[4], 'version') or '0')
  if version <= previa then
    result.superseded = true
    result.message = 'La versión ' .. version .. ' no supera la aplicada (' .. previa .. '); el acumulado no cambia.'
  else
    local anteriores = {}
    if previa > 0 then
      local plano = redis.call('HGETALL', KEYS[4])
      for i = 1, #plano, 2 do
        if string.sub(plano[i], 1, 2) == 'v:' then
          anteriores[string.sub(plano[i], 3)] = tonumber(plano[i + 1])
        end
      end
    end

    local diferencias = {}
    for candidato, cantidad in pairs(votes) do
      local diferencia = tonumber(cantidad) - (anteriores[candidato] or 0)
      if diferencia ~= 0 then
        diferencias[candidato] = diferencia
      end
    end
    for candidato, cantidad in pairs(anteriores) do
      if votes[candidato] == nil and cantidad ~= 0 then
        diferencias[candidato] = -cantidad
      end
    end

    local tipo = 'nueva'
    if previa > 0 then
      tipo = 'correccion'
    end
    local baja = false
    local cambios = {}

    -- Mesa: valores absolutos de la nueva versión.
    local mesa = {}
    for candidato, _ in pairs(diferencias) do
      mesa['v:' .. candidato] = tonumber(votes[candidato] or 0)
    end
    redis.call('DEL', KEYS[4])
    for candidato, cantidad in pairs(votes) do
      redis.call('HSET', KEYS[4], 'v:' .. candidato, cantidad)
    end
    redis.call('HSET', KEYS[4], 'version', version, 'acta', acta.acta)
    cambios['mesa:' .. acta.mesa] = mesa

    -- Puesto, municipio y departamento: HINCRBY devuelve el nuevo valor
    -- absoluto, que es lo que viaja en el evento.
    local niveles = {
      { clave = KEYS[5], topico = 'pto:' .. acta.puesto },
      { clave = KEYS[6], topico = 'mun:' .. acta.municipio },
      { clave = KEYS[7], topico = 'dep' }
    }
    for _, nivel in ipairs(niveles) do
      local delta = {}
      for candidato, diferencia in pairs(diferencias) do
        delta['v:' .. candidato] = redis.call('HINCRBY', nivel.clave, 'v:' .. candidato, diferencia)
        if diferencia < 0 then
          baja = true
        end
      end
      if tipo == 'nueva' then
        local informadas = redis.call('HINCRBY', nivel.clave, 'mesas_informadas', 1)
        delta.mesas_informadas = informadas
        local total = tonumber(redis.call('HGET', KEYS[9], nivel.topico) or '0')
        if total and total > 0 then
          -- Se guarda como texto con dos decimales para que el hash y el evento
          -- lleven exactamente el mismo número.
          local porcentaje = string.format('%.2f', informadas * 100 / total)
          redis.call('HSET', nivel.clave, 'porcentaje', porcentaje)
          delta.porcentaje = tonumber(porcentaje)
        end
      end
      cambios[nivel.topico] = delta
    end

    -- El XADD va en el mismo script que actualiza los acumulados: estado y
    -- evento son atómicos. MAXLEN ~ recorta por nodos completos del stream.
    local evento = {
      acta = acta.acta,
      version = version,
      tipo = tipo,
      ts_recepcion = tonumber(ARGV[8]),
      mesa = acta.mesa,
      puesto = acta.puesto,
      municipio = acta.municipio,
      baja = baja,
      cambios = cambios
    }
    result.evento = redis.call('XADD', KEYS[8], 'MAXLEN', '~', ARGV[9], '*', 'e', cjson.encode(evento))
    result.tipo = tipo
  end
end

-- Redis ejecuta el script sin intercalar otras operaciones. Si Redis lo completa
-- pero el proceso o la red cae antes de responder, el reenvío con el mismo idem
-- obtiene este resultado guardado y no vuelve a sumar los votos. Configure AOF
-- y/o replicación de Redis para reducir el riesgo ante una caída del propio Redis.
redis.call('SET', KEYS[1], cjson.encode({
  digest = ARGV[1],
  result = result,
  acta = cjson.decode(ARGV[6])
}), 'EX', ARGV[3])
return cjson.encode(result)
