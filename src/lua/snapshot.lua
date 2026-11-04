-- Snapshot atómico para RF-6.
-- KEYS[1] st:eventos  KEYS[2..n] acc:{topic} de cada tópico pedido.
-- Devuelve { último ID del stream, HGETALL tópico 1, HGETALL tópico 2, ... }.
-- Al ser un script, ninguna acta se aplica entre la lectura del estado y la del
-- ID: el estado corresponde exactamente a ese ID.
local ultimo = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local resultado = { '0-0' }
if #ultimo > 0 then
  resultado[1] = ultimo[1][1]
end
for i = 2, #KEYS do
  resultado[i] = redis.call('HGETALL', KEYS[i])
end
return resultado
