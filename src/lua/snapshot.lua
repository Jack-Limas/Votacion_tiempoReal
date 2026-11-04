-- Atomic snapshot for RF-6.
-- KEYS[1] events stream  KEYS[2..n] acc:{topic} of each requested topic.
-- Returns { last stream ID, HGETALL topic 1, HGETALL topic 2, ... }.
-- Being a script, no acta is applied between reading the state and reading
-- the ID: the state corresponds exactly to that ID.
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local result = { '0-0' }
if #last > 0 then
  result[1] = last[1][1]
end
for i = 2, #KEYS do
  result[i] = redis.call('HGETALL', KEYS[i])
end
return result
