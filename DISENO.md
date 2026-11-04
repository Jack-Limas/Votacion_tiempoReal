# Diseño del módulo público (RF-4, RF-5, RF-6)

Este documento cubre la difusión de results al público: el backplane en Redis,
la suscripción por tópico, el envío por lotes y la reconexión con recuperación.
Las cifras salen de las pruebas incluidas en el repositorio (ver README).

## 1. Arquitectura

```
 jurado ──POST /api/actas──► instancia k ──EVAL aplicarActa.lua──► Valkey
                                              │  idempotencia (RF-1, Edison)
                                              │  acc:mesa / acc:pto / acc:mun / acc:dep
                                              └─ XADD st:eventos  (mismo script: atómico)

 Valkey st:eventos ──XREAD BLOCK (conexión dedicada, sin grupos)──► cada una de las 8 instancias
                                                                      │
                                       registro de tópicos (Map<topic, Set<conexión>>)
                                                                      │
                                                         SSE  GET /stream?sub=...
```

- **Transporte público: SSE.** El público solo recibe; SSE es HTTP plano, pasa
  por el balanceador y los proxies sin negociar upgrade y cuesta menos memoria
  por conexión que un WebSocket. La capa de fan-out es agnóstica al transporte:
  cada conexión expone solo `send(string)` (`src/publico/sse.js` la adapta).
- **Sin estado compartido fuera de Redis.** Cada instancia guarda únicamente
  sus conexiones locales. Las instancias no se hablan entre sí.

## 2. Decisiones tomadas (lo que el enunciado no fijaba)

- **Script Lua.** Edison ya tenía el script de recepción (idempotencia +
  acumulado `aggregate`). Se movió sin cambios de lógica a
  `src/lua/aplicarActa.lua` y se le agregó, en el mismo script, la
  actualización de `acc:mesa:{id}`, `acc:pto:{id}`, `acc:mun:{id}`, `acc:dep` y
  el `XADD` a `st:eventos`. Es un stub mínimo de RF-2/RF-3: la versión mayor
  gana y una versión nueva revierte la anterior por diferencia. El hash
  `aggregate` de Edison se mantiene tal cual (no revierte correcciones; queda
  para quien implemente RF-2).
- **Nombres de claves.** Se conserva el prefijo de Edison con hash tag
  (`electoral:{results}:`), así que `st:eventos` es
  `electoral:{results}:st:eventos` y `acc:mun:11` es
  `electoral:{results}:acc:mun:11`. Todas las claves que toca el script caen en
  el mismo slot, de modo que el script sigue siendo válido en Redis Cluster.
- **Porcentaje de mesas informadas.** Requiere el total de mesas por tópico.
  Se usa un catálogo sintético determinista (`src/publico/catalogo.js`: 40
  municipios, 1.842 puestos, 6.400 mesas) que cada instancia siembra al
  arrancar en el hash `catalogo`. Se guarda con dos decimales como texto para
  que el hash y el evento lleven exactamente el mismo número.
- **Campos del estado.** `v:<candidato>`, `mesas_informadas` y `porcentaje`. El
  prefijo `v:` evita choques entre un candidato y los demás campos.
- **Primera lectura del stream.** `XREAD ... $` con `BLOCK 0` solo la primera
  vez; después siempre el último ID recibido. Bloquear sin límite en la primera
  lectura evita tener que repetir `$` tras un timeout, que perdería los eventos
  llegados entre una lectura y otra.
- **Valkey con AOF** (`--appendonly yes --appendfsync everysec`, igual que la
  configuración de Redis del módulo de verificación) en un solo compose con las
  8 réplicas. En producción (ElastiCache) se usa AOF/réplica.

## 3. Backplane: un stream global contra uno por municipio

Se usa **un solo stream `st:eventos`**. Cada instancia lee todos los eventos y
solo paga el fan-out de sus suscriptores locales.

| | Stream global | Un stream por municipio (40) | Uno por puesto (1.842) |
|---|---|---|---|
| Eventos escritos en la jornada | ~6.600 (6.400 actas + ~180 correcciones) | ~6.600 × 2 (municipio + departamento) | ~6.600 × 3 |
| Streams que lee cada instancia | 1 | 40 + dep | 1.842 + 40 + dep |
| Entregas Redis → instancias | 6.600 × 8 = 52.800 | igual o mayor (ver abajo) | igual o mayor |
| Tráfico Redis → instancias (≈0,6 KB/evento) | ≈32 MB en 2 h; pico ≈ 5 ev/s × 8 × 0,6 KB ≈ 24 KB/s | ≥ 32 MB | ≥ 32 MB |
| Cursor de reconexión | un ID monótono para todos los tópicos | un cursor por stream | un cursor por stream |

El argumento central: **sin sesiones pegajosas, cada instancia recibe una
muestra aleatoria de todo el público**. Con ~50.000 clientes por instancia
repartidos en 40 municipios, la probabilidad de que una instancia no tenga
ningún suscriptor de un municipio es prácticamente cero, así que con streams
por municipio cada instancia terminaría leyendo los 40 de todas formas. Además
el tópico `dep` necesita todos los eventos. Partir el canal no ahorra tráfico,
multiplica la memoria (cada acta escrita en 2 o 3 streams) y obliga a manejar
un cursor por stream en RF-6. El flujo de escritura es pequeño (≈ 7.000
eventos); lo caro es el fan-out hacia el público, y ese ya es proporcional a los
suscriptores locales.

El evento lleva **valores absolutos nuevos** (no incrementos) de mesa, puesto,
municipio y departamento, más `acta`, `version`, `tipo` (`nueva|correccion`),
`ts_recepcion` y `baja` (si alguna cifra bajó). Con valores absolutos, aplicar
un evento dos veces o reducir varios eventos a "último valor por campo" da el
mismo resultado, que es lo que hacen la agregación por ventana y el replay.

## 4. RF-5: registro de tópicos

Tópicos: `dep`, `mun:{id}`, `pto:{id}`. Una conexión puede seguir varios
(`GET /stream?sub=mun:11,dep`).

```
topics: Map<topic, Set<conexión>>
conexión: { send(string), topics: Set<topic>, cursors: Map<topic, id> }
```

| Operación | Costo | Cómo |
|---|---|---|
| Alta (suscribir a un tópico) | O(1) | `Map.get` + `Set.add` |
| Baja de un tópico | O(1) | `Set.delete`; si el set queda vacío se borra la entrada del mapa |
| Baja de la conexión completa | O(t), t = tópicos de la conexión (≤ 10) | recorre `conn.topics` |
| Recorrido para entregar un evento | O(k), k = suscriptores del tópico | itera solo `topics.get(topic)` |

Nunca se recorren todas las conexiones de la instancia para entregar un
evento. Un evento toca a lo sumo 3 tópicos (su puesto, su municipio y `dep`);
un tópico sin suscriptores locales cuesta un `Map.get`. Al cerrarse la conexión
(`req.on('close')`) se da de baja de todos sus tópicos y el contador de
suscriptores de `/metrics` vuelve a 0.

Verificación (prueba `test/integration/rf5-topicos.test.js`, contra las 8
réplicas): un cliente en `mun:11` recibe solo mensajes de `mun:11` y uno en
`pto:176` solo de `pto:176`; al desconectarse, ambos desaparecen del mapa; 16
peticiones a `/whoami` respondieron 8 instancias distintas.

## 5. RF-4: agregación por ventana y envío por lotes

`src/publico/fanout.js`, modo `FANOUT_MODE=batched` (por defecto):

1. Los eventos del stream **no se envían al llegar**. Se acumulan en un mapa de
   tópicos sucios: por tópico, el último valor absoluto de cada campo que
   cambió, el `motivo` (si una corrección bajó un número) y `upTo`, el último
   ID del stream procesado. Un tópico sin suscriptores locales se ignora.
2. Cada `FLUSH_MS` (1000 ms) se arma **un mensaje por tópico sucio**, solo con
   los campos que cambiaron, y se **serializa una vez**: todos los suscriptores
   reciben el mismo string.
3. Se envía en lotes de `BATCH_SIZE` y entre lotes se cede el Event Loop con
   `await new Promise(r => setImmediate(r))`.
4. Los flushes no se solapan: si toca uno mientras el anterior sigue enviando,
   la ventana nueva sigue acumulando y se envía, fusionada, al terminar.
5. Cada conexión descarta los deltas con `upTo <= cursor` de ese tópico. Los
   IDs se comparan como números (ms y secuencia): como texto, `9-0 > 10-0`.
6. Una corrección que baja un número viaja con
   `motivo: {tipo:"correccion", acta, version}` para que la UI lo explique.

`FANOUT_MODE=naive` envía cada evento a cada suscriptor en un bucle síncrono.
Solo existe para la comparación.

### Por qué setImmediate

`process.nextTick` y las promesas resueltas son **microtareas**: se ejecutan
todas antes de que el Event Loop vuelva a la fase de I/O, así que un bucle que
"cede" con ellas sigue bloqueando sockets, timers y las peticiones de los
jurados. `setImmediate` programa la continuación en la fase *check*, después del
*poll* de I/O. Entre dos lotes alcanzan a correr las peticiones pendientes. La
prueba unitaria `cede el Event Loop entre lotes` lo verifica: un callback
programado con `setImmediate` corre justo después del primer lote.

### BATCH_SIZE = 100

Con 500 por lote, cada lote tardó ~40 ms en este equipo (las 8 réplicas, nginx
y el generador compiten por la misma VM de Docker). Con 100, el p99 por segundo
quedó por debajo de 20 ms. Ceder más seguido cuesta poco: un `setImmediate` por
cada 100 envíos.

### Medición (prueba de carga, `scripts/carga.js`)

10.000 conexiones SSE a través de nginx; simulador de los primeros 40 minutos
de la jornada comprimidos x30 (3.817 envíos: 3.305 actas que generan evento,
57 de ellas correcciones; además 240 reenvíos y 272 dudosas). Mezcla de
suscripciones: 50% `dep`, 35% municipio, 15% puesto. CSV por segundo e
instancia en `results/`.

| | naive | batched (BATCH_SIZE=100) |
|---|---|---|
| Conexiones | 10.000 (1.250 por instancia) | 10.000 (≈3.330 en 3 instancias*) |
| Retraso del Event Loop, p99 desde el arranque | 483–685 ms | 1,2–6,8 ms |
| Retraso del Event Loop, máx | 0,9–1,9 s | 26–88 ms |
| p99 por segundo (mediana / p99 / máx de los segundos) | 293 / 1.229 / 1.229 ms | 0,96 / 11,7 / 18 ms |
| Envíos de jurados fallidos durante la prueba | 22 | 0 |
| Mensajes emitidos | 19.335.972 | 591.846 |
| Cifra ingenua de la prueba (actas × conexiones) | 32.850.000 | 33.050.000 |
| Retraso recepción → publicación p50 / p99 | 327–589 / 1.008–1.798 ms | 486–641 / 990–1.170 ms |

\* Tras recrear las réplicas, nginx tenía la lista de IPs desactualizada y
concentró las conexiones en 3 instancias, que soportaron 2,7 veces más carga
que en naive y aun así quedaron por debajo de 20 ms de p99. Antes de una
corrida conviene `docker compose restart nginx`.

En naive el p99 por segundo sale de solo 12 muestras: las mismas instancias
bloqueadas tardaban en responder `/metrics`. El retraso de publicación de
batched (~0,5 s de mediana) es el costo esperado de una ventana de 1 s.

### Mensajes emitidos frente a los 2.560 millones

- **Prueba de carga:** 591.846 mensajes frente a 33.050.000 ingenuos (56 veces
  menos). La compresión x30 del tiempo infla esta reducción, porque una ventana
  de 1 s junta 30 veces más actas que en tiempo real.
- **Jornada completa en tiempo real** (`npm run estimar`): plan del simulador,
  400.000 clientes conectados las 2 h, suscriptores repartidos según las mesas.
  La cifra ingenua del caso es 6.400 × 400.000 = **2.560.000.000**.

| Mezcla de suscripciones | FLUSH_MS | Por evento con tópicos | Agregado por ventana | Frente a 2.560 M |
|---|---|---|---|---|
| 50% dep, 35% mun, 15% pto | 1000 | 1.379 M | 879 M | 34% (3x menos) |
| 50% dep, 35% mun, 15% pto | 2000 | 1.379 M | 652 M | 25% (4x) |
| 10% dep, 60% mun, 30% pto | 1000 | 528 M | 368 M | 14% (7x) |
| 10% dep, 60% mun, 30% pto | 2000 | 528 M | 287 M | 11% (9x) |
| 0% dep, 70% mun, 30% pto | 1000 | 333 M | 253 M | 10% (10x) |

La mayor parte de la reducción la aporta el tópico (RF-5). La ventana agrega
poco para `dep` y para la capital (`mun:1`, un tercio de las mesas), porque en
el pico cambian casi cada segundo y sus suscriptores reciben ~1 mensaje por
segundo. Para bajar más hay que alargar la ventana de esos tópicos (el caso
dice que el usuario no distingue una actualización por acta de una cada 2 s).

### Métricas

`GET /metrics` (instancia) y `GET /metrics/all` (todas, leídas de Redis: cada
instancia publica `metrics:{instancia}` cada segundo con TTL de 5 s):

- Retraso del Event Loop (`monitorEventLoopDelay`, resolución 10 ms): p50, p99
  y máx del último segundo y desde el arranque. Se resta la resolución porque
  el histograma registra el intervalo completo entre disparos. `reset()`
  descarta la primera muestra siguiente, así que el reinicio por segundo está
  desfasado medio `FLUSH_MS` del flush y hay un segundo histograma que nunca se
  reinicia.
- Conexiones por instancia y suscriptores por tópico.
- Mensajes emitidos por segundo y total, frente a la cifra ingenua (cada evento
  × conexiones de la instancia).
- Retraso entre la recepción del acta (`ts_recepcion`) y su envío: p50 y p99.
- Reconexiones resueltas por replay y por snapshot (RF-6).

## 6. RF-6: reconexión con recuperación

**Identificador del cliente:** por cada tópico, el ID del stream (`upTo`) del
último mensaje que aplicó. El ID lo asigna Redis en el `XADD`, es monótono y es
el mismo para las 8 instancias. Por eso un cliente puede reconectar en
cualquiera de ellas.

**Protocolo** (transporte SSE; el cliente maneja la reconexión y no depende de
la reconexión automática de `EventSource`, porque RF-10 pedirá backoff con
jitter):

```
GET /stream?sub=mun:11@1793757239457-0,dep@1793757239457-0,pto:176
{"t":"snapshot","topic":"pto:176","upTo":"<id>","estado":{...}}          tópico sin cursor
{"t":"delta","topic":"mun:11","upTo":"<id>","cambios":{...},"motivo":{...}}
```

**Qué entrega el servidor al reconectar** (`src/publico/resume.js`):

1. La conexión entra al registro **antes** de leer Redis, con cada cursor en
   `PENDIENTE`. Los deltas en vivo que llegan mientras tanto se guardan y al
   final se filtran por `upTo > cursor`. Así no se pierde nada entre la lectura
   y el alta, y nada retrocede.
2. Tópico sin cursor: **snapshot**. Un script Lua (`src/lua/snapshot.lua`) lee
   los hashes y el último ID del stream de forma atómica.
3. Tópico con cursor: en un `MULTI` se leen
   `XRANGE st:eventos (cursor + COUNT MAX_REPLAY+1`, el primer ID retenido y el
   último. Luego se decide:
   - **snapshot** si el cursor es anterior al primer ID retenido (lo que siguió
     pudo recortarse), si hay más de `MAX_REPLAY` eventos pendientes, o si el
     cursor es posterior al último ID (stream reiniciado);
   - **replay** en otro caso: se filtran los eventos del tópico y se reducen a
     **un** delta (último valor por campo), con `upTo` = último ID y el
     `motivo` de la corrección que bajó un número, si la hubo.

**Por qué conviene la foto completa a partir de cierto punto:** el replay
transfiere los eventos pendientes desde Redis (~0,7 KB cada uno) aunque el
cliente reciba un solo delta. El snapshot cuesta un `HGETALL` de un hash de
~10 campos. Con `MAX_REPLAY = 500`, el replay cubre más de 1,5 minutos en la
peor ráfaga (300 actas/min) y unos 5 minutos en el pico normal (~90/min), así
que una desconexión de 30 s siempre se resuelve con replay. Más allá de eso, la
foto es más barata.

**Retención, expiración y memoria:**

- `XADD ... MAXLEN ~ N` dentro del mismo script que aplica el acta; `N` se
  configura con `MAXLEN` (10.000 por defecto). El `~` recorta por nodos
  completos (100 entradas), así que es O(1) amortizado y el stream puede pasar
  de N en hasta un nodo.
- Medido con `MEMORY USAGE`: 3.305 eventos ocupan 2.396.528 bytes, unos **725 B
  por evento**. La jornada completa (~6.600 eventos con correcciones) ocupa
  ≈ 4,8 MB, y el tope de 10.000 eventos ≈ 7,3 MB. Es tan barato que se retiene
  la jornada entera y el snapshot solo aparece por `MAX_REPLAY` o por un
  stream recortado a propósito.
- No hay TTL por entrada: `EXPIRE` borraría el stream completo. Tras la
  jornada se puede expirar la clave operativamente (p. ej. 48 h).
- Pub/Sub de Redis no se usa para RF-6: no tiene memoria. Si una instancia o
  un cliente está ausente, lo publicado se pierde. El stream sí la tiene.

**Verificación** (`test/integration/rf6-reconexion.test.js`, contra las 8
réplicas): A queda conectado; B se desconecta 30 s mientras entran 30 actas y
una corrección que baja un candidato, y reconecta con sus cursores en otra
instancia. Al final, los hashes del estado de A y B coinciden en `dep`,
`mun:7` y `pto:699`.

- Camino **replay** (MAXLEN 10.000): los 3 tópicos se resolvieron por replay y
  el motivo de la corrección llegó a A en vivo y a B en el replay.
- Camino **snapshot** (`MAXLEN=20`, 250 actas durante una ausencia de 10 s):
  el stream quedó en 22 entradas, los 3 tópicos se resolvieron por snapshot
  (métrica `reconexiones.snapshot = 3`) y los hashes también coincidieron.
