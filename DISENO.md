# Diseño del módulo público (RF-4, RF-5, RF-6)

Este documento cubre la difusión de resultados al público: el backplane en Redis,
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
