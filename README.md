# Plataforma de transmisión electoral

## Estructura base

La base incluye `src/config`, `src/controllers`, `src/services`, `src/sockets`,
`src/utils` y `public`. Para crear estos directorios desde Bash:

```bash
mkdir -p src/{config,controllers,services,sockets,utils} public
```

O desde PowerShell:

```powershell
@('src/config', 'src/controllers', 'src/services', 'src/sockets', 'src/utils', 'public') |
  ForEach-Object { New-Item -ItemType Directory -Force -Path $_ | Out-Null }
```

Instala dependencias con `npm install`; usa `npm run dev` para desarrollo y
`npm start` para ejecutar el servicio.

### Redis y transporte

`src/config/redis.js` exporta tres conexiones Redis independientes: comandos,
suscripción y publicación. El cliente de suscripción no se comparte con
comandos normales. Este diseño permite coordinar instancias mediante Pub/Sub y
estructuras Redis sin sesiones pegajosas.

Express y `ws` comparten el mismo servidor HTTP; WebSocket escucha en
`WS_PATH`. El punto de entrada exporta `app`, `server` y `webSocketServer`,
además de `startServer` y `stopServer`, para facilitar inyección de manejadores
y pruebas. SSE puede añadirse como una ruta HTTP sobre el mismo servidor.

Configura `CORS_ORIGIN` como una lista de orígenes separados por comas en los
entornos desplegados. El valor `*` permite solicitudes desde cualquier origen;
WebSocket comparte el servidor HTTP y no filtra conexiones por encabezado
`Origin`.

## Módulo actual de jurados y digitadores

API Node.js para recibir actas con idempotencia Redis y una cola web persistente
que reintenta los envíos al recuperar conectividad.

## Ejecución

1. Copie `.env.example` a `.env` y ajuste la URL de Redis.
2. Instale dependencias con `npm install`.
3. Inicie Redis y ejecute `npm start`.
4. Ejecute las pruebas con `npm test`.

## Despliegue en Render

Cree un servicio **Web Service** desde el repositorio y configure:

- **Build Command:** `npm install`
- **Start Command:** `npm start` (el script ejecuta `node src/server.js`)
- **Health Check Path:** `/health`
- **Runtime:** Node.js 20 o posterior

Render asigna `PORT` al proceso; el servidor HTTP y WebSocket lo comparten y
escuchan en ese puerto. No es necesario definir `PORT` manualmente en el panel.
Agregue `REDIS_URL` en las variables de entorno con la URL privada de la
instancia Redis utilizada por el servicio. Use `rediss://` si esa instancia
requiere TLS; el cliente `redis` negocia TLS a partir del esquema de la URL.
El cliente reintenta las desconexiones con backoff y reporta errores en los
logs. Si Redis no está disponible al procesar un acta, la API responde `503`
para que el cliente conserve el envío y vuelva a intentarlo.

`CORS_ORIGIN` es opcional: omítala para aceptar cualquier origen, o defina una
lista de orígenes separada por comas. No se requieren variables adicionales
para habilitar CORS o WebSockets.

El servicio requiere una instancia Redis. Para conservar confirmaciones frente
a reinicios, configure Redis con AOF y/o replicación según el nivel de
durabilidad requerido; el TTL de idempotencia debe cubrir la ventana máxima en
que los clientes puedan reenviar un acta.

## Recepción de actas

`POST /api/actas` recibe el payload JSON del acta.

- `201`: acta nueva aceptada y sumada.
- `200`: reenvío ya procesado; el cuerpo indica `duplicate: true` y conserva
  el resultado original.
- `202`: suma inconsistente; queda marcada como dudosa en el stream Redis
  `electoral:{results}:verification`, no se suma y requiere verificación.
- `409`: la misma clave `idem` se presentó con un contenido distinto.
- `422`: JSON correcto pero campos inválidos.
- `503`: no se pudo confirmar con Redis; el cliente debe conservar y reintentar.

El acumulado se mantiene en el hash `electoral:{results}:aggregate`. Cada
registro válido, la actualización de votos, el registro idempotente y las actas
dudosas se procesan con un script Lua. Las claves de Redis comparten el hash
tag `{results}` para que el script también funcione en Redis Cluster. Si cambia
`REDIS_KEY_PREFIX`, mantenga un hash tag común para las tres claves.

La aplicación responde por HTTP en lugar de exigir WebSockets: cada intento es
independiente, puede hacerse después de reconectar y se protege con `idem`.
Así no se depende de mantener una conexión persistente durante cortes móviles.

## Cola offline del navegador

Incluya `public/acta-outbox.js` desde un módulo JavaScript y conecte los
callbacks a la interfaz de jurado:

```js
import { createActaOutbox } from './acta-outbox.js';

const outbox = createActaOutbox({
  onResult: (result) => mostrarResultadoAlJurado(result),
  onError: (error) => mostrarEstadoDeConexion(error.message)
});

outbox.start();
await outbox.enqueueActa(datosDelActa);
```

La cola guarda el payload en IndexedDB antes de transmitirlo y conserva la
misma clave `idem` para todos los intentos. Reintenta al recibir el evento
`online` y periódicamente mientras la página está abierta. Una respuesta
definitiva (aceptación, duplicado, rechazo o envío a verificación) se comunica
por `onResult` y retira el elemento de la cola; errores de red, `429` y `5xx`
lo conservan para reintentar. IndexedDB depende del almacenamiento del
navegador y puede estar sujeto a políticas de limpieza del dispositivo.

## Verification desk (auditors and coordinator)

Doubtful actas are consumed from the verification stream into a priority
queue with exclusive, fenced leases; consoles live at `/verification/` and use
the WebSocket endpoint `/ws/verification`. Run `docker compose up -d` for a
local Redis. Design, Redis keys, TTLs, costs and failure behaviour are in
[docs/verification.md](docs/verification.md).

## Módulo público: difusión de results (RF-4, RF-5, RF-6)

Los jurados envían actas por `POST /api/actas`. El mismo script Lua que suma el
acta escribe el evento en el stream `st:eventos`. Cada instancia lee ese stream
y lo reparte por SSE a los clientes suscritos. El diseño y las cifras están en
[docs/results-design.md](docs/results-design.md).

### Correr en local

Requisitos: Docker con Compose y Node.js 20 o superior.

```bash
docker compose up -d --build     # 8 réplicas + nginx (http://localhost:8080) + Valkey (localhost:6379)
npm install
npm test                         # pruebas unitarias + integración contra el entorno
npm run simular -- --velocidad=60   # jornada completa comprimida x60 (2 h en 2 min)
```

Si el entorno no está arriba, las pruebas de integración se omiten e indican
cómo levantarlo. Para empezar con los datos en limpio:
`node scripts/reset-datos.js` (FLUSHDB y vuelve a sembrar el catálogo).

### Endpoints públicos

- `GET /stream?sub=mun:11,pto:176,dep`: SSE. Tópicos `dep`, `mun:{id}` y `pto:{id}`.
- `GET /whoami`: identificador de la instancia (`INSTANCE_ID` o el hostname).
- `GET /metrics`: métricas de la instancia (Event Loop, conexiones, suscriptores, mensajes, retraso de publicación).
- `GET /metrics/all`: métricas de todas las instancias, leídas de Redis.

### Prueba de carga (RF-4)

Corre dentro de la red de compose. Usa `--no-deps` para que compose no recree
las réplicas con otra configuración:

```bash
FANOUT_MODE=naive docker compose up -d --force-recreate app && docker compose restart nginx
docker compose run --rm --no-deps carga --conexiones=10000 --procesos=4 --velocidad=30 --desde=0 --hasta=40
docker compose up -d --force-recreate app && docker compose restart nginx      # batched
docker compose run --rm --no-deps carga --conexiones=10000 --procesos=4 --velocidad=30 --desde=0 --hasta=40
npm run estimar -- --flush=1000 --mezcla=dep:0.5,mun:0.35,pto:0.15             # jornada completa
```

Cada corrida reinicia los datos y deja `results/carga-<modo>-<conexiones>.csv`
(por segundo e instancia) y un `.json` con el resumen.

### Variables de entorno

| Variable | Por defecto | Uso |
|---|---|---|
| `PORT` | 3000 | Puerto HTTP |
| `REDIS_URL` | redis://localhost:6379 | Valkey/Redis |
| `INSTANCE_ID` | hostname | Identificador de la instancia |
| `MAXLEN` | 10000 | Retención del stream (`XADD MAXLEN ~`) |
| `STREAM_BLOCK_MS` | 5000 | `BLOCK` del `XREAD` |
| `STREAM_READ_COUNT` | 200 | `COUNT` del `XREAD` |
| `MAX_TOPICS` | 10 | Tópicos por conexión |
| `FANOUT_MODE` | batched | `batched` o `naive` (comparación) |
| `FLUSH_MS` | 1000 | Ventana de agregación |
| `BATCH_SIZE` | 100 | Envíos por lote antes de ceder el Event Loop |
| `HEARTBEAT_MS` | 15000 | Comentario SSE para mantener viva la conexión |
| `EL_RESOLUTION_MS` | 10 | Resolución de `monitorEventLoopDelay` |
| `METRICS_TTL_S` | 5 | TTL de las métricas publicadas en Redis |
| `MAX_REPLAY` | 500 | Eventos pendientes máximos para resolver una reconexión con replay |

### Reconexión (RF-6)

El cliente guarda, por tópico, el `upTo` del último mensaje que aplicó y al
reconectar lo envía: `GET /stream?sub=mun:11@<upTo>,dep@<upTo>`. Un tópico sin
cursor recibe un snapshot. Con cursor recibe un delta con lo que se perdió
(replay) o un snapshot si el cursor salió de la retención o hay más de
`MAX_REPLAY` eventos pendientes. La referencia de cliente está en
`scripts/lib/cliente-sse.js`.

Prueba A/B: incluida en `npm test` (B se desconecta 30 s). Para forzar el camino
snapshot:

```bash
MAXLEN=20 docker compose up -d --force-recreate app && docker compose restart nginx
AB_ACTAS=250 AB_ESPERADO=snapshot AB_DESCONEXION_MS=10000 node --test test/integration/rf6-reconexion.test.js
```
