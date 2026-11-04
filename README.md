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
entornos desplegados. El valor `*` es solo un valor inicial de desarrollo.

## Módulo actual de jurados y digitadores

API Node.js para recibir actas con idempotencia Redis y una cola web persistente
que reintenta los envíos al recuperar conectividad.

## Ejecución

1. Copie `.env.example` a `.env` y ajuste la URL de Redis.
2. Instale dependencias con `npm install`.
3. Inicie Redis y ejecute `npm start`.
4. Ejecute las pruebas con `npm test`.

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
