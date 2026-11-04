# Módulo de jurados y digitadores

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

## Recepción

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
