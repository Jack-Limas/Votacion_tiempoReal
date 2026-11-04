// Fan-out de eventos del stream hacia las conexiones locales suscritas.
// El costo de entregar un evento es O(k) por tópico afectado (k = suscriptores
// locales de ese tópico), no O(conexiones de la instancia).

export function serializar(mensaje) {
  return `data: ${JSON.stringify(mensaje)}\n\n`;
}

export function mensajeDelta(topic, upTo, cambios) {
  return { t: 'delta', topic, upTo, cambios };
}

export function createFanout({ registry }) {
  let emitidos = 0;

  return {
    get emitidos() {
      return emitidos;
    },
    onEntries(entries) {
      for (const { id, evento } of entries) {
        for (const [topic, cambios] of Object.entries(evento.cambios)) {
          const subscribers = registry.subscribers(topic);
          if (subscribers.size === 0) continue;
          const frame = serializar(mensajeDelta(topic, id, cambios));
          for (const conn of subscribers) {
            conn.send(frame);
            emitidos += 1;
          }
        }
      }
    }
  };
}
