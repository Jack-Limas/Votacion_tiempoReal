const DEFAULT_DATABASE = 'jurados-actas';
const STORE_NAME = 'pending-actas';

function createIdem() {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  if (globalThis.crypto?.getRandomValues) {
    const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  }
  throw new Error('This browser cannot generate secure idem keys.');
}

function openDatabase(databaseName) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(databaseName, 1);
    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        database.createObjectStore(STORE_NAME, { keyPath: 'idem' });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error('Could not open the local queue.'));
    request.onblocked = () => reject(new Error('The local database is blocked by another tab.'));
  });
}

async function withStore(databaseName, mode, action) {
  const database = await openDatabase(databaseName);
  try {
    return await new Promise((resolve, reject) => {
      const transaction = database.transaction(STORE_NAME, mode);
      const store = transaction.objectStore(STORE_NAME);
      let result;
      try {
        result = action(store);
      } catch (error) {
        reject(error);
        return;
      }
      transaction.oncomplete = () => resolve(result);
      transaction.onerror = () => reject(transaction.error ?? new Error('The local queue operation failed.'));
      transaction.onabort = () => reject(transaction.error ?? new Error('The local queue operation was aborted.'));
    });
  } finally {
    database.close();
  }
}

function report(callback, ...args) {
  try {
    callback(...args);
  } catch (error) {
    console.error('Acta outbox callback failed:', error);
  }
}

export function createActaOutbox({
  endpoint = '/api/actas',
  databaseName = DEFAULT_DATABASE,
  onResult = () => {},
  onError = (error) => console.error(error),
  retryIntervalMs = 15000
} = {}) {
  let flushInProgress;
  let retryTimer;
  let running = false;
  const handleOnline = () => {
    void flush().catch((error) => report(onError, error));
  };

  async function getPending() {
    return withStore(databaseName, 'readonly', (store) => {
      const request = store.getAll();
      return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result.sort((a, b) => a.createdAt - b.createdAt));
        request.onerror = () => reject(request.error ?? new Error('Could not read the local queue.'));
      });
    });
  }

  async function removePending(idem) {
    return withStore(databaseName, 'readwrite', (store) => store.delete(idem));
  }

  async function enqueueActa(acta) {
    if (!acta || typeof acta !== 'object' || Array.isArray(acta)) {
      throw new TypeError('The acta must be an object.');
    }
    const payload = { ...acta, idem: acta.idem || createIdem() };
    if (typeof payload.idem !== 'string' || payload.idem.trim() === '') {
      throw new TypeError('The acta must have a non-empty idem key.');
    }
    await withStore(databaseName, 'readwrite', (store) => store.put({
      idem: payload.idem,
      payload,
      createdAt: Date.now()
    }));

    void flush().catch((error) => report(onError, error));
    return payload.idem;
  }

  async function transmit(record) {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(record.payload)
    });

    if (response.status === 429 || response.status >= 500) {
      throw new Error(`The server asked to retry (HTTP ${response.status}).`);
    }

    let result;
    try {
      result = await response.json();
    } catch (error) {
      throw new Error('The server did not return a valid JSON confirmation.', { cause: error });
    }

    await removePending(record.idem);
    report(onResult, result, record.payload, response.status);
  }

  function flush() {
    if (flushInProgress) {
      return flushInProgress;
    }
    flushInProgress = (async () => {
      if (navigator.onLine === false) {
        return;
      }

      const pending = await getPending();
      for (const record of pending) {
        try {
          await transmit(record);
        } catch (error) {
          report(onError, error, record.payload);
          break;
        }
      }
    })().finally(() => {
      flushInProgress = undefined;
    });
    return flushInProgress;
  }

  function start() {
    if (running) {
      return;
    }
    running = true;
    globalThis.addEventListener('online', handleOnline);
    // Jittered retry (0.5x-1.5x the interval): thousands of jurados that lost
    // the network together do not all retry on the same second.
    const schedule = () => {
      if (!running) return;
      retryTimer = globalThis.setTimeout(() => {
        void flush().catch((error) => report(onError, error)).finally(schedule);
      }, retryIntervalMs * (0.5 + Math.random()));
    };
    schedule();
    void flush().catch((error) => report(onError, error));
  }

  function stop() {
    running = false;
    globalThis.removeEventListener('online', handleOnline);
    if (retryTimer) {
      globalThis.clearTimeout(retryTimer);
      retryTimer = undefined;
    }
  }

  return { enqueueActa, flush, start, stop, getPending };
}
