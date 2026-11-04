// Slow clients and back-pressure (RF-9).
//
// Each connection exposes bufferedBytes(): bytes accepted by Node but not yet
// handed to the kernel (res.writableLength for SSE, ws.bufferedAmount for
// WebSocket). That number only grows when the client reads slower than we
// write, so it is the signal the policy watches:
//
//   buffered < softBytes            -> normal: send every delta.
//   softBytes <= buffered           -> degraded: stop sending detail to this
//                                      connection (its cursors stop moving).
//   degraded and buffered drained   -> recovered: send ONE snapshot of its
//      below softBytes / 4             topics, which replaces everything it
//                                      missed, and go back to normal.
//   buffered >= hardBytes, or still -> disconnect with a "reconnect" signal; the
//   degraded after maxDegradedMs       client comes back with its cursors (RF-6)
//                                      and its buffered bytes are released at
//                                      once. The time limit matters because a
//                                      degraded client gets no more detail, so
//                                      a client that never reads would otherwise
//                                      pin its backlog forever.
//
// The check is O(1) per delivery and the recovery loop only walks the
// degraded connections, never the whole instance.

export function createBackpressure({ softBytes, hardBytes, maxDegradedMs = 30_000, metrics, onRecover, logger = console }) {
  const degraded = new Set();

  function disconnect(conn) {
    degraded.delete(conn);
    conn.degraded = false;
    metrics.slow('disconnected');
    conn.disconnectSlow();
  }

  return {
    softBytes,
    hardBytes,
    get degradedCount() {
      return degraded.size;
    },
    // True when the connection may receive incremental detail right now.
    admit(conn) {
      if (conn.closed) return false;
      const buffered = conn.bufferedBytes();
      if (buffered >= hardBytes) {
        disconnect(conn);
        return false;
      }
      if (conn.degraded) return false;
      if (buffered >= softBytes) {
        conn.degraded = true;
        conn.degradedSince = Date.now();
        degraded.add(conn);
        metrics.slow('degraded');
        return false;
      }
      return true;
    },
    forget(conn) {
      degraded.delete(conn);
    },
    async recover() {
      for (const conn of [...degraded]) {
        if (conn.closed) {
          degraded.delete(conn);
          continue;
        }
        const buffered = conn.bufferedBytes();
        if (buffered >= hardBytes || Date.now() - conn.degradedSince >= maxDegradedMs) {
          disconnect(conn);
        } else if (buffered <= softBytes / 4) {
          degraded.delete(conn);
          conn.degraded = false;
          try {
            await onRecover(conn);
            metrics.slow('recovered');
          } catch (error) {
            logger.error('Snapshot for a recovered slow client failed:', error.message);
            conn.close();
          }
        }
      }
    }
  };
}
