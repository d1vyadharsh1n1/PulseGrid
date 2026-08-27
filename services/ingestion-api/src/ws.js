import { WebSocketServer } from 'ws';
import { createClient } from 'redis';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const PROCESSED_CHANNEL = 'pulsegrid:processed_events';

export function attachWebSocketServer(httpServer) {
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  const clients = new Set();

  wss.on('connection', (ws) => {
    clients.add(ws);
    ws.on('close', () => {
      clients.delete(ws);
    });
    ws.on('error', () => {
      try { ws.close(); } catch (_) { /* noop */ }
      clients.delete(ws);
    });
  });

  const subClient = createClient({ url: REDIS_URL });
  subClient.on('error', (err) => {
    console.error('WS Redis subscriber error:', err.message);
  });

  (async () => {
    try {
      await subClient.connect();
    } catch (err) {
      console.error('WS Redis subscriber failed to connect:', err.message);
      return;
    }
    console.log('WS broadcaster subscribed to', PROCESSED_CHANNEL);
    await subClient.subscribe(PROCESSED_CHANNEL, (message) => {
      if (clients.size === 0) return;
      for (const ws of clients) {
        if (ws.readyState === 1 /* OPEN */) {
          try {
            ws.send(message);
          } catch (_) {
            try { ws.close(); } catch (__) { /* noop */ }
            clients.delete(ws);
          }
        }
      }
    });
  })();

  return wss;
}
