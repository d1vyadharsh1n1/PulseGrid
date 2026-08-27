import express from 'express';
import { connectRedis, redis } from './redis.js';
import { eventsRouter } from './routes/events.js';
import { getDlqDepth, getDlqRecent } from './dlq.js';
import { attachWebSocketServer } from './ws.js';

const app = express();
const port = process.env.PORT ?? 3000;

app.use(express.json());

app.get('/health', async (_req, res) => {
  try {
    const pong = await redis.ping();

    if (pong !== 'PONG') {
      throw new Error(`Unexpected Redis ping response: ${pong}`);
    }

    return res.json({
      status: 'ok',
      service: 'ingestion-api',
      redis: 'ok',
    });
  } catch (err) {
    console.error('Health check failed:', err.message);
    return res.status(503).json({
      status: 'error',
      service: 'ingestion-api',
      redis: 'error',
      message: err.message,
    });
  }
});

app.get('/health/dlq', async (_req, res) => {
  try {
    const [depth, recent] = await Promise.all([getDlqDepth(), getDlqRecent(10)]);
    return res.json({
      status: 'ok',
      service: 'ingestion-api',
      dlq: {
        stream: 'dlq_events',
        depth,
        recent,
      },
    });
  } catch (err) {
    console.error('DLQ health check failed:', err.message);
    return res.status(503).json({
      status: 'error',
      service: 'ingestion-api',
      dlq: { stream: 'dlq_events' },
      message: err.message,
    });
  }
});

app.use('/events', eventsRouter);

app.use((err, _req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ error: 'Request body must be valid JSON' });
  }

  return next(err);
});

async function start() {
  try {
    await connectRedis();
    console.log('Connected to Redis');
  } catch (err) {
    console.error('Failed to connect to Redis:', err.message);
    process.exit(1);
  }

  const httpServer = app.listen(port, () => {
    console.log(`Ingestion API listening on http://localhost:${port}`);
  });

  attachWebSocketServer(httpServer);
}

start();
