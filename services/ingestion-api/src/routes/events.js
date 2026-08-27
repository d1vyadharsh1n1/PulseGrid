import { Router } from 'express';
import { redis } from '../redis.js';
import { eventPayloadSchema } from '../schemas/event.js';

const STREAM_KEY = 'events';

export const eventsRouter = Router();

eventsRouter.post('/', async (req, res) => {
  const parsed = eventPayloadSchema.safeParse(req.body);

  if (!parsed.success) {
    return res.status(400).json({
      error: 'Invalid event payload',
      details: parsed.error.flatten().fieldErrors,
    });
  }

  const event = parsed.data;

  try {
    // Redis Stream entries are string field/value pairs, so `value` is stored as a string.
    const id = await redis.xAdd(STREAM_KEY, '*', {
      source_id: event.source_id,
      event_type: event.event_type,
      value: String(event.value),
      timestamp: event.timestamp,
    });

    return res.status(201).json({
      status: 'accepted',
      id,
    });
  } catch (err) {
    console.error('Failed to write event to Redis stream:', err.message);
    return res.status(503).json({
      error: 'Failed to persist event',
    });
  }
});
