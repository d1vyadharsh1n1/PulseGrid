import { redis, connectRedis } from './redis.js';
import { evaluate } from './ruleEngine.js';
import { isAlreadySent } from './idempotency.js';
import { tryConsumeToken } from './rateLimiter.js';
import { publishAlert } from './snsPublisher.js';
import { pushToDlq } from './dlq.js';

const STREAM_KEY = 'events';
const GROUP_NAME = 'processors';
const CONSUMER_NAME = process.env.WORKER_ID ?? 'worker-1';
const BLOCK_MS = 5000;
const BATCH_SIZE = 10;

const RATE_CAPACITY = 10;
const RATE_WINDOW_MS = 10 * 1000;

const DEDUP_TTL_MS = 60 * 1000;
const DEDUP_CHANNEL = 'sns';

const BROADCAST_CHANNEL = 'pulsegrid:processed_events';

async function broadcastProcessed(payload) {
  try {
    await redis.publish(BROADCAST_CHANNEL, JSON.stringify(payload));
  } catch (_) {
    // best effort; never fail the pipeline for dashboard broadcast
  }
}

function isBusyGroup(err) {
  return err?.code === 'BUSYGROUP' || String(err?.message ?? '').includes('BUSYGROUP');
}

async function ensureConsumerGroup() {
  try {
    await redis.xGroupCreate(STREAM_KEY, GROUP_NAME, '0', { MKSTREAM: true });
    console.log(`Created consumer group "${GROUP_NAME}" on stream "${STREAM_KEY}"`);
  } catch (err) {
    if (isBusyGroup(err)) {
      console.log(`Consumer group "${GROUP_NAME}" already exists`);
      return;
    }
    throw err;
  }
}

function toEvent(fields) {
  return {
    source_id: fields.source_id,
    event_type: fields.event_type,
    value: fields.value,
    timestamp: fields.timestamp,
  };
}

async function processMessage(id, fields) {
  const event = toEvent(fields);

  const withinLimit = await tryConsumeToken(event.source_id, {
    capacity: RATE_CAPACITY,
    windowMs: RATE_WINDOW_MS,
  });

  if (!withinLimit) {
    console.log(`RATE_LIMITED source=${event.source_id} event=${id} — dropping (cap=${RATE_CAPACITY}/${RATE_WINDOW_MS}ms)`);
    await broadcastProcessed({ event_id: id, disposition: 'rate_limited', ...event, alert: null });
    await redis.xAck(STREAM_KEY, GROUP_NAME, id);
    return;
  }

  const alert = evaluate(event);

  if (!alert) {
    await broadcastProcessed({ event_id: id, disposition: 'processed_no_alert', ...event, alert: null });
    await redis.xAck(STREAM_KEY, GROUP_NAME, id);
    return;
  }

  console.log('ALERT', {
    id,
    reason: alert.reason,
    source_id: alert.source_id,
    event_type: alert.event_type,
    value: alert.value,
    timestamp: alert.timestamp,
  });

  const duplicate = await isAlreadySent(id, DEDUP_CHANNEL, DEDUP_TTL_MS);
  if (duplicate) {
    console.log(`DEDUP_SKIP event=${id} channel=${DEDUP_CHANNEL} — already sent within TTL`);
    await broadcastProcessed({ event_id: id, disposition: 'dedup_skipped', ...event, alert: alert.reason });
    await redis.xAck(STREAM_KEY, GROUP_NAME, id);
    return;
  }

  let disposition = 'processed_alert_sent';
  try {
    const publishResult = await publishAlert(alert);
    if (publishResult.skipped) {
      disposition = 'processed_alert_skipped';
      console.log(`SNS_SKIP event=${id} reason=${publishResult.reason}`);
    } else if (publishResult.sent) {
      disposition = 'processed_alert_sent';
      console.log(`SNS_SENT event=${id} messageId=${publishResult.messageId}`);
    }
  } catch (err) {
    console.error(`SNS_FAILED (permanent) event=${id}: ${err.name}: ${err.message}`);
    try {
      const dlqId = await pushToDlq({
        eventId: id,
        event,
        reason: 'sns_delivery_failed',
        error: err,
        stage: 'sns_publish',
      });
      disposition = 'dlq';
      console.error(`DLQ_PUSH event=${id} dlq_id=${dlqId}`);
    } catch (dlqErr) {
      console.error(`DLQ_PUSH_FAILED event=${id}: ${dlqErr.name}: ${dlqErr.message}`);
      await redis.xAck(STREAM_KEY, GROUP_NAME, id);
      throw dlqErr;
    }
  }

  await broadcastProcessed({ event_id: id, disposition, ...event, alert: alert.reason });
  await redis.xAck(STREAM_KEY, GROUP_NAME, id);
}

async function readLoop() {
  console.log(
    `Worker "${CONSUMER_NAME}" reading stream "${STREAM_KEY}" as group "${GROUP_NAME}"`,
  );

  while (true) {
    const results = await redis.xReadGroup(
      GROUP_NAME,
      CONSUMER_NAME,
      { key: STREAM_KEY, id: '>' },
      { COUNT: BATCH_SIZE, BLOCK: BLOCK_MS },
    );

    if (!results) {
      continue;
    }

    for (const stream of results) {
      for (const entry of stream.messages) {
        try {
          await processMessage(entry.id, entry.message);
        } catch (err) {
          console.error(`Failed to process message ${entry.id} (not acked):`, err.message);
        }
      }
    }
  }
}

async function start() {
  await connectRedis();
  console.log('Connected to Redis');
  await ensureConsumerGroup();
  await readLoop();
}

start().catch((err) => {
  console.error('Worker failed:', err);
  process.exit(1);
});
