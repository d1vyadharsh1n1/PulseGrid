import { redis } from './redis.js';

const DLQ_STREAM = 'dlq_events';

export const DLQ_STREAM_KEY = DLQ_STREAM;

export async function pushToDlq({ eventId, event, reason, error, stage }) {
  const entry = {
    event_id: eventId ?? '',
    stage: stage ?? 'unknown',
    reason: reason ?? 'unknown',
    source_id: event?.source_id ?? '',
    event_type: event?.event_type ?? '',
    value: event?.value != null ? String(event.value) : '',
    timestamp: event?.timestamp ?? '',
    error_name: error?.name ?? '',
    error_message: error?.message ?? String(error ?? ''),
    dlq_ts: new Date().toISOString(),
  };

  const fields = Object.entries(entry).flatMap(([k, v]) => [k, String(v ?? '')]);
  return await redis.sendCommand(['XADD', DLQ_STREAM, '*', ...fields]);
}

export async function getDlqDepth() {
  return await redis.sendCommand(['XLEN', DLQ_STREAM]);
}

export async function getDlqRecent(n = 10) {
  const raw = await redis.sendCommand(['XREVRANGE', DLQ_STREAM, '+', '-', 'COUNT', String(n)]);
  if (!Array.isArray(raw)) return [];
  return raw.map((entry) => {
    const [id, fields] = entry;
    const obj = { id };
    if (Array.isArray(fields)) {
      for (let i = 0; i + 1 < fields.length; i += 2) {
        obj[String(fields[i])] = fields[i + 1];
      }
    }
    return obj;
  });
}
