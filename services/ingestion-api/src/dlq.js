import { redis } from './redis.js';

const DLQ_STREAM = 'dlq_events';

export async function getDlqDepth() {
  try {
    return await redis.sendCommand(['XLEN', DLQ_STREAM]);
  } catch (err) {
    return 0;
  }
}

export async function getDlqRecent(n = 10) {
  let raw;
  try {
    raw = await redis.sendCommand(['XREVRANGE', DLQ_STREAM, '+', '-', 'COUNT', String(n)]);
  } catch (err) {
    return [];
  }
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
