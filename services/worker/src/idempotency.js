import { redis } from './redis.js';

const DEFAULT_TTL_MS = 60 * 1000;
const KEY_PREFIX = 'dedup';

export function dedupKey(eventId, channel) {
  return `${KEY_PREFIX}:${eventId}:${channel}`;
}

export async function isAlreadySent(eventId, channel, ttlMs = DEFAULT_TTL_MS) {
  const key = dedupKey(eventId, channel);
  const result = await redis.set(key, '1', { NX: true, PX: ttlMs });
  return result === null;
}
