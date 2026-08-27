import { createClient } from 'redis';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

export const redis = createClient({ url: REDIS_URL });

redis.on('error', (err) => {
  console.error('Redis client error:', err.message);
});

export async function connectRedis() {
  if (!redis.isOpen) {
    await redis.connect();
  }
}
