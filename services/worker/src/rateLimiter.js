import { redis } from './redis.js';

const BUCKET_PREFIX = 'ratelimit';
const DEFAULT_CAPACITY = 10;
const DEFAULT_WINDOW_MS = 10 * 1000;
const DEFAULT_TOKENS_PER_EVENT = 1;

const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local tokens = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local refill = capacity / windowMs

local data = redis.call('HMGET', key, 'tokens', 'ts')
local currTokens = data[1]
local lastTs = data[2]

if currTokens == false then
  currTokens = capacity
  lastTs = now
else
  currTokens = tonumber(currTokens)
  lastTs = tonumber(lastTs)
  local elapsed = now - lastTs
  local added = elapsed * refill
  if added > 0 then
    currTokens = math.min(capacity, currTokens + added)
    lastTs = now
  end
end

if currTokens >= tokens then
  currTokens = currTokens - tokens
  redis.call('HMSET', key, 'tokens', currTokens, 'ts', lastTs)
  redis.call('PEXPIRE', key, windowMs)
  return 1
else
  redis.call('HMSET', key, 'tokens', currTokens, 'ts', lastTs)
  redis.call('PEXPIRE', key, windowMs)
  return 0
end
`;

export function bucketKey(sourceId, capacity = DEFAULT_CAPACITY, windowMs = DEFAULT_WINDOW_MS) {
  return `${BUCKET_PREFIX}:${capacity}-${windowMs}:${sourceId}`;
}

export async function tryConsumeToken(
  sourceId,
  {
    capacity = DEFAULT_CAPACITY,
    windowMs = DEFAULT_WINDOW_MS,
    tokens = DEFAULT_TOKENS_PER_EVENT,
  } = {},
) {
  const key = bucketKey(sourceId, capacity, windowMs);
  const now = Date.now();
  const result = await redis.sendCommand([
    'EVAL',
    TOKEN_BUCKET_LUA,
    '1',
    key,
    String(capacity),
    String(windowMs),
    String(tokens),
    String(now),
  ]);
  return result === 1;
}
