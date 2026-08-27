const DEFAULT_BASE_MS = 1000;
const DEFAULT_MAX_DELAY_MS = 30 * 1000;
const DEFAULT_MAX_ATTEMPTS = 5;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function isRetriableError(err) {
  if (!err) return false;
  if (err.retryable === true) return true;
  if (typeof err.$metadata?.httpStatusCode === 'number') {
    const status = err.$metadata.httpStatusCode;
    if (status === 429) return true;
    if (status >= 500 && status < 600) return true;
    return false;
  }
  const code = err.code;
  if (
    code === 'ECONNREFUSED' ||
    code === 'ECONNRESET' ||
    code === 'ETIMEDOUT' ||
    code === 'ENOTFOUND' ||
    code === 'EAI_AGAIN' ||
    code === 'EPIPE' ||
    code === 'TimeoutError'
  ) {
    return true;
  }
  const message = String(err.message ?? '');
  if (/timeout/i.test(message)) return true;
  if (/socket hang up/i.test(message)) return true;
  return false;
}

export function delayForAttempt(attempt, baseMs = DEFAULT_BASE_MS, maxDelayMs = DEFAULT_MAX_DELAY_MS) {
  if (attempt <= 1) return 0;
  const raw = baseMs * 2 ** (attempt - 2);
  const capped = Math.min(raw, maxDelayMs);
  return Math.floor(Math.random() * capped);
}

export async function withRetry(fn, options = {}) {
  const {
    baseMs = DEFAULT_BASE_MS,
    maxDelayMs = DEFAULT_MAX_DELAY_MS,
    maxAttempts = DEFAULT_MAX_ATTEMPTS,
    isRetriable = isRetriableError,
    onRetry,
    operationName = 'operation',
  } = options;

  let lastErr;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      lastErr = err;
      const isLastAttempt = attempt >= maxAttempts;
      const canRetry = !isLastAttempt && isRetriable(err);

      if (!canRetry) {
        throw err;
      }

      const delay = delayForAttempt(attempt + 1, baseMs, maxDelayMs);
      if (onRetry) {
        onRetry({ attempt, maxAttempts, delay, operationName, err });
      }
      await sleep(delay);
    }
  }

  throw lastErr;
}
