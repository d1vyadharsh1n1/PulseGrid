const TEMPERATURE_EVENT = 'temperature_reading';
const TEMP_MAX = 8;
const TEMP_MIN = 2;
const TEMP_OUT_OF_RANGE = 'temperature_out_of_range';

/**
 * Deterministic threshold checks. Returns an alert object or null.
 * `value` may be a number or a string (Redis Stream fields are strings).
 */
export function evaluate(event) {
  const value = Number(event.value);

  if (
    event.event_type === TEMPERATURE_EVENT &&
    Number.isFinite(value) &&
    (value > TEMP_MAX || value < TEMP_MIN)
  ) {
    return {
      alert: true,
      reason: TEMP_OUT_OF_RANGE,
      source_id: event.source_id,
      event_type: event.event_type,
      value,
      timestamp: event.timestamp,
    };
  }

  return null;
}
