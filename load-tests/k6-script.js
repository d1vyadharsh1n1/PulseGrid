import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { randomIntBetween, randomItem, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const API_URL = __ENV.API_BASE || 'http://localhost:3000';
const INGEST_ENDPOINT = `${API_URL}/events`;

const errorRate = new Rate('ingest_errors');
const alertLatency = new Trend('ingest_latency_ms', true);

const SENSOR_POOL = Array.from({ length: 40 }, (_, i) => `SENSOR_${String(i).padStart(3, '0')}`);

export const options = {
  scenarios: {
    event_ingestion: {
      executor: 'ramping-arrival-rate',
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '30s', target: 50 },
        { duration: '30s', target: 150 },
        { duration: '30s', target: 300 },
      ],
    },
  },

  thresholds: {
    http_req_duration: [
      { threshold: 'p(95) < 200', abortOnFail: false },
      { threshold: 'p(99) < 500', abortOnFail: false },
    ],
    http_req_failed: [{ threshold: 'rate < 0.01', abortOnFail: false }],
    ingest_errors: [{ threshold: 'rate < 0.01', abortOnFail: false }],
    checks: [{ threshold: 'rate > 0.99', abortOnFail: false }],
  },

  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
};

function randomTemperature() {
  const roll = Math.random();
  if (roll < 0.70) {
    return +(Math.random() * 6 + 2).toFixed(2);
  }
  if (roll < 0.85) {
    return +(Math.random() * 5 + 8.1).toFixed(2);
  }
  return +(Math.random() * 1.9 + 0.1).toFixed(2);
}

function buildEvent() {
  const now = new Date();
  const eventId = uuidv4().slice(0, 8);
  return {
    source_id: randomItem(SENSOR_POOL),
    event_type: 'temperature_reading',
    value: randomTemperature(),
    timestamp: now.toISOString(),
    _trace_id: eventId,
  };
}

export default function () {
  const payload = buildEvent();

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': `k6-loadtest/${__VU}`,
    },
    tags: {
      source_id: payload.source_id,
      value_range: payload.value >= 2 && payload.value <= 8 ? 'in_range' : 'out_of_range',
    },
  };

  const res = http.post(INGEST_ENDPOINT, JSON.stringify(payload), params);

  const ok = check(res, {
    'status is 201 or 202': (r) => r.status === 201 || r.status === 202,
    'has event id': (r) => {
      try {
        const body = r.json();
        return Boolean(body && body.id);
      } catch {
        return false;
      }
    },
  });

  errorRate.add(!ok || res.status >= 400);
  alertLatency.add(res.timings.duration);

  sleep(0);
}
