import http from 'node:http';

const API_HOST = process.env.API_HOST || 'localhost';
const API_PORT = Number(process.env.API_PORT || 3000);
const ENDPOINT = '/events';

const MAX_SOCKETS = 1024;
const agent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 1000,
  maxSockets: MAX_SOCKETS,
  maxFreeSockets: 256,
  timeout: 8000,
});

const MAX_IN_FLIGHT = MAX_SOCKETS * 2;
const SENSOR_POOL = Array.from({ length: 40 }, (_, i) => `SENSOR_${String(i).padStart(3, '0')}`);

const STAGES = [
  { durationMs: 30_000, startRate: 0,   endRate: 10 },
  { durationMs: 30_000, startRate: 10,  endRate: 50 },
  { durationMs: 30_000, startRate: 50,  endRate: 150 },
  { durationMs: 30_000, startRate: 150, endRate: 300 },
];

const TOTAL_DURATION_MS = STAGES.reduce((s, st) => s + st.durationMs, 0);

function currentStageRate(elapsedMs) {
  let offset = 0;
  for (const st of STAGES) {
    if (elapsedMs <= offset + st.durationMs) {
      const t = (elapsedMs - offset) / st.durationMs;
      return st.startRate + (st.endRate - st.startRate) * t;
    }
    offset += st.durationMs;
  }
  return STAGES[STAGES.length - 1].endRate;
}

function currentStageIndex(elapsedMs) {
  let offset = 0;
  for (let i = 0; i < STAGES.length; i++) {
    offset += STAGES[i].durationMs;
    if (elapsedMs <= offset) return i;
  }
  return STAGES.length - 1;
}

function randomSensor() {
  return SENSOR_POOL[(Math.random() * SENSOR_POOL.length) | 0];
}

function randomTemperature() {
  const roll = Math.random();
  if (roll < 0.70) return +(Math.random() * 6 + 2).toFixed(2);
  if (roll < 0.85) return +(Math.random() * 5 + 8.1).toFixed(2);
  return +(Math.random() * 1.9 + 0.1).toFixed(2);
}

function buildPayload() {
  return JSON.stringify({
    source_id: randomSensor(),
    event_type: 'temperature_reading',
    value: randomTemperature(),
    timestamp: new Date().toISOString(),
  });
}

const latenciesMs = [];
const queueWaitMs = [];
let requests = 0;
let success = 0;
let errors = 0;
let dropped = 0;
let statusCounts = new Map();
let stageStartIdx = -1;
let inFlight = 0;

function postEvent() {
  if (inFlight >= MAX_IN_FLIGHT) {
    dropped++;
    statusCounts.set('DROP', (statusCounts.get('DROP') || 0) + 1);
    return;
  }

  const payload = buildPayload();
  const queuedAt = performance.now();
  inFlight++;

  const req = http.request({
    host: API_HOST,
    port: API_PORT,
    path: ENDPOINT,
    method: 'POST',
    agent,
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
      'User-Agent': 'pulsegrid-node-loadtest/1.1',
      'Connection': 'keep-alive',
    },
    timeout: 8000,
  });

  let ended = false;
  let socketIssued = false;

  function onFinish(code, serverLatency) {
    if (ended) return;
    ended = true;
    inFlight = Math.max(0, inFlight - 1);
    requests++;
    latenciesMs.push(serverLatency ?? (performance.now() - queuedAt));
    statusCounts.set(code, (statusCounts.get(code) || 0) + 1);
    if (code >= 200 && code < 300) success++;
    else errors++;
  }

  req.on('socket', (sock) => {
    socketIssued = true;
    const serverStart = performance.now();
    queueWaitMs.push(serverStart - queuedAt);

    sock.once('close', () => {});
    req.on('response', (res) => {
      res.on('data', () => {});
      res.on('end', () => onFinish(res.statusCode, performance.now() - serverStart));
    });
    req.on('error', () => onFinish(0, performance.now() - serverStart));
    req.on('timeout', () => { onFinish(0, performance.now() - serverStart); req.destroy(); });
  });

  req.on('error', () => {
    if (!socketIssued) onFinish(0, performance.now() - queuedAt);
  });

  req.write(payload);
  req.end();
}

const startTs = Date.now();
let lastTick = -1;
let lastRequests = 0;
let lastIssuedSlack = 0;
let issuedCounter = 0;

function tick() {
  const now = Date.now();
  const elapsed = now - startTs;
  if (elapsed >= TOTAL_DURATION_MS) return;

  const rate = currentStageRate(elapsed);
  const si = currentStageIndex(elapsed);
  if (si !== stageStartIdx) {
    stageStartIdx = si;
    const st = STAGES[si];
    const secs = st.durationMs / 1000;
    console.log(`\n── STAGE ${si + 1}/${STAGES.length}: ramp ${st.startRate} → ${st.endRate} req/s over ${secs}s ──`);
  }

  const wantedIssued = Math.floor((elapsed / 1000) * rate);
  const toIssue = Math.max(0, wantedIssued - issuedCounter);
  const cap = Math.min(toIssue, MAX_IN_FLIGHT - inFlight + 32);
  for (let i = 0; i < cap; i++) {
    setImmediate(postEvent);
    issuedCounter++;
  }

  const elapsedSec = Math.floor(elapsed / 1000);
  if (elapsedSec !== lastTick) {
    lastTick = elapsedSec;
    const rpsTick = requests - lastRequests;
    lastRequests = requests;
    const errRate = requests === 0 ? 0 : (errors / requests) * 100;
    const sockQ = agent.requests?.[`${API_HOST}:${API_PORT}:`]?.length || 0;
    console.log(
      `t=${String(elapsedSec).padStart(3)}s | target=${rate.toFixed(0).padStart(3)}/s | actual=${String(rpsTick).padStart(3)}/s | done=${String(requests).padStart(6)} | in_flight=${String(inFlight).padStart(4)} | sock_q=${String(sockQ).padStart(4)} | drop=${dropped} | err=${errRate.toFixed(2)}%`,
    );
  }
}

const timer = setInterval(tick, 25);

setTimeout(() => {
  clearInterval(timer);
  const elapsedTotalSec = (Date.now() - startTs) / 1000;
  const graceMs = 10_000;
  console.log(`\n── Ramp finished. Waiting ${graceMs / 1000}s for ${inFlight} in-flight to drain… ──`);

  setTimeout(() => {
    agent.destroy();

    latenciesMs.sort((a, b) => a - b);
    queueWaitMs.sort((a, b) => a - b);
    const n = latenciesMs.length;
    const qn = queueWaitMs.length;

    function percentile(arr, pct) {
      if (arr.length === 0) return 0;
      const idx = Math.ceil(pct / 100 * arr.length) - 1;
      return arr[Math.max(0, Math.min(arr.length - 1, idx))];
    }

    const avg = n === 0 ? 0 : latenciesMs.reduce((s, x) => s + x, 0) / n;
    const errorRate = requests === 0 ? 0 : (errors / requests) * 100;
    const throughput = requests / elapsedTotalSec;

    const p50 = percentile(latenciesMs, 50);
    const p90 = percentile(latenciesMs, 90);
    const p95 = percentile(latenciesMs, 95);
    const p99 = percentile(latenciesMs, 99);

    const passP95 = p95 < 200;
    const passP99 = p99 < 500;
    const passErr = errorRate < 1;

    const statusList = [...statusCounts.entries()]
      .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
      .map(([code, c]) => `${code}:${c}`)
      .join('  ');

    console.log('\n═══════════════════════════════════════════════════════════════════');
    console.log(' PULSEGRID LOAD TEST — FINAL RESULT  (server-only latency, ms)');
    console.log('═══════════════════════════════════════════════════════════════════');
    console.log(` Duration          : ${elapsedTotalSec.toFixed(1)}s`);
    console.log(` Total requests    : ${requests}`);
    console.log(` Throughput (avg)  : ${throughput.toFixed(1)} req/s`);
    console.log(` Succeeded         : ${success}`);
    console.log(` Failed            : ${errors}  (${errorRate.toFixed(2)}%)`);
    console.log(` Dropped (backpres): ${dropped}`);
    console.log(` Status codes      : ${statusList || '(none)'}`);
    console.log('');
    console.log(' SERVER LATENCY (ms) — measured from socket issue to response end:');
    console.log(`   avg   : ${avg.toFixed(1)}`);
    console.log(`   min   : ${n ? latenciesMs[0].toFixed(1) : '0.0'}`);
    console.log(`   p(50) : ${p50.toFixed(1)}`);
    console.log(`   p(90) : ${p90.toFixed(1)}`);
    console.log(`   p(95) : ${p95.toFixed(1)}   ${passP95 ? '✅ < 200ms' : '❌ >= 200ms (README target)'}`);
    console.log(`   p(99) : ${p99.toFixed(1)}   ${passP99 ? '✅ < 500ms' : '❌ >= 500ms (README target)'}`);
    console.log(`   max   : ${n ? latenciesMs[n - 1].toFixed(1) : '0.0'}`);
    console.log('');
    if (qn) {
      console.log(' CLIENT QUEUE WAIT (ms) — requests waiting for a free local socket:');
      console.log(`   p(50) : ${percentile(queueWaitMs, 50).toFixed(1)}`);
      console.log(`   p(95) : ${percentile(queueWaitMs, 95).toFixed(1)}`);
      console.log(`   max   : ${queueWaitMs[qn - 1].toFixed(1)}`);
      console.log('   (if this is large → your client is the bottleneck, not the API)');
      console.log('');
    }
    console.log(' THRESHOLD VERDICTS:');
    console.log(`   • p95 < 200ms        : ${passP95 ? '✅ PASS' : '❌ FAIL'}  (${p95.toFixed(0)}ms)`);
    console.log(`   • p99 < 500ms        : ${passP99 ? '✅ PASS' : '❌ FAIL'}  (${p99.toFixed(0)}ms)`);
    console.log(`   • error rate < 1%    : ${passErr ? '✅ PASS' : '❌ FAIL'}  (${errorRate.toFixed(2)}%)`);
    console.log('');
    console.log(' → Write throughput, p95, p99, err% into README Load Testing table.');
    console.log('═══════════════════════════════════════════════════════════════════');

    process.exit(errors === requests ? 1 : 0);
  }, graceMs);
}, TOTAL_DURATION_MS);
