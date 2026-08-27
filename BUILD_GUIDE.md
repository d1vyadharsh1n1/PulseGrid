# PulseGrid — Build Guide & Reference Document

This is your working reference while building. It breaks the project into stages so you're never staring at the whole architecture at once, and links to the actual docs you'll need at each stage instead of you having to search mid-build.

**Rule for today and every session after:** get one stage fully working end-to-end (even ugly) before adding the next. A working 3-stage pipeline beats a half-wired 8-stage one, both for your sanity and for what you can actually defend in an interview.

---

## Stage 0 — Environment Setup (Day 1, ~1 hr)

1. Init the repo structure (see `README.md` → Project Structure).
2. `docker-compose.yml` with just Redis + Postgres to start — don't add your own services to Compose until they're working standalone.
3. Confirm Redis is reachable: `docker exec -it <redis-container> redis-cli ping` → should return `PONG`.

**Reference:**
- Docker Compose docs: https://docs.docker.com/compose/
- Redis Docker image: https://hub.docker.com/_/redis

---

## Stage 1 — Ingestion API + Redis Streams (Day 1–2)

This is the most important stage. Everything else depends on this being solid.

1. Basic Express server with `POST /events`.
2. Write to a Redis Stream with `XADD`. Learn these commands in this order: `XADD`, `XLEN`, `XRANGE`, `XREAD` — get comfortable reading a stream manually via `redis-cli` before writing any consumer code.
3. Once basic add/read works, move to consumer groups: `XGROUP CREATE`, `XREADGROUP`, `XACK`. This is the part that gives you "no message loss" — understand *why* unacked messages get redelivered (`XPENDING`, `XCLAIM`) before moving on.

**Reference:**
- Redis Streams intro (read this fully, it's the backbone of the whole project): https://redis.io/docs/latest/develop/data-types/streams/
- `node-redis` client docs: https://github.com/redis/node-redis
- Consumer groups deep dive: https://redis.io/docs/latest/develop/data-types/streams/#consumer-groups

**Checkpoint before moving on:** kill your consumer process mid-read (Ctrl+C while it's processing), restart it, and confirm the in-flight message gets redelivered via `XPENDING` + `XCLAIM`. If you can demo this, you've proven the core durability claim.

---

## Stage 2 — Worker Pool: Rule Engine + Anomaly Scoring (Day 2–3)

1. Rule engine first — simple, deterministic (`value > threshold`). Get this working and tested before touching anomaly scoring.
2. Anomaly scoring — rolling z-score per `source_id`. Keep a small in-memory (or Redis-backed) rolling window of recent values per source; flag if the new value is > N standard deviations from the rolling mean.
3. Keep rule engine and anomaly scorer as separate, independently testable modules — this separation is also a good thing to point to in an interview as a deliberate design choice (single responsibility).

**Reference:**
- Rolling z-score anomaly detection (concept, language-agnostic): https://en.wikipedia.org/wiki/Standard_score
- If you want a slightly more robust approach than plain z-score: exponentially weighted moving average (EWMA) — https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average

---

## Stage 3 — Delivery: AWS SNS + WebSocket Broadcast (Day 3–4)

1. Set up an SNS topic in AWS Console, subscribe your email/phone to it.
2. Use the AWS SDK v3 for Node.js to publish to SNS from the worker.
3. Add retry with exponential backoff around the SNS call (write this yourself first — don't reach for a library immediately, understanding the backoff math is the point).
4. WebSocket broadcast to the dashboard — simplest version first (broadcast every alert to every connected client), refine later if needed.

**Reference:**
- AWS SDK for JavaScript v3 — SNS: https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/sns/
- Exponential backoff explained (AWS's own guidance, good reference for your resume claim too): https://docs.aws.amazon.com/general/latest/gr/api-retries.html
- `ws` library docs: https://github.com/websockets/ws

---

## Stage 4 — Idempotency + Rate Limiting (Day 4–5)

1. Idempotency: before delivering an alert, `SETNX` a dedup key like `dedup:{event_id}:{channel}` with a TTL. If the key already exists, skip delivery — this is a two-line change with outsized interview value.
2. Rate limiting: token-bucket per `source_id`, implemented with Redis (there's a well-known Lua-script pattern for atomic token-bucket in Redis — worth understanding, not just copying).

**Reference:**
- Redis `SETNX` / `SET ... NX`: https://redis.io/commands/set/
- Token bucket algorithm (concept): https://en.wikipedia.org/wiki/Token_bucket
- Redis rate-limiting patterns (official guide): https://redis.io/docs/latest/develop/use/patterns/rate-limiting/

---

## Stage 5 — Circuit Breaker + Lambda Enrichment (Day 5–6)

1. Write a basic Lambda function (even a trivial one — "look up threat intel for this source" or similar) and deploy it via AWS Console or SAM CLI.
2. Implement a circuit breaker by hand around the Lambda invocation: track consecutive failures, trip to "open" after N failures, reject calls immediately while open, half-open after a cooldown to test recovery.
3. **Write this yourself before considering a library** (e.g. `opossum`) — the point of this project is that you can explain the state machine (closed → open → half-open) in an interview without hesitation.

**Reference:**
- Circuit Breaker pattern (the canonical explanation, Martin Fowler): https://martinfowler.com/bliki/CircuitBreaker.html
- AWS Lambda Node.js getting started: https://docs.aws.amazon.com/lambda/latest/dg/lambda-nodejs.html
- `opossum` (Node.js circuit breaker library, good to compare your hand-rolled version against once yours works): https://github.com/nodeshift/opossum

---

## Stage 6 — Dead-Letter Queue (Day 6)

1. After max retries on any delivery channel, push the failed event + failure reason to a separate Redis Stream (`dlq:events`).
2. Add a `GET /health/dlq` endpoint returning DLQ depth and recent entries.
3. Meta-alert: if DLQ depth crosses a threshold, fire a separate "system unhealthy" alert — this closes the loop nicely for a demo ("the system monitors its own failure to monitor").

**Reference:**
- Dead-letter queue pattern (AWS's explanation, concept transfers directly): https://docs.aws.amazon.com/sns/latest/dg/sns-dead-letter-queues.html

---

## Stage 7 — Dashboard (Day 6–7)

1. React app, connect to the WebSocket for a live event feed.
2. Alert history table (from Postgres via a simple REST endpoint).
3. DLQ health panel.
4. Keep this simple — it's the least differentiating part of the project relative to your other resume entries (you already have frontend-heavy work). Don't over-invest time here at the expense of the backend resilience work, which is the actual point of this project.

---

## Stage 8 — Load Testing (Day 7)

1. Write a k6 script that fires events at your ingestion API at increasing rates.
2. Capture: sustained throughput, p95/p99 latency, error rate.
3. Kill a worker mid-run to test the durability claim under real load, not just manually.
4. **Write down the actual numbers immediately** — this becomes both your README's Load Testing table and your resume bullet. Don't round up or estimate.

**Reference:**
- k6 getting started: https://grafana.com/docs/k6/latest/get-started/

---

## Stage 9 — Dockerize + CI/CD (Day 7–8)

1. Dockerfile per service (ingestion-api, worker, dashboard).
2. Full `docker-compose.yml` wiring everything together.
3. GitHub Actions workflow: build + push images on push to `main` (you already have a working example of this from your cold-chain project — reuse that pattern).

**Reference:**
- GitHub Actions for Docker: https://docs.github.com/en/actions/publishing-packages/publishing-docker-images

---

## Quick-Reference: Redis Commands You'll Actually Use

| Command | Purpose |
|---|---|
| `XADD stream_key * field value` | Append event to stream |
| `XGROUP CREATE stream_key group_name $` | Create consumer group starting from new messages |
| `XREADGROUP GROUP group consumer COUNT n BLOCK ms STREAMS key >` | Read as part of a consumer group |
| `XACK stream_key group_name id` | Acknowledge successful processing |
| `XPENDING stream_key group_name` | See unacked (potentially stuck) messages |
| `XCLAIM stream_key group_name consumer min-idle-time id` | Reclaim a stuck message for reprocessing |
| `SET key value NX PX ms` | Idempotency key with auto-expiry |

---

## Interview-Readiness Checklist

Before this goes on your resume, you should be able to answer all of these without notes:

- [ ] Why Redis Streams over plain pub/sub, specifically in terms of what happens to a message if no consumer is connected
- [ ] What a consumer group is and how `XACK`/`XPENDING`/`XCLAIM` prevent message loss on worker crash
- [ ] Walk through your circuit breaker's exact state transitions (closed → open → half-open) and what triggers each
- [ ] Why idempotency keys are needed even with reliable delivery (i.e., what failure mode they specifically prevent)
- [ ] Your actual measured throughput/latency numbers, and roughly how you measured them
- [ ] What happens end-to-end if AWS SNS is down for 5 minutes
- [ ] Why the DLQ exists separately from normal retry logic, and what "give up" actually means for an alert that matters

If any of these feel shaky once you're done building, that's the sign to go back and solidify that piece before it goes on the resume — not to skip the question in an interview.
