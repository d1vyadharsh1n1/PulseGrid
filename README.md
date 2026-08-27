# PulseGrid — Distributed Real-Time Alerting & Event Processing Platform

A production-grade event ingestion, anomaly-detection, and multi-channel alerting pipeline built with the same resilience patterns used in real-world systems: durable message streams, circuit breakers, dead-letter queues, idempotent delivery, and rate limiting.

PulseGrid ingests high-volume events (IoT sensors, application telemetry, transaction streams — the pipeline is domain-agnostic), scores them for anomalies, and reliably delivers alerts across multiple channels (SMS/email via AWS SNS, live dashboard via WebSocket), even when downstream services fail or the system is under heavy load.

---

## Table of Contents
- [Why This Exists](#why-this-exists)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Core Design Decisions](#core-design-decisions)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Load Testing & Results](#load-testing--results)
- [API Reference](#api-reference)
- [Roadmap](#roadmap)

---

## Why This Exists

Most student/portfolio projects that involve "real-time alerts" use a naive pub/sub pattern: an event comes in, gets published to a channel, and if no one is listening at that exact moment, it's gone forever. That's fine for a demo, but it's not how production alerting systems behave — a fraud alert, a cold-chain temperature breach, or a security incident cannot silently vanish because a worker was mid-restart.

PulseGrid is built around a simple question: **what does it take to guarantee an alert is never lost, never duplicated, and still delivered fast under load?** Answering that honestly requires message durability, retry logic, deduplication, and graceful degradation — the actual hard parts of distributed systems that most portfolio projects skip.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────────────┐
│ Event        │────▶│  Ingestion   │────▶│   Redis Streams     │
│ Producers    │     │  API (Express│     │  (durable, replay-  │
│ (IoT/app/    │     │  + WebSocket)│     │   able message log) │
│  synthetic)  │     └──────────────┘     └──────────┬──────────┘
└─────────────┘                                       │
                                                        ▼
                                          ┌─────────────────────────┐
                                          │  Worker Pool (Node.js)  │
                                          │  Consumer Group          │
                                          │  ┌─────────────────┐    │
                                          │  │ Rule Engine      │    │
                                          │  │ (thresholds)     │    │
                                          │  ├─────────────────┤    │
                                          │  │ Anomaly Scoring  │    │
                                          │  │ (rolling z-score) │    │
                                          │  └─────────────────┘    │
                                          └────────────┬────────────┘
                                                        │
                     ┌──────────────────────────────────┼──────────────────────────┐
                     ▼                                  ▼                          ▼
          ┌────────────────────┐          ┌────────────────────┐      ┌──────────────────────┐
          │ Circuit Breaker →   │          │ AWS SNS →           │      │ WebSocket Broadcast → │
          │ AWS Lambda           │          │ Email/SMS            │      │ React Dashboard       │
          │ (serverless          │          │ (retry + exponential │      │ (live event feed)     │
          │  enrichment)         │          │  backoff)             │      │                       │
          └────────────────────┘          └────────────────────┘      └──────────────────────┘
                     │                                  │
                     ▼                                  ▼
          ┌─────────────────────────────────────────────────────┐
          │   Dead-Letter Queue (Redis) — failed deliveries      │
          │   retried with exponential backoff, alertable after  │
          │   N consecutive failures                              │
          └─────────────────────────────────────────────────────┘

Idempotency layer: Redis SETNX-based dedup keys prevent duplicate
alerts from retries/replays across the entire pipeline.

Rate limiting: Redis token-bucket limits per-source ingestion so
one noisy producer cannot starve the queue for everyone else.

Deployment: Dockerized services, docker-compose for local dev,
GitHub Actions CI/CD builds and pushes images on every push to main.
```

### Data flow, step by step
1. **Producers** (real or synthetic) POST events to the ingestion API, or stream over WebSocket for high-frequency sources.
2. The **Ingestion API** validates the payload, stamps it with a dedup key, and appends it to a **Redis Stream** (`XADD`) — this is the durability boundary. Once an event is in the stream, a crash downstream cannot lose it.
3. A **worker pool** reads from the stream via a **consumer group** (`XREADGROUP`), so multiple workers can process in parallel without double-processing the same event, and unacknowledged events are automatically redelivered if a worker dies mid-processing.
4. Each event passes through the **rule engine** (static thresholds, e.g. "temperature > 8°C") and **anomaly scorer** (rolling window z-score against recent history for that source).
5. Flagged events are enriched via an **AWS Lambda** call, wrapped in a **circuit breaker** — if Lambda is slow or failing, the breaker trips and the pipeline degrades gracefully (skips enrichment, still delivers the base alert) instead of blocking the whole pipeline.
6. Alerts are delivered three ways in parallel: **AWS SNS** (SMS/email, with retry + backoff), **WebSocket broadcast** (live dashboard), and logged to Postgres for history/audit.
7. Any delivery that fails after max retries lands in the **dead-letter queue**, which is itself monitored — repeated DLQ growth triggers a meta-alert ("the alerting system itself is unhealthy").

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion API | Node.js, Express, WebSocket (`ws`) | Familiar, fast to iterate, WebSocket needed for live dashboard + high-frequency producers |
| Message Broker | Redis Streams | Durable, replayable, supports consumer groups — unlike plain pub/sub, nothing is lost if a consumer is offline |
| Worker Runtime | Node.js (separate process from API) | Decouples ingestion throughput from processing throughput |
| Resilience | Custom circuit breaker, retry/backoff, DLQ, idempotency keys | The core engineering focus of this project |
| Cloud | AWS Lambda (enrichment), AWS SNS (delivery) | Matches real serverless-alerting patterns |
| Database | PostgreSQL | Alert history, audit trail |
| Frontend | React | Live event feed, alert history, DLQ health panel |
| Load Testing | k6 or autocannon | Produces real throughput/latency numbers instead of estimated ones |
| Infra | Docker, Docker Compose, GitHub Actions | Local dev parity + CI/CD |

---

## Core Design Decisions

**Why Redis Streams instead of plain Redis Pub/Sub or a simple queue?**
Pub/Sub is fire-and-forget — if no consumer is connected at publish time, the message is gone. Streams persist messages to a log, support consumer groups for parallel processing with per-consumer acknowledgment, and allow replay from any point. This is the single decision that makes the "zero message loss" claim defensible.

**Why a circuit breaker around the Lambda call specifically?**
Enrichment is a nice-to-have, not the core alert. If Lambda is slow (cold starts, throttling) or down, blocking the whole pipeline on it would delay or drop time-critical alerts. The breaker trips after N consecutive failures, short-circuits future calls for a cooldown window, and lets the pipeline continue without enrichment — a small deliberate trade-off between completeness and reliability.

**Why idempotency keys?**
Any retry-based system risks duplicate delivery (e.g., SNS succeeds but the acknowledgment is lost, triggering a retry). A `SETNX`-based dedup key per event+channel prevents a user from getting the same SMS twice.

**Why rate limiting per source?**
A single misbehaving or misconfigured producer (e.g., a sensor stuck in a retry loop) shouldn't be able to degrade service for every other source sharing the pipeline. Token-bucket limiting caps this per-source.

---

## Project Structure

```
pulsegrid/
├── services/
│   ├── ingestion-api/
│   │   ├── src/
│   │   │   ├── routes/
│   │   │   ├── middleware/       # rate limiter, validation
│   │   │   └── websocket/
│   │   └── Dockerfile
│   │
│   ├── worker/
│   │   ├── src/
│   │   │   ├── ruleEngine.js
│   │   │   ├── anomalyScorer.js
│   │   │   ├── circuitBreaker.js
│   │   │   ├── deadLetterQueue.js
│   │   │   └── consumers/
│   │   └── Dockerfile
│   │
│   └── dashboard/                # React frontend
│       ├── src/
│       └── Dockerfile
│
├── load-tests/
│   └── k6-script.js
│
├── .github/workflows/
│   └── ci-cd.yml
│
├── docker-compose.yml
└── README.md
```

---

## Setup

### Prerequisites
- Node.js >= 20.x
- Docker + Docker Compose

### Environment variables

Copy the root template and fill in values before running services locally:

```bash
cp .env.example .env
```

See [`.env.example`](.env.example) for all variables. Key ones for Stage 0:

| Variable | Purpose |
|---|---|
| `REDIS_URL` | Redis connection (default matches docker-compose) |
| `DATABASE_URL` | Postgres connection (default matches docker-compose) |
| `PORT` | Ingestion API port (default `3000`) |
| `AWS_*`, `SNS_TOPIC_ARN` | Required later for SNS delivery — placeholders are fine for now |

`.env` is git-ignored; never commit real credentials.

### Install dependencies

This repo uses **npm workspaces** — one install at the root links all three services:

```bash
npm install
```

### Start infrastructure (Stage 0)

Redis and Postgres only — application services run locally for now:

```bash
docker compose up -d
```

Confirm Redis is reachable:

```bash
docker compose exec redis redis-cli ping
# → PONG
```

### Run services (scaffolding)

Each service has its own dev script; from the repo root:

```bash
npm run dev:api        # Ingestion API → http://localhost:3000
npm run dev:worker     # Worker (placeholder until Stage 2)
npm run dev:dashboard  # Dashboard → http://localhost:5173
```

Quick health check:

```bash
curl http://localhost:3000/health
```

For the full staged build plan, see [`BUILD_GUIDE.md`](BUILD_GUIDE.md).

---

## Load Testing & Results

*(Fill this in once you've run real tests — this section is what makes your resume bullet defensible. Suggested metrics to capture:)*

| Metric | How to measure | Target to aim for |
|---|---|---|
| Throughput | Events/sec sustained via k6 | 300–500 events/sec on a laptop is a believable, strong number |
| p95 alert delivery latency | Time from ingestion to SNS/WebSocket delivery | Under 200ms is a strong, credible claim |
| Message loss under crash | Kill a worker mid-processing, verify redelivery | 0% — this is the headline claim |
| Duplicate delivery rate | Force retries, check dedup effectiveness | 0% with idempotency keys enabled |
| DLQ recovery | Force N failures, verify DLQ capture + alert | 100% capture, verified manually |

**Do not put a number on the resume you haven't actually measured.** A specific, true number (e.g. "420 events/sec, 180ms p95") is far more credible in an interview than a round, suspicious one (e.g. "1000+ events/sec") that you can't explain how you got.

---

## API Reference

### `POST /events`
Ingest a single event.
```json
{
  "source_id": "SENSOR_042",
  "event_type": "temperature_reading",
  "value": 9.4,
  "timestamp": "2026-08-23T10:15:00Z"
}
```

### `WS /events/stream`
WebSocket endpoint for high-frequency producers to stream events without per-request HTTP overhead.

### `GET /alerts`
Paginated alert history from Postgres.

### `GET /health/dlq`
Dead-letter queue depth and recent failure reasons — used by the dashboard's system-health panel.

---

## Roadmap
- [ ] Horizontal scaling: multiple worker replicas reading from the same consumer group
- [ ] Configurable per-source thresholds via the dashboard (no redeploy needed)
- [ ] Grafana/Prometheus metrics export
- [ ] Replace Lambda enrichment with a pluggable enrichment interface (swap providers easily)
