# Distributed Task Queue System

A production-grade distributed job processing system built with Node.js, BullMQ, Redis, and PostgreSQL. Features priority queues, exponential backoff retry, dead letter queue, idempotent deduplication, and a real-time monitoring dashboard.

**Status:** Production Ready — Deployed on Railway + Vercel

[Live Dashboard](https://task-queue-frontend.vercel.app) | [Frontend Repo](https://github.com/shreeiya17/task-queue-frontend)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   [React Dashboard] ──WebSocket──► [Express Server :3000]   │
│                                            │                │
│   [REST API Client] ──POST /jobs──────────►│                │
│                                            ▼                │
│                                     [BullMQ Queue]          │
│                                      (Redis backed)         │
│                                            │                │
│                           ┌────────────────┼────────┐       │
│                           ▼                ▼        ▼       │
│                       [Worker 1]  [Worker 2]  [Worker N]    │
│                           │                                 │
│                           ▼                                 │
│                     [PostgreSQL]                            │
│                  (job logs + audit trail)                   │
└─────────────────────────────────────────────────────────────┘
```

**Redis** — Hot path. Stores live queue state (waiting, active, delayed jobs). BullMQ uses Redis Lists and Sorted Sets internally.

**PostgreSQL** — Cold path. Permanent job history, status updates, error logs, DLQ records. Every state transition is persisted.

**Server → Dashboard** — Socket.io WebSocket. Server polls PostgreSQL every second and broadcasts job events and stats to all connected dashboards.

---

## Features

| Feature | Implementation |
|---|---|
| Priority Queues | Redis Sorted Sets — CRITICAL(1) / HIGH(3) / NORMAL(5) / LOW(8) / BULK(10) |
| Retry Logic | Exponential backoff: 1s → 2s → 4s with ±20% random jitter (thundering herd prevention) |
| Dead Letter Queue | Failed jobs after 3 attempts → DLQ with full error trace + HTTP replay API |
| Idempotent Deduplication | MD5 content-hash jobId — duplicate submissions silently skipped |
| Delayed Jobs | BullMQ Sorted Set scored by future timestamp — schedule jobs up to days ahead |
| Concurrency Control | Configurable worker pool — benchmarked optimal at 50 workers for this workload |
| Real-time Dashboard | React + Socket.io — live queue depth, job lifecycle, p95 latency |
| Graceful Shutdown | SIGTERM handler — finishes in-flight jobs before process exit |

---

## Performance

Benchmarked with autocannon load testing on local development machine (MacBook Air M-series):

| Concurrency | Throughput | Duration (200 jobs) |
|---|---|---|
| 1 worker | 19 jobs/sec | 10.5s |
| 5 workers | 95 jobs/sec | 2.1s |
| 10 workers | 188 jobs/sec | 1.1s |
| 20 workers | 371 jobs/sec | 0.54s |
| **50 workers** | **851 jobs/sec** | **0.23s** ← optimal |

Beyond 50, Redis connection overhead dominates — throughput decreases.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js v20 |
| Queue Library | BullMQ v5 |
| Queue Backend | Redis 7 |
| Database | PostgreSQL 16 |
| API Framework | Express.js |
| WebSocket | Socket.io |
| Frontend | React + Vite |
| Charts | Recharts |

---

## Project Structure

```
task-queue-system/
├── src/
│   ├── config/
│   │   ├── redis.js          # ioredis connection — maxRetriesPerRequest: null (BullMQ requirement)
│   │   ├── db.js             # PostgreSQL pool + schema initialisation
│   │   └── constants.js      # JOB_STATUS, PRIORITY, QUEUE_NAMES, RETRY config
│   ├── queues/
│   │   └── taskQueue.js      # BullMQ Queue definition + enqueueJob() + getQueueStats()
│   ├── workers/
│   │   └── worker.js         # BullMQ Worker — routes jobs to processors, handles events
│   ├── processors/
│   │   ├── emailProcessor.js # Simulates email delivery with progress reporting
│   │   ├── reportProcessor.js# Simulates heavy PDF generation (~1.3s)
│   │   └── testProcessor.js  # Fails N times then succeeds — used to test retry logic
│   ├── routes/
│   │   └── jobs.js           # REST API — enqueue, list, stats, cancel, DLQ replay
│   ├── utils/
│   │   └── backoff.js        # Exponential backoff + jitter calculation
│   └── server.js             # Express + Socket.io — broadcasts job events to dashboard
├── dashboard/
│   └── src/
│       └── App.jsx           # React dashboard with live WebSocket updates
├── src/benchmark.js          # Concurrency benchmark — tests 1/5/10/20/50 workers
├── .env.example
└── package.json
```

---

## Getting Started

### Prerequisites

- Node.js v18+
- Redis (via Homebrew on Mac: `brew install redis && brew services start redis`)
- PostgreSQL (via Homebrew: `brew install postgresql@16 && brew services start postgresql@16`)

### Installation

```bash
# Clone the repository
git clone https://github.com/shreeiya17/task-queue-system.git
cd task-queue-system

# Install dependencies
npm install

# Copy environment variables
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Install dashboard dependencies
cd dashboard && npm install && cd ..
```

### Environment Variables

```env
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taskqueue
DB_USER=your_db_user
DB_PASSWORD=your_db_password
PORT=3000
```

### Running Locally

```bash
# Terminal 1 — start the API server
npm run dev

# Terminal 2 — start the worker process
npm run worker

# Terminal 3 — start the React dashboard
cd dashboard && npm run dev
```

Open `http://localhost:5173` for the dashboard.
Open `http://localhost:3000/health` to verify the API.

### Running the Benchmark

```bash
# Stop the worker first (Ctrl+C in Terminal 2), then:
node src/benchmark.js

# Restart worker after:
npm run worker
```

---

## API Reference

### Enqueue a Job

```
POST /api/jobs
Content-Type: application/json

{
  "type": "email",           // "email" | "report" | "test"
  "data": { ... },           // job payload — any JSON object
  "priority": 5,             // 1=CRITICAL, 3=HIGH, 5=NORMAL, 8=LOW, 10=BULK
  "delayMs": 0               // optional: delay in milliseconds before processing
}

Response: { "success": true, "jobId": "md5-hash-here" }
```

### Check Job Status

```
GET /api/jobs/:id

Response: full job row from PostgreSQL including status, attempts, result, error
```

### List Jobs

```
GET /api/jobs?status=completed&type=email&page=1&limit=20
```

### Queue Statistics

```
GET /api/jobs/stats

Response: { waiting, active, completed, failed, delayed, dlq }
```

### Cancel a Job

```
DELETE /api/jobs/:id
```

### Replay a DLQ Job

```
POST /api/jobs/dlq/:id/replay

Removes job from DLQ and re-enqueues with HIGH priority
```

---

## Key Engineering Decisions

**Why Redis for the queue instead of PostgreSQL?**
Redis operates in-memory with microsecond latency. BullMQ uses `RPOPLPUSH` — a single atomic command that moves a job from waiting to active, preventing two workers from stealing the same job. PostgreSQL could do this with `SELECT FOR UPDATE SKIP LOCKED` but adds disk I/O overhead to every job pickup.

**Why exponential backoff with jitter?**
Without jitter, if 1000 jobs fail simultaneously they all retry at exactly the same millisecond — a thundering herd that can overwhelm a recovering service. Adding ±20% random jitter spreads retries across a time window. This is the same technique documented in AWS's retry library.

**Why is the stats broadcast in server.js and not worker.js?**
Worker.js and server.js run as separate Node.js processes with isolated memory. `global.io` set in server.js is not accessible from worker.js. All Socket.io broadcasting is therefore done by server.js, which polls PostgreSQL every second for job state changes and emits them to connected dashboards.

**Why MD5 for idempotency keys?**
The jobId is derived from `MD5(type + data)`. If a client retries a failed HTTP submission, the same payload produces the same hash, so BullMQ silently skips the duplicate. The trade-off: two legitimately different jobs with identical payloads would be deduplicated — acceptable for this use case since job data includes timestamps.

**Why concurrency=50 for the benchmark sweet spot?**
Throughput scaled near-linearly from 1 to 50 workers. Beyond 50, each additional worker opens more Redis connections and the connection management overhead exceeds the processing gain. The optimal point depends on job I/O characteristics — CPU-bound jobs would reach the ceiling earlier.

---

## Concepts Demonstrated

- **Producer-consumer pattern** — decoupled job creation and execution
- **Message queue architecture** — async processing for non-blocking APIs
- **At-least-once delivery** — stalled job detection via Redis lock TTL
- **Exponential backoff with jitter** — fault-tolerant retry without thundering herd
- **Dead letter queue** — poison pill handling with replay capability
- **Idempotency** — safe client retries without duplicate job creation
- **Priority scheduling** — Redis Sorted Sets for O(log N) priority operations
- **Real-time observability** — WebSocket push for live system monitoring
- **Horizontal worker scaling** — multiple worker instances share one Redis queue
