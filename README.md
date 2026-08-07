# MeterGate

**The open-source AI gateway with bank-grade billing precision.**

MeterGate is a commercial-grade LLM API aggregation gateway. Unlike existing open-source relay projects (one-api, new-api, LiteLLM) that stop at "subtract from total quota", MeterGate implements the full billing stack that production AI platforms actually need:

- **Dual-track metering** — atomic Redis pre-charge (no oversell) + async exact accumulation (no rounding drift)
- **Streaming-accurate billing** — SSE token counting with local tokenizer cross-validation against upstream `usage` (>1% variance triggers audit)
- **Daily reconciliation** — three-layer reconciliation (metering logs ⇄ ledger ⇄ Redis balance) with auto refunds within a 0.05% tolerance
- **Immutable double-entry ledger** — append-only `ledger_entries`, replayable, auditable, idempotent (`request_id` unique constraints)

## Why MeterGate?

All open-source LLM relays have the same two blind spots — **we measured them**:

| System | Throughput (100 concurrent) | Quota deduction | Reconciliation |
|--------|----------------------------|-----------------|----------------|
| one-api (SQLite default) | **58 req/s** | read-modify-write, **non-atomic** | none |
| one-api (PostgreSQL) | 495 req/s | non-atomic | none |
| new-api (PostgreSQL) | 146 req/s | atomic SQL, batch queue | partial |
| LiteLLM (PostgreSQL) | 213 req/s | Redis counter + DB fallback | none |
| **MeterGate (target)** | **5,000+ req/s** | **Redis Lua atomic** | **3-layer, auto-refund** |

> Benchmark environment: 16-core machine, local mock upstream (166K req/s baseline). Full report in `docs/benchmark.md`.

## Features

- [x] OpenAI-compatible endpoint (`/v1/chat/completions`, SSE streaming) — **M1 done**
- [x] Streaming token metering with local tokenizer cross-validation — **M1 done** (approximate estimator; tiktoken seam ready)
- [x] Request-level metering events (request_id idempotency key) — **M1 done** (log sink; Kafka seam ready)
- [x] **Dual-track billing** — Redis Lua atomic pre-charge (fast path, 402 on insufficient balance) + async settle into PostgreSQL (slow path, terminal-state orders, idempotent) — **M2 done**
- [x] Reconcile CLI (`cmd/reconcile`): day summary by status, anomaly scan, frozen-balance sweep — **M2 done**
- [x] **Routing engine** — multi-channel, inverse-square price weighting (~9:1 at 3x price gap, verified), 30s failure window, per-channel circuit breaker, automatic failover (4xx never retries) — **M3 done**
- [x] **Reconciliation engine** — Layer A (order integrity) + Layer B (frozen leak) + Layer C (day close); auto-refunds for negative-amount orders (small = auto-executed, large = manual), idempotent re-runs — **M4 done**
- [x] **Admin API** — balance / orders / refunds / reconcile trigger (Bearer key auth) — **M4 done**
- [x] **Batch-committed bill writes** — Settler buffers 500 orders / 50ms → single multi-row INSERT (fsync 15K/s → ~30/s) — **M5 done**
- [x] **ClickHouse detail tier** — per-request `billing_detail` (180-day TTL, Layer A verified: PG orders == CH details, count and amount) — **M5 done**
- [x] **Kafka event bus** — `metering.events` topic (hash-partitioned by request_id), async producer never blocks the gateway — **M5 done**
- [ ] Pluggable ledger (`LedgerAdapter`: PostgreSQL first, TigerBeetle later)
- [ ] In-memory routing snapshot (zero DB calls on hot path) — partial: immutable atomic snapshot swap in place
- [ ] Batch-committed bill writes (500 rows/commit, fsync 15K/s → 30/s)
- [ ] Three-layer reconciliation + auto-refund

## Architecture

```
client ──▶ Gateway (stateless, Go)
             │  fast path: Redis Lua pre-charge (atomic, no oversell)
             │  hot path:  stream + local token metering
             ▼
           metering events ──┬─▶ Kafka `metering.events` (event bus, M5)
                             └─▶ ClickHouse `billing_detail` (audit, TTL 180d)
             ▼
           Settler (batch 500/50ms) ──▶ PostgreSQL orders (terminal state,
                                        idempotent) ──▶ reconcile CLI
```

## Quick Start

```bash
# dependencies (or point at your own PG/Redis)
docker run -d --name mg-pg -e POSTGRES_PASSWORD=mg -e POSTGRES_DB=metergate -p 5435:5432 postgres:16-alpine
docker run -d --name mg-redis -p 6380:6379 redis:7-alpine

# build
go build -o metergate ./cmd/metergate
go build -o reconcile ./cmd/reconcile

# run against any OpenAI-compatible upstream, with full billing
METERGATE_UPSTREAM=http://127.0.0.1:9901/v1/chat/completions \
METERGATE_UPSTREAM_KEY=sk-upstream \
METERGATE_API_KEYS=sk-your-key \
METERGATE_REDIS_ADDR=127.0.0.1:6380 \
METERGATE_PG_DSN=postgres://postgres:mg@127.0.0.1:5435/metergate \
./metergate

# call it like OpenAI
curl http://localhost:3000/v1/chat/completions \
  -H "Authorization: Bearer sk-your-key" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

Every request is pre-charged atomically in Redis (rejected with `402` when
the balance cannot cover the estimate) and settled asynchronously into
PostgreSQL — terminal order rows keyed by `request_id`, safe to replay.

```bash
# daily reconciliation
./reconcile --pg-dsn postgres://postgres:mg@127.0.0.1:5435/metergate \
            --redis 127.0.0.1:6380 --sweep
```

## Roadmap

- [x] **M1** Single-node gateway + streaming metering + request-level events — **done**
- [x] **M2** Dual-track billing (Redis pre-charge + async settle into PostgreSQL) + reconcile CLI — **done**
- [x] **M3** Routing engine (price-weighted, 30s failure window, circuit breaker) — **done**
- [x] **M4** Three-layer reconciliation + auto-refund + admin API — **done**
- [x] **M5** ClickHouse detail tier + batch-commit pipeline + Kafka event bus — **done**

## Event bus & detail tier (M5)

```bash
METERGATE_KAFKA=127.0.0.1:9092 METERGATE_CLICKHOUSE=127.0.0.1:9000 ./metergate
```

- Metering events fan out to Kafka (hash-partitioned by `request_id` for
  per-request ordering) and ClickHouse (per-request detail rows, 180-day
  TTL) without blocking the gateway hot path — async producers, delivery
  errors logged (audit log is the recovery source).
- Settler batches 500 orders / 50ms into a single multi-row INSERT
  (one commit per batch instead of one per request).
- **Layer A verified end-to-end**: PG orders and ClickHouse details agree
  on count and amount (30 reqs → 30/30 rows, 3450/3450 micros).

## Reconciliation & Admin (M4)

```bash
# CLI
./reconcile --pg-dsn postgres://... --redis 127.0.0.1:6379 --auto-refund

# Admin API (enable with METERGATE_ADMIN_PORT / METERGATE_ADMIN_KEY)
curl -H "Authorization: Bearer admin-secret" \
     "http://localhost:3001/admin/balance?user=sk-xxx"
curl -X POST -H "Authorization: Bearer admin-secret" \
     -d '{"day":"2026-08-06","auto_refund":true}' \
     http://localhost:3001/admin/reconcile
```

Reconciliation detects order anomalies (e.g. negative amounts from settle
bugs) and issues refund entries: small amounts credit the Redis balance
immediately, large amounts pend for manual approval. Every refund is
idempotent (`idempotency_key`) — re-running reconcile never double-credits
(verified end-to-end: -500µ order → +500µ credited → re-run = 0 refunds).

## Routing (M3)

Multi-channel routing mirrors OpenRouter's default strategy. See
`configs/routing.example.yaml`:

```yaml
channels:
  - id: openai
    base_url: https://api.openai.com/v1/chat/completions
    key_env: CHANNEL_OPENAI_KEY
    input_per_1m: 2500000
    output_per_1m: 10000000
models:
  - model: gpt-4o
    channels: [openai, azure-openai]   # failover + price-weighted balancing
```

```bash
METERGATE_CONFIG=./configs/routing.example.yaml ./metergate
```

- **Inverse-square price weighting**: a $1 channel gets ~9x the traffic of
  a $3 channel (verified in load test: 9.5:1 at 200 requests).
- **30s failure window**: channels with significant recent failures are
  deprioritized but kept as last-resort fallbacks.
- **Circuit breaker**: Closed → Open (fast-fail) → HalfOpen (probe) → Closed;
  exponential backoff capped at 5 minutes.
- **Failover**: primary → fallbacks; 4xx client errors never retry
  (verified: killing the primary channel yields 100% successful requests
  through the fallback).

## License

MIT
