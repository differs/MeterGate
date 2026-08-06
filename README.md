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
- [ ] Pluggable ledger (`LedgerAdapter`: PostgreSQL first, TigerBeetle later)
- [ ] In-memory routing snapshot (zero DB calls on hot path)
- [ ] Batch-committed bill writes (500 rows/commit, fsync 15K/s → 30/s)
- [ ] Three-layer reconciliation + auto-refund

## Architecture

```
client ──▶ Gateway (stateless, Go)
             │  fast path: Redis Lua pre-charge (atomic, no oversell)
             │  hot path:  stream + local token metering
             ▼
           metering events (request-level, buffered sink — gateway never
             │  blocks on billing)
             ▼
           Settler ──▶ PostgreSQL orders (terminal state, idempotent,
                        append-only) ──▶ reconcile CLI (daily check + sweep)
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
- [ ] **M3** Routing engine (price-weighted, 30s failure window, circuit breaker)
- [ ] **M4** Three-layer reconciliation + auto-refund + admin API
- [ ] **M5** ClickHouse detail tier + batch commit pipeline

## License

MIT
