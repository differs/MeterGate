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
| **MeterGate** (full billing: Redis pre-charge + PG batch + CH detail + Kafka) | **22,416 req/s** | **Redis Lua atomic + clawback** | **3-layer, auto-refund** |

> Benchmark environment: 16-core machine, dockerized PG/Redis/ClickHouse/Redpanda,
> local mock upstream (166K req/s baseline). MeterGate's number includes the
> COMPLETE billing pipeline (Redis pre-charge, batched PG settle, ClickHouse
> details, Kafka event bus + consumer) — the open-source numbers are bare
> forwarding without billing precision guarantees. **v2 after hot-path
> optimization: 22.4K req/s** (was 5.5K; see `docs/benchmark.md` for the
> bottleneck-by-bottleneck breakdown). Full report in `docs/benchmark.md`.

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
- [x] **Kafka consumer mode** — settle driven by the event bus (durable, multi-instance safe; replaces the in-process sink) — **done**
- [x] **LedgerAdapter** — `internal/ledger`: order/refund/balance/replay boundary, PostgreSQL default, TigerBeetle swappable — **done**
- [x] **Auto Router** — `openrouter/auto`-style model picking, pure local scoring (cost/quality dial 0-10, zero per-request latency) — **done**
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
- [x] **Extras** Kafka consumer mode, LedgerAdapter, Auto Router — **done**

## Auto Router & LedgerAdapter (extras)

```yaml
# configs/routing.example.yaml
auto_router:
  cost_quality: 7      # 0 = always most capable, 10 = always cheapest
  models: [deepseek-chat, gpt-4o-mini, gpt-4o]
```

```bash
curl -d '{"model":"auto","messages":[{"role":"user","content":"hi"}]}' \
     http://localhost:3000/v1/chat/completions
```

- **Auto Router**: pure local scoring (prompt length, reasoning hints,
  tool calls) — zero external calls, <1ms per request. Simple prompts go
  cheap, complex prompts go capable (verified end-to-end).
- **LedgerAdapter** (`internal/ledger`): the money-store boundary —
  orders/refunds/balance/replay. PostgreSQL is the default; a TigerBeetle
  adapter implements the same interface when the scale demands it.

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

## Writing

- **Problem log (12 issues, 2 money leaks, how each was found & fixed)**: [docs/PROBLEMS.md](docs/PROBLEMS.md)
- **Blog 1 — Billing precision under load**: [docs/blog/billing-precision.md](docs/blog/billing-precision.md)
- **Blog 2 — Accuracy engineering: five gaps deadlier than performance**: [docs/blog/accuracy-engineering.md](docs/blog/accuracy-engineering.md)
  (why integer math, dual-track metering, and how a 213k-request audit
  caught two real money leaks)
- **Show HN**: [docs/posts/show-hn.md](docs/posts/show-hn.md)
- **Chinese community post**: [docs/posts/chinese-community.md](docs/posts/chinese-community.md)

## Commercial stack (P0: can take money)

```bash
# portal API (admin-key protected in dev; session auth in production)
curl -X POST localhost:3002/api/register -d '{"username":"alice","password":"password123"}'
curl -X POST "localhost:3002/api/keys?user_id=1" -d '{"name":"prod"}'
curl -X POST "localhost:3002/api/recharge?user_id=1" -d '{"amount_micros":100000000,"idempotency_key":"r1"}'
curl -X POST localhost:3002/api/recharge/pay -d '{"recharge_id":1,"channel":"mock"}'
# then use the issued sk- key against /v1/chat/completions — stored keys
# and the static allowlist both authenticate (cached, 60s TTL)
```

Verified end-to-end: register → login → key → recharge → mock pay →
consume with the new key → balance exact; duplicate recharge (same
idempotency_key) and duplicate callback both no-op (money safe).

## Sessions (JWT + OIDC)

```bash
METERGATE_JWT_SECRET=<32+ bytes> METERGATE_OIDC_PROVIDER_URL=https://accounts.google.com METERGATE_OIDC_CLIENT_ID=... METERGATE_OIDC_CLIENT_SECRET=... ./metergate
```

- Login returns a session JWT (HS256, 24h); portal endpoints accept the
  JWT OR the admin key (dev).
- `GET /api/oidc/login` → redirect to the IdP (state CSRF cookie);
  `/api/oidc/callback` exchanges the code, verifies the id_token
  (issuer/audience/signature via JWKS), auto-registers the local
  account (external_identities subject linking) and returns a session
  JWT.
- Verified: login → JWT → create key with JWT; forged/expired/tampered
  JWTs rejected (unit + e2e).

## Horizontal scaling

Verified (1/2/4 instances, per-core efficiency constant) and documented:
[docs/scaling-deployment.md](docs/scaling-deployment.md) — topology,
component sizing (Kafka partitions, PG shards, Redis cluster), step-by-step
expansion (1 → 4 → 16 instances), K8s manifests
([deploy/k8s/gateway.yaml](deploy/k8s/gateway.yaml)), Kafka sizing
([deploy/kafka-sizing.md](deploy/kafka-sizing.md)), PG sharding
([deploy/pg-sharding.md](deploy/pg-sharding.md)) and a post-expansion
verification checklist.

## License

MIT
