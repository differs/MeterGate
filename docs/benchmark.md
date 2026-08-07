# MeterGate Benchmark Report

> Method: 16-core machine; dockerized PostgreSQL 16, Redis 7, ClickHouse
> 24.8, Redpanda (Kafka) as the full billing stack; local mock upstream
> (166K req/s baseline). `hey` load generator. Tested 2026-08.

## 1. MeterGate throughput (full billing pipeline enabled)

### v2 (after hot-path optimizations)

| Scenario | req/s | avg latency |
|----------|-------|-------------|
| non-stream, 100 concurrent (30s) | **22,416** | 4.5ms |
| non-stream, 500 concurrent (30s) | **26,408** | 19ms |
| streaming, 50 concurrent (30s) | **312** | 160ms (theoretical limit 322) |

### v1 (pre-optimization, for reference)

| Scenario | req/s | avg latency |
|----------|-------|-------------|
| non-stream, 100 concurrent (30s) | 5,511 | 18ms |
| non-stream, 500 concurrent (30s) | 1,253 | 395ms |
| streaming, 50 concurrent (30s) | 323 | 155ms |

### What the 4x speedup was

| Bottleneck found by load test | Fix | Gain |
|-------------------------------|-----|------|
| synchronous metering log on hot path (77MB of JSON in one run) | Debug level (audit lives in Kafka/CH/PG) | major |
| Settler global mutex (all events, one lock) | 16-shard buffers, hash-routed, per-shard flusher | major |
| 500 independent INSERTs per batch (500 fsyncs) | true multi-row INSERT, one statement one commit | major |
| Kafka per-message CommitMessages (~1ms RPC each → 489 events/s) | batched commits (500/200ms) → 85K events/s | 174x on the consumer |
| residual batch stuck when topic goes quiet | bounded FetchMessage + drain flush on deadline | correctness |
| commit before durable write (loss window) | Settler.FlushSync before commit | correctness |

### Billing integrity (367,746 requests, full Kafka consumer mode)

| Check | Result |
|-------|--------|
| event loss | **0** |
| Layer A: PG orders == ClickHouse details | **exact** (367,746 rows / 82,742,850 micros both sides) |
| balance precision | **exact to the micro** |
| frozen after drain | **0** |
| Kafka consumer lag after drain | **0**

## 2. Billing integrity under load (213,600 requests, 3 scenarios)

| Check | Result |
|-------|--------|
| event loss (sink drops) | **0** |
| Layer A: PG orders == ClickHouse details (count & amount) | **exact** (213,600 / 48,414,690 micros both sides) |
| balance precision (1e11 start, pre-charge + settle + clawback) | **exact to the micro** |
| frozen (unsettled pre-charge) after drain | **0** |

## 3. Open-source relays measured (production-recommended configs)

| System | DB | 100 concurrent | 500 concurrent | streaming (50c) |
|--------|----|----------------|----------------|-----------------|
| one-api | SQLite (default) | **58 req/s** / 1.65s | — | — |
| one-api | PostgreSQL + mem cache | **495 req/s** / 201ms | 318 / 1.40s | 181 req/s |
| new-api | PostgreSQL + mem cache | **146 req/s** / 677ms | 261 / 1.69s | 144 req/s |
| litellm | PostgreSQL (Prisma) | **213 req/s** / 468ms | 179 / 2.08s | 101 req/s |

> The open-source numbers are bare forwarding without billing precision
> guarantees; MeterGate's 5,511 req/s includes the complete billing
> pipeline (Redis Lua pre-charge, batched PG settle, ClickHouse details,
> Kafka event bus) and was verified for exact billing integrity under load.

## 4. Root cause of the open-source ceiling

Every relay does 3-5 synchronous DB round-trips per request (auth, channel
selection, quota update, request log INSERT). That is the shared ceiling:
one-api defaults to `ORDER BY RANDOM()` in the DB for channel selection and
non-atomic quota deduction; litellm's Python data plane (GIL) + per-request
Prisma calls cap it at ~200 req/s.

## 5. Bugs MeterGate's load test caught (all fixed)

| Bug | Impact | Fix |
|-----|--------|-----|
| Settler flushed synchronously in the event handler | event backlog → drops at >4K req/s | async trigger-based flusher (single goroutine) |
| pre-charge settle never clawed back overages | long streams under-charged users | `DECRBY balance` shortfall in settle Lua |
| zero-completion insurance still billed prompt tokens | failed requests charged | NO_CHARGE orders amount = 0 (free) |
| non-stream failure path emitted no event | failed requests invisible to billing | failed events emitted (audit + NO_CHARGE) |
