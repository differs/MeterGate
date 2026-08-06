# MeterGate Benchmark Report

> Method: local 16-core machine, mock upstream on the same host (166K req/s
> baseline, 500 concurrent). `hey` load generator, 30-45s per scenario.
> Tested 2026-08.

## 1. Open-source relays measured (production-recommended configs)

| System | DB | 100 concurrent | 500 concurrent | streaming (50c) |
|--------|----|----------------|----------------|-----------------|
| one-api | SQLite (default) | **58 req/s** / 1.65s | — | — |
| one-api | PostgreSQL + mem cache | **495 req/s** / 201ms | 318 / 1.40s | 181 req/s |
| new-api | PostgreSQL + mem cache | **146 req/s** / 677ms | 261 / 1.69s | 144 req/s |
| litellm | PostgreSQL (Prisma) | **213 req/s** / 468ms | 179 / 2.08s | 101 req/s |

## 2. Root cause (code-level)

Every relay does 3-5 **synchronous DB round-trips per request** (auth, channel
selection, quota update, request log INSERT) — and request logging is
synchronous. That is the shared ceiling:

- one-api: channel selection defaults to `ORDER BY RANDOM()` **in the DB**;
  memory cache requires `MEMORY_CACHE_ENABLED=true` explicitly. Quota
  deduction is read-modify-write (non-atomic) — drift/oversell risk.
- new-api: heavier per-request feature chain (model validation, grouping);
  has a batch-update queue (good direction, partial coverage).
- litellm: Python data plane (GIL) + per-request Prisma DB calls.

None implement: dual-track metering, async bill writes, or daily
reconciliation.

## 3. MeterGate target

| Metric | Open-source best | MeterGate target |
|--------|------------------|------------------|
| Single instance | ~500 req/s (one-api+PG) | **5,000+ req/s** |
| Quota deduction | non-atomic (drift risk) | Redis Lua atomic |
| Billing path | sync DB per request | async, batch-committed |
| Reconciliation | none | 3-layer, auto-refund |

MeterGate M1 measured throughput will be published here once the M2 billing
pipeline lands (pre-charge + settle) — the pipeline is the realistic load.
