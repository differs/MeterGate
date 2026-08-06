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

- [ ] OpenAI-compatible endpoint (`/v1/chat/completions`, SSE streaming)
- [ ] Dual-track billing engine (pre-charge + exact accumulation)
- [ ] Streaming token metering with local tokenizer cross-validation
- [ ] Daily reconciliation CLI with auto-refund
- [ ] Pluggable ledger (`LedgerAdapter`: PostgreSQL first, TigerBeetle later)
- [ ] In-memory routing snapshot (zero DB calls on hot path)
- [ ] Batch-committed bill writes (500 rows/commit, fsync 15K/s → 30/s)

## Architecture

```
client ──▶ Edge LB ──▶ Gateway (stateless, Go)
                         │  routing snapshot (in-memory, no DB on hot path)
                         ├─▶ Redis: pre-charge / rate-limit (atomic Lua)
                         ├─▶ Kafka: metering events (request-level)
                         ├─▶ ClickHouse: billing detail (audit, replayable)
                         └─▶ PostgreSQL: account ledger (strong consistency)
                                      ▲
                         reconcile CLI ┘  three-layer daily check + auto-refund
```

## Quick Start

```bash
# coming soon — docker compose one-liner
docker compose up -d
curl http://localhost:3000/v1/chat/completions \
  -H "Authorization: Bearer sk-your-key" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'
```

## Roadmap

- [ ] **M1** Single-node gateway + streaming metering + dual-track billing (PostgreSQL)
- [ ] **M2** Reconciliation CLI (Layer A: detail vs summary)
- [ ] **M3** Routing engine (price-weighted, 30s failure window, circuit breaker)
- [ ] **M4** Three-layer reconciliation + auto-refund + admin API
- [ ] **M5** ClickHouse detail tier + batch commit pipeline

## License

MIT
