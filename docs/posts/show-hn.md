## Show HN: MeterGate — an open-source LLM gateway with bank-grade billing precision

**MeterGate** is an open-source, commercial-grade LLM API gateway. Unlike
one-api / new-api / LiteLLM — which all stop at "subtract from total quota"
— it implements the full billing stack a production AI platform needs, and
we verified it under load.

**Why we built it**

Every open-source LLM relay has the same blind spot, and we measured it:
non-atomic quota deduction (read-modify-write — concurrent requests lose
updates, so bills drift), no reconciliation, no refunds. At 20-40% gross
margins, one billing incident eats a month of profit; two kill trust. We
read the source and benchmarked:

| system | 100 concurrent | quota deduction | reconciliation |
|--------|----------------|-----------------|----------------|
| one-api (SQLite default) | 58 req/s | non-atomic | none |
| one-api (PostgreSQL) | 495 req/s | non-atomic | none |
| new-api (PostgreSQL) | 146 req/s | atomic SQL | partial |
| litellm (PostgreSQL) | 213 req/s | Redis counter | none |
| **MeterGate** (full billing on) | **5,511 req/s** | Redis Lua atomic + clawback | 3-layer, auto-refund |

**What's inside**

- Dual-track metering: atomic Redis Lua pre-charge (no oversell, 402 on
  insufficient balance) + async batched settle into PostgreSQL (500
  rows/50ms commit, idempotent via request_id)
- Streaming-accurate billing: local tokenizer cross-validated against
  upstream usage (>1% variance → audit); per-channel abort billing policy
- Zero-completion insurance: failed requests are free (verified)
- Three-layer reconciliation (orders ⇄ details ⇄ Redis) with auto-refunds
  (small auto-executed, large manual, idempotent re-runs)
- Routing: inverse-square price weighting, 30s failure window, circuit
  breaker, failover (4xx never retries) — OpenRouter's default strategy
- Event bus (Kafka) + ClickHouse detail tier + LedgerAdapter (PG default,
  TigerBeetle swappable) + `model:"auto"` local scoring router

**Billing integrity under load — 213,600 requests, 3 scenarios:**

- event loss: **0**
- PG orders == ClickHouse details: **exact** (count and amount)
- balance: **exact to the micro** (pre-charge + settle + clawback)
- frozen (unsettled pre-charge) after drain: **0**

The load test caught 4 real bugs, two of them money leaks (settle never
clawed back overages on long streams; zero-completion insurance still
billed prompt tokens). Unit tests couldn't find these — only 213k audited
real orders could. Details in the blog post.

Everything runs with `docker compose up -d` (PG + Redis + ClickHouse +
Redpanda + mock upstream).

- GitHub: https://github.com/differs/MeterGate
- Blog: https://github.com/differs/MeterGate/blob/main/docs/blog/billing-precision.md
- Benchmark: https://github.com/differs/MeterGate/blob/main/docs/benchmark.md

MIT licensed, pure Go, zero-dependency data plane. Feedback welcome —
especially on the billing semantics and the reconciliation rules.
