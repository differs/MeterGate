-- MeterGate billing schema (PostgreSQL).
-- Orders are inserted in their TERMINAL state: no UPDATEs, no status
-- transitions — append-only by construction (see docs/PostgreSQL账务层优化设计.md L1).
--
-- amount_micros: int64 micro-units of the base currency (1e-6).

CREATE TABLE IF NOT EXISTS orders (
    request_id         TEXT PRIMARY KEY,          -- idempotency key (gateway-issued)
    user_id            TEXT NOT NULL,
    model              TEXT NOT NULL,
    provider           TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'SETTLED',  -- SETTLED | NO_CHARGE
    prompt_tokens      BIGINT NOT NULL DEFAULT 0,
    completion_tokens  BIGINT NOT NULL DEFAULT 0,
    total_tokens       BIGINT NOT NULL DEFAULT 0,
    amount_micros      BIGINT NOT NULL DEFAULT 0,
    duration_ms        BIGINT NOT NULL DEFAULT 0,
    ttft_ms            BIGINT NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_created
    ON orders (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status_created
    ON orders (status, created_at);

-- account_ledger: per-user per-window summary (L2 detail/summary split).
-- M2 creates the table; the summary writer lands with the L2 pipeline.
CREATE TABLE IF NOT EXISTS account_ledger (
    id               BIGSERIAL PRIMARY KEY,
    account_id       TEXT NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    direction        SMALLINT NOT NULL,           -- 1=credit 2=debit
    amount_micros    BIGINT NOT NULL,
    req_count        INT NOT NULL DEFAULT 0,
    idempotency_key  TEXT UNIQUE NOT NULL,        -- account_id:window_start:direction
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_account
    ON account_ledger (account_id, window_start);
