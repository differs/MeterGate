-- MeterGate commercial schema: users, API keys, recharges, payments.
-- Money stays in int64 micro-units. Recharges are idempotent by
-- idempotency_key; payments verify by signature + idempotency key.

CREATE TABLE IF NOT EXISTS users (
    id           BIGSERIAL PRIMARY KEY,
    username     TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,          -- bcrypt
    status       SMALLINT NOT NULL DEFAULT 1, -- 1=enabled 0=disabled
    rpm_limit    BIGINT NOT NULL DEFAULT 0,   -- user-level aggregate RPM (0=unlimited)
    tpm_limit    BIGINT NOT NULL DEFAULT 0,   -- user-level aggregate TPM (0=unlimited)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES users(id),
    key_hash   TEXT UNIQUE NOT NULL,      -- sha256 of the sk- key (never raw)
    name       TEXT NOT NULL DEFAULT '',
    status     SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    rpm_limit       INT NOT NULL DEFAULT 0,     -- requests per minute (0=unlimited)
    tpm_limit       BIGINT NOT NULL DEFAULT 0,  -- tokens per minute
    concurrency_limit INT NOT NULL DEFAULT 0    -- in-flight requests
);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys (user_id);

-- recharge: user top-up order (money IN)
CREATE TABLE IF NOT EXISTS recharges (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT NOT NULL REFERENCES users(id),
    amount_micros    BIGINT NOT NULL,
    currency         TEXT NOT NULL DEFAULT 'CNY',
    status           TEXT NOT NULL DEFAULT 'PENDING', -- PENDING|PAID|FAILED|REFUNDED
    idempotency_key  TEXT UNIQUE NOT NULL,            -- client/order generated
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    paid_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_recharges_user ON recharges (user_id, created_at DESC);

-- payment: channel-side payment record (mock in dev; alipay/wechat/stripe later)
CREATE TABLE IF NOT EXISTS payments (
    id               BIGSERIAL PRIMARY KEY,
    recharge_id      BIGINT NOT NULL REFERENCES recharges(id),
    channel          TEXT NOT NULL,        -- mock | alipay | wechat | stripe
    channel_txn_id   TEXT NOT NULL,        -- channel-side transaction id
    amount_micros    BIGINT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PAID',
    raw_callback     TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel, channel_txn_id)       -- replay-safe callback
);

-- OIDC identity linking (subject → local user)
CREATE TABLE IF NOT EXISTS external_identities (
    provider TEXT NOT NULL,
    subject  TEXT NOT NULL,
    user_id  BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, subject)
);
