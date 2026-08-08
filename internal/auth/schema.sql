-- MeterGate commercial schema: users, API keys, recharges, payments.
-- Money stays in int64 micro-units. Recharges are idempotent by
-- idempotency_key; payments verify by signature + idempotency key.

-- Layers 4-5 of the six-layer budget model: org → team → project.
-- orgs aggregate teams; teams aggregate projects.
CREATE TABLE IF NOT EXISTS orgs (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    rpm_limit  BIGINT NOT NULL DEFAULT 0,  -- org aggregate RPM (0=unlimited)
    tpm_limit  BIGINT NOT NULL DEFAULT 0,  -- org aggregate TPM
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS teams (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    org_id     BIGINT REFERENCES orgs(id), -- layer 5: team belongs to an org
    rpm_limit  BIGINT NOT NULL DEFAULT 0,  -- team aggregate RPM (0=unlimited)
    tpm_limit  BIGINT NOT NULL DEFAULT 0,  -- team aggregate TPM
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_teams_org ON teams (org_id);

-- Layer 3: multiple users share one project-level RPM/TPM budget.
CREATE TABLE IF NOT EXISTS projects (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    team_id    BIGINT REFERENCES teams(id), -- layer 4: project belongs to a team
    rpm_limit  BIGINT NOT NULL DEFAULT 0,  -- project aggregate RPM (0=unlimited)
    tpm_limit  BIGINT NOT NULL DEFAULT 0,  -- project aggregate TPM
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Idempotent migration for existing deployments.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS team_id BIGINT REFERENCES teams(id);
CREATE INDEX IF NOT EXISTS idx_projects_team ON projects (team_id);

CREATE TABLE IF NOT EXISTS users (
    id           BIGSERIAL PRIMARY KEY,
    username     TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,          -- bcrypt
    status       SMALLINT NOT NULL DEFAULT 1, -- 1=enabled 0=disabled
    rpm_limit    BIGINT NOT NULL DEFAULT 0,   -- user-level aggregate RPM (0=unlimited)
    tpm_limit    BIGINT NOT NULL DEFAULT 0,   -- user-level aggregate TPM (0=unlimited)
    project_id   BIGINT REFERENCES projects(id), -- layer 3: shared project budget
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Idempotent migration for existing deployments.
ALTER TABLE users ADD COLUMN IF NOT EXISTS project_id BIGINT REFERENCES projects(id);
CREATE INDEX IF NOT EXISTS idx_users_project ON users (project_id);

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
    concurrency_limit INT NOT NULL DEFAULT 0,   -- in-flight requests
    end_user_rpm_limit BIGINT NOT NULL DEFAULT 0, -- layer 6: per end-user RPM
    end_user_tpm_limit BIGINT NOT NULL DEFAULT 0  -- layer 6: per end-user TPM
);
-- Idempotent migration for existing deployments.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS end_user_rpm_limit BIGINT NOT NULL DEFAULT 0;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS end_user_tpm_limit BIGINT NOT NULL DEFAULT 0;
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
