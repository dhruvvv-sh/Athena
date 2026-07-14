-- FileTransfer schema. Applied idempotently by the master on startup.

CREATE TABLE IF NOT EXISTS ft_transfers (
    id           TEXT PRIMARY KEY,
    flow_id      TEXT NOT NULL DEFAULT '',
    source       TEXT NOT NULL,
    source_kind  TEXT NOT NULL DEFAULT 'file',
    target       TEXT NOT NULL,
    target_kind  TEXT NOT NULL DEFAULT 'file',
    size_bytes   BIGINT NOT NULL DEFAULT 0,
    chunk_size   BIGINT NOT NULL,
    checksum     TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'pending',
    error        TEXT NOT NULL DEFAULT '',
    priority     INT  NOT NULL DEFAULT 0,
    requested_by TEXT NOT NULL DEFAULT '',
    latency_ms   BIGINT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Added after the initial release; ALTER is idempotent so existing tables get the column.
ALTER TABLE ft_transfers ADD COLUMN IF NOT EXISTS flow_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_ft_transfers_status ON ft_transfers(status);
CREATE INDEX IF NOT EXISTS idx_ft_transfers_flow ON ft_transfers(flow_id);

-- Claimable work items. A worker claims a task atomically (see store.ClaimTask).
CREATE TABLE IF NOT EXISTS ft_tasks (
    id           BIGSERIAL PRIMARY KEY,
    transfer_id  TEXT NOT NULL REFERENCES ft_transfers(id) ON DELETE CASCADE,
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | claimed | done | failed
    node_id      TEXT,
    claimed_at   TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ft_tasks_claimable ON ft_tasks(status, created_at) WHERE status = 'pending';

-- Per-chunk state, shared so any receiver node can accept a chunk and assemble.
CREATE TABLE IF NOT EXISTS ft_chunks (
    transfer_id TEXT NOT NULL REFERENCES ft_transfers(id) ON DELETE CASCADE,
    seq         INT  NOT NULL,
    "offset"    BIGINT NOT NULL,
    size        BIGINT NOT NULL,
    checksum    TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending',
    node_id     TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transfer_id, seq)
);

-- Audit log of every file-transfer request received at POST /api/transfers, whether it
-- was accepted (a transfer created) or rejected (authorization/validation failure).
CREATE TABLE IF NOT EXISTS ft_requests (
    id           BIGSERIAL PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    flow_id      TEXT NOT NULL DEFAULT '',
    source       TEXT NOT NULL DEFAULT '',
    target       TEXT NOT NULL DEFAULT '',
    sender_app   TEXT NOT NULL DEFAULT '',
    target_app   TEXT NOT NULL DEFAULT '',
    client_cn    TEXT NOT NULL DEFAULT '',
    remote_addr  TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL DEFAULT '',   -- accepted | rejected
    status_code  INT  NOT NULL DEFAULT 0,
    error        TEXT NOT NULL DEFAULT '',
    transfer_id  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_ft_requests_created ON ft_requests(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ft_requests_outcome ON ft_requests(outcome);

-- Read/Write/Delete permissions by path prefix and principal.
CREATE TABLE IF NOT EXISTS ft_permissions (
    id          BIGSERIAL PRIMARY KEY,
    principal   TEXT NOT NULL,
    path_prefix TEXT NOT NULL,
    can_read    BOOLEAN NOT NULL DEFAULT false,
    can_write   BOOLEAN NOT NULL DEFAULT false,
    can_delete  BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (principal, path_prefix)
);

-- Registered worker nodes (health / observability).
CREATE TABLE IF NOT EXISTS ft_nodes (
    id         TEXT PRIMARY KEY,
    role       TEXT NOT NULL DEFAULT 'worker',
    endpoint   TEXT NOT NULL DEFAULT '',
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
