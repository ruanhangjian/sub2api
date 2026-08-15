CREATE TABLE IF NOT EXISTS image_tasks (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL UNIQUE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    platform VARCHAR(32) NOT NULL,
    target VARCHAR(32) NOT NULL,
    model VARCHAR(255) NOT NULL DEFAULT '',
    method VARCHAR(16) NOT NULL DEFAULT 'POST',
    request_path TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    request_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_body BYTEA NOT NULL,
    request_hash VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(255),
    status VARCHAR(32) NOT NULL DEFAULT 'queued',
    http_status INTEGER NOT NULL DEFAULT 0,
    result JSONB,
    error JSONB,
    hold_amount DECIMAL(20,10) NOT NULL DEFAULT 0,
    hold_released_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_image_tasks_status_created
    ON image_tasks(status, created_at);
CREATE INDEX IF NOT EXISTS idx_image_tasks_expires_at
    ON image_tasks(expires_at);
CREATE INDEX IF NOT EXISTS idx_image_tasks_owner
    ON image_tasks(user_id, api_key_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_image_tasks_owner_idempotency
    ON image_tasks(user_id, api_key_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';
