CREATE TABLE IF NOT EXISTS archive_policy_runs (
    id UUID PRIMARY KEY,
    triggered_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    trigger_mode TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'running',
    report_only BOOLEAN NOT NULL DEFAULT FALSE,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    queued_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    total_reclaimable_bytes BIGINT NOT NULL DEFAULT 0,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_archive_policy_runs_created_at ON archive_policy_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_archive_policy_runs_status ON archive_policy_runs(status, trigger_mode, created_at DESC);
