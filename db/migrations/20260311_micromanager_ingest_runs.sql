CREATE TABLE IF NOT EXISTS micromanager_ingest_runs (
    id UUID PRIMARY KEY,
    triggered_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    trigger_mode TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'running',
    report_only BOOLEAN NOT NULL DEFAULT FALSE,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    ingested_count INTEGER NOT NULL DEFAULT 0,
    experiment_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_micromanager_ingest_runs_created_at ON micromanager_ingest_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_micromanager_ingest_runs_status ON micromanager_ingest_runs(status, trigger_mode, created_at DESC);
