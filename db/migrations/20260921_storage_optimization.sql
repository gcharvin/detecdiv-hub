-- Durable, resumable storage-optimization runs.  Physical work stays on the
-- storage-visible worker; the API only plans and queues these runs.
ALTER TABLE raw_datasets
    ADD COLUMN IF NOT EXISTS storage_optimization_status TEXT NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS storage_optimization_run_id UUID,
    ADD COLUMN IF NOT EXISTS storage_optimization_saved_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS storage_optimized_at TIMESTAMPTZ;

ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS storage_optimization_status TEXT NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS storage_optimization_run_id UUID,
    ADD COLUMN IF NOT EXISTS storage_optimization_saved_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS storage_optimized_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS storage_optimization_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_dataset_id UUID REFERENCES raw_datasets(id) ON DELETE CASCADE,
    project_id UUID REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    requested_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    requested_by TEXT,
    scope_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    codec TEXT NOT NULL DEFAULT 'deflate',
    policy_version TEXT NOT NULL DEFAULT 'v1',
    source_bytes BIGINT NOT NULL DEFAULT 0,
    output_bytes BIGINT NOT NULL DEFAULT 0,
    saved_bytes BIGINT NOT NULL DEFAULT 0,
    total_files INTEGER NOT NULL DEFAULT 0,
    completed_files INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((raw_dataset_id IS NOT NULL)::int + (project_id IS NOT NULL)::int = 1)
);

CREATE TABLE IF NOT EXISTS storage_optimization_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES storage_optimization_runs(id) ON DELETE CASCADE,
    job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
    relative_path TEXT NOT NULL,
    file_format TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    source_bytes BIGINT NOT NULL DEFAULT 0,
    output_bytes BIGINT,
    saved_bytes BIGINT NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    error_text TEXT,
    verification_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE(run_id, relative_path)
);

CREATE INDEX IF NOT EXISTS storage_optimization_runs_status_idx ON storage_optimization_runs(status, created_at);
CREATE INDEX IF NOT EXISTS storage_optimization_files_pending_idx ON storage_optimization_files(run_id, status, relative_path);
