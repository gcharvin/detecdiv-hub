CREATE TABLE IF NOT EXISTS acquisition_sessions (
    id UUID PRIMARY KEY,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    raw_dataset_id UUID REFERENCES raw_datasets(id) ON DELETE SET NULL,
    experiment_project_id UUID REFERENCES experiment_projects(id) ON DELETE SET NULL,
    session_key TEXT NOT NULL UNIQUE,
    acquisition_label TEXT NOT NULL,
    microscope_name TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    landing_storage_root_id BIGINT REFERENCES storage_roots(id) ON DELETE SET NULL,
    landing_relative_path TEXT,
    local_spool_path TEXT,
    transfer_status TEXT NOT NULL DEFAULT 'not_started',
    progress_percent DOUBLE PRECISION,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    acquisition_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_acquisition_sessions_owner_status
    ON acquisition_sessions(owner_user_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_acquisition_sessions_landing_root
    ON acquisition_sessions(landing_storage_root_id, landing_relative_path);
CREATE INDEX IF NOT EXISTS idx_acquisition_sessions_raw_dataset_id
    ON acquisition_sessions(raw_dataset_id);
CREATE INDEX IF NOT EXISTS idx_acquisition_sessions_experiment_project_id
    ON acquisition_sessions(experiment_project_id);
