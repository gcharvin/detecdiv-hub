CREATE TABLE IF NOT EXISTS external_publication_records (
    id UUID PRIMARY KEY,
    experiment_project_id UUID NOT NULL REFERENCES experiment_projects(id) ON DELETE CASCADE,
    system_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    external_id TEXT,
    external_url TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    last_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_external_publication_records_experiment_project_id ON external_publication_records(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_external_publication_records_system_key ON external_publication_records(system_key, status);
