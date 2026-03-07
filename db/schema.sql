CREATE TABLE IF NOT EXISTS storage_roots (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    root_type TEXT NOT NULL,
    host_scope TEXT NOT NULL,
    path_prefix TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_datasets (
    id UUID PRIMARY KEY,
    external_key TEXT UNIQUE,
    microscope_name TEXT,
    acquisition_label TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'discovered',
    completeness_status TEXT NOT NULL DEFAULT 'unknown',
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_dataset_locations (
    id BIGSERIAL PRIMARY KEY,
    raw_dataset_id UUID NOT NULL REFERENCES raw_datasets(id) ON DELETE CASCADE,
    storage_root_id BIGINT NOT NULL REFERENCES storage_roots(id) ON DELETE RESTRICT,
    relative_path TEXT NOT NULL,
    is_preferred BOOLEAN NOT NULL DEFAULT FALSE,
    access_mode TEXT NOT NULL DEFAULT 'read',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(raw_dataset_id, storage_root_id, relative_path)
);

CREATE TABLE IF NOT EXISTS detecdiv_projects (
    id UUID PRIMARY KEY,
    project_key TEXT UNIQUE,
    project_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'indexed',
    health_status TEXT NOT NULL DEFAULT 'ok',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_locations (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    storage_root_id BIGINT NOT NULL REFERENCES storage_roots(id) ON DELETE RESTRICT,
    relative_path TEXT NOT NULL,
    project_file_name TEXT,
    access_mode TEXT NOT NULL DEFAULT 'readwrite',
    is_preferred BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, storage_root_id, relative_path)
);

CREATE TABLE IF NOT EXISTS project_raw_links (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    raw_dataset_id UUID NOT NULL REFERENCES raw_datasets(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL DEFAULT 'source',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, raw_dataset_id, link_type)
);

CREATE TABLE IF NOT EXISTS pipelines (
    id UUID PRIMARY KEY,
    pipeline_key TEXT UNIQUE,
    display_name TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0',
    runtime_kind TEXT NOT NULL DEFAULT 'matlab',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS execution_targets (
    id UUID PRIMARY KEY,
    target_key TEXT UNIQUE,
    display_name TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    host_name TEXT,
    supports_gpu BOOLEAN NOT NULL DEFAULT FALSE,
    supports_matlab BOOLEAN NOT NULL DEFAULT FALSE,
    supports_python BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'online',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    project_id UUID REFERENCES detecdiv_projects(id) ON DELETE SET NULL,
    raw_dataset_id UUID REFERENCES raw_datasets(id) ON DELETE SET NULL,
    pipeline_id UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    execution_target_id UUID REFERENCES execution_targets(id) ON DELETE SET NULL,
    requested_mode TEXT NOT NULL DEFAULT 'auto',
    resolved_mode TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 100,
    requested_by TEXT,
    requested_from_host TEXT,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    artifact_kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_projects_name ON detecdiv_projects(project_name);
CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_project_id ON jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_raw_datasets_status ON raw_datasets(status, completeness_status);

