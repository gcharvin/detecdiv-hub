CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    user_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    email TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    external_key TEXT UNIQUE,
    microscope_name TEXT,
    acquisition_label TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private',
    status TEXT NOT NULL DEFAULT 'discovered',
    completeness_status TEXT NOT NULL DEFAULT 'unknown',
    total_bytes BIGINT NOT NULL DEFAULT 0,
    last_size_scan_at TIMESTAMPTZ,
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
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    project_key TEXT UNIQUE,
    project_name TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private',
    status TEXT NOT NULL DEFAULT 'indexed',
    health_status TEXT NOT NULL DEFAULT 'ok',
    fov_count INTEGER NOT NULL DEFAULT 0,
    roi_count INTEGER NOT NULL DEFAULT 0,
    classifier_count INTEGER NOT NULL DEFAULT 0,
    processor_count INTEGER NOT NULL DEFAULT 0,
    pipeline_run_count INTEGER NOT NULL DEFAULT 0,
    available_raw_count INTEGER NOT NULL DEFAULT 0,
    missing_raw_count INTEGER NOT NULL DEFAULT 0,
    run_json_count INTEGER NOT NULL DEFAULT 0,
    h5_count INTEGER NOT NULL DEFAULT 0,
    h5_bytes BIGINT NOT NULL DEFAULT 0,
    latest_run_status TEXT,
    latest_run_at TIMESTAMPTZ,
    project_mat_bytes BIGINT NOT NULL DEFAULT 0,
    project_dir_bytes BIGINT NOT NULL DEFAULT 0,
    estimated_raw_bytes BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    last_size_scan_at TIMESTAMPTZ,
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

CREATE TABLE IF NOT EXISTS project_acl (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    access_level TEXT NOT NULL DEFAULT 'viewer',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, user_id)
);

CREATE TABLE IF NOT EXISTS project_groups (
    id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(owner_user_id, group_key)
);

CREATE TABLE IF NOT EXISTS project_group_members (
    id BIGSERIAL PRIMARY KEY,
    group_id UUID NOT NULL REFERENCES project_groups(id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(group_id, project_id)
);

CREATE TABLE IF NOT EXISTS project_notes (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    author_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    note_text TEXT NOT NULL,
    is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_deletion_events (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    requested_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'previewed',
    delete_project_files BOOLEAN NOT NULL DEFAULT FALSE,
    delete_linked_raw_data BOOLEAN NOT NULL DEFAULT FALSE,
    reclaimable_bytes BIGINT NOT NULL DEFAULT 0,
    preview_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at TIMESTAMPTZ
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

ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'private';
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS total_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS last_size_scan_at TIMESTAMPTZ;

ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'private';
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS fov_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS roi_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS classifier_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS processor_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS pipeline_run_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS available_raw_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS missing_raw_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS run_json_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS h5_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS h5_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS latest_run_status TEXT;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS latest_run_at TIMESTAMPTZ;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS project_mat_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS project_dir_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS estimated_raw_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS total_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS last_size_scan_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_projects_name ON detecdiv_projects(project_name);
CREATE INDEX IF NOT EXISTS idx_projects_owner_user_id ON detecdiv_projects(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_projects_visibility ON detecdiv_projects(visibility);
CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_project_id ON jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_raw_datasets_status ON raw_datasets(status, completeness_status);
CREATE INDEX IF NOT EXISTS idx_raw_datasets_owner_user_id ON raw_datasets(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_project_acl_user_id ON project_acl(user_id);
CREATE INDEX IF NOT EXISTS idx_project_groups_owner_user_id ON project_groups(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_project_group_members_project_id ON project_group_members(project_id);
CREATE INDEX IF NOT EXISTS idx_project_notes_project_id ON project_notes(project_id);
CREATE INDEX IF NOT EXISTS idx_project_deletion_events_project_id ON project_deletion_events(project_id);
