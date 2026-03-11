CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    user_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    email TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    password_hash TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    client_label TEXT,
    last_seen_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
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

CREATE TABLE IF NOT EXISTS experiment_projects (
    id UUID PRIMARY KEY,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    experiment_key TEXT UNIQUE,
    title TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private',
    status TEXT NOT NULL DEFAULT 'indexed',
    summary TEXT,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    total_raw_bytes BIGINT NOT NULL DEFAULT 0,
    total_derived_bytes BIGINT NOT NULL DEFAULT 0,
    last_indexed_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS experiment_raw_links (
    id BIGSERIAL PRIMARY KEY,
    experiment_project_id UUID NOT NULL REFERENCES experiment_projects(id) ON DELETE CASCADE,
    raw_dataset_id UUID NOT NULL REFERENCES raw_datasets(id) ON DELETE CASCADE,
    link_type TEXT NOT NULL DEFAULT 'acquisition',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(experiment_project_id, raw_dataset_id, link_type)
);

CREATE TABLE IF NOT EXISTS detecdiv_projects (
    id UUID PRIMARY KEY,
    experiment_project_id UUID REFERENCES experiment_projects(id) ON DELETE SET NULL,
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

CREATE TABLE IF NOT EXISTS indexing_jobs (
    id UUID PRIMARY KEY,
    requested_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    source_kind TEXT NOT NULL DEFAULT 'project_root',
    source_path TEXT NOT NULL,
    storage_root_name TEXT,
    host_scope TEXT NOT NULL DEFAULT 'server',
    root_type TEXT NOT NULL DEFAULT 'project_root',
    visibility TEXT NOT NULL DEFAULT 'private',
    clear_existing_for_root BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'queued',
    phase TEXT NOT NULL DEFAULT 'queued',
    total_projects INTEGER NOT NULL DEFAULT 0,
    scanned_projects INTEGER NOT NULL DEFAULT 0,
    indexed_projects INTEGER NOT NULL DEFAULT 0,
    failed_projects INTEGER NOT NULL DEFAULT 0,
    deleted_projects INTEGER NOT NULL DEFAULT 0,
    mat_files_seen INTEGER NOT NULL DEFAULT 0,
    current_project_path TEXT,
    message TEXT,
    error_text TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

CREATE TABLE IF NOT EXISTS storage_migration_batches (
    id UUID PRIMARY KEY,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    batch_name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_path TEXT NOT NULL,
    storage_root_name TEXT,
    host_scope TEXT NOT NULL DEFAULT 'server',
    root_type TEXT NOT NULL DEFAULT 'legacy_root',
    strategy TEXT NOT NULL DEFAULT 'discover_only',
    status TEXT NOT NULL DEFAULT 'planned',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS storage_migration_items (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES storage_migration_batches(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    legacy_path TEXT NOT NULL,
    legacy_key TEXT,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned',
    action TEXT NOT NULL DEFAULT 'review',
    proposed_experiment_key TEXT,
    proposed_project_key TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'private';
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS total_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS last_size_scan_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT;

ALTER TABLE detecdiv_projects ADD COLUMN IF NOT EXISTS experiment_project_id UUID REFERENCES experiment_projects(id) ON DELETE SET NULL;
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
ALTER TABLE indexing_jobs ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'queued';
ALTER TABLE indexing_jobs ADD COLUMN IF NOT EXISTS mat_files_seen INTEGER NOT NULL DEFAULT 0;
ALTER TABLE indexing_jobs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_projects_name ON detecdiv_projects(project_name);
CREATE INDEX IF NOT EXISTS idx_projects_owner_user_id ON detecdiv_projects(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_projects_visibility ON detecdiv_projects(visibility);
CREATE INDEX IF NOT EXISTS idx_projects_experiment_project_id ON detecdiv_projects(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_title ON experiment_projects(title);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_owner_user_id ON experiment_projects(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_visibility ON experiment_projects(visibility);
CREATE INDEX IF NOT EXISTS idx_experiment_raw_links_experiment_project_id ON experiment_raw_links(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_experiment_raw_links_raw_dataset_id ON experiment_raw_links(raw_dataset_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_project_id ON jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_raw_datasets_status ON raw_datasets(status, completeness_status);
CREATE INDEX IF NOT EXISTS idx_raw_datasets_owner_user_id ON raw_datasets(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_project_acl_user_id ON project_acl(user_id);
CREATE INDEX IF NOT EXISTS idx_project_groups_owner_user_id ON project_groups(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_project_group_members_project_id ON project_group_members(project_id);
CREATE INDEX IF NOT EXISTS idx_project_notes_project_id ON project_notes(project_id);
CREATE INDEX IF NOT EXISTS idx_project_deletion_events_project_id ON project_deletion_events(project_id);
CREATE INDEX IF NOT EXISTS idx_indexing_jobs_status_created_at ON indexing_jobs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_indexing_jobs_requested_by_user_id ON indexing_jobs(requested_by_user_id);
CREATE INDEX IF NOT EXISTS idx_indexing_jobs_heartbeat_at ON indexing_jobs(heartbeat_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_storage_migration_batches_owner_user_id ON storage_migration_batches(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_storage_migration_batches_created_at ON storage_migration_batches(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_storage_migration_items_batch_id ON storage_migration_items(batch_id);
CREATE INDEX IF NOT EXISTS idx_storage_migration_items_status ON storage_migration_items(status);
CREATE INDEX IF NOT EXISTS idx_external_publication_records_experiment_project_id ON external_publication_records(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_external_publication_records_system_key ON external_publication_records(system_key, status);
