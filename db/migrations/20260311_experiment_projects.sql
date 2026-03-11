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

ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS experiment_project_id UUID REFERENCES experiment_projects(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_projects_experiment_project_id ON detecdiv_projects(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_title ON experiment_projects(title);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_owner_user_id ON experiment_projects(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_experiment_projects_visibility ON experiment_projects(visibility);
CREATE INDEX IF NOT EXISTS idx_experiment_raw_links_experiment_project_id ON experiment_raw_links(experiment_project_id);
CREATE INDEX IF NOT EXISTS idx_experiment_raw_links_raw_dataset_id ON experiment_raw_links(raw_dataset_id);
