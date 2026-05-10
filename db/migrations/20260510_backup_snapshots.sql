-- Backup snapshot records stored by workers so the API can list
-- snapshots without calling restic directly.
CREATE TABLE backup_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id TEXT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    hostname TEXT NOT NULL DEFAULT '',
    tags JSONB NOT NULL DEFAULT '[]',
    paths JSONB NOT NULL DEFAULT '[]',
    raw_dataset_id UUID REFERENCES raw_datasets(id) ON DELETE CASCADE,
    project_id UUID REFERENCES detecdiv_projects(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX backup_snapshots_raw_idx ON backup_snapshots(raw_dataset_id) WHERE raw_dataset_id IS NOT NULL;
CREATE INDEX backup_snapshots_project_idx ON backup_snapshots(project_id) WHERE project_id IS NOT NULL;
CREATE INDEX backup_snapshots_time_idx ON backup_snapshots(time DESC);
