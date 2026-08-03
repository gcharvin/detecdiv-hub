ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS lifecycle_tier TEXT NOT NULL DEFAULT 'hot';

ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS archive_status TEXT NOT NULL DEFAULT 'none';

ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS archive_uri TEXT;

ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS archive_compression TEXT;

CREATE INDEX IF NOT EXISTS idx_detecdiv_projects_archive_status
    ON detecdiv_projects(archive_status);
