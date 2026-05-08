-- Backup fields on raw_datasets
ALTER TABLE raw_datasets
    ADD COLUMN IF NOT EXISTS backup_status       VARCHAR     NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS backup_excluded     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_backup_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS backup_snapshot_id  TEXT;

-- Backup fields on detecdiv_projects
ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS backup_status       VARCHAR     NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS backup_excluded     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_backup_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS backup_snapshot_id  TEXT;

-- Backup runs log (one row per scheduled or manual run)
CREATE TABLE IF NOT EXISTS backup_runs (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    triggered_by_user_id    UUID        REFERENCES users(id) ON DELETE SET NULL,
    trigger_mode            VARCHAR     NOT NULL DEFAULT 'manual',
    status                  VARCHAR     NOT NULL DEFAULT 'running',
    config_json             JSONB       NOT NULL DEFAULT '{}',
    result_json             JSONB       NOT NULL DEFAULT '{}',
    raw_datasets_total      INTEGER     NOT NULL DEFAULT 0,
    raw_datasets_backed_up  INTEGER     NOT NULL DEFAULT 0,
    raw_datasets_skipped    INTEGER     NOT NULL DEFAULT 0,
    raw_datasets_failed     INTEGER     NOT NULL DEFAULT 0,
    projects_total          INTEGER     NOT NULL DEFAULT 0,
    projects_backed_up      INTEGER     NOT NULL DEFAULT 0,
    projects_skipped        INTEGER     NOT NULL DEFAULT 0,
    projects_failed         INTEGER     NOT NULL DEFAULT 0,
    total_bytes_backed_up   BIGINT      NOT NULL DEFAULT 0,
    error_text              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at              TIMESTAMPTZ,
    finished_at             TIMESTAMPTZ
);
