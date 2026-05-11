CREATE TABLE IF NOT EXISTS external_experiment_records (
    id UUID PRIMARY KEY,
    system_key TEXT NOT NULL,
    external_id TEXT NOT NULL,
    title TEXT NOT NULL,
    external_url TEXT,
    owner_name TEXT,
    started_at TIMESTAMPTZ,
    updated_external_at TIMESTAMPTZ,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(system_key, external_id)
);

CREATE TABLE IF NOT EXISTS external_user_records (
    id UUID PRIMARY KEY,
    system_key TEXT NOT NULL,
    external_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    email TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    matched_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    match_status TEXT NOT NULL DEFAULT 'pending',
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(system_key, external_id)
);

DELETE FROM external_publication_records
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY experiment_project_id, system_key
                ORDER BY created_at ASC, id ASC
            ) AS rn
        FROM external_publication_records
    ) ranked
    WHERE rn > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_external_publication_records_experiment_system
    ON external_publication_records(experiment_project_id, system_key);
CREATE INDEX IF NOT EXISTS idx_external_experiment_records_system_title
    ON external_experiment_records(system_key, title);
CREATE INDEX IF NOT EXISTS idx_external_experiment_records_synced_at
    ON external_experiment_records(system_key, last_synced_at DESC);
CREATE INDEX IF NOT EXISTS idx_external_user_records_system_name
    ON external_user_records(system_key, display_name);
CREATE INDEX IF NOT EXISTS idx_external_user_records_matched_user_id
    ON external_user_records(matched_user_id);
