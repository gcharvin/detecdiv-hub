CREATE TABLE IF NOT EXISTS labguru_yeast_strains (
    id UUID PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sys_id TEXT,
    description TEXT,
    owner_name TEXT,
    external_url TEXT,
    search_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_text TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_external_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_since TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_labguru_yeast_strains_name ON labguru_yeast_strains(name);
CREATE INDEX IF NOT EXISTS idx_labguru_yeast_strains_sys_id ON labguru_yeast_strains(sys_id);
CREATE INDEX IF NOT EXISTS idx_labguru_yeast_strains_active_sync
    ON labguru_yeast_strains(is_active, last_synced_at DESC);
CREATE INDEX IF NOT EXISTS idx_labguru_yeast_strains_search
    ON labguru_yeast_strains USING GIN (to_tsvector('simple', search_text));
