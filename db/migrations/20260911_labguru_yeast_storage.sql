CREATE TABLE IF NOT EXISTS labguru_storage_locations (
    id UUID PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    parent_external_id TEXT,
    name TEXT NOT NULL,
    location_type TEXT,
    external_url TEXT,
    name_with_hierarchy TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_since TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS labguru_storage_boxes (
    id UUID PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    storage_external_id TEXT,
    name TEXT NOT NULL,
    external_url TEXT,
    rows INTEGER,
    cols INTEGER,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_since TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS labguru_yeast_stocks (
    id UUID PRIMARY KEY,
    yeast_strain_id UUID NOT NULL REFERENCES labguru_yeast_strains(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    container_type TEXT,
    box_external_id TEXT,
    box_name TEXT,
    box_url TEXT,
    position TEXT,
    owner_name TEXT,
    stored_by_name TEXT,
    stored_on DATE,
    external_url TEXT,
    storage_path_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_external_at TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    missing_since TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_labguru_storage_locations_parent
    ON labguru_storage_locations(parent_external_id);
CREATE INDEX IF NOT EXISTS idx_labguru_storage_boxes_storage
    ON labguru_storage_boxes(storage_external_id);
CREATE INDEX IF NOT EXISTS idx_labguru_yeast_stocks_strain_active
    ON labguru_yeast_stocks(yeast_strain_id, is_active);
CREATE INDEX IF NOT EXISTS idx_labguru_yeast_stocks_box
    ON labguru_yeast_stocks(box_external_id);
