CREATE TABLE IF NOT EXISTS misc_storage_items (
    id UUID PRIMARY KEY,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    storage_root_id BIGINT NOT NULL REFERENCES storage_roots(id) ON DELETE RESTRICT,
    relative_path TEXT NOT NULL,
    display_name TEXT NOT NULL,
    item_kind TEXT NOT NULL DEFAULT 'directory',
    category TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'indexed',
    visibility TEXT NOT NULL DEFAULT 'private',
    scan_depth INTEGER NOT NULL DEFAULT 0,
    scan_status TEXT NOT NULL DEFAULT 'measured',
    total_bytes BIGINT NOT NULL DEFAULT 0,
    child_dir_count INTEGER NOT NULL DEFAULT 0,
    child_file_count INTEGER NOT NULL DEFAULT 0,
    last_size_scan_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    notes TEXT,
    lifecycle_tier TEXT NOT NULL DEFAULT 'hot',
    archive_status TEXT NOT NULL DEFAULT 'none',
    archive_uri TEXT,
    backup_status TEXT NOT NULL DEFAULT 'none',
    backup_excluded BOOLEAN NOT NULL DEFAULT FALSE,
    last_backup_at TIMESTAMPTZ,
    backup_snapshot_id TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(storage_root_id, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_misc_storage_items_owner_user_id
    ON misc_storage_items(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_misc_storage_items_storage_root
    ON misc_storage_items(storage_root_id, relative_path);
CREATE INDEX IF NOT EXISTS idx_misc_storage_items_category
    ON misc_storage_items(category, status);
CREATE INDEX IF NOT EXISTS idx_misc_storage_items_total_bytes
    ON misc_storage_items(total_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_misc_storage_items_lifecycle
    ON misc_storage_items(lifecycle_tier, archive_status);
CREATE INDEX IF NOT EXISTS idx_misc_storage_items_backup
    ON misc_storage_items(backup_status, backup_excluded);
