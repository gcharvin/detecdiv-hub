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

CREATE INDEX IF NOT EXISTS idx_storage_migration_batches_owner_user_id ON storage_migration_batches(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_storage_migration_batches_created_at ON storage_migration_batches(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_storage_migration_items_batch_id ON storage_migration_items(batch_id);
CREATE INDEX IF NOT EXISTS idx_storage_migration_items_status ON storage_migration_items(status);
