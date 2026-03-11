ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS lifecycle_tier TEXT NOT NULL DEFAULT 'hot';
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS archive_status TEXT NOT NULL DEFAULT 'none';
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS archive_uri TEXT;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS archive_compression TEXT;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS reclaimable_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE raw_datasets ADD COLUMN IF NOT EXISTS last_accessed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS storage_lifecycle_events (
    id UUID PRIMARY KEY,
    raw_dataset_id UUID NOT NULL REFERENCES raw_datasets(id) ON DELETE CASCADE,
    requested_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    event_kind TEXT NOT NULL,
    from_tier TEXT,
    to_tier TEXT,
    archive_status TEXT,
    reclaimable_bytes BIGINT NOT NULL DEFAULT 0,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_datasets_lifecycle_tier ON raw_datasets(lifecycle_tier, archive_status);
CREATE INDEX IF NOT EXISTS idx_storage_lifecycle_events_raw_dataset_id ON storage_lifecycle_events(raw_dataset_id, created_at DESC);
