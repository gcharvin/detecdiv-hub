CREATE TABLE IF NOT EXISTS storage_providers (
    id UUID PRIMARY KEY,
    provider_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    provider_kind TEXT NOT NULL DEFAULT 'posix_mount',
    mount_root TEXT,
    quota_mode TEXT NOT NULL DEFAULT 'measured_only',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    capabilities_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_storage_accounts (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES storage_providers(id) ON DELETE CASCADE,
    provider_user_key TEXT NOT NULL,
    home_storage_root_id BIGINT REFERENCES storage_roots(id) ON DELETE SET NULL,
    home_relative_path TEXT,
    quota_bytes BIGINT,
    quota_status TEXT NOT NULL DEFAULT 'unknown',
    provisioning_status TEXT NOT NULL DEFAULT 'planned',
    last_synced_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, provider_id),
    UNIQUE(provider_id, provider_user_key)
);

CREATE TABLE IF NOT EXISTS storage_quota_snapshots (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES storage_providers(id) ON DELETE CASCADE,
    storage_account_id UUID REFERENCES user_storage_accounts(id) ON DELETE SET NULL,
    provider_user_key TEXT NOT NULL,
    quota_bytes BIGINT,
    provider_used_bytes BIGINT,
    hub_project_bytes BIGINT NOT NULL DEFAULT 0,
    hub_raw_bytes BIGINT NOT NULL DEFAULT 0,
    hub_artifact_bytes BIGINT NOT NULL DEFAULT 0,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS storage_provisioning_events (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    provider_id UUID REFERENCES storage_providers(id) ON DELETE SET NULL,
    storage_account_id UUID REFERENCES user_storage_accounts(id) ON DELETE SET NULL,
    event_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'recorded',
    message TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_storage_providers_kind ON storage_providers(provider_kind, is_active);
CREATE INDEX IF NOT EXISTS idx_user_storage_accounts_user_id ON user_storage_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_user_storage_accounts_provider_id ON user_storage_accounts(provider_id);
CREATE INDEX IF NOT EXISTS idx_user_storage_accounts_home_root ON user_storage_accounts(home_storage_root_id);
CREATE INDEX IF NOT EXISTS idx_storage_quota_snapshots_user_measured ON storage_quota_snapshots(user_id, measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_storage_quota_snapshots_provider_measured ON storage_quota_snapshots(provider_id, measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_storage_provisioning_events_user_created ON storage_provisioning_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_storage_provisioning_events_provider_created ON storage_provisioning_events(provider_id, created_at DESC);
