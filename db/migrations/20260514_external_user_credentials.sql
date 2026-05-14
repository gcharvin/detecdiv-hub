CREATE TABLE IF NOT EXISTS external_user_credentials (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    system_key TEXT NOT NULL,
    credential_kind TEXT NOT NULL DEFAULT 'api_token',
    encrypted_token TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'stored',
    expires_at TIMESTAMPTZ,
    last_verified_at TIMESTAMPTZ,
    last_error TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, system_key)
);

CREATE INDEX IF NOT EXISTS idx_external_user_credentials_user_system
    ON external_user_credentials(user_id, system_key);
CREATE INDEX IF NOT EXISTS idx_external_user_credentials_status
    ON external_user_credentials(system_key, status, expires_at);
