-- One-time, idempotent catalog registration for a second Synology homes mount.
-- Pass all site-specific values with psql -v, including home_mount_source
-- (the per-user SMB `home` share, not the aggregate `homes` share). This does
-- not create directories, queue jobs, or relocate any dataset/project locations.
-- Quotas are DSM per-user quotas on the Synology-managed `homes` share, not a
-- quota on a dedicated top-level shared folder.
\set ON_ERROR_STOP on

BEGIN;

INSERT INTO storage_roots (name, root_type, host_scope, path_prefix)
VALUES (:'root_name', 'user_home_root', 'detecdiv-server', :'mount_root')
ON CONFLICT (name) DO NOTHING;

INSERT INTO storage_providers (
    id, provider_key, display_name, provider_kind, mount_root, quota_mode,
    is_active, capabilities_json, config_json
)
VALUES (
    gen_random_uuid(), :'provider_key', :'display_name', 'synology_dsm', :'mount_root',
    'provider_enforced', FALSE, '{}'::jsonb,
    jsonb_build_object(
        'dsm_base_url', :'dsm_base_url',
        'credentials_env_prefix', :'credentials_env_prefix',
        'ssh_host', :'ssh_host',
        'quota_share', 'homes',
        'home_mount_source', :'home_mount_source'
    )
)
ON CONFLICT (provider_key) DO NOTHING;

INSERT INTO user_storage_accounts (
    id, user_id, provider_id, provider_user_key, home_storage_root_id,
    home_relative_path, quota_bytes, quota_status, provisioning_status, metadata_json
)
SELECT
    gen_random_uuid(), users.id, storage_providers.id, :'provider_user_key', storage_roots.id,
    :'home_relative_path', 10000000000000, 'desired', 'planned',
    '{"rollout":"pending_worker_access","layout":"synology_home","quota_scope":"user"}'::jsonb
FROM users
CROSS JOIN storage_providers
CROSS JOIN storage_roots
WHERE users.user_key = :'hub_user_key'
  AND storage_providers.provider_key = :'provider_key'
  AND storage_roots.name = :'root_name'
ON CONFLICT (user_id, provider_id) DO NOTHING;

COMMIT;
