-- SUPERSEDED by ops/map_alexander_secondary_home.sql for the selected homes
-- layout. Do not run unless the user explicitly returns to a dedicated share.
-- Stage the dedicated Alexander share in the Hub catalog.
-- The provider must remain inactive until its SMB mount and write identity
-- have been verified on detecdiv-server.
\set ON_ERROR_STOP on

BEGIN;

INSERT INTO storage_roots (name, root_type, host_scope, path_prefix)
VALUES (
    'user-homes2-alexander',
    'user_home_root',
    'detecdiv-server',
    '/mnt/detecdiv-alexander'
)
ON CONFLICT (name) DO UPDATE
SET root_type = EXCLUDED.root_type,
    host_scope = EXCLUDED.host_scope,
    path_prefix = EXCLUDED.path_prefix,
    updated_at = NOW();

UPDATE storage_providers
SET mount_root = '/mnt/detecdiv-alexander',
    config_json = config_json ||
        '{"share_name":"Alexander","quota_scope":"shared_folder","default_quota_bytes":10000000000000}'::jsonb,
    updated_at = NOW()
WHERE provider_key = 'synology-secondary'
  AND is_active = FALSE;

UPDATE user_storage_accounts AS account
SET home_storage_root_id = root.id,
    home_relative_path = 'maliavko/DetecdivHub',
    quota_bytes = 10000000000000,
    quota_status = 'desired',
    provisioning_status = 'planned',
    metadata_json = account.metadata_json ||
        '{"layout":"dedicated_share","share_name":"Alexander","quota_scope":"shared_folder","rollout":"pending_worker_mount"}'::jsonb,
    updated_at = NOW()
FROM users AS hub_user,
     storage_providers AS provider,
     storage_roots AS root
WHERE account.user_id = hub_user.id
  AND account.provider_id = provider.id
  AND hub_user.user_key = 'alexander'
  AND provider.provider_key = 'synology-secondary'
  AND provider.is_active = FALSE
  AND root.name = 'user-homes2-alexander'
  AND account.provisioning_status IN ('planned', 'provider_user_ready');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM storage_providers
        WHERE provider_key = 'synology-secondary'
          AND is_active = FALSE
          AND mount_root = '/mnt/detecdiv-alexander'
    ) THEN
        RAISE EXCEPTION 'Secondary provider is missing or active; refusing staged mapping';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM user_storage_accounts AS account
        JOIN users AS hub_user ON hub_user.id = account.user_id
        JOIN storage_providers AS provider ON provider.id = account.provider_id
        JOIN storage_roots AS root ON root.id = account.home_storage_root_id
        WHERE hub_user.user_key = 'alexander'
          AND provider.provider_key = 'synology-secondary'
          AND provider.is_active = FALSE
          AND root.name = 'user-homes2-alexander'
          AND account.quota_bytes = 10000000000000
          AND account.provisioning_status = 'planned'
    ) THEN
        RAISE EXCEPTION 'Alexander storage account did not reach the expected staged state';
    END IF;
END
$$;

COMMIT;
