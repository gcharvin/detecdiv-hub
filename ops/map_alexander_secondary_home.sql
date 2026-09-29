-- Stage Alexander under the secondary Synology homes share.
-- This only changes inactive Hub metadata. It does not enable the provider,
-- create a directory, alter NAS permissions, or move any data.
\set ON_ERROR_STOP on

BEGIN;

UPDATE storage_providers
SET mount_root = '/homes2',
    config_json = (COALESCE(config_json, '{}'::jsonb) - 'share_name' - 'quota_scope' - 'default_quota_bytes') ||
        '{"quota_share":"homes","home_mount_source":"//10.20.8.250/home"}'::jsonb,
    updated_at = NOW()
WHERE provider_key = 'synology-secondary'
  AND is_active = FALSE;

UPDATE user_storage_accounts AS account
SET home_storage_root_id = root.id,
    home_relative_path = 'maliavko/DetecdivHub',
    quota_bytes = 10000000000000,
    quota_status = 'desired',
    provisioning_status = 'planned',
    metadata_json = (COALESCE(account.metadata_json, '{}'::jsonb) - 'layout' - 'quota_scope' - 'share_name') ||
        '{"layout":"synology_home","quota_scope":"user","rollout":"pending_worker_access"}'::jsonb,
    updated_at = NOW()
FROM users AS hub_user, storage_providers AS provider, storage_roots AS root
WHERE account.user_id = hub_user.id
  AND account.provider_id = provider.id
  AND hub_user.user_key = 'alexander'
  AND provider.provider_key = 'synology-secondary'
  AND provider.is_active = FALSE
  AND root.name = 'user-homes2'
  AND root.path_prefix = '/homes2'
  AND account.provisioning_status = 'planned';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM storage_providers AS provider
        JOIN user_storage_accounts AS account ON account.provider_id = provider.id
        JOIN users AS hub_user ON hub_user.id = account.user_id
        JOIN storage_roots AS root ON root.id = account.home_storage_root_id
        WHERE provider.provider_key = 'synology-secondary'
          AND provider.is_active = FALSE
          AND provider.mount_root = '/homes2'
          AND provider.config_json->>'quota_share' = 'homes'
          AND provider.config_json->>'home_mount_source' = '//10.20.8.250/home'
          AND COALESCE(provider.config_json->>'quota_scope', '') <> 'shared_folder'
          AND hub_user.user_key = 'alexander'
          AND root.name = 'user-homes2'
          AND root.path_prefix = '/homes2'
          AND account.home_relative_path = 'maliavko/DetecdivHub'
          AND account.quota_bytes = 10000000000000
          AND account.quota_status = 'desired'
          AND account.provisioning_status = 'planned'
    ) THEN
        RAISE EXCEPTION 'Alexander secondary-home mapping is not in the expected inactive staged state';
    END IF;
END
$$;

COMMIT;
