-- SUPERSEDED by the per-user homes quota model. Do not run unless the user
-- explicitly returns to a dedicated shared-folder quota.
-- Stage an editable 10 TB (decimal) share-quota target for Alexander.
-- No DSM share, mount, directory, or dataset is changed by this script.
\set ON_ERROR_STOP on

BEGIN;

UPDATE storage_providers
SET config_json = config_json ||
    '{"quota_scope":"shared_folder","default_quota_bytes":10000000000000}'::jsonb,
    updated_at = NOW()
WHERE provider_key = 'synology-secondary'
  AND is_active = FALSE;

UPDATE user_storage_accounts AS account
SET quota_bytes = COALESCE(account.quota_bytes, 10000000000000),
    quota_status = CASE WHEN account.quota_bytes IS NULL THEN 'desired' ELSE account.quota_status END,
    metadata_json = account.metadata_json ||
        '{"layout":"dedicated_share","quota_scope":"shared_folder","rollout":"pending_share"}'::jsonb,
    updated_at = NOW()
FROM users AS hub_user, storage_providers AS provider
WHERE account.user_id = hub_user.id
  AND account.provider_id = provider.id
  AND hub_user.user_key = 'alexander'
  AND provider.provider_key = 'synology-secondary'
  AND provider.is_active = FALSE
  AND account.provisioning_status = 'planned';

COMMIT;
