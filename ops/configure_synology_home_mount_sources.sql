-- Stage the per-user SMB `home` share source expected by the worker write guard.
-- This does not activate either provider, alter NAS ACLs, or move any data.
\set ON_ERROR_STOP on

BEGIN;

UPDATE storage_providers
SET config_json = COALESCE(config_json, '{}'::jsonb) ||
        jsonb_build_object(
            'home_mount_source',
            CASE provider_key
                WHEN 'synology-main' THEN '//10.20.11.250/home'
                WHEN 'synology-secondary' THEN '//10.20.8.250/home'
            END
        ),
    updated_at = NOW()
WHERE provider_kind = 'synology_dsm'
  AND (
      (provider_key = 'synology-main' AND mount_root = '/homes')
      OR (provider_key = 'synology-secondary' AND mount_root = '/homes2')
  );

DO $$
BEGIN
    IF (
        SELECT COUNT(*)
        FROM storage_providers
        WHERE provider_kind = 'synology_dsm'
          AND (
              (provider_key = 'synology-main' AND mount_root = '/homes'
               AND config_json->>'home_mount_source' = '//10.20.11.250/home')
              OR (provider_key = 'synology-secondary' AND mount_root = '/homes2'
                  AND config_json->>'home_mount_source' = '//10.20.8.250/home')
          )
    ) <> 2 THEN
        RAISE EXCEPTION 'Expected both Synology providers with their distinct /homes and /homes2 home-share sources';
    END IF;
END
$$;

COMMIT;
