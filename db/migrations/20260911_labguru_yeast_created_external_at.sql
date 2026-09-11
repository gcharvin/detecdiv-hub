ALTER TABLE labguru_yeast_strains
    ADD COLUMN IF NOT EXISTS created_external_at TIMESTAMPTZ;

UPDATE labguru_yeast_strains
SET created_external_at = CASE
    WHEN COALESCE(payload_json->>'created_at', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN (payload_json->>'created_at')::TIMESTAMPTZ
    WHEN COALESCE(payload_json->>'created_on', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN (payload_json->>'created_on')::TIMESTAMPTZ
    WHEN COALESCE(payload_json->>'creation_date', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN (payload_json->>'creation_date')::TIMESTAMPTZ
    ELSE NULL
END
WHERE created_external_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_labguru_yeast_strains_created_external
    ON labguru_yeast_strains(created_external_at DESC)
    WHERE is_active = TRUE;
