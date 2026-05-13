CREATE OR REPLACE FUNCTION normalize_labguru_external_url()
RETURNS trigger AS $$
BEGIN
    IF NEW.system_key = 'labguru' AND NEW.external_url IS NOT NULL THEN
        IF NEW.external_url LIKE '/%' THEN
            NEW.external_url := 'https://cle.inserm.fr' || NEW.external_url;
        ELSIF NEW.external_url ~ '^https?://[^/]+/knowledge/experiments/'
            AND NEW.external_url NOT LIKE 'https://cle.inserm.fr/%' THEN
            NEW.external_url := regexp_replace(
                NEW.external_url,
                '^https?://[^/]+/knowledge/experiments/',
                'https://cle.inserm.fr/knowledge/experiments/'
            );
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_external_experiment_records_labguru_url
    ON external_experiment_records;
CREATE TRIGGER trg_external_experiment_records_labguru_url
    BEFORE INSERT OR UPDATE OF system_key, external_url
    ON external_experiment_records
    FOR EACH ROW
    EXECUTE FUNCTION normalize_labguru_external_url();

DROP TRIGGER IF EXISTS trg_external_publication_records_labguru_url
    ON external_publication_records;
CREATE TRIGGER trg_external_publication_records_labguru_url
    BEFORE INSERT OR UPDATE OF system_key, external_url
    ON external_publication_records
    FOR EACH ROW
    EXECUTE FUNCTION normalize_labguru_external_url();
