ALTER TABLE raw_datasets
ADD COLUMN IF NOT EXISTS display_settings_uri TEXT;
