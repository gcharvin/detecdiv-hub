ALTER TABLE users
ADD COLUMN IF NOT EXISTS admin_portal_access BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE users
ADD COLUMN IF NOT EXISTS lab_status TEXT NOT NULL DEFAULT 'yes';

ALTER TABLE users
ADD COLUMN IF NOT EXISTS default_path TEXT;

UPDATE users
SET lab_status = 'alumni'
WHERE user_key IN ('aizea', 'andrei', 'audrey', 'baptiste', 'guillaume', 'pierre', 'theo', 'tony');
