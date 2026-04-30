ALTER TABLE detecdiv_projects
    ADD COLUMN IF NOT EXISTS notes TEXT;

ALTER TABLE raw_datasets
    ADD COLUMN IF NOT EXISTS notes TEXT;

UPDATE detecdiv_projects p
SET notes = src.note_text
FROM (
    SELECT DISTINCT ON (project_id)
        project_id,
        note_text
    FROM project_notes
    ORDER BY project_id, updated_at DESC, id DESC
) AS src
WHERE p.id = src.project_id
  AND p.notes IS NULL;
