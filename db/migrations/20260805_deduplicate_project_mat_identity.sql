BEGIN;

-- A project MAT file is a single catalog resource even when it is reachable
-- through overlapping storage roots. Prefer the row attached to the broadest
-- (shortest-prefix) storage root; those are the durable per-user roots. The
-- later, narrower hub_* scans are the rows that caused these duplicates.
CREATE TEMP TABLE project_dedup_map ON COMMIT DROP AS
WITH duplicate_paths AS (
    SELECT metadata_json->>'project_mat_abs' AS project_mat_abs
    FROM detecdiv_projects
    WHERE COALESCE(metadata_json->>'project_mat_abs', '') <> ''
    GROUP BY metadata_json->>'project_mat_abs'
    HAVING COUNT(*) > 1
), ranked AS (
    SELECT
        p.id,
        d.project_mat_abs,
        ROW_NUMBER() OVER (
            PARTITION BY d.project_mat_abs
            ORDER BY
                COALESCE(MIN(CHAR_LENGTH(RTRIM(sr.path_prefix, '/'))), 2147483647),
                p.updated_at DESC,
                p.created_at DESC,
                p.id
        ) AS rank
    FROM duplicate_paths d
    JOIN detecdiv_projects p
      ON p.metadata_json->>'project_mat_abs' = d.project_mat_abs
    LEFT JOIN project_locations pl ON pl.project_id = p.id
    LEFT JOIN storage_roots sr ON sr.id = pl.storage_root_id
    GROUP BY p.id, d.project_mat_abs, p.updated_at, p.created_at
), winners AS (
    SELECT project_mat_abs, id AS winner_id
    FROM ranked
    WHERE rank = 1
)
SELECT r.project_mat_abs, w.winner_id, r.id AS loser_id
FROM ranked r
JOIN winners w USING (project_mat_abs)
WHERE r.rank > 1;

-- Retain a legacy free-text note or experiment association when the canonical
-- row does not already have one.
UPDATE detecdiv_projects winner
SET
    notes = COALESCE(
        NULLIF(winner.notes, ''),
        (SELECT NULLIF(loser.notes, '')
         FROM project_dedup_map m
         JOIN detecdiv_projects loser ON loser.id = m.loser_id
         WHERE m.winner_id = winner.id AND NULLIF(loser.notes, '') IS NOT NULL
         ORDER BY loser.updated_at DESC
         LIMIT 1)
    ),
    experiment_project_id = COALESCE(
        winner.experiment_project_id,
        (SELECT loser.experiment_project_id
         FROM project_dedup_map m
         JOIN detecdiv_projects loser ON loser.id = m.loser_id
         WHERE m.winner_id = winner.id AND loser.experiment_project_id IS NOT NULL
         ORDER BY loser.updated_at DESC
         LIMIT 1)
    )
WHERE winner.id IN (SELECT winner_id FROM project_dedup_map);

-- Merge all dependent rows before deleting the duplicate project records.
-- Imported locations remain alternate locations; the broad root stays preferred.
INSERT INTO project_locations (
    project_id, storage_root_id, relative_path, project_file_name,
    access_mode, is_preferred, created_at, updated_at
)
SELECT
    m.winner_id, pl.storage_root_id, pl.relative_path, pl.project_file_name,
    pl.access_mode, FALSE, pl.created_at, pl.updated_at
FROM project_dedup_map m
JOIN project_locations pl ON pl.project_id = m.loser_id
ON CONFLICT (project_id, storage_root_id, relative_path) DO NOTHING;

INSERT INTO project_raw_links (project_id, raw_dataset_id, link_type, created_at)
SELECT m.winner_id, prl.raw_dataset_id, prl.link_type, prl.created_at
FROM project_dedup_map m
JOIN project_raw_links prl ON prl.project_id = m.loser_id
ON CONFLICT (project_id, raw_dataset_id, link_type) DO NOTHING;

INSERT INTO project_acl (project_id, user_id, access_level, created_at)
SELECT m.winner_id, pa.user_id, pa.access_level, pa.created_at
FROM project_dedup_map m
JOIN project_acl pa ON pa.project_id = m.loser_id
ON CONFLICT (project_id, user_id) DO UPDATE
SET access_level = CASE
    WHEN EXCLUDED.access_level = 'owner' OR project_acl.access_level = 'owner' THEN 'owner'
    WHEN EXCLUDED.access_level = 'editor' OR project_acl.access_level = 'editor' THEN 'editor'
    ELSE project_acl.access_level
END;

INSERT INTO project_group_members (group_id, project_id, created_at)
SELECT pgm.group_id, m.winner_id, pgm.created_at
FROM project_dedup_map m
JOIN project_group_members pgm ON pgm.project_id = m.loser_id
ON CONFLICT (group_id, project_id) DO NOTHING;

UPDATE project_notes pn
SET project_id = m.winner_id
FROM project_dedup_map m
WHERE pn.project_id = m.loser_id;

UPDATE project_deletion_events pde
SET project_id = m.winner_id
FROM project_dedup_map m
WHERE pde.project_id = m.loser_id;

UPDATE project_locks pl
SET project_id = m.winner_id
FROM project_dedup_map m
WHERE pl.project_id = m.loser_id;

UPDATE jobs j
SET project_id = m.winner_id
FROM project_dedup_map m
WHERE j.project_id = m.loser_id;

UPDATE backup_snapshots bs
SET project_id = m.winner_id
FROM project_dedup_map m
WHERE bs.project_id = m.loser_id;

DELETE FROM detecdiv_projects p
USING project_dedup_map m
WHERE p.id = m.loser_id;

-- Enforce the physical identity at the database boundary as well as in the
-- indexer. Projects without an indexed server MAT path are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS uq_detecdiv_projects_project_mat_abs
ON detecdiv_projects ((metadata_json->>'project_mat_abs'))
WHERE COALESCE(metadata_json->>'project_mat_abs', '') <> '';

COMMIT;
