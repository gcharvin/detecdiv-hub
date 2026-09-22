-- Persist directory traversal progress so TIFF inventory yields between
-- bounded worker slices instead of holding a worker for a full-tree walk.
CREATE TABLE IF NOT EXISTS storage_optimization_scan_directories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES storage_optimization_runs(id) ON DELETE CASCADE,
    relative_path TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_storage_optimization_scan_directory_path UNIQUE (run_id, relative_path)
);

CREATE INDEX IF NOT EXISTS storage_optimization_scan_directories_pending_idx
    ON storage_optimization_scan_directories(run_id, status, relative_path);
