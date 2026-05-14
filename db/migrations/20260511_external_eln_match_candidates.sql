CREATE TABLE IF NOT EXISTS external_match_candidates (
    id UUID PRIMARY KEY,
    raw_dataset_id UUID NOT NULL REFERENCES raw_datasets(id) ON DELETE CASCADE,
    system_key TEXT NOT NULL,
    external_experiment_record_id UUID NOT NULL REFERENCES external_experiment_records(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    candidate_rank INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'proposed',
    match_method TEXT NOT NULL DEFAULT 'deterministic_v1',
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    reviewed_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(raw_dataset_id, system_key, external_experiment_record_id)
);

CREATE INDEX IF NOT EXISTS idx_external_match_candidates_status_score
    ON external_match_candidates(system_key, status, score DESC);
CREATE INDEX IF NOT EXISTS idx_external_match_candidates_raw_dataset_id
    ON external_match_candidates(raw_dataset_id);
CREATE INDEX IF NOT EXISTS idx_external_match_candidates_external_record_id
    ON external_match_candidates(external_experiment_record_id);
