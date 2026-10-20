-- Apply before deploying the API/workers. Separate control columns cannot be
-- overwritten by concurrent result_json progress/heartbeat updates.
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS force_stop_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS force_stop_requested_by TEXT,
    ADD COLUMN IF NOT EXISTS force_stop_requested_by_user_id UUID,
    ADD COLUMN IF NOT EXISTS force_stopped_at TIMESTAMPTZ;
