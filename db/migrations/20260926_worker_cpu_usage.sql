ALTER TABLE worker_instances
    ADD COLUMN IF NOT EXISTS host_cpu_count INTEGER,
    ADD COLUMN IF NOT EXISTS available_cpu_count INTEGER,
    ADD COLUMN IF NOT EXISTS current_job_cpu_cores DOUBLE PRECISION;
