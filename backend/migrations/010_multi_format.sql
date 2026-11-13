-- One generation request fans out from one immutable extraction snapshot.
CREATE TABLE IF NOT EXISTS generation_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_run_id UUID NOT NULL UNIQUE REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    formats TEXT[] NOT NULL,
    extracted_gcs_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS generation_batch_id UUID
    REFERENCES generation_batches(id) ON DELETE CASCADE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_output_mode
    ON pipeline_runs(generation_batch_id, mode) WHERE generation_batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_generation_batches_paper ON generation_batches(paper_id);

-- Jobs and their owning runs commit together. The gateway drains this outbox
-- after Redis outages/restarts without asking the browser to restart generation.
CREATE TABLE IF NOT EXISTS generation_job_outbox (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_generation_jobs_pending ON generation_job_outbox(created_at)
    WHERE dispatched_at IS NULL;
