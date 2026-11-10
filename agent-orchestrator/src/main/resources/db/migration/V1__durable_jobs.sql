CREATE TABLE investigation_job (
    id UUID PRIMARY KEY,
    submission_key VARCHAR(128) NOT NULL UNIQUE,
    scenario VARCHAR(32) NOT NULL CHECK (scenario IN ('HEALTHY','PARTIAL','TIMEOUT','REQUIRED_FAILURE')),
    budget_ms BIGINT NOT NULL CHECK (budget_ms BETWEEN 1 AND 86400000),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    state VARCHAR(16) NOT NULL DEFAULT 'QUEUED' CHECK (state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_until TIMESTAMPTZ,
    worker_id VARCHAR(128),
    failure_code VARCHAR(32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'RUNNING' AND lease_until IS NOT NULL AND worker_id IS NOT NULL)
        OR (state <> 'RUNNING' AND lease_until IS NULL AND worker_id IS NULL))
);
CREATE INDEX investigation_job_ready ON investigation_job (available_at, id) WHERE state = 'QUEUED';
CREATE INDEX investigation_job_expired ON investigation_job (lease_until, id) WHERE state = 'RUNNING';
CREATE TABLE investigation_attempt (
    job_id UUID NOT NULL REFERENCES investigation_job(id),
    number INTEGER NOT NULL CHECK (number BETWEEN 1 AND 10),
    worker_id VARCHAR(128) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('RUNNING','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    failure_code VARCHAR(32),
    PRIMARY KEY (job_id, number),
    CHECK ((state = 'RUNNING') = (finished_at IS NULL))
);
-- One persisted outcome, not one external call. Both result and terminal state commit in one transaction.
CREATE TABLE investigation_result (
    job_id UUID PRIMARY KEY REFERENCES investigation_job(id),
    attempt_number INTEGER NOT NULL,
    report TEXT NOT NULL CHECK (char_length(report) BETWEEN 1 AND 100000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (job_id, attempt_number) REFERENCES investigation_attempt(job_id, number)
);
