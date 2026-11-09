-- Run state that changes. Spans live in ClickHouse; these tables map a run's step
-- numbers to its spans and checkpoints, and link branches to their parents.

CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,           -- the run's trace ID
    task_id       TEXT NOT NULL,
    task          JSONB NOT NULL,             -- the task spec, frozen at start
    driver        TEXT NOT NULL,
    parent_run_id TEXT REFERENCES runs (run_id),
    parent_step   INT,
    change        JSONB,                      -- what a branch changed
    status        TEXT NOT NULL,              -- running, done, error
    outcome       TEXT,                       -- pass, fail
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS runs_parent ON runs (parent_run_id, parent_step);

-- What a run consumed, and the files it left behind (path -> sha256), for
-- bisect cost and branch compare.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS llm_calls          INT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS input_tokens       BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS output_tokens      BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cache_read_tokens  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cache_write_tokens BIGINT NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cost_usd           DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS final_files        JSONB;

-- Runs an eval started: one continuation of one case under one variant.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS eval_case    TEXT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS eval_variant TEXT;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS eval_batch   TEXT;
CREATE INDEX IF NOT EXISTS runs_eval ON runs (eval_batch, eval_case, eval_variant);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs (run_id),
    step          INT NOT NULL,
    driver        TEXT NOT NULL,
    level         INT NOT NULL,
    driver_ref    TEXT NOT NULL,              -- the driver's own ID; the driver keeps the bytes
    size_bytes    BIGINT,
    pinned        BOOLEAN NOT NULL DEFAULT false,
    expires_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS steps (
    run_id        TEXT NOT NULL REFERENCES runs (run_id),
    step_index    INT NOT NULL,
    span_id       TEXT NOT NULL,
    kind          TEXT NOT NULL,              -- llm, tool
    checkpoint_id TEXT REFERENCES checkpoints (checkpoint_id),
    PRIMARY KEY (run_id, step_index)
);

-- An eval case is a checkpoint, a success check and an optional change. Its
-- checkpoint is pinned and never expires.
CREATE TABLE IF NOT EXISTS eval_cases (
    case_id       TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES runs (run_id),
    step          INT NOT NULL,
    checkpoint_id TEXT NOT NULL REFERENCES checkpoints (checkpoint_id),
    check_cmd     JSONB,
    change        JSONB,
    notes         TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
