-- One row per DQ-agent thread (upserted at every pause and at the end).

CREATE TABLE IF NOT EXISTS meta.agent_runs (
    thread_id              text PRIMARY KEY,
    run_id                 uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    status                 text NOT NULL CHECK (status IN ('running', 'awaiting_context', 'awaiting_approval',
                                                           'approved', 'rejected', 'failed')),
    model                  text NOT NULL,
    prompt_version         text NOT NULL,
    turns                  integer NOT NULL DEFAULT 0,
    tool_calls             integer NOT NULL DEFAULT 0,
    tool_calls_by_name     jsonb NOT NULL DEFAULT '{}',
    input_tokens           integer NOT NULL DEFAULT 0,
    output_tokens          integer NOT NULL DEFAULT 0,
    cached_tokens          integer NOT NULL DEFAULT 0,
    cost_usd               numeric(12, 6) NOT NULL DEFAULT 0,
    duration_ms            integer,
    validation_attempts    integer NOT NULL DEFAULT 0,
    first_validation_errors jsonb,
    issues_total           integer,
    issues_addressed       integer,
    issues_ignored         integer,
    needs_human_count      integer,
    columns_default        integer,
    columns_agent          integer,
    columns_human_edited   integer,
    warnings               jsonb,
    rules_hash             text,
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS agent_runs_run_idx ON meta.agent_runs (run_id);
COMMENT ON TABLE meta.agent_runs IS 'DQ agent threads: outcome, effort (turns, tool calls), tokens, cost, validation and coverage.';
COMMENT ON COLUMN meta.agent_runs.cost_usd IS 'Money spent on LLM calls in this thread (replayed calls cost 0).';
COMMENT ON COLUMN meta.agent_runs.first_validation_errors IS 'Errors of the first failed validation, if any (how far off the first proposal was).';
GRANT SELECT ON meta.agent_runs TO pipeline_ro;
