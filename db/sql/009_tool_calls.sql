-- One row per agent tool call (also manual `tool` CLI calls).

CREATE TABLE IF NOT EXISTS meta.tool_calls (
    call_id      uuid PRIMARY KEY,
    run_id       uuid REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    tool         text NOT NULL,
    args         jsonb NOT NULL,
    result_bytes integer,
    latency_ms   integer,
    ok           boolean NOT NULL,
    error        text,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tool_calls_run_idx ON meta.tool_calls (run_id);
COMMENT ON TABLE meta.tool_calls IS 'Agent tool calls: arguments, result size, latency, success.';
COMMENT ON COLUMN meta.tool_calls.result_bytes IS 'Size of the JSON result returned to the agent (tools cap it at ~2 KB).';
GRANT SELECT ON meta.tool_calls TO pipeline_ro;
