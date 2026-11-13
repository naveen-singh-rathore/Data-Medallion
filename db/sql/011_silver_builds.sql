-- One row per silver build: the approved rules applied to a bronze run and stored in the rules'
-- target table (default silver.records_clean). The target table itself is (re)created by
-- pipeline/silver/build.py, because its columns come from the rules.

CREATE TABLE IF NOT EXISTS meta.silver_builds (
    build_id       uuid PRIMARY KEY,
    run_id         uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    target_table   text NOT NULL,
    rules_hash     text NOT NULL,
    rules_yaml     text NOT NULL,
    sql_digest     text NOT NULL,
    status         text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error_message  text,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    duration_ms    integer,
    rows_in        integer,
    rows_kept      integer,
    rows_flagged   integer,
    rows_rejected  integer,
    rows_duplicate integer,
    column_stats   jsonb
);
CREATE INDEX IF NOT EXISTS silver_builds_run_idx ON meta.silver_builds (run_id, started_at DESC);

COMMENT ON TABLE meta.silver_builds IS 'Silver builds: approved rules applied to a bronze run, with the rules used and a row funnel.';
COMMENT ON COLUMN meta.silver_builds.rules_yaml IS 'The approved rules exactly as applied (the file can change later; this cannot).';
COMMENT ON COLUMN meta.silver_builds.sql_digest IS 'sha256 prefix of the compiled SQL that produced the table.';
COMMENT ON COLUMN meta.silver_builds.rows_in IS 'Bronze rows with parse_status = ok for the run; must equal kept + flagged + rejected + duplicate.';
COMMENT ON COLUMN meta.silver_builds.rows_flagged IS 'Rows kept with at least one error/warning in _errors (e.g. a failed row check with action flag).';
COMMENT ON COLUMN meta.silver_builds.column_stats IS 'Per target column: values changed by a rule, null before/after.';

GRANT SELECT ON meta.silver_builds TO pipeline_ro;
