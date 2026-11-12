-- Deterministic profiles of a bronze run: input for the data-quality agent. Safe to re-run.
CREATE TABLE IF NOT EXISTS meta.column_profiles (
    run_id           uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    column_name      text NOT NULL,
    column_position  int NOT NULL,
    profile          jsonb NOT NULL,
    profiler_version text NOT NULL,
    profiled_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, column_name)
);

COMMENT ON TABLE meta.column_profiles IS 'One profile per column of a bronze run, computed from parse_status = ok rows only.';
COMMENT ON COLUMN meta.column_profiles.column_name IS 'Normalized column name, as used as a key in bronze.records.payload.';
COMMENT ON COLUMN meta.column_profiles.column_position IS '1-based position of the column in the source header.';
COMMENT ON COLUMN meta.column_profiles.profile IS 'Compact JSON facts (completeness, cardinality, types, patterns, dates, numbers, ...), ~2 KB max.';
COMMENT ON COLUMN meta.column_profiles.profiler_version IS 'Version of the profiling code that produced this row.';

CREATE TABLE IF NOT EXISTS meta.dataset_profiles (
    run_id           uuid PRIMARY KEY REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    summary          jsonb NOT NULL,
    issues           jsonb NOT NULL,
    profiler_version text NOT NULL,
    profiled_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE meta.dataset_profiles IS 'Dataset-level profile of a bronze run: summary, cross-column checks and issue flags.';
COMMENT ON COLUMN meta.dataset_profiles.summary IS 'Row counts, duplicates, overall empty share and cross-column check results.';
COMMENT ON COLUMN meta.dataset_profiles.issues IS 'Deterministic issue flags [{column, code, severity, evidence}], sorted by severity.';
COMMENT ON COLUMN meta.dataset_profiles.profiler_version IS 'Version of the profiling code that produced this row.';
