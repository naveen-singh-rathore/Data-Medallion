-- One row per gold build (pipeline/gold/build.py): the gold tables are rebuilt from the run's current
-- silver build in a staging schema, reconciled, and swapped into `gold` in one transaction.

CREATE TABLE IF NOT EXISTS meta.gold_builds (
    build_id         uuid PRIMARY KEY,
    run_id           uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    silver_build_id  uuid,
    rules_hash       text,
    target_schema    text NOT NULL,
    status           text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error_message    text,
    roles            jsonb,
    row_counts       jsonb,
    checks           jsonb,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    duration_ms      integer
);
CREATE INDEX IF NOT EXISTS gold_builds_run_idx ON meta.gold_builds (run_id, started_at DESC);
COMMENT ON TABLE meta.gold_builds IS 'Gold builds: source silver build, resolved column roles, row counts and reconciliation checks.';
COMMENT ON COLUMN meta.gold_builds.roles IS 'Business role -> silver column used (id, opened, closed, category, priority, status, location, money, sla).';
COMMENT ON COLUMN meta.gold_builds.checks IS 'Reconciliation checks: name -> {expected, actual, ok}. The build fails if any is not ok.';
GRANT SELECT ON meta.gold_builds TO pipeline_ro;
