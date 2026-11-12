-- Per-batch load stats and rows the database refused. Safe to re-run.
CREATE TABLE IF NOT EXISTS meta.ingestion_batches (
    run_id      uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    batch_no    int NOT NULL,
    first_line  int NOT NULL,
    last_line   int NOT NULL,
    records     int NOT NULL,
    loaded      int NOT NULL,
    malformed   int NOT NULL,
    load_failed int NOT NULL,
    method      text NOT NULL CHECK (method IN ('copy', 'row_fallback')),
    duration_ms int NOT NULL,
    PRIMARY KEY (run_id, batch_no)
);

COMMENT ON TABLE meta.ingestion_batches IS 'One row per load batch of an ingestion run.';
COMMENT ON COLUMN meta.ingestion_batches.loaded IS 'Records written to bronze.records (ok + malformed).';
COMMENT ON COLUMN meta.ingestion_batches.malformed IS 'Subset of loaded with parse_status = malformed.';
COMMENT ON COLUMN meta.ingestion_batches.load_failed IS 'Records the database refused; written to bronze.load_errors.';
COMMENT ON COLUMN meta.ingestion_batches.method IS 'copy = whole batch via COPY; row_fallback = COPY failed, loaded row by row.';

CREATE TABLE IF NOT EXISTS bronze.load_errors (
    id            bigserial PRIMARY KEY,
    run_id        uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    batch_no      int NOT NULL,
    source_line   int NOT NULL,
    raw_line      text NOT NULL,
    row_hash      text NOT NULL,
    error_message text NOT NULL,
    failed_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS load_errors_run_id_idx ON bronze.load_errors (run_id);

COMMENT ON TABLE bronze.load_errors IS 'Records the database refused to store in bronze.records, kept so no data is lost.';
COMMENT ON COLUMN bronze.load_errors.error_message IS 'Database error raised when inserting the record.';
