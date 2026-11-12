-- One row per CSV record, stored schema-on-read. Safe to re-run.
CREATE TABLE IF NOT EXISTS bronze.records (
    bronze_id    bigserial PRIMARY KEY,
    run_id       uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    source_file  text NOT NULL,
    source_line  int NOT NULL,
    raw_line     text NOT NULL,
    payload      jsonb NOT NULL,
    row_hash     text NOT NULL,
    parse_status text NOT NULL CHECK (parse_status IN ('ok', 'malformed')),
    parse_error  text,
    batch_no     int NOT NULL,
    ingested_at  timestamptz NOT NULL,
    UNIQUE (run_id, source_line)
);

-- No unique constraint on row_hash: duplicate rows are kept; dedup is silver's job.
CREATE INDEX IF NOT EXISTS records_row_hash_idx ON bronze.records (row_hash);
CREATE INDEX IF NOT EXISTS records_run_id_idx ON bronze.records (run_id);

COMMENT ON TABLE bronze.records IS 'Every CSV record exactly as read: raw text plus a {column: raw string} payload. Duplicates kept.';
COMMENT ON COLUMN bronze.records.source_line IS 'Physical line (1-based) where the record starts in the source file.';
COMMENT ON COLUMN bronze.records.raw_line IS 'Exact record text minus the final line terminator (NUL bytes shown as \x00).';
COMMENT ON COLUMN bronze.records.payload IS 'Header name -> value as raw string; extra fields under "_extra_fields".';
COMMENT ON COLUMN bronze.records.row_hash IS 'SHA-256 of raw_line (UTF-8); identical records share a hash across runs and batch sizes.';
COMMENT ON COLUMN bronze.records.parse_status IS 'ok = field count matches header; malformed = anything else (see parse_error).';
COMMENT ON COLUMN bronze.records.ingested_at IS 'Start time of the run; identical for all records in a run.';
