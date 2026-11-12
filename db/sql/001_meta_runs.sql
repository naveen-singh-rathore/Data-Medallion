-- One row per ingestion attempt, with the load funnel. Safe to re-run.
CREATE TABLE IF NOT EXISTS meta.ingestion_runs (
    run_id                uuid PRIMARY KEY,
    source_file           text NOT NULL,
    file_sha256           text NOT NULL UNIQUE,
    file_size_bytes       bigint,
    encoding              text,
    status                text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error_message         text,
    started_at            timestamptz NOT NULL,
    finished_at           timestamptz,
    duration_ms           int,
    header                jsonb,
    physical_lines        int,
    records_parsed        int,
    rows_loaded_ok        int,
    rows_loaded_malformed int,
    load_failed           int,
    malformed_by_reason   jsonb,
    batches               int,
    batches_fallback      int,
    duplicate_hash_groups int
);

COMMENT ON TABLE meta.ingestion_runs IS 'One row per ingestion attempt of a source file, with its load funnel.';
COMMENT ON COLUMN meta.ingestion_runs.file_sha256 IS 'SHA-256 of the file bytes; the idempotency key (same bytes = same file).';
COMMENT ON COLUMN meta.ingestion_runs.encoding IS 'Text encoding that decoded the file (utf-8-sig, cp1252 or latin-1).';
COMMENT ON COLUMN meta.ingestion_runs.header IS 'Original header names, in file order, before normalization.';
COMMENT ON COLUMN meta.ingestion_runs.physical_lines IS 'Lines in the file including the header (a quoted newline adds a line).';
COMMENT ON COLUMN meta.ingestion_runs.records_parsed IS 'Data records found (excludes the header).';
COMMENT ON COLUMN meta.ingestion_runs.rows_loaded_ok IS 'Records loaded into bronze.records with parse_status = ok.';
COMMENT ON COLUMN meta.ingestion_runs.rows_loaded_malformed IS 'Records loaded into bronze.records with parse_status = malformed.';
COMMENT ON COLUMN meta.ingestion_runs.load_failed IS 'Records the database refused; kept in bronze.load_errors.';
COMMENT ON COLUMN meta.ingestion_runs.malformed_by_reason IS 'Count of malformed records per reason, e.g. {"too many fields": 22}.';
COMMENT ON COLUMN meta.ingestion_runs.batches_fallback IS 'Batches whose COPY failed and were loaded row by row.';
COMMENT ON COLUMN meta.ingestion_runs.duplicate_hash_groups IS 'Distinct row_hash values that occur more than once in this run.';
