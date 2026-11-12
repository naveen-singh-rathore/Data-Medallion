-- One Postgres schema per medallion layer. Safe to re-run.
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA meta   IS 'Pipeline metadata: runs, column profiles, LLM call logs.';
COMMENT ON SCHEMA bronze IS 'Raw data as ingested: all columns as text, plus load lineage.';
COMMENT ON SCHEMA silver IS 'Cleaned, typed, deduplicated data.';
COMMENT ON SCHEMA gold   IS 'Business-level aggregates ready for analysis.';
