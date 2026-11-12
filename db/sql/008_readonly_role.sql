-- Read-only role for agent tools and validator dry-runs. NOLOGIN: the pipeline switches to it
-- with SET LOCAL ROLE inside a READ ONLY transaction (no password lives in this file).

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_ro') THEN
        CREATE ROLE pipeline_ro NOLOGIN;
    END IF;
END $$;
COMMENT ON ROLE pipeline_ro IS 'SELECT-only access to bronze and meta, EXECUTE on silver helper functions.';

GRANT USAGE ON SCHEMA bronze, meta, silver TO pipeline_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, meta TO pipeline_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, meta GRANT SELECT ON TABLES TO pipeline_ro;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA silver TO pipeline_ro;
ALTER ROLE pipeline_ro SET statement_timeout = '5s';
