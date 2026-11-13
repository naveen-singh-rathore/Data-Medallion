-- Semantic classification (pipeline/silver/classify.py): an LLM assigns a closed-list label to rows a
-- rule could not standardise, from their free text. Results are cached per text, stored per row, and
-- applied to the silver table as <col>_ai / <col>_ai_confidence / <col>_source (never overwriting <col>).

CREATE TABLE IF NOT EXISTS meta.classification_cache (
    text_hash      text PRIMARY KEY,
    task           text NOT NULL,
    prompt_version text NOT NULL,
    model          text NOT NULL,
    label          text NOT NULL,
    confidence     numeric(4, 3) NOT NULL,
    call_id        uuid,
    created_at     timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.classification_cache IS 'One LLM label per distinct (task, prompt version, model, labels, masked text); reruns read here.';
COMMENT ON COLUMN meta.classification_cache.text_hash IS 'sha256 of task + prompt_version + model + allowed labels + normalized masked text.';

CREATE TABLE IF NOT EXISTS silver.ai_labels (
    run_id         uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    column_name    text NOT NULL,
    bronze_id      bigint NOT NULL,
    label          text NOT NULL,
    confidence     numeric(4, 3) NOT NULL,
    text_hash      text NOT NULL,
    model          text NOT NULL,
    prompt_version text NOT NULL,
    labelled_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, column_name, bronze_id)
);
COMMENT ON TABLE silver.ai_labels IS 'AI label per silver row and column; silver builds join it into <col>_ai and <col>_ai_confidence.';

CREATE TABLE IF NOT EXISTS meta.classification_runs (
    classification_id uuid PRIMARY KEY,
    run_id            uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    column_name       text NOT NULL,
    mode              text NOT NULL,
    model             text NOT NULL,
    prompt_version    text NOT NULL,
    targets           integer NOT NULL,
    unique_texts      integer NOT NULL,
    cache_hits        integer NOT NULL,
    llm_calls         integer NOT NULL,
    cost_usd          numeric(12, 6) NOT NULL,
    duration_ms       integer NOT NULL,
    recovered         integer NOT NULL,
    low_confidence    integer NOT NULL,
    distribution      jsonb NOT NULL,
    consistency       jsonb,
    created_at        timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.classification_runs IS 'One row per classification pass: targets, cache use, LLM calls, cost, labels recovered.';
COMMENT ON COLUMN meta.classification_runs.recovered IS 'Target rows given a label with confidence >= the threshold (used as <col>_source = ai).';
COMMENT ON COLUMN meta.classification_runs.consistency IS 'Optional report-only check: AI label vs rule label on a sample of rule-mapped rows.';

GRANT SELECT ON meta.classification_cache, meta.classification_runs, silver.ai_labels TO pipeline_ro;
