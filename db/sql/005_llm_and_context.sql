-- LLM call log + response cache, business context per run, rendered dataset cards.

CREATE TABLE IF NOT EXISTS meta.llm_calls (
    call_id        uuid PRIMARY KEY,
    run_id         uuid REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    purpose        text NOT NULL,
    provider       text NOT NULL,
    model          text NOT NULL,
    prompt_version text NOT NULL,
    request_hash   text NOT NULL,
    input_tokens   integer,
    output_tokens  integer,
    cached_tokens  integer,
    cost_usd       numeric(12, 6) NOT NULL DEFAULT 0,
    latency_ms     integer,
    cache_hit      boolean NOT NULL DEFAULT false,
    status         text NOT NULL CHECK (status IN ('ok', 'error')),
    error          text,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS llm_calls_run_idx ON meta.llm_calls (run_id);
COMMENT ON TABLE meta.llm_calls IS 'One row per LLM request the pipeline makes, including cache hits and failures.';
COMMENT ON COLUMN meta.llm_calls.purpose IS 'What the call was for, e.g. context_draft.';
COMMENT ON COLUMN meta.llm_calls.request_hash IS 'sha256 of model + prompt_version + messages + schema + tools (canonical JSON); key into meta.llm_cache.';
COMMENT ON COLUMN meta.llm_calls.input_tokens IS 'Uncached input tokens from the provider usage field (on a cache hit: the original call''s usage).';
COMMENT ON COLUMN meta.llm_calls.cached_tokens IS 'Input tokens served from the provider prompt cache (cache_read_input_tokens).';
COMMENT ON COLUMN meta.llm_calls.cost_usd IS 'Money spent by this call from the pricing table; 0 on a cache hit.';
COMMENT ON COLUMN meta.llm_calls.cache_hit IS 'True when the response came from meta.llm_cache and no API call was made.';

CREATE TABLE IF NOT EXISTS meta.llm_cache (
    request_hash text PRIMARY KEY,
    response     jsonb NOT NULL,
    model        text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.llm_cache IS 'Raw provider responses by request hash; replay mode answers only from here.';

CREATE TABLE IF NOT EXISTS meta.dataset_context (
    run_id      uuid PRIMARY KEY REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    text        text NOT NULL,
    draft       jsonb,
    source      text NOT NULL CHECK (source IN ('human', 'inferred')),
    approved    boolean NOT NULL DEFAULT false,
    llm_call_id uuid REFERENCES meta.llm_calls (call_id) ON DELETE SET NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.dataset_context IS 'Business context for a run: written by a human, or drafted by one cheap LLM call and awaiting approval.';
COMMENT ON COLUMN meta.dataset_context.text IS 'Context as prose (the human text, or the rendered draft).';
COMMENT ON COLUMN meta.dataset_context.draft IS 'Structured context (domain, summary, metrics, critical columns, column tags) when available.';
COMMENT ON COLUMN meta.dataset_context.approved IS 'Human context is approved on entry; an inferred draft needs `context --approve`.';

CREATE TABLE IF NOT EXISTS meta.dataset_cards (
    run_id            uuid NOT NULL REFERENCES meta.ingestion_runs (run_id) ON DELETE CASCADE,
    card_hash         text NOT NULL,
    renderer_version  text NOT NULL,
    budget_tokens     integer NOT NULL,
    est_tokens        integer NOT NULL,
    exact_tokens      integer,
    chars             integer NOT NULL,
    tiers_full        jsonb NOT NULL,
    compressed_counts jsonb NOT NULL,
    columns_total     integer NOT NULL,
    columns_flagged   integer NOT NULL,
    issues_total      integer NOT NULL,
    issues_shown      integer NOT NULL,
    over_budget       boolean NOT NULL DEFAULT false,
    card_text         text NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, card_hash)
);
COMMENT ON TABLE meta.dataset_cards IS 'Rendered dataset cards (the agent''s index of a run) with size metrics; same text = same hash = one row.';
COMMENT ON COLUMN meta.dataset_cards.est_tokens IS 'ceil(chars / 4).';
COMMENT ON COLUMN meta.dataset_cards.exact_tokens IS 'Provider token count (count_tokens), when requested with `card --exact`.';
COMMENT ON COLUMN meta.dataset_cards.tiers_full IS 'Per tier: true if every line was rendered, false if part was compressed into a pointer line.';
COMMENT ON COLUMN meta.dataset_cards.compressed_counts IS 'How many columns / issues each pointer line stands for.';
