-- Keep human and inferred context side by side; record how card tokens were measured.

DO $$
BEGIN
    -- 005 created dataset_context with PRIMARY KEY (run_id); move to (run_id, source) once.
    IF (SELECT array_agg(a.attname::text ORDER BY a.attname)
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
         WHERE i.indrelid = 'meta.dataset_context'::regclass AND i.indisprimary) = ARRAY['run_id'] THEN
        ALTER TABLE meta.dataset_context DROP CONSTRAINT dataset_context_pkey;
        ALTER TABLE meta.dataset_context ADD PRIMARY KEY (run_id, source);
    END IF;
END $$;
COMMENT ON TABLE meta.dataset_context IS 'Business context per run and source: a human row and/or an LLM-drafted row; the human row is used when present.';

ALTER TABLE meta.dataset_cards ADD COLUMN IF NOT EXISTS chars_per_token numeric(6, 3);
ALTER TABLE meta.dataset_cards ADD COLUMN IF NOT EXISTS token_basis text;
COMMENT ON COLUMN meta.dataset_cards.est_tokens IS 'ceil(chars / CARD_CHARS_PER_TOKEN) (default 2).';
COMMENT ON COLUMN meta.dataset_cards.chars_per_token IS 'Measured chars per exact token when exact_tokens exists, else the configured estimate ratio.';
COMMENT ON COLUMN meta.dataset_cards.token_basis IS 'exact: budget decisions used the provider count; estimate: chars / CARD_CHARS_PER_TOKEN.';
