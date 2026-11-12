# Time log

| Step | Time spent | Notes |
| --- | --- | --- |
| 1. Project skeleton | _fill in_ | Structure, config, DB connection, logging, layer schemas, CLI stubs, Makefile, tests, docs stubs |
| 2. Bronze ingestion | _fill in_ | Batched COPY with row fallback, lineage, file-hash idempotency, reconciliation funnel, 17 bronze tests |
| 3. Deterministic profiling | _fill in_ | Column facts, dataset summary, cross-column checks, 17 issue rules, CLI table, 11 profiling tests |
| 3b. Profiler v1.1 upgrade | _fill in_ | Audit of items 1–34, central config, sentinels/spikes, normalization variants + typos, hidden chars/mojibake, new date formats, partial rows; 25 new tests |
| 4. LLM foundation, context, dataset card | _fill in_ | Thin client with cache/replay/record + call log, context (human or one cheap draft), budget-aware card with coverage guarantee, metrics CLI; 18 new tests |
| 3c. Profiler v1.2 | _fill in_ | DATE_ORDER investigation (real data, not parsing), format-pair evidence, duplicate stats + key-repeat hints, examples on every issue, number kinds, zero_share; 7 new tests |
| 4b. Step 4 fix-ups | _fill in_ | Exact-token budget, compact card (3,498 → 2,356 tokens), central data minimization, context v2 + human/inferred compare; 13 new/updated tests |
| 5. Rule schema, compiler, validator, agent tools | _fill in_ | Strict rule schema + JSON schema, SQL compiler with safe casts, 4-layer validator, default rules, 7 read-only masked tools + registry + call log; 32 new tests |
