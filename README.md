# Data Medallion

## Overview

_TODO_

## Architecture

_TODO_

## How to run

_TODO_

## Bronze

`python -m pipeline.cli ingest <file.csv> [--force]` loads any CSV into `bronze.records`
without needing to know its columns. `python -m pipeline.cli runs` lists past runs.

- **Faithful, schema-on-read:** each record keeps its exact text (`raw_line`) and a
  `payload` JSONB of `{column: raw string}`. Nothing is trimmed, cast or nulled.
  Header names that are empty or repeated are renamed (`column_<n>`, `<name>_2`);
  the original header is stored on the run.
- **Malformed rows are kept, not dropped:** too many fields (extras go in
  `_extra_fields`), too few fields, blank lines and unclosed quotes are loaded with
  `parse_status = 'malformed'` and an exact `parse_error`.
- **Lineage:** every row records its `run_id`, `source_file`, `source_line` (where the
  record starts), `batch_no` and `ingested_at`. `row_hash` = sha256 of `raw_line`.
- **Idempotent:** a file is identified by its sha256. Re-ingesting an
  already-loaded file is skipped. `--force`, or a previously failed run, replaces the
  old run. Repeated rows inside one file are kept; deduplication is silver's job.
- **Batched and atomic:** one transaction per run, with a savepoint per batch
  (`BATCH_SIZE`, default 1000). Each batch is loaded with COPY. If that fails, the
  batch is retried row by row, and rows the database refuses go to
  `bronze.load_errors`. The `running` run row is committed first, so a failed load
  stays visible as `failed`.
- **Reconciliation funnel** (`meta.ingestion_runs`, per batch in
  `meta.ingestion_batches`): physical lines → records parsed → loaded ok + loaded
  malformed + load failed. The three must add up to records parsed (checked in memory
  and in the DB) or the run fails.

## Silver

_TODO_

## Gold

_TODO_

## Agents (assessment)

_TODO_

## Metrics (raw_tickets.csv, 10,280 rows × 13 columns)

`python -m pipeline.cli metrics` prints these for any run. Profiler v1.2, card renderer 1.1:

| Metric | Value |
| --- | --- |
| Full profile (13 column profiles + summary + issues) | 30.4 KB, ~7,775 tokens (est.) |
| Dataset card, renderer 1.0 (before compaction) | 6,770 chars, **3,498 exact tokens** |
| Dataset card, renderer 1.1 | **2,356 exact tokens** (1.79 chars/token), budget 4,000 |
| Issues shown / total | 32 / 32 (nothing compressed) |
| Columns flagged / total | 12 / 13 |
| Context draft v1 call | 1,771 in / 603 out tokens, $0.000479 |
| Context draft v2 call (minimized prompt) | 2,151 in / 472 out tokens, $0.000451 |

Token budget: estimate = ceil(chars / 2) (`CARD_CHARS_PER_TOKEN`); `card --exact` asks the
provider and, if the card is over budget, re-renders with the measured ratio. The card is
dense (numbers, identifiers, punctuation), so chars/4 undercounted it about 2×.

## Business context

The pipeline keeps a human context and an LLM-drafted one side by side; the human one is
used. `python -m pipeline.cli context --compare` shows where they differ.

**Inferred (claude-haiku-5-5, prompt `context_draft_v2`, unapproved):** *facilities
maintenance tickets.* "Each row is a facilities service request logged by building occupants
and routed to internal staff or external vendors for resolution. Operations managers use this
data to track how quickly issues are resolved against SLA targets, how often requests
escalate, and what repairs cost across buildings and request categories."
Critical columns: created_at, resolved_at, category, priority, status, building, cost,
sla_hours. Tags: submitted_by and assigned_to = person (so they are masked in every LLM input).

**Human (`context/raw_tickets.md`):** _to be added_ - then run
`context --context-file ../context/raw_tickets.md` and `context --compare`.

## Silver rules and agent tools

Rules are YAML (`rules/<run>/proposed.yaml`, approved: `rules/silver_rules.yaml`), validated by
`pipeline/silver/validator.py` and compiled to SQL by `pipeline/silver/compiler.py`
(compiled SQL is written to `build/compiled/<run>/<rules_hash>.sql` for review). Example:

```bash
cd backend
../.venv/bin/python -m pipeline.cli default-rules                 # defaults for issue-free columns
../.venv/bin/python -m pipeline.cli validate-rules ../rules/examples/three_columns.yaml
../.venv/bin/python -m pipeline.cli tool list                     # the agent's tools
../.venv/bin/python -m pipeline.cli tool list_issues --args '{"severity": "high"}'
```

## Scaling to 100x

- **Bronze:** one transaction per run is fine for ~10k rows. For very large files,
  commit per batch and keep a resume checkpoint (`last_committed_line` on the run), so
  a crashed load resumes from where it stopped instead of restarting. Memory already
  stays flat, because records are streamed by a generator.
- **Profiling:** compute counts with SQL aggregates in Postgres (completeness,
  distinct counts, top values and duplicate hashes, via `GROUP BY` on
  `payload->>col`), so no rows are pulled into Python. Run the per-value regex work
  (pattern masks, type and date detection) on a fixed-size, seeded sample of about
  50k rows per column, and report shares from the sample. Profile size stays the
  same, because it grows with the number of columns, not rows. At 10M rows the
  slow parts today would be loading every row into a DataFrame (memory), the
  per-value Python loops (date parsing, type regexes, hidden-character scans), and
  the partial-row and near-duplicate passes over the whole frame; distinct counts
  and top values would move to streaming sketches (HyperLogLog, count-min,
  t-digest).

## Tradeoffs

_TODO_

## Decisions

See [docs/decisions.md](docs/decisions.md).
