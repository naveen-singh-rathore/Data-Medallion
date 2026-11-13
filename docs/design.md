# Design

How the pipeline is put together and why. Numbers are from the final review of `data/raw_tickets.csv`
(10,280 rows × 13 columns). The dated log of every decision and the alternatives rejected is in
[decisions.md](decisions.md); metric definitions are in [metrics.md](metrics.md).

## 1. The one idea

**Agents propose, code executes, people approve.**

| Who | Does | Never does |
| --- | --- | --- |
| Code | counts, parses, compiles SQL, builds tables, reconciles | guess meaning |
| Model | reads a summary, investigates with read-only tools, proposes rules as data, labels free text from a closed list | touch data, write SQL, approve |
| Person | approves each rule, answers the questions the model could not | review SQL or 10,000 rows |

Everything else follows from keeping those three apart: the model's output is a small, typed,
reviewable document (`SilverRules`), so it can be validated, diffed, approved, replayed and compiled.

## 2. Layers

| Layer | Contract | How it is enforced |
| --- | --- | --- |
| Bronze | the file, exactly, plus lineage | `raw_line` + JSONB payload of raw strings; malformed rows kept with a reason; funnel must reconcile or the run fails |
| Silver | one row per bronze row, typed, with a status and the reason for every change | compiled from approved rules; `_status` kept / flagged / rejected / duplicate; `<col>_raw`, `_quality_flags`, `_errors`; staging table swapped in one transaction |
| Gold | numbers with their coverage and limits | hand-written SQL with metric contracts; 5 reconciliation checks; `data_quality_findings` beside the marts |
| Meta | everything that happened | runs, batches, profiles, cards, LLM calls and cache, tool calls, agent runs, builds |

One Postgres schema per layer, so the boundary is visible in any SQL client and grantable. Nothing
is ever dropped between layers: a rejected or duplicate row stays in silver with its status, and the
`_valid` view is what gold reads.

Final numbers: bronze 10,280 → silver 7,699 kept + 1,736 flagged (9,435 usable), 22 rejected, 823
duplicates → gold fact 9,435; marts 942 / 3,541 / 196 rows; 6 findings; 5/5 checks.

## 3. Techniques

### Profiling: measure first, in code

The profiler turns 2 MB of messy values into evidence: per column completeness, null-like tokens,
cardinality and role, pattern masks, type shares, date formats with day/month evidence, sentinels,
spelling variants; per dataset duplicates, key repeats, date order, partial rows. It emits 32 issues
(5 high, 25 medium, 2 low) with an id, a severity and evidence. No model is involved, so the facts
the agent reasons from are exact and repeatable.

- **Evidence over inference:** "month-first" is decided by counting values that only parse one way
  (1,863 vs 0), not by asking a model.
- **A shuffle test for the headline finding:** 35.4% of tickets resolve before they open; pairing
  the same dates at random gives 35.0% and the correlation is 0.01. The dates are unrelated, not
  swapped, so the rule is `flag`, never `swap`.
- **Pure function of the file:** thresholds live in one config whose hash is stored with the
  profile, and value samples are seeded with the file's sha256.

### Context engineering: the dataset card

The agent never sees the profile (hundreds of KB). It sees a **card**: a deterministic text index
with a token budget (2,421 tokens of a 4,000 budget here), filled in tiers, with a coverage
guarantee: every issue is either shown or counted with a pointer to the tool that returns it, so
nothing is silently left out. The card is byte-identical for the same file, which makes it cacheable
across turns (prompt caching) and replayable.

- **Data minimisation:** each column gets a policy from its profile and business tags. Label-like,
  non-sensitive columns show raw values; everything else shows shapes (`TKT-1234` → `<AAA-9999>`).
  Person and contact columns are masked in the card, in every tool result and in classification text.

### The DQ agent

LangGraph state machine, checkpointed in Postgres (a pause survives the process):

`prepare → agent ⇄ tools → validate → map_leftovers → approve (human) → save`

| Guardrail | Mechanism |
| --- | --- |
| Output shape | `submit_rules` tool whose schema is the strict `SilverRules` model (extra fields forbidden) |
| Can't hurt data | tools run as a NOLOGIN read-only role, in a read-only transaction, with a statement timeout |
| Can't run forever | 20 tool calls, 25 turns; at the budget, pending calls are answered "not run" and only `submit_rules` is offered; hard stop at 40 turns |
| Can't skip an issue | coverage layer: every high and medium issue id must be fixed by a rule or ignored with a reason |
| Can't submit nonsense | validator: schema → meaning → dry run on 2,000 real rows → coverage; errors go back to the model (≤ 2 KB, at most 3 attempts) |
| Can't decide alone | LangGraph interrupt: nothing is saved until a person approves; edited rules are re-validated |
| Says what it doesn't know | `needs_human` questions with options and a recommendation; confidence < 0.6 requires one |

The prompt gives a role, the inputs, a working method (look, hypothesise, test before submitting),
decision policies (never invent values, prefer NULL to rejecting, zero is ambiguous, dates from
evidence) and what a good `why` looks like (business impact with a number).

Final run: 11 turns, 20 tool calls, $0.59, 109 s, validated on the first submission, 30/30 issues
covered, 3 questions.

**What it got wrong, and what that says.** This run proposed no mapping for `priority` and missed
823 tickets whose notes say they duplicate another ticket; an earlier run got priority right and
missed the epoch date format and the IT category. The misses are different each time. So the design
does not depend on any single run being right: the validator catches what can be checked, the
leftover-mapping helper and the review page surface what remains, a person decides, and the decision
is recorded and replayable.

### Rules as data, one compiler

`SilverRules` is a menu, not a language: cleaning steps, null tokens, sentinels, mappings, date
formats by registry id, ranges, row checks, dedup. One compiler turns it into one set-based query,
built as named layers (clean → tokens → mapping → typing → range → row checks → output columns),
with every identifier and literal passed through `psycopg.sql`. A golden-file test pins the SQL.

The cost of a menu is that new needs require compiler work; the three row fixes
(`swapped_columns`, `junk_row`, `declared_duplicate`) were added that way, and each still goes
through the same validator and approval.

### Semantic classification

For rows the rules cannot standardise (320 here), a cheap model picks one label from a closed list.

| Concern | Technique |
| --- | --- |
| Cost | dedupe to distinct texts (314), 50 per call, cheap model: $0.009 |
| Speed | independent batches on a bounded thread pool: 26 s instead of about 57 s |
| Reruns | per-text cache keyed by task, prompt version, model and label list: rerun is 100% cache, 0.5 s |
| Hallucination | JSON schema with an enum of the approved values + `other`; answers outside it are dropped |
| Uncertainty | below 0.6 confidence the row stays unlabelled and is counted as a finding (34 rows) |
| Auditability | labels live in `category_ai` with a confidence and `category_source`; `category` is never overwritten |
| Privacy | emails, phones and person names are scrubbed from the text first |
| Quality signal | the model also labels 200 rows the rules already mapped: 99.5% agreement |

### Gold: contracts and findings

Gold is plain SQL because metrics must be exact and explainable. Every time metric carries its
coverage (`usable / resolved`) and an excluded count; cost carries its coverage; volume sums to the
fact table by construction. The build fails unless five reconciliation checks pass, and it writes
`data_quality_findings` in the same transaction, so the finding that invalidates a metric is stored
next to the metric and the UI badges it "unreliable".

### Human in the loop

Review is per rule: what it does, why, which issues it fixes, its effect on all rows, its YAML.
Approve, reject with a reason (the column falls back to its default rule) or undo; values outside a
standard list get a choice (merge / new / empty / leave) with an AI suggestion. A decision is
fingerprinted, so it goes stale if the rule changes afterwards. Finishing the review resumes the
same agent thread, so the approval in the UI and the agent's state are one record.

### Deterministic replay

Three modes: `replay` (answers only from the recorded cache: no key, no network), `live` (cache
first, then the API), `record` (always call, overwrite). The cache key is a hash of the full
request, so a recording is only as replayable as the requests are deterministic. Making the agent
run replay on a different database meant removing every place where the request depended on
something other than the file:

1. the card header named the run id and upload name → the file's sha256;
2. value samples were seeded with the run id → the file's sha256 (stored with the profile);
3. a prompt was built from a dict whose key order `jsonb` storage changes → sorted;
4. a tool read rows without `ORDER BY` and broke ties by row order → file order.

Approval is bound to the file, not the run: the provenance file records the file's sha256, the
approved context, the review decisions, the profile's sampling seed and a fingerprint of the agent's
proposal. `make demo` rebuilds everything from that and then replays the recorded review to its
approval pause and checks the fingerprint (11 turns, 20 tool calls, match). A contract test hashes
every prompt and schema sent to a model, so changing one is a deliberate act.

## 4. Cost and scale

| Step | Cost on this file | What it scales with |
| --- | --- | --- |
| Profiler, compiler, builds | $0 | rows (set-based SQL; sample or sketch the regex work at 10M) |
| Context draft | $0.0004 | one call per dataset |
| DQ agent | $0.59 | columns × tool calls, per schema, not per row or per file |
| Leftover mapping | $0.001 | distinct leftover labels |
| Classification | $0.009 | distinct texts (cache makes the daily tail small) |

At 100× the rows the agent's cost does not change: it reads a fixed-size card and a 2,000-row dry
run, and its approved rules are reused for every later file with the same schema. The per-row costs
are in SQL. The README's "Scaling to 100x" section lists what changes per layer, and its "Daily
incremental loads" section gives the design for merging daily files (not built).

## 5. Engineering

- `ruff` with bugbear, bandit, complexity, exception, logging, timezone, typing and docstring rules;
  `ruff format`; 216 tests against a real Postgres.
- One exception hierarchy for expected failures (`PipelineError`); the CLI prints them as one line
  and exits 2, anything else keeps its traceback; builds record failures and log the traceback.
- Structured `key=value` logs with `stage` and `run_id`; counts and durations per stage; every LLM
  call logged with tokens, cost, latency and cache hit.
- Idempotent everywhere: a file is identified by its hash; silver and gold build in staging and swap
  in one transaction; three consecutive demo runs produce identical tables apart from build ids.

## 6. Known limits

- Silver is rebuilt in full from one run: a second file replaces the first (design for incremental
  loads in the README).
- No schema drift detection and no separate schema agent; typing is part of the DQ agent's rules.
- Classifier accuracy against human labels is not measured yet (a 40-ticket sheet is prepared).
- Dependencies are not pinned to a lock file.
- Time metrics for this file are built but not meaningful; that is a property of the data, and it is
  the first thing gold says.
