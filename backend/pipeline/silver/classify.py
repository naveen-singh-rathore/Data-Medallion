"""Semantic classification: recover a closed-list label from free text where rules could not.

Target column: the approved rule whose source the business context tags `category` and that has a
standard-value mapping. Target rows (in the current silver build): the column is empty or holds a
value outside the standard list (unmapped labels and sentences that leaked into the column).

Per target row the model sees ONE masked text: the column's own raw text plus the free-text
columns (context tag `free_text`), with emails, phone numbers and person names (name-like values of
person-tagged columns) replaced (pipeline.privacy.scrub_text). It answers one of the standard values
or "other", plus a confidence; the JSON schema makes the list closed.

Scale and cost: identical normalized texts are classified once; results are cached per text hash
(task + prompt version + model + labels + text) in meta.classification_cache, so a rerun makes no
calls; uncached texts go in batches of BATCH to the cheap model. Every call is logged by call_llm.

Results: silver.ai_labels (per row), applied to the silver table as <col>_ai (NULL below
MIN_CONFIDENCE), <col>_ai_confidence and <col>_source = rule | ai | none. <col> itself is never
changed. Quality flags: ai_labelled / ai_low_confidence.
"""

import csv
import hashlib
import json
import random
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.config import REPO_ROOT, settings
from pipeline.context import get_context
from pipeline.db import transaction
from pipeline.errors import PipelineError
from pipeline.llm import call_llm
from pipeline.logging_setup import get_logger
from pipeline.privacy import context_tags, name_like, scrub_text
from pipeline.silver.build import approved_rules, is_current, latest_build
from pipeline.silver.rules_schema import ColumnRule, SilverRules

TASK = "category_classify"
PROMPT_VERSION = "category_classify_v1"
BATCH = 50
MIN_CONFIDENCE = 0.6
MAX_TEXT = 400
OTHER = "other"
EVAL_DIR = REPO_ROOT / "data" / "eval"
SYSTEM = (
    "You label facilities tickets with exactly one category from a closed list. Each ticket shows the "
    "text found in its category field (may be a non-standard label, a sentence, or empty) and its "
    "free-text fields. Pick the category a facilities operations manager would report it under. Use "
    f"'{OTHER}' when no listed category fits. Confidence: 0-1, your probability that the label is right; "
    "below 0.6 when the text is vague, empty, or fits several categories. Placeholders like [PERSON] "
    "replace private data."
)


class ClassifyError(PipelineError, RuntimeError):
    """Classification can't run (no approved rules, no current silver build, no target column)."""


# --- what to classify ----------------------------------------------------------------------------


def target_rule(rules: SilverRules, tags: dict[str, str]) -> ColumnRule:
    """The category column rule to classify: a mapped column tagged 'category' in the context."""
    for c in rules.columns:
        if c.mapping and tags.get(c.source) == "category":
            return c
    raise ClassifyError("no approved rule with a standard-value list for a column tagged 'category'")


def labels_for(rule: ColumnRule) -> list[str]:
    """The closed label list: the rule's standard values plus 'other'."""
    return sorted(set(rule.mapping or {}) | {OTHER})


def _norm(text: str) -> str:
    return " ".join(text.split())[:MAX_TEXT]


def text_hash(text: str, labels: list[str], model: str) -> str:
    """Cache key for one text: task, prompt version, model, label list and text."""
    key = json.dumps([TASK, PROMPT_VERSION, model, labels, text], ensure_ascii=False)
    return hashlib.sha256(key.encode()).hexdigest()


def _table(build: dict[str, Any]) -> sql.Identifier:
    schema, _, table = build["target_table"].partition(".")
    return sql.Identifier(schema, table)


def _rows(run_id: str, rules: SilverRules, rule: ColumnRule, tags: dict[str, str], where: str) -> list[dict[str, Any]]:
    """Rows of the current build with the raw texts the prompt needs (from bronze, so unchanged)."""
    build = latest_build(run_id)
    if not is_current(build, rules):
        raise ClassifyError("no current silver build for the approved rules: run build-silver first")
    free = [c.source for c in rules.columns if tags.get(c.source) == "free_text"]
    canonical = sql.SQL("ARRAY[{}]::text[]").format(sql.SQL(", ").join(sql.Literal(t) for t in rule.mapping or {}))
    cond = {"targets": sql.SQL("({col} IS NULL OR NOT ({col} = ANY({canon})))"),
            "rule_mapped": sql.SQL("{col} = ANY({canon})")}[where].format(col=sql.Identifier(rule.name), canon=canonical)  # fmt: skip
    query = sql.SQL("SELECT s.bronze_id, s.{col} AS current, b.payload FROM {t} s JOIN bronze.records b"
                    " ON b.bronze_id = s.bronze_id WHERE s._status IN ('kept', 'flagged') AND {cond}"
                    " ORDER BY s.bronze_id").format(col=sql.Identifier(rule.name), t=_table(build), cond=cond)  # fmt: skip
    with transaction() as conn:
        rows = conn.cursor(row_factory=dict_row).execute(query).fetchall()
    for r in rows:
        r["parts"] = [(rule.source, r["payload"].get(rule.source) or "")] + [
            (f, r["payload"].get(f) or "") for f in free
        ]
    return rows


def person_names(run_id: str, rules: SilverRules, tags: dict[str, str]) -> list[str]:
    """Distinct values of person columns, removed from ticket text before it reaches the model."""
    cols = [c.source for c in rules.columns if tags.get(c.source) in ("person", "contact_pii")]
    if not cols:
        return []
    with transaction() as conn:
        values = conn.execute(
            sql.SQL("SELECT DISTINCT btrim(v) FROM bronze.records, LATERAL unnest(ARRAY[{}]) AS v"
                    " WHERE run_id = %s AND v IS NOT NULL").format(
                sql.SQL(", ").join(sql.SQL("payload ->> {}").format(sql.Literal(c)) for c in cols)),
            (run_id,)).fetchall()  # fmt: skip
    return sorted({v for (v,) in values if v and name_like(v)})


def ticket_text(parts: list[tuple[str, str]], names: list[str]) -> str:
    """The text the model sees: 'field: value' parts, with emails, phones and names scrubbed."""
    shown = [f"{field}: {scrub_text(_norm(value), names)}" for field, value in parts if value and value.strip()]
    return _norm(" | ".join(shown))


# --- the model -----------------------------------------------------------------------------------


def _schema(labels: list[str]) -> dict[str, Any]:
    item = {"type": "object", "additionalProperties": False, "required": ["id", "category", "confidence"],
            "properties": {"id": {"type": "integer"}, "category": {"type": "string", "enum": labels},
                           "confidence": {"type": "number"}}}  # fmt: skip
    return {"type": "object", "additionalProperties": False, "required": ["items"],
            "properties": {"items": {"type": "array", "items": item}}}  # fmt: skip


def _prompt(rule: ColumnRule, labels: list[str], texts: list[str], context: dict[str, Any] | None) -> str:
    hints = [f"- {t}: e.g. {', '.join((rule.mapping or {}).get(t, [])[:6]) or t}" for t in labels if t != OTHER]
    tickets = "\n".join(json.dumps({"id": i, "ticket": t}, ensure_ascii=False) for i, t in enumerate(texts))
    domain = (context or {}).get("text", "").strip().splitlines()[0] if context else ""
    return (f"Dataset: {domain}\nCategories (with spellings seen in the data):\n" + "\n".join(hints)
            + f"\n- {OTHER}: none of the above\n\nLabel every ticket below (one item per id).\n{tickets}")  # fmt: skip


def _ask(rule: ColumnRule, labels: list[str], texts: list[str], run_id: str, context: Any, mode: str | None,
         client: Any) -> tuple[dict[int, tuple[str, float]], Any]:  # fmt: skip
    result = call_llm(TASK, settings.cheap_model, [{"role": "user", "content": _prompt(rule, labels, texts, context)}],
                      PROMPT_VERSION, uuid.UUID(run_id), json_schema=_schema(labels), system=SYSTEM, effort="low",
                      options={"max_tokens": 8000}, mode=mode, client=client)  # fmt: skip
    out: dict[int, tuple[str, float]] = {}
    for item in (result.data or {}).get("items", []):
        i, label = item.get("id"), item.get("category")
        if isinstance(i, int) and 0 <= i < len(texts) and label in labels and i not in out:
            out[i] = (label, min(max(float(item.get("confidence", 0)), 0.0), 1.0))
    return out, result


def classify_texts(texts: list[str], rule: ColumnRule, run_id: str, context: Any, mode: str | None = None,
                   client: Any = None) -> tuple[dict[str, tuple[str, float]], dict[str, Any]]:  # fmt: skip
    """text -> (label, confidence) for distinct texts, cache first. Returns (results, usage)."""
    labels = labels_for(rule)
    model = settings.cheap_model
    hashes = {t: text_hash(t, labels, model) for t in texts}
    with transaction() as conn:
        cached = {h: (lab, float(conf)) for h, lab, conf in conn.execute(
            "SELECT text_hash, label, confidence FROM meta.classification_cache WHERE text_hash = ANY(%s)",
            (list(hashes.values()),)).fetchall()}  # fmt: skip
    results = {t: cached[h] for t, h in hashes.items() if h in cached}
    todo = [t for t in texts if hashes[t] not in cached]
    usage = {"unique_texts": len(texts), "cache_hits": len(results), "llm_calls": 0, "cost_usd": 0.0, "missing": 0,
             "call_latency_ms": []}  # fmt: skip
    batches = [todo[start : start + BATCH] for start in range(0, len(todo), BATCH)]
    # Batches are independent and cached per text, so they run in parallel (bounded); results are
    # consumed in batch order, so cache writes and usage totals are the same as a sequential run.
    workers = max(1, min(settings.classify_concurrency, len(batches)))
    log = get_logger("classify", run_id=run_id)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="classify") as pool:
        asked = pool.map(lambda batch: _ask(rule, labels, batch, run_id, context, mode, client), batches)
        for n, (batch, (answers, result)) in enumerate(zip(batches, asked, strict=True), start=1):
            _record_batch(batch, answers, result, hashes, model, results, usage)
            log.debug("batch labelled", batch=n, of=len(batches), texts=len(batch), answered=len(answers),
                      latency_ms=result.latency_ms, cache_hit=result.cache_hit)  # fmt: skip
    return results, usage


def _record_batch(batch: list[str], answers: dict[int, tuple[str, float]], result: Any, hashes: dict[str, str],
                  model: str, results: dict[str, tuple[str, float]], usage: dict[str, Any]) -> None:  # fmt: skip
    """Store one batch's labels in the classification cache and add its cost/latency to `usage`."""
    usage["llm_calls"] += 1
    usage["cost_usd"] += float(result.cost_usd)
    usage["call_latency_ms"].append(result.latency_ms)
    usage["missing"] += len(batch) - len(answers)
    with transaction() as conn:
        for i, (label, conf) in answers.items():
            conn.execute(
                "INSERT INTO meta.classification_cache (text_hash, task, prompt_version, model, label, confidence, call_id)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (text_hash) DO NOTHING",
                (hashes[batch[i]], TASK, PROMPT_VERSION, model, label, round(conf, 3), result.call_id),
            )  # fmt: skip
            results[batch[i]] = (label, round(conf, 3))


# --- apply to silver -----------------------------------------------------------------------------


def apply_ai_labels(conn: Any, table: sql.Composable, run_id: str, rules: SilverRules) -> list[str]:
    """Add/refresh <col>_ai, <col>_ai_confidence, <col>_source on `table` from silver.ai_labels.
    Used after classification (live table) and by every silver build (staging table)."""
    done = []
    for (name,) in conn.execute(
        "SELECT DISTINCT column_name FROM silver.ai_labels WHERE run_id = %s", (run_id,)
    ).fetchall():
        rule = next((c for c in rules.columns if c.name == name and c.mapping), None)
        if rule is None:
            continue
        col, ai, conf, src = (sql.Identifier(name), sql.Identifier(f"{name}_ai"), sql.Identifier(f"{name}_ai_confidence"),
                              sql.Identifier(f"{name}_source"))  # fmt: skip
        canonical = sql.SQL("ARRAY[{}]::text[]").format(sql.SQL(", ").join(sql.Literal(t) for t in rule.mapping or {}))
        for ident, kind in ((ai, "text"), (conf, "numeric(4, 3)"), (src, "text")):
            conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} " + kind).format(table, ident))
        conn.execute(sql.SQL(
            "UPDATE {t} s SET {ai} = CASE WHEN l.confidence >= {min} THEN l.label END, {conf} = l.confidence,"
            " {src} = CASE WHEN s.{col} = ANY({canon}) THEN 'rule' WHEN l.confidence >= {min} THEN 'ai' ELSE 'none' END,"
            # idempotent: drop earlier ai flags of this column, then add the current one (if any)
            " _quality_flags = CASE WHEN s._quality_flags ? {name} OR (l.label IS NOT NULL AND NOT coalesce(s.{col} = ANY({canon}), false))"
            "   THEN jsonb_set(coalesce(s._quality_flags, '{{}}'), ARRAY[{name}],"
            "        (coalesce(s._quality_flags -> {name}, '[]') - 'ai_labelled' - 'ai_low_confidence')"
            "        || coalesce(to_jsonb(CASE WHEN l.label IS NULL OR s.{col} = ANY({canon}) THEN NULL"
            "                                  WHEN l.confidence >= {min} THEN 'ai_labelled' ELSE 'ai_low_confidence' END), '[]'))"
            "   ELSE s._quality_flags END"
            " FROM (SELECT s2.bronze_id, l2.label, l2.confidence FROM {t} s2 LEFT JOIN silver.ai_labels l2"
            "   ON l2.bronze_id = s2.bronze_id AND l2.run_id = %(run)s AND l2.column_name = {name}) l"
            " WHERE l.bronze_id = s.bronze_id").format(
            t=table, ai=ai, conf=conf, src=src, col=col, canon=canonical, name=sql.Literal(name),
            min=sql.Literal(MIN_CONFIDENCE)), {"run": run_id})  # fmt: skip
        done.append(name)
    return done


def _refresh_live_table(run_id: str, rules: SilverRules) -> None:
    build = latest_build(run_id)
    table = _table(build)  # type: ignore[arg-type]
    schema, _, name = build["target_table"].partition(".")  # type: ignore[index]
    valid = sql.Identifier(schema, f"{name}_valid")
    with transaction() as conn:
        conn.execute(sql.SQL("DROP VIEW IF EXISTS {}").format(valid))  # SELECT * views fix their columns
        apply_ai_labels(conn, table, run_id, rules)
        conn.execute(
            sql.SQL("CREATE VIEW {} AS SELECT * FROM {} WHERE _status IN ('kept', 'flagged')").format(valid, table)
        )
        conn.execute(sql.SQL("GRANT SELECT ON {} TO pipeline_ro").format(valid))


# --- entry points --------------------------------------------------------------------------------


def classify_run(run_id: uuid.UUID | str, mode: str | None = None, client: Any = None, check: int = 0,
                 approved_path: Path | None = None) -> dict[str, Any]:  # fmt: skip
    """Classify the target rows of the run's current silver build; store, apply, report."""
    run_id = str(run_id)
    log = get_logger("classify", run_id=run_id)
    started = time.perf_counter()
    rules, _ = approved_rules(run_id, approved_path)
    context = get_context(uuid.UUID(run_id))
    tags = context_tags(context)
    rule = target_rule(rules, tags)
    names = person_names(run_id, rules, tags)
    rows = _rows(run_id, rules, rule, tags, "targets")
    for r in rows:
        r["text"] = ticket_text(r["parts"], names)
    with_text = [r for r in rows if r["text"]]
    results, usage = classify_texts(sorted({r["text"] for r in with_text}), rule, run_id, context, mode, client)
    labelled = [(r, results[r["text"]]) for r in with_text if r["text"] in results]
    with transaction() as conn:
        conn.execute("DELETE FROM silver.ai_labels WHERE run_id = %s AND column_name = %s", (run_id, rule.name))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO silver.ai_labels (run_id, column_name, bronze_id, label, confidence, text_hash, model, prompt_version)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                [(run_id, rule.name, r["bronze_id"], lab, conf, text_hash(r["text"], labels_for(rule), settings.cheap_model),
                  settings.cheap_model, PROMPT_VERSION) for r, (lab, conf) in labelled],
            )  # fmt: skip
    _refresh_live_table(run_id, rules)
    recovered = [lab for _, (lab, conf) in labelled if conf >= MIN_CONFIDENCE]
    consistency = consistency_check(run_id, rules, rule, tags, names, context, mode, client, check) if check else None
    report = {
        "classification_id": str(uuid.uuid4()), "column": rule.name, "targets": len(rows), "with_text": len(with_text),
        **{k: v for k, v in usage.items() if k != "call_latency_ms"},
        "avg_call_latency_ms": round(sum(usage["call_latency_ms"]) / len(usage["call_latency_ms"])) if usage["call_latency_ms"] else 0,
        "recovered": len(recovered), "low_confidence": len(labelled) - len(recovered),
        "distribution": dict(Counter(recovered).most_common()), "duration_ms": int((time.perf_counter() - started) * 1000),
        "consistency": consistency, "mode": mode or settings.llm_mode,
    }  # fmt: skip
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.classification_runs (classification_id, run_id, column_name, mode, model, prompt_version,"
            " targets, unique_texts, cache_hits, llm_calls, cost_usd, duration_ms, recovered, low_confidence,"
            " distribution, consistency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (report["classification_id"], run_id, rule.name, report["mode"], settings.cheap_model, PROMPT_VERSION,
             report["targets"], report["unique_texts"], report["cache_hits"], report["llm_calls"], report["cost_usd"],
             report["duration_ms"], report["recovered"], report["low_confidence"], Jsonb(report["distribution"]),
             Jsonb(consistency) if consistency else None),
        )  # fmt: skip
    log.info("classification done", **{k: report[k] for k in ("targets", "unique_texts", "cache_hits", "llm_calls",
                                                               "recovered", "low_confidence", "duration_ms")})  # fmt: skip
    return report


def consistency_check(run_id: str, rules: SilverRules, rule: ColumnRule, tags: dict[str, str], names: list[str],
                      context: Any, mode: str | None, client: Any, n: int) -> dict[str, Any]:  # fmt: skip
    """Report only: AI label vs the rule's label on a fixed random sample of rule-mapped rows."""
    rows = _rows(run_id, rules, rule, tags, "rule_mapped")
    random.Random(7).shuffle(rows)
    sample = [r for r in rows if (r.update(text=ticket_text(r["parts"], names)) or r["text"])][:n]
    results, usage = classify_texts(sorted({r["text"] for r in sample}), rule, run_id, context, mode, client)
    pairs = [(r["current"], results[r["text"]][0]) for r in sample if r["text"] in results]
    agree = sum(a == b for a, b in pairs)
    return {"sampled": len(pairs), "agreement": round(agree / len(pairs), 3) if pairs else None,
            "top_disagreements": [[a, b, n] for (a, b), n in Counter((a, b) for a, b in pairs if a != b).most_common(8)],
            "llm_calls": usage["llm_calls"], "cost_usd": usage["cost_usd"]}  # fmt: skip


def make_eval_sheet(run_id: uuid.UUID | str, n: int = 40, path: Path | None = None,
                    approved_path: Path | None = None) -> Path:  # fmt: skip
    """CSV of n distinct target tickets (fixed seed) for a person to label. The AI's answer is NOT
    in the sheet, so the labels are independent."""
    run_id = str(run_id)
    rules, _ = approved_rules(run_id, approved_path)
    tags = context_tags(get_context(uuid.UUID(run_id)))
    rule = target_rule(rules, tags)
    names = person_names(run_id, rules, tags)
    seen, picked = set(), []
    rows = _rows(run_id, rules, rule, tags, "targets")
    random.Random(42).shuffle(rows)
    for r in rows:
        text = ticket_text(r["parts"], names)
        if text and text not in seen:
            seen.add(text)
            picked.append((r, text))
        if len(picked) == n:
            break
    path = path or EVAL_DIR / f"{rule.name}_eval_{run_id[:8]}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bronze_id", "text_shown_to_ai", "human_label", f"allowed: {' | '.join(labels_for(rule))}"])
        for r, text in picked:
            w.writerow([r["bronze_id"], text, "", ""])
    return path


def evaluate(path: Path, run_id: uuid.UUID | str) -> dict[str, Any]:
    """Accuracy / coverage of the stored AI labels against a filled-in eval sheet, plus cost and latency."""
    run_id = str(run_id)
    with path.open(encoding="utf-8") as f:
        sheet = [r for r in csv.DictReader(f) if (r.get("human_label") or "").strip()]
    if not sheet:
        raise ClassifyError(f"no human_label filled in {path}")
    ids = [int(r["bronze_id"]) for r in sheet]
    with transaction() as conn:
        got = {b: (lab, float(c)) for b, lab, c in conn.execute(
            "SELECT bronze_id, label, confidence FROM silver.ai_labels WHERE run_id = %s AND bronze_id = ANY(%s)",
            (run_id, ids)).fetchall()}  # fmt: skip
        last = conn.cursor(row_factory=dict_row).execute(
            "SELECT * FROM meta.classification_runs WHERE run_id = %s AND llm_calls > 0 ORDER BY created_at DESC LIMIT 1",
            (run_id,)).fetchone()  # fmt: skip
    rows = [(r["human_label"].strip(), got.get(int(r["bronze_id"]))) for r in sheet]
    answered = [(h, a) for h, a in rows if a]
    confident = [(h, a) for h, a in answered if a[1] >= MIN_CONFIDENCE]
    return {
        "labelled": len(rows), "answered": len(answered),
        "accuracy_all": round(sum(h == a[0] for h, a in answered) / len(answered), 3) if answered else None,
        "coverage": round(len(confident) / len(rows), 3),
        "accuracy_confident": round(sum(h == a[0] for h, a in confident) / len(confident), 3) if confident else None,
        "errors": [[h, a[0], a[1]] for h, a in confident if h != a[0]],
        "cost_usd": float(last["cost_usd"]) if last else None, "llm_calls": last["llm_calls"] if last else None,
        "duration_ms": last["duration_ms"] if last else None,
    }  # fmt: skip
