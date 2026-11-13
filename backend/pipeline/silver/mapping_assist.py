"""Finish the standard-value lists: one cheap-model call decides every leftover value.

After the agent's rules pass validation, some values can still fall outside a column's mapping
(on raw_tickets: priority 'hi', 'lo', 'crit'; 67 category values such as 'Lift', 'AC'). They
are found by running the compiled rules over the whole run (flag `unmapped`), grouped by the
same normalization key the mapping matches on, and sent with their counts, the standard values
and the business context to the cheap model, which decides per value:
  merge -> an existing standard value · new -> its own standard value · empty -> placeholder
  keep  -> leave it as a separate value (unsure)
Every decision carries a reason and is shown to the reviewer, who only changes what is wrong.
Only label-like, non-sensitive columns are sent (pipeline.privacy), and of those only values that
look like labels (<= 40 chars, <= 4 words): sentences that leaked into a label column stay with a person. Answers that name unknown
values or targets are dropped. The result must pass the full validator, else nothing changes.
"""

import uuid
from collections import Counter
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row

from pipeline.config import settings
from pipeline.context import get_context
from pipeline.db import get_ro_conn
from pipeline.llm import call_llm
from pipeline.logging_setup import get_logger
from pipeline.privacy import column_policies
from pipeline.profile_values import normalization_key
from pipeline.profiling import load_profile
from pipeline.silver.compiler import compile_rules
from pipeline.silver.rules_schema import SilverRules
from pipeline.silver.validator import validate_rules

PROMPT_VERSION = "mapping_assist_v1"
ACTIONS = ("merge", "new", "empty", "keep")
MAX_VALUES = 200  # per column, most frequent first
LABEL_MAX_CHARS, LABEL_MAX_WORDS = 40, 4  # longer values are free text that leaked in: never sent, left to a person


def label_like(value: str) -> bool:
    """Short values that read as labels (not sentences)."""
    return len(value) <= LABEL_MAX_CHARS and len(value.split()) <= LABEL_MAX_WORDS


SYSTEM = (
    "You finish value mappings for a data-cleaning rule. Each column has a list of standard values, each "
    "with spellings already merged into it. Some values in the data are not in the list yet. For EVERY "
    "listed leftover value decide one action:\n"
    "- merge: it means the same as an existing standard value (abbreviations, synonyms, typos, "
    "sub-types the business would report under that value). Set target to that standard value exactly.\n"
    "- new: a genuinely different value the business needs to count separately. Set target to a short "
    "lowercase snake_case name in the style of the existing standard values.\n"
    "- empty: a placeholder or meaningless value (e.g. '-', 'tbd', 'see notes').\n"
    "- keep: you are not sure; a person will decide.\n"
    "Prefer merge when the meaning clearly matches; do not invent meaning. Reason: at most 15 words, "
    "plain English for a business reader."
)


def leftovers(rules: SilverRules, run_id: str) -> dict[str, list[dict[str, Any]]]:
    """column -> [{value, spellings, count}] for values outside the mapping (whole run)."""
    mapped = [c for c in rules.columns if c.mapping]
    if not mapped:
        return {}
    compiled = compile_rules(rules, run_id)
    parts = sql.SQL(" UNION ALL ").join(
        sql.SQL("SELECT {} AS col, b.payload ->> {} AS raw FROM c JOIN bronze.records b ON b.bronze_id = c.bronze_id"
                " WHERE c._quality_flags -> {} ? 'unmapped'").format(
            sql.Literal(c.name), sql.Literal(c.source), sql.Literal(c.name))
        for c in mapped
    )  # fmt: skip
    query = sql.SQL("WITH c AS ({}) SELECT col, raw, count(*) AS n FROM ({}) AS u GROUP BY col, raw").format(
        compiled.query, parts
    )
    with get_ro_conn("120s") as conn:
        rows = conn.cursor(row_factory=dict_row).execute(query, compiled.params).fetchall()
    grouped: dict[str, dict[str, Counter[str]]] = {}
    for r in rows:
        raw = (r["raw"] or "").strip()
        if raw:
            grouped.setdefault(r["col"], {}).setdefault(normalization_key(raw), Counter())[raw] += r["n"]
    return {col: sorted(({"value": spellings_of(s)[0], "spellings": spellings_of(s), "count": sum(s.values())}
                         for s in keys.values()), key=lambda x: (-x["count"], x["value"]))
            for col, keys in grouped.items()}  # fmt: skip


def spellings_of(counts: Counter[str]) -> list[str]:
    """Most frequent first, ties alphabetical: the same representative every time."""
    return [x for x, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def _schema(columns: list[str]) -> dict[str, Any]:
    decision = {"type": "object", "additionalProperties": False, "required": ["value", "action", "target", "reason"],
                "properties": {"value": {"type": "string"}, "action": {"type": "string", "enum": list(ACTIONS)},
                               "target": {"type": "string"}, "reason": {"type": "string"}}}  # fmt: skip
    column = {"type": "object", "additionalProperties": False, "required": ["column", "decisions"],
              "properties": {"column": {"type": "string", "enum": columns},
                             "decisions": {"type": "array", "items": decision}}}  # fmt: skip
    return {"type": "object", "additionalProperties": False, "required": ["columns"],
            "properties": {"columns": {"type": "array", "items": column}}}  # fmt: skip


def _prompt(rules: SilverRules, todo: dict[str, list[dict[str, Any]]], context: dict[str, Any] | None) -> str:
    lines = [f"BUSINESS CONTEXT\n{context['text'].strip() if context else '(none)'}", ""]
    for c in rules.columns:
        if c.name not in todo:
            continue
        lines.append(f"COLUMN {c.name}")
        lines.append("Standard values (with spellings already merged):")
        # Sorted: recorded answers come back from jsonb with reordered keys, and the prompt must not depend on it.
        lines += [f"  {t}: {', '.join(v[:12]) or '-'}" for t, v in sorted((c.mapping or {}).items())]
        lines.append("Leftover values (value × rows):")
        lines += [f"  {x['value']} × {x['count']}" for x in todo[c.name][:MAX_VALUES]]
        lines.append("")
    return "\n".join(lines)


def _apply(rules: dict[str, Any], decisions: dict[str, list[dict[str, Any]]]) -> None:
    for c in rules["columns"]:
        for d in decisions.get(c["name"], []):
            if d["action"] == "merge":
                c["mapping"][d["target"]].append(d["value"])
            elif d["action"] == "new":
                c["mapping"].setdefault(d["target"], [])
                if d["value"] != d["target"]:
                    c["mapping"][d["target"]].append(d["value"])
            elif d["action"] == "empty":
                tokens = c.get("null_tokens") or []
                c["null_tokens"] = tokens + [s for s in d["spellings"] if s.lower() not in {t.lower() for t in tokens}]


def complete(run_id: str, rules: SilverRules, client: Any = None, mode: str | None = None) -> dict[str, Any]:
    """Decide every leftover value. Returns {rules, suggestions, dropped, call_id, cost_usd, ...}.

    `rules` is unchanged (and suggestions empty) when nothing is left over or nothing may be sent.
    Raises LLMError when the model can't be asked (e.g. replay mode without a recording).
    """
    log = get_logger("mapping_assist", run_id=run_id)
    profile = load_profile(uuid.UUID(run_id))
    context = get_context(uuid.UUID(run_id))
    policies = column_policies(profile["columns"], context)
    found = leftovers(rules, run_id)
    sources = {c.name: c.source for c in rules.columns}
    todo = {col: [v for v in vals if label_like(v["value"])] for col, vals in found.items()
            if policies.get(sources[col]) == "raw"}  # fmt: skip
    todo = {col: vals for col, vals in todo.items() if vals}
    skipped = sorted(set(found) - {col for col in found if policies.get(sources[col]) == "raw"})
    long_text = {col: sum(1 for v in vals if not label_like(v["value"])) for col, vals in found.items()}
    out: dict[str, Any] = {"rules": rules, "suggestions": {}, "dropped": [], "skipped_private": skipped,
                           "not_sent_long_text": {c: n for c, n in long_text.items() if n},
                           "call_id": None, "cost_usd": 0.0, "input_tokens": 0, "output_tokens": 0}  # fmt: skip
    if not todo:
        return out
    messages = [{"role": "user", "content": _prompt(rules, todo, context)}]
    result = call_llm("mapping_assist", settings.cheap_model, messages, PROMPT_VERSION, uuid.UUID(run_id),
                      json_schema=_schema(sorted(todo)), system=SYSTEM, effort="low", mode=mode, client=client)  # fmt: skip
    data = rules.model_dump(mode="json")
    by_key = {col: {normalization_key(v["value"]): v for v in vals} for col, vals in todo.items()}
    standard = {c["name"]: set(c.get("mapping") or {}) for c in data["columns"]}
    decisions: dict[str, list[dict[str, Any]]] = {}
    seen: set[tuple[str, str]] = set()
    for col in (result.data or {}).get("columns", []):
        name = col.get("column")
        for d in col.get("decisions", []):
            left = by_key.get(name, {}).get(normalization_key(str(d.get("value", ""))))
            target = str(d.get("target") or "").strip()
            if left is None or (name, left["value"]) in seen:
                out["dropped"].append({"column": name, "value": d.get("value"), "why": "not a leftover value"})
                continue
            if d["action"] == "merge" and target not in standard[name]:
                out["dropped"].append({"column": name, "value": left["value"], "why": f"unknown target {target!r}"})
                continue
            if d["action"] == "new":
                target = target or left["value"]
                standard[name].add(target)
            seen.add((name, left["value"]))
            decisions.setdefault(name, []).append({**left, "action": d["action"], "reason": str(d.get("reason", ""))[:200],
                                                   "target": target if d["action"] in ("merge", "new") else None})  # fmt: skip
    _apply(data, decisions)
    completed = SilverRules.model_validate(data)
    report = validate_rules(completed, run_id)
    out |= {"call_id": result.call_id, "cost_usd": float(result.cost_usd), "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens}  # fmt: skip
    if not report.ok:
        log.warning("mapping suggestions rejected by the validator; rules unchanged", errors=report.errors[:3])
        out["dropped"].append({"column": None, "value": None, "why": "validator: " + "; ".join(report.errors)[:300]})
        return out
    out |= {"rules": completed, "suggestions": decisions, "report": report}
    log.info("leftover values decided", columns=len(decisions), values=sum(map(len, decisions.values())),
             dropped=len(out["dropped"]), skipped_private=skipped)  # fmt: skip
    return out


def write(folder: Any, done: dict[str, Any], model: str) -> dict[str, Any] | None:
    """Store the decisions (always) and, when rules changed, proposed.yaml + validation.json.
    Returns the validation report dict when the rules changed, else None."""
    import json

    from pipeline.silver.rules_schema import to_yaml

    record = {"suggestions": done["suggestions"], "dropped": done["dropped"], "skipped_private": done["skipped_private"],
              "not_sent_long_text": done.get("not_sent_long_text", {}),
              "call_id": done["call_id"], "model": model, "cost_usd": done["cost_usd"]}  # fmt: skip
    (folder / "mapping_suggestions.json").write_text(json.dumps(record, indent=2, default=str))
    if not done["suggestions"]:
        return None
    report = done["report"]
    report_dict = {"ok": report.ok, "layer": report.layer, "errors": report.errors, "warnings": report.warnings,
                   "stats": report.stats}  # fmt: skip
    (folder / "proposed.yaml").write_text(to_yaml(done["rules"]))
    (folder / "validation.json").write_text(json.dumps(report_dict, indent=2, default=str))
    return report_dict
