"""Validate silver rules in four layers; stop at the first layer with errors.

1. schema    - Pydantic; messages name the field path (columns[2] (cost).why)
2. semantic  - sources, names, format ids, day/month order, step/type fit, references
3. dry-run   - compile on a fixed 2,000-row sample, run as pipeline_ro in a rolled-back
               read-only transaction (10 s timeout); per-column stats
4. coverage  - every high and medium issue id is addressed (by a rule, or ignored + reason)

Errors block the rules; warnings go to the human reviewer. `feedback()` is the compact
(<= 2 KB) text sent back to the agent.
"""

import difflib
import re
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from pydantic import ValidationError

from pipeline.bronze import normalize_header
from pipeline.db import get_ro_conn, transaction
from pipeline.profile_dates import format_by_id, format_ids, is_ambiguous_shape
from pipeline.profile_values import normalization_key
from pipeline.silver.compiler import OUTPUT_COLUMNS, compile_rules
from pipeline.silver.rules_schema import (
    NUMBER_STEPS,
    ROW_CHECK_COLUMNS,
    TEXT_STEPS,
    ColumnRule,
    SilverRules,
)

SAMPLE_ROWS = 2000
DRY_RUN_TIMEOUT = "10s"
FEEDBACK_BYTES = 2048
LINE_CHARS = 900  # long enough for a full list of issue ids
WARN_INVALID = 0.05
WARN_UNPARSEABLE = 0.02  # of non-empty values: typed columns that can't read 2% of their values always warn
WARN_ROWS_WITH_ERRORS = 0.10
WARN_NEWLY_NULL = 0.30
WARN_CONFIDENCE = 0.6
DATE_TYPES = ("date", "timestamp")


@dataclass
class ValidationReport:
    """Validator outcome: the last layer run, errors (block), warnings (for the reviewer), stats."""

    ok: bool
    layer: str  # last layer run: schema | semantic | dry_run | coverage
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def feedback(self) -> str:
        """Compact, actionable text for the agent (<= 2 KB)."""
        head = (
            f"VALID ({self.layer} passed)" if self.ok else f"INVALID at layer {self.layer}: {len(self.errors)} error(s)"
        )
        lines = [head, *(f"ERROR {e}" for e in self.errors), *(f"WARN {w}" for w in self.warnings)]
        for name, s in self.stats.get("columns", {}).items():
            lines.append(f"STAT {name}: null {s['null_before']:.0%}->{s['null_after']:.0%} invalid {s['invalid']:.1%}"
                         f" sentinels {s['sentinel_hits']} mapped {s['mapped']} unmapped {s['unmapped']}")  # fmt: skip
        if "rows" in self.stats:
            r = self.stats["rows"]
            lines.append(f"STAT rows: {r['sampled']} sampled, errors {r['with_errors']:.1%}, rejected {r['rejected']},"
                         f" dedup removals {r['dedup_removed']}")  # fmt: skip
        return _fit(lines, FEEDBACK_BYTES)


def _fit(lines: list[str], limit: int) -> str:
    """Join lines; if over `limit` bytes, keep a prefix and say how many lines were cut."""
    kept: list[str] = []
    for i, line in enumerate(lines):
        tail = f"... {len(lines) - i} more lines"
        candidate = "\n".join([*kept, line[:LINE_CHARS]])
        if len(candidate.encode()) + len(tail) + 1 > limit:
            return "\n".join([*kept, tail])
        kept.append(line[:LINE_CHARS])
    return "\n".join(kept)


# --- layer 1: schema ---------------------------------------------------------------------


def _path(loc: tuple[Any, ...], raw: dict[str, Any]) -> str:
    """columns.2.why -> columns[2] (cost).why"""
    parts: list[str] = []
    node: Any = raw
    for key in loc:
        if isinstance(key, int):
            name = (
                node[key].get("name")
                if isinstance(node, list) and key < len(node) and isinstance(node[key], dict)
                else None
            )
            parts[-1] += f"[{key}]" + (f" ({name})" if name else "")
        else:
            parts.append(str(key))
        node = node[key] if isinstance(node, dict | list) and _has(node, key) else None
    return ".".join(parts) or "(root)"


def _has(node: Any, key: Any) -> bool:
    return (isinstance(node, dict) and key in node) or (
        isinstance(node, list) and isinstance(key, int) and key < len(node)
    )


def check_schema(raw: Any) -> tuple[SilverRules | None, list[str]]:
    """Parse into SilverRules; return errors with field paths instead of raising."""
    if isinstance(raw, SilverRules):
        return raw, []
    try:
        return SilverRules.model_validate(raw), []
    except ValidationError as exc:
        return None, [f"{_path(e['loc'], raw if isinstance(raw, dict) else {})}: {e['msg']}" for e in exc.errors()]


# --- layer 2: semantic -------------------------------------------------------------------


@dataclass(frozen=True)
class RunFacts:
    """What the meaning checks need from a run: its header and its issues by id."""

    header: list[str]
    issues: dict[str, dict[str, Any]]  # id -> issue


def run_facts(run_id: str) -> RunFacts:
    """Load the run's header and profiled issues."""
    with transaction() as conn:
        header = conn.execute("SELECT header FROM meta.ingestion_runs WHERE run_id = %s", (run_id,)).fetchone()
        issues = conn.execute("SELECT issues FROM meta.dataset_profiles WHERE run_id = %s", (run_id,)).fetchone()
    if not header or not issues:
        raise ValueError(f"run {run_id} is not ingested and profiled")
    return RunFacts(normalize_header(header[0]), {i["id"]: i for i in issues[0] if "id" in i})


def _column_semantics(rules: SilverRules, facts: RunFacts) -> list[str]:
    """Meaning checks per column rule: sources exist, names are free, settings fit the type."""
    names = [c.name for c in rules.columns]
    errors = [f"columns: target name {name!r} used {n} times" for name, n in Counter(names).items() if n > 1]
    for c in rules.columns:
        where = f"columns ({c.name})"
        for check in (_identity_errors, _date_errors, _type_fit_errors, _mapping_errors, _range_errors):
            errors += check(c, where, facts, names)
    return errors


def _identity_errors(c: ColumnRule, where: str, facts: RunFacts, names: list[str]) -> list[str]:
    errors = []
    if c.source not in facts.header:
        errors.append(f"{where}.source: {c.source!r} is not a column of this run")
    if c.name in OUTPUT_COLUMNS or c.name.startswith("_") or (c.name.endswith("_raw") and c.name[:-4] in names):
        errors.append(f"{where}.name: {c.name!r} clashes with a reserved output column")
    return errors


def _date_errors(c: ColumnRule, where: str, _facts: RunFacts, _names: list[str]) -> list[str]:
    if c.type not in DATE_TYPES:
        return (
            [f"{where}.parse_formats: only for date/timestamp columns"] if c.parse_formats or c.day_month_order else []
        )
    errors = []
    if not c.parse_formats:
        errors.append(f"{where}.parse_formats: required for type {c.type}")
    formats = [format_by_id(f) for f in c.parse_formats]
    unknown = [f for f, fmt in zip(c.parse_formats, formats, strict=True) if fmt is None]
    if unknown:
        errors.append(f"{where}.parse_formats: {unknown_format_message(unknown)}")
    if any(f and is_ambiguous_shape(f) for f in formats) and not c.day_month_order:
        errors.append(f"{where}.day_month_order: required (dmy|mdy) for day/month-ambiguous formats")
    return errors


def _type_fit_errors(c: ColumnRule, where: str, _facts: RunFacts, _names: list[str]) -> list[str]:
    errors = []
    if c.type != "text" and TEXT_STEPS & set(c.steps):
        errors.append(f"{where}.steps: {sorted(TEXT_STEPS & set(c.steps))} only apply to text columns")
    if c.type not in ("integer", "numeric") and NUMBER_STEPS & set(c.steps):
        errors.append(f"{where}.steps: {sorted(NUMBER_STEPS & set(c.steps))} only apply to integer/numeric columns")
    if c.type == "boolean" and not (c.boolean_true and c.boolean_false):
        errors.append(f"{where}: boolean columns need boolean_true and boolean_false")
    return errors


def _mapping_errors(c: ColumnRule, where: str, _facts: RunFacts, _names: list[str]) -> list[str]:
    errors = []
    if c.mapping is not None and c.type != "text":
        errors.append(f"{where}.mapping: only for text columns")
    seen: dict[str, str] = {}  # a spelling (normalised) may belong to one standard value only
    for target, variants in (c.mapping or {}).items():
        for v in [target, *variants]:
            key = normalization_key(v)
            if seen.setdefault(key, target) != target:
                errors.append(f"{where}.mapping: {v!r} maps to both {seen[key]!r} and {target!r}")
    return errors


def _range_errors(c: ColumnRule, where: str, _facts: RunFacts, _names: list[str]) -> list[str]:
    r = c.range
    if r is not None and isinstance(r.min, int | float) and isinstance(r.max, int | float) and r.min > r.max:
        return [f"{where}.range: min > max"]
    return []


def _reference_semantics(rules: SilverRules, facts: RunFacts) -> list[str]:
    errors = []
    types = {c.name: c.type for c in rules.columns}
    if rules.dedup:
        dedup_cols = [*rules.dedup.key, *([rules.dedup.order_by] if rules.dedup.order_by else [])]
        errors.extend(f"dedup: {col!r} is not a target column" for col in dedup_cols if col not in types)
    for i, check in enumerate(rules.row_checks):
        cols = [str(check.params[k]) for k in ROW_CHECK_COLUMNS[check.kind]]
        errors.extend(
            f"row_checks[{i}] ({check.kind}): {col!r} is not a target column" for col in cols if col not in types
        )
        if check.kind == "swapped_columns" and all(c in types for c in cols):
            a = next(c for c in rules.columns if c.name == cols[0])
            if not a.mapping:
                errors.append(
                    f"row_checks[{i}] (swapped_columns): {cols[0]!r} needs a standard-value mapping to recognise its labels"
                )
            if any(types[c] != "text" for c in cols):
                errors.append(f"row_checks[{i}] (swapped_columns): both columns must be text")
        if check.kind == "date_order" and any(types.get(c) not in DATE_TYPES for c in cols if c in types):
            errors.append(f"row_checks[{i}] (date_order): both columns must be date/timestamp")
    cited = [(f"columns ({c.name}).addresses", a) for c in rules.columns for a in c.addresses]
    cited += [("dedup.addresses", a) for a in (rules.dedup.addresses if rules.dedup else [])]
    cited += [(f"row_checks[{i}].addresses", a) for i, ch in enumerate(rules.row_checks) for a in ch.addresses]
    cited += [("addressed_issues", r.issue_id) for r in rules.addressed_issues]
    cited += [("needs_human", i) for q in rules.needs_human for i in q.issue_ids]
    for where, issue in cited:
        if issue not in facts.issues:
            errors.append(f"{where}: unknown issue id {issue!r}")
    return errors


def _mapping_warnings(rules: SilverRules, run_id: str) -> list[str]:
    """Variants that never occur in the data (by normalization key)."""
    warnings = []
    with get_ro_conn() as conn:
        for c in rules.columns:
            if not c.mapping:
                continue
            present = {r[0] for r in conn.execute(
                "SELECT DISTINCT silver.norm_key(payload ->> %s) FROM bronze.records WHERE run_id = %s", (c.source, run_id)
            )}  # fmt: skip
            missing = [v for t, vs in c.mapping.items() for v in vs if normalization_key(v) not in present]
            if missing:
                warnings.append(f"columns ({c.name}).mapping: variants never seen in the data: {missing[:5]}")
    return warnings


# --- layer 3: dry run --------------------------------------------------------------------


def dry_run(rules: SilverRules, run_id: str) -> tuple[dict[str, Any], list[str]]:
    """Run the compiled SQL on the fixed sample as pipeline_ro; return stats and any SQL error."""
    compiled = compile_rules(rules, run_id, sample=SAMPLE_ROWS)
    query = sql.SQL("SELECT c.*, b.payload AS __payload FROM ({}) AS c JOIN bronze.records b ON b.bronze_id = c.bronze_id").format(
        compiled.query)  # fmt: skip
    try:
        with get_ro_conn(DRY_RUN_TIMEOUT) as conn:
            rows = conn.cursor(row_factory=dict_row).execute(query, compiled.params).fetchall()
    except Exception as exc:  # noqa: BLE001 - reported to the agent, never raised
        return {}, [f"dry_run: SQL failed: {str(exc).splitlines()[0][:200]}"]
    return _stats(rules, rows), []


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def unknown_format_message(unknown: list[str]) -> str:
    """Name the valid registry ids (the profile may show only the top formats) and the closest match."""
    ids = format_ids()
    hints = []
    for u in unknown:
        close = difflib.get_close_matches(u.replace("_", " "), ids, n=1, cutoff=0.4)
        hints.append(f"{u!r} -> {close[0]!r}" if close else f"{u!r} -> ?")
    return f"unknown format id(s): {', '.join(hints)}. Valid ids: {', '.join(ids)}"


def _stats(rules: SilverRules, rows: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(rows) or 1
    columns = {}
    for c in rules.columns:
        before = sum(_blank(r["__payload"].get(c.source)) for r in rows)
        after = sum(r[c.name] is None for r in rows)
        flags = Counter(f for r in rows for f in ((r["_quality_flags"] or {}).get(c.name) or []))
        invalid = sum(
            any(e.startswith(f"{c.name}:") and not e.endswith("required_missing") for e in r["_errors"]) for r in rows
        )
        newly_null = sum(not _blank(r["__payload"].get(c.source)) and r[c.name] is None for r in rows)
        columns[c.name] = {
            "null_before": before / n, "null_after": after / n, "invalid": invalid / n,
            "newly_null_share": newly_null / max(n - before, 1),
            "sentinel_hits": sum(v for k, v in flags.items() if k.startswith("sentinel:")),
            "mapped": flags["mapped"], "unmapped": flags["unmapped"],
            "unparseable": flags["unparseable"], "unparseable_share": flags["unparseable"] / max(n - before, 1),
        }  # fmt: skip
    rows_stats = {
        "sampled": len(rows),
        "with_errors": sum(bool(r["_errors"]) for r in rows) / n,
        "rejected": sum(r["_rejected"] for r in rows),
        "dedup_removed": sum(r["_dup_rank"] > 1 for r in rows),
    }
    return {"columns": columns, "rows": rows_stats}


def _stat_warnings(rules: SilverRules, stats: dict[str, Any]) -> list[str]:
    warnings = []
    for name, s in stats.get("columns", {}).items():
        if s.get("unparseable_share", 0) >= WARN_UNPARSEABLE:
            warnings.append(f"{name}: {s['unparseable_share']:.1%} of non-empty sampled values can't be read as the "
                            f"rule's type (>= {WARN_UNPARSEABLE:.0%}); they become empty. Check parse_formats/steps")  # fmt: skip
        elif s["invalid"] > WARN_INVALID:
            warnings.append(f"{name}: {s['invalid']:.1%} of sampled values are invalid (> {WARN_INVALID:.0%})")
        if s["newly_null_share"] > WARN_NEWLY_NULL:
            warnings.append(
                f"{name}: rules null {s['newly_null_share']:.0%} of previously non-null values (> {WARN_NEWLY_NULL:.0%})"
            )
    if stats.get("rows", {}).get("with_errors", 0) > WARN_ROWS_WITH_ERRORS:
        warnings.append(
            f"rows: {stats['rows']['with_errors']:.1%} of sampled rows have errors (> {WARN_ROWS_WITH_ERRORS:.0%})"
        )
    asked = {i for q in rules.needs_human for i in q.issue_ids}
    warnings.extend(
        f"{c.name}: confidence {c.confidence} < {WARN_CONFIDENCE} without a needs_human question"
        for c in rules.columns
        if c.confidence < WARN_CONFIDENCE and not (set(c.addresses) & asked)
    )
    return warnings


# --- layer 4: coverage -------------------------------------------------------------------


def coverage_errors(rules: SilverRules, facts: RunFacts) -> list[str]:
    """Every high/medium issue id must be resolved: by a rule that cites it, or ignored with a reason."""
    required = [i for i, issue in facts.issues.items() if issue["severity"] in ("high", "medium")]
    resolved = {r.issue_id: r for r in rules.addressed_issues}
    cited = {a for c in rules.columns for a in c.addresses}
    cited |= set(rules.dedup.addresses if rules.dedup else [])
    cited |= {a for ch in rules.row_checks for a in ch.addresses}
    missing = [i for i in required if i not in resolved]
    uncited = [i for i in required if i in resolved and resolved[i].resolution == "rule" and i not in cited]
    errors = []
    if missing:
        errors.append(f"coverage: {len(missing)} high/medium issue(s) not in addressed_issues: {missing}")
    if uncited:
        errors.append(f"coverage: resolution=rule but no rule lists it in addresses: {uncited}")
    return errors


# --- entry point -------------------------------------------------------------------------


def validate_rules(rules_or_dict: SilverRules | dict[str, Any], run_id: str | uuid.UUID) -> ValidationReport:
    """Validate in four layers (schema, meaning, dry run, coverage), stopping at the first failing one."""
    run_id = str(run_id)
    rules, errors = check_schema(rules_or_dict)
    if errors or rules is None:
        return ValidationReport(False, "schema", errors)
    if rules.source_run_id != run_id:
        return ValidationReport(False, "semantic", [f"source_run_id {rules.source_run_id} != run {run_id}"])
    facts = run_facts(run_id)
    errors = _column_semantics(rules, facts) + _reference_semantics(rules, facts)
    if errors:
        return ValidationReport(False, "semantic", errors)
    warnings = _mapping_warnings(rules, run_id)
    stats, errors = dry_run(rules, run_id)
    if errors:
        return ValidationReport(False, "dry_run", errors, warnings)
    warnings += _stat_warnings(rules, stats)
    errors = coverage_errors(rules, facts)
    return ValidationReport(not errors, "coverage", errors, warnings, stats)


def summarize(report: ValidationReport) -> str:
    """Human-readable one-liner per column (for CLI/README)."""
    return re.sub(r"\n{2,}", "\n", report.feedback())
