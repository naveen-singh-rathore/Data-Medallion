"""Read-only tools for the data-quality agent. Each takes the run id plus a Pydantic args
model, returns JSON-serialisable data, is deterministic, and masks every data value.
SQL runs as pipeline_ro in a read-only transaction (5 s timeout)."""

from collections import Counter
from typing import Any, Literal

from psycopg import sql
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field

from pipeline.agent_tools.context import RunView, ToolError
from pipeline.agent_tools.masking import mask_value
from pipeline.card import short_evidence
from pipeline.db import get_ro_conn
from pipeline.privacy import redact_issue, redact_profile
from pipeline.profile_dates import _day_month_evidence, format_by_id
from pipeline.silver.compiler import compile_rules
from pipeline.silver.rules_schema import ColumnRule, SilverRules
from pipeline.silver.validator import SAMPLE_ROWS, _column_semantics, run_facts

OK_ROWS = "run_id = %(run_id)s AND parse_status = 'ok'"


class Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- list_issues / get_column_profile -------------------------------------------------


class ListIssuesArgs(Args):
    severity: Literal["high", "medium", "low"] | None = None
    column: str | None = None


def list_issues(view: RunView, args: ListIssuesArgs) -> dict[str, Any]:
    """Issues with their ids (cite these in rule `addresses`), filtered by severity/column."""
    found = [i for i in view.issues if (args.severity is None or i["severity"] == args.severity)
             and (args.column is None or args.column in (i["column"] or "").split(" > "))]  # fmt: skip
    return {"total": len(found), "issues": [
        {"id": i["id"], "severity": i["severity"], "evidence": short_evidence(redact_issue(i, view.policies)["evidence"])}
        for i in found
    ]}  # fmt: skip


class ColumnArgs(Args):
    column: str


def get_column_profile(view: RunView, args: ColumnArgs) -> dict[str, Any]:
    """The full profiler facts for one column (values masked per privacy policy)."""
    return redact_profile(view.profile(args.column), view.policies) | {"value_policy": view.policies[args.column]}


# --- value counts / samples / regex ------------------------------------------------------


class ValueCountsArgs(Args):
    column: str
    limit: int = Field(default=20, ge=1, le=50)
    pattern_mask: str | None = Field(default=None, max_length=40, description="only values with this mask, e.g. A-9999")
    normalized: bool = Field(default=False, description="group by the normalization key (case/accents/separators)")


def get_value_counts(view: RunView, args: ValueCountsArgs) -> dict[str, Any]:
    """Most frequent values (masked columns are grouped by pattern instead of value)."""
    profile = view.profile(args.column)
    value = sql.SQL("(payload ->> {})").format(sql.Literal(args.column))
    raw = view.policies[args.column] == "raw"
    group = (sql.SQL("silver.norm_key({})") if args.normalized else sql.SQL("{}")).format(value) if raw else \
        sql.SQL("silver.pattern_mask(btrim({}))").format(value)  # fmt: skip
    where = sql.SQL(OK_ROWS)
    if args.pattern_mask:
        where = sql.SQL("{} AND silver.pattern_mask(btrim({})) = {}").format(
            where, value, sql.Literal(args.pattern_mask)
        )
    query = sql.SQL("SELECT {g} AS v, count(*) AS n, min(length({v})) AS lo, max(length({v})) AS hi FROM bronze.records"
                    " WHERE {w} GROUP BY 1 ORDER BY 2 DESC, 1 NULLS FIRST LIMIT {lim}").format(
        g=group, v=value, w=where, lim=sql.Literal(args.limit))  # fmt: skip
    with get_ro_conn() as conn:
        rows = conn.execute(query, {"run_id": view.run_id}).fetchall()
    if raw:
        values = [[mask_value(args.column, v, view.tags, profile), n] for v, n, _, _ in rows]
    else:
        values = [[f"<{v}>({lo}-{hi})" if v else v, n] for v, n, lo, hi in rows]
    return {"column": args.column, "grouped_by": "value" if raw and not args.normalized else
            "normalization_key" if raw else "pattern", "values": values}  # fmt: skip


class SampleRowsArgs(Args):
    column: str | None = None
    pattern_mask: str | None = Field(default=None, max_length=40)
    n: int = Field(default=5, ge=1, le=10)


def sample_rows(view: RunView, args: SampleRowsArgs) -> dict[str, Any]:
    """Deterministic sample rows (by row hash), optionally only rows where `column` has `pattern_mask`."""
    where = sql.SQL(OK_ROWS)
    if args.column:
        view.profile(args.column)
        if args.pattern_mask:
            where = sql.SQL("{} AND silver.pattern_mask(btrim(payload ->> {})) = {}").format(
                where, sql.Literal(args.column), sql.Literal(args.pattern_mask))  # fmt: skip
    query = sql.SQL("SELECT source_line, payload FROM bronze.records WHERE {} ORDER BY md5(row_hash), bronze_id LIMIT {}").format(
        where, sql.Literal(args.n))  # fmt: skip
    with get_ro_conn() as conn:
        rows = conn.execute(query, {"run_id": view.run_id}).fetchall()
    return {"rows": [{"line": line, **{c: mask_value(c, payload.get(c), view.tags, p) for c, p in view.profiles.items()}}
                     for line, payload in rows]}  # fmt: skip


class RegexArgs(Args):
    column: str
    pattern: str = Field(max_length=200, description="Postgres regular expression (~), matched against trimmed values")


def check_regex(view: RunView, args: RegexArgs) -> dict[str, Any]:
    """Share of non-blank values matching `pattern`, plus 5 non-matching examples (masked)."""
    profile = view.profile(args.column)
    value = sql.SQL("btrim(payload ->> {})").format(sql.Literal(args.column))
    base = sql.SQL("FROM bronze.records WHERE {} AND coalesce({}, '') <> ''").format(sql.SQL(OK_ROWS), value)
    try:
        with get_ro_conn() as conn:
            total, matched = conn.execute(
                sql.SQL("SELECT count(*), count(*) FILTER (WHERE {v} ~ {p}) {b}").format(
                    v=value, p=sql.Literal(args.pattern), b=base
                ),
                {"run_id": view.run_id},
            ).fetchone()  # type: ignore[misc]
            misses = conn.execute(sql.SQL("SELECT {v}, count(*) {b} AND NOT ({v} ~ {p}) GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 200").format(
                v=value, p=sql.Literal(args.pattern), b=base), {"run_id": view.run_id}).fetchall()  # fmt: skip
    except Exception as exc:
        raise ToolError(f"regex failed: {str(exc).splitlines()[0][:150]}") from exc
    return {"column": args.column, "non_blank": total, "match_share": round(matched / total, 4) if total else 0.0,
            "non_matching": _merge_masked([(mask_value(args.column, v, view.tags, profile), n) for v, n in misses])}  # fmt: skip


def _merge_masked(pairs: list[tuple[Any, int]], limit: int = 5) -> list[list[Any]]:
    """Masking can make different values look the same: add their counts up."""
    merged: Counter[Any] = Counter()
    for value, n in pairs:
        merged[value] += n
    return [[v, n] for v, n in sorted(merged.items(), key=lambda kv: (-kv[1], str(kv[0])))[:limit]]


# --- test_rule ---------------------------------------------------------------------------


class TestRuleArgs(Args):
    column_rule: ColumnRule


def _one_rule(view: RunView, rule: ColumnRule) -> SilverRules:
    rules = SilverRules(source_run_id=view.run_id, generated_by="agent", columns=[rule])
    errors = _column_semantics(rules, run_facts(view.run_id))
    if errors:
        raise ToolError("; ".join(errors))
    return rules


def test_rule(view: RunView, args: TestRuleArgs) -> dict[str, Any]:
    """Compile ONE column rule and run it on the fixed 2,000-row sample: before/after, null and
    invalid shares, and up to 10 failing raw values."""
    rule = args.column_rule
    profile = view.profile(rule.source)
    compiled = compile_rules(_one_rule(view, rule), view.run_id, sample=SAMPLE_ROWS)
    query = sql.SQL("SELECT c.*, b.payload ->> {} AS __raw FROM ({}) c JOIN bronze.records b ON b.bronze_id = c.bronze_id").format(
        sql.Literal(rule.source), compiled.query)  # fmt: skip
    with get_ro_conn() as conn:
        rows = conn.cursor(row_factory=dict_row).execute(query, compiled.params).fetchall()
    n = len(rows) or 1

    def shown(v: Any) -> Any:
        return mask_value(rule.source, v if v is None or isinstance(v, str) else str(v), view.tags, profile)

    blank = [r["__raw"] is None or not r["__raw"].strip() for r in rows]
    flags = Counter(f for r in rows for f in ((r["_quality_flags"] or {}).get(rule.name) or []))
    failing = Counter(
        (r["__raw"], e.split(":", 1)[1]) for r in rows for e in r["_errors"] if e.startswith(f"{rule.name}:")
    )
    return {
        "sampled": len(rows),
        "before_top": [[shown(v), c] for v, c in Counter(r["__raw"] for r in rows).most_common(5)],
        "after_top": [[shown(v), c] for v, c in Counter(r[rule.name] for r in rows).most_common(5)],
        "null_before": round(sum(blank) / n, 4),
        "null_after": round(sum(r[rule.name] is None for r in rows) / n, 4),
        "invalid": round(sum(c for (_, reason), c in failing.items() if reason != "required_missing") / n, 4),
        "flags": dict(sorted(flags.items())),
        "failing": [[shown(v), reason, c] for (v, reason), c in sorted(failing.items(), key=lambda kv: (-kv[1], str(kv[0])))[:10]],
    }  # fmt: skip


# --- check_cross_column ------------------------------------------------------------------


class CrossColumnArgs(Args):
    kind: Literal["date_order"] = "date_order"
    a: str
    b: str
    a_formats: list[str] = Field(min_length=1)
    b_formats: list[str] = Field(min_length=1)
    day_month_order: Literal["dmy", "mdy"] | None = None


def _is_ambiguous(raw: str | None, format_ids: list[str]) -> bool:
    if not raw:
        return False
    for format_id in format_ids:
        fmt = format_by_id(format_id)
        if fmt and fmt.regex.fullmatch(raw.strip()):
            return _day_month_evidence(fmt, raw.strip()) == "ambiguous"
    return False


def check_cross_column(view: RunView, args: CrossColumnArgs) -> dict[str, Any]:
    """Rows where a > b with the given parse formats: share over all compared rows and over rows
    where neither value depends on a day/month guess, plus 5 examples (masked)."""
    why = "Cross-column check only; not a rule that will be applied to silver."
    rules = [ColumnRule(name=f"x_{i}", source=col, type="timestamp", steps=["trim"], parse_formats=fmts,
                        day_month_order=args.day_month_order, confidence=1.0, why=why)
             for i, (col, fmts) in enumerate([(args.a, args.a_formats), (args.b, args.b_formats)])]  # fmt: skip
    for r in rules:
        view.profile(r.source)
        _one_rule(view, r)
    compiled = compile_rules(SilverRules(source_run_id=view.run_id, generated_by="agent", columns=rules), view.run_id)
    query = sql.SQL(
        "SELECT b.source_line, c.x_0, c.x_1, c.x_0_raw, c.x_1_raw FROM ({}) c"
        " JOIN bronze.records b ON b.bronze_id = c.bronze_id ORDER BY b.source_line"
    ).format(compiled.query)
    with get_ro_conn() as conn:
        rows = conn.execute(query, compiled.params).fetchall()
    compared = [r for r in rows if r[1] and r[2]]
    later = [r for r in compared if r[1] > r[2]]
    certain = [
        r for r in compared if not _is_ambiguous(r[3], args.a_formats) and not _is_ambiguous(r[4], args.b_formats)
    ]
    certain_later = [r for r in certain if r[1] > r[2]]
    pa, pb = view.profile(args.a), view.profile(args.b)
    return {
        "compared": len(compared), "violations": len(later),
        "share": round(len(later) / len(compared), 4) if compared else 0.0,
        "unambiguous_compared": len(certain),
        "unambiguous_share": round(len(certain_later) / len(certain), 4) if certain else None,
        "examples": [{"line": line, "a": mask_value(args.a, ra, view.tags, pa), "b": mask_value(args.b, rb, view.tags, pb),
                      "gap_days": round((a - b).total_seconds() / 86400, 1)} for line, a, b, ra, rb in later[:5]],
    }  # fmt: skip
