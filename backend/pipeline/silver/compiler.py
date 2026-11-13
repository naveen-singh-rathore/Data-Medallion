"""Rules -> SQL. The only place silver SQL is written.

Everything that comes from a rule (names, sources, tokens, formats, ranges) enters the query
through psycopg.sql.Identifier / Literal - never string formatting. The fixed SQL around it
only uses the safe helpers in db/sql/007_silver_functions.sql, so a bad value becomes NULL plus
an error entry, never an exception.

Query shape (one SELECT over bronze.records, nested layers):
  s1 clean text (steps, blanks -> NULL) -> s2 null tokens / sentinels -> s3 mapping ->
  s4 typed values + per-value problems -> s5 row checks -> final: values, <col>_raw,
  _quality_flags (per column: fixes applied), _errors, _rejected, _dup_rank.
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg import sql

from pipeline.config import REPO_ROOT
from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profile_dates import format_by_id, pg_pattern
from pipeline.profile_values import normalization_key
from pipeline.silver.rules_schema import (
    ROW_CHECK_COLUMNS,
    ColumnRule,
    RowCheck,
    SilverRules,
    rules_hash,
)

COMPILED_DIR = REPO_ROOT / "build" / "compiled"
OUTPUT_COLUMNS = frozenset(
    {"bronze_id", "row_hash", "_quality_flags", "_errors", "_rejected", "_dup_rank", "_duplicate_of"}
)
LONG_TEXT_CHARS, LONG_TEXT_WORDS = 40, 4  # longer than this in a label column = text that leaked in (swapped_columns)
FLAG_CHUNK = 40  # jsonb_build_object takes at most 100 arguments

_CURRENCY = f"[{CFG.currency_symbols}]|\\m({'|'.join(CFG.currency_codes)})\\M"
STEP_SQL: dict[str, tuple[str, str]] = {  # (prefix, suffix) around the inner expression
    "trim": ("regexp_replace(", ", '^[[:space:]\\u00a0]+|[[:space:]\\u00a0]+$', '', 'g')"),
    "collapse_spaces": ("regexp_replace(", ", '[[:space:]\\u00a0]+', ' ', 'g')"),
    "remove_hidden_chars": (
        "regexp_replace(",
        ", '[\\u200b-\\u200d\\u2060\\ufeff\\u0001-\\u0008\\u000b\\u000c\\u000e-\\u001f\\u007f]', '', 'g')",
    ),
    "lower": ("lower(", ")"),
    "upper": ("upper(", ")"),
    "title": ("initcap(", ")"),
    "strip_currency": ("btrim(regexp_replace(", f", '{_CURRENCY}', '', 'gi'))"),
    "strip_thousands": ("silver.strip_thousands(", ")"),
    "strip_percent": ("regexp_replace(", ", '[[:space:]]*%[[:space:]]*$', '')"),
    "strip_units": ("regexp_replace(", ", '^([+-]?[0-9.,]+)[[:space:]]*[[:alpha:]]+\\.?$', '\\1')"),
    "european_decimal": ("silver.european_decimal(", ")"),
}


@dataclass(frozen=True)
class CompiledSQL:
    """The compiled silver query, its rendered text, parameters and the columns it produces."""

    query: sql.Composed
    text: str  # rendered SQL (parameters stay as %(run_id)s / %(sample)s)
    params: dict[str, Any]
    rules_hash: str
    columns: list[str]
    raw_columns: list[str]


def _id(name: str, suffix: str = "") -> sql.Identifier:
    return sql.Identifier(f"{name}{suffix}")


def _text_array(values: list[str]) -> sql.Composable:
    if not values:
        return sql.SQL("ARRAY[]::text[]")
    return sql.SQL("ARRAY[{}]::text[]").format(sql.SQL(", ").join(sql.Literal(v) for v in values))


def has_raw_column(rule: ColumnRule) -> bool:
    """<col>_raw is kept when the value can change type or be nulled by a sentinel/mapping."""
    return rule.type != "text" or bool(rule.sentinels) or bool(rule.mapping)


# --- per-column expressions ----------------------------------------------------------


def clean_expr(rule: ColumnRule, raw: sql.Composable | None = None) -> sql.Composable:
    """SQL for the column's cleaning steps applied to its raw (or swapped) value."""
    expr: sql.Composable = raw if raw is not None else sql.SQL("(payload ->> {})").format(sql.Literal(rule.source))
    for step in rule.steps:
        prefix, suffix = STEP_SQL[step]
        expr = sql.Composed([sql.SQL(prefix), expr, sql.SQL(suffix)])
    return sql.SQL("silver.blank_to_null({})").format(expr)


def sentinel_expr(rule: ColumnRule, clean: sql.Composable) -> sql.Composable:
    """SQL that is true when the cleaned value is one of the rule's sentinels."""
    if not rule.sentinels:
        return sql.SQL("false")
    text_hit = sql.SQL("{} = ANY({})").format(clean, _text_array(rule.sentinels))
    numbers = [s for s in rule.sentinels if _is_number(s)]
    if rule.type in ("integer", "numeric") and numbers:
        number_hit = sql.SQL("silver.try_numeric({}) = ANY(ARRAY[{}]::numeric[])").format(
            clean, sql.SQL(", ").join(sql.Literal(float(n)) for n in numbers)
        )
        return sql.SQL("coalesce({} OR {}, false)").format(text_hit, number_hit)
    return sql.SQL("coalesce({}, false)").format(text_hit)


def _is_number(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def mapping_keys(rule: ColumnRule) -> dict[str, str]:
    """normalization key -> target value (each target also maps to itself)."""
    keys: dict[str, str] = {}
    for target, variants in (rule.mapping or {}).items():
        for variant in [target, *variants]:
            keys.setdefault(normalization_key(variant), target)
    return keys


def mapped_expr(rule: ColumnRule, value: sql.Composable) -> tuple[sql.Composable, sql.Composable]:
    """(mapped value, is-unmapped flag)."""
    if not rule.mapping:
        return value, sql.SQL("false")
    keys = mapping_keys(rule)
    key = sql.SQL("silver.norm_key({})").format(value)
    whens = sql.SQL(" ").join(
        sql.SQL("WHEN {} THEN {}").format(sql.Literal(k), sql.Literal(t)) for k, t in keys.items()
    )
    unmapped = {"keep": value, "null": sql.SQL("NULL"), "invalid": sql.SQL("NULL")}[rule.unmapped_action]
    mapped = sql.SQL("CASE {} {} ELSE {} END").format(key, whens, unmapped)
    is_unmapped = sql.SQL("({} IS NOT NULL AND NOT coalesce({} = ANY({}), false))").format(
        value, key, _text_array(list(keys))
    )
    return mapped, is_unmapped


def typed_expr(rule: ColumnRule, value: sql.Composable) -> sql.Composable:
    """SQL that casts the value to the rule's type (NULL when it cannot be read)."""
    if rule.type == "text":
        return value
    if rule.type == "integer":
        return sql.SQL("silver.try_int({})").format(value)
    if rule.type == "numeric":
        return sql.SQL("silver.try_numeric({})").format(value)
    if rule.type == "boolean":
        return sql.SQL("silver.try_bool({}, {}, {})").format(
            value, _text_array(rule.boolean_true), _text_array(rule.boolean_false)
        )
    attempts = []
    for format_id in rule.parse_formats:
        fmt = format_by_id(format_id)
        if fmt is None:  # the validator rejects unknown ids; the compiler just refuses
            raise ValueError(f"unknown date format id {format_id!r}")
        pattern = pg_pattern(fmt, day_first=rule.day_month_order == "dmy")
        attempts.append(
            sql.SQL("silver.try_ts({}, {}, {})").format(value, sql.Literal(fmt.regex.pattern), sql.Literal(pattern))
        )
    parsed = sql.SQL("coalesce({})").format(sql.SQL(", ").join(attempts)) if attempts else sql.SQL("NULL::timestamp")
    return sql.SQL("({})::date").format(parsed) if rule.type == "date" else parsed


def range_expr(rule: ColumnRule, typed: sql.Composable) -> sql.Composable:
    """SQL that is true when the typed value is outside the rule's range."""
    if not rule.range or (rule.range.min is None and rule.range.max is None):
        return sql.SQL("false")
    cast = {"date": "::date", "timestamp": "::timestamp"}.get(rule.type, "::numeric")
    parts = []
    if rule.range.min is not None:
        parts.append(sql.SQL("{} < {}" + cast).format(typed, sql.Literal(rule.range.min)))
    if rule.range.max is not None:
        parts.append(sql.SQL("{} > {}" + cast).format(typed, sql.Literal(rule.range.max)))
    return sql.SQL("coalesce({}, false)").format(sql.SQL(" OR ").join(parts))


# --- row checks ----------------------------------------------------------------------


def _raw(source: str) -> sql.Composable:
    return sql.SQL("(payload ->> {})").format(sql.Literal(source))


def swap_condition(a: ColumnRule, b: ColumnRule) -> sql.Composable:
    """a (a label column) holds long text AND b (a text column) holds one of a's known values."""
    ra, rb = _raw(a.source), _raw(b.source)
    long_text = sql.SQL("(length(btrim({ra})) > {c} OR coalesce(array_length(regexp_split_to_array(btrim({ra}), '[[:space:]]+'), 1), 0) > {w})").format(
        ra=ra, c=sql.Literal(LONG_TEXT_CHARS), w=sql.Literal(LONG_TEXT_WORDS))  # fmt: skip
    return sql.SQL("coalesce({} AND silver.norm_key(btrim({})) = ANY({}), false)").format(
        long_text, rb, _text_array(sorted(mapping_keys(a)))
    )


def junk_failure(check: RowCheck, rules: SilverRules) -> sql.Composable:
    """>= min_empty_share of the row's cells empty or placeholders, or the id fails the dominant pattern."""
    p = check.params
    tokens = sorted(
        {t.lower() for t in CFG.null_like_tokens} | {t.lower() for c in rules.columns for t in c.null_tokens}
    )
    cells = [sql.SQL("CASE WHEN silver.blank_to_null(btrim({r})) IS NULL OR lower(btrim({r})) = ANY({t})"
                     " OR btrim({r}) ~ '^[[:punct:]]+$' THEN 1 ELSE 0 END").format(r=_raw(c.source), t=_text_array(tokens))
             for c in rules.columns]  # fmt: skip
    share = sql.SQL("(({})::numeric / {} >= {})").format(sql.SQL(" + ").join(cells), sql.Literal(len(cells)),
                                                          sql.Literal(float(str(p["min_empty_share"]))))  # fmt: skip
    bad_id = sql.SQL("({id} IS NOT NULL AND NOT {id} ~ {pat})").format(
        id=sql.Identifier(str(p["id"])), pat=sql.Literal(str(p["id_pattern"]))
    )
    return sql.SQL("coalesce({} OR {}, false)").format(share, bad_id)


def duplicate_of_expr(check: RowCheck) -> sql.Composable:
    """SQL for the ticket id a row says it duplicates (prefix + captured number)."""
    p = check.params
    return sql.SQL("({} || substring({} FROM {}))").format(sql.Literal(str(p["prefix"])), sql.Identifier(str(p["source"])),
                                                         sql.Literal(str(p["pattern"])))  # fmt: skip


def row_check_failure(check: RowCheck, rules: SilverRules | None = None) -> sql.Composable:
    """SQL that is true when a row check fires for the row."""
    p = check.params
    if check.kind == "swapped_columns":
        by_name = {c.name: c for c in rules.columns}  # type: ignore[union-attr]
        return swap_condition(by_name[str(p["a"])], by_name[str(p["b"])])
    if check.kind == "junk_row":
        return junk_failure(check, rules)  # type: ignore[arg-type]
    if check.kind == "declared_duplicate":
        return sql.SQL("({} IS NOT NULL)").format(duplicate_of_expr(check))
    if check.kind == "date_order":
        return sql.SQL("coalesce({} > {}, false)").format(sql.Identifier(p["a"]), sql.Identifier(p["b"]))
    if check.kind == "not_both_null":
        return sql.SQL("({} IS NULL AND {} IS NULL)").format(sql.Identifier(p["a"]), sql.Identifier(p["b"]))
    values = p["values"] if isinstance(p["values"], list) else [p["values"]]
    return sql.SQL("({} IS NULL AND {}::text = ANY({}))").format(
        sql.Identifier(p["column"]), sql.Identifier(p["other"]), _text_array(values)
    )


def _check_label(check: RowCheck) -> str:
    names = [check.params[k] for k in ROW_CHECK_COLUMNS[check.kind]]
    return f"row:{check.kind}:{','.join(str(n) for n in names)}"


# --- assembly ------------------------------------------------------------------------


def _source_filter(sample: int | None) -> sql.Composable:
    base = sql.SQL("run_id = %(run_id)s AND parse_status = 'ok'")
    if sample is None:
        return base
    return sql.SQL(
        "{} AND bronze_id IN (SELECT bronze_id FROM bronze.records WHERE run_id = %(run_id)s"
        " AND parse_status = 'ok' ORDER BY md5(row_hash), bronze_id LIMIT %(sample)s)"
    ).format(base)


@dataclass(frozen=True)
class _RowCheckPlan:
    """What the row checks change per column, derived once from the rules."""

    raw_of: dict[str, sql.Composable]  # swapped_columns: the raw value a column reads after the swap
    swapped_in: dict[str, list[tuple[int, str]]]  # column -> (check index, flag label)
    nulled_by: dict[str, list[int]]  # null_second: column -> check indexes that null it
    declared: list[tuple[int, RowCheck]]  # declared_duplicate checks


def _plan_row_checks(rules: SilverRules) -> _RowCheckPlan:
    by_name = {c.name: c for c in rules.columns}
    raw_of: dict[str, sql.Composable] = {}
    swapped_in: dict[str, list[tuple[int, str]]] = {}
    for i, ch in enumerate(rules.row_checks):
        if ch.kind == "swapped_columns":  # exchange raw values before any cleaning
            a, b = by_name[str(ch.params["a"])], by_name[str(ch.params["b"])]
            cond = swap_condition(a, b)
            raw_of[a.name] = sql.SQL("CASE WHEN {} THEN {} ELSE {} END").format(cond, _raw(b.source), _raw(a.source))
            raw_of[b.name] = sql.SQL("CASE WHEN {} THEN {} ELSE {} END").format(cond, _raw(a.source), _raw(b.source))
            for col in (a.name, b.name):
                swapped_in.setdefault(col, []).append((i, f"swapped:{a.name},{b.name}"))
    nulled_by: dict[str, list[int]] = {c.params["b"]: [] for c in rules.row_checks if c.action == "null_second"}
    for i, check in enumerate(rules.row_checks):
        if check.action == "null_second":
            nulled_by[check.params["b"]].append(i)
    declared = [(i, ch) for i, ch in enumerate(rules.row_checks) if ch.kind == "declared_duplicate"]
    return _RowCheckPlan(raw_of, swapped_in, nulled_by, declared)


# --- the query layers (each SELECTs *, plus its own columns, from the previous one) --------------


def _clean_layer(cols: list[ColumnRule], plan: _RowCheckPlan) -> list[sql.Composable]:
    """s1: cleaning steps on the raw text."""
    return [sql.SQL("{} AS {}").format(clean_expr(c, plan.raw_of.get(c.name)), _id(c.name, "__c")) for c in cols]


def _token_layer(cols: list[ColumnRule]) -> list[sql.Composable]:
    """s2: null-token and sentinel matches."""
    out: list[sql.Composable] = []
    for c in cols:
        clean = _id(c.name, "__c")
        token = (sql.SQL("coalesce(lower({}) = ANY({}), false)").format(clean, _text_array([t.lower() for t in c.null_tokens]))
                 if c.null_tokens else sql.SQL("false"))  # fmt: skip
        out += [sql.SQL("{} AS {}").format(token, _id(c.name, "__tok")),
                sql.SQL("{} AS {}").format(sentinel_expr(c, clean), _id(c.name, "__sen"))]  # fmt: skip
    return out


def _mapping_layer(cols: list[ColumnRule]) -> list[sql.Composable]:
    """s3: the value after nulling, its standard value, and whether it was left unmapped."""
    out: list[sql.Composable] = []
    for c in cols:
        value = sql.SQL("CASE WHEN {} OR {} THEN NULL ELSE {} END").format(
            _id(c.name, "__tok"), _id(c.name, "__sen"), _id(c.name, "__c")
        )
        mapped, unmapped = mapped_expr(c, value)
        out += [sql.SQL("{} AS {}").format(value, _id(c.name, "__v")), sql.SQL("{} AS {}").format(mapped, _id(c.name, "__m")),
                sql.SQL("{} AS {}").format(unmapped, _id(c.name, "__unm"))]  # fmt: skip
    return out


def _typed_layer(cols: list[ColumnRule]) -> list[sql.Composable]:
    """s4: the typed value, whether casting failed, whether it is out of range."""
    out: list[sql.Composable] = []
    for c in cols:
        typed = typed_expr(c, _id(c.name, "__m"))
        out += [sql.SQL("{} AS {}").format(typed, _id(c.name, "__t")),
                sql.SQL("({} IS NOT NULL AND {} IS NULL) AS {}").format(_id(c.name, "__m"), typed, _id(c.name, "__bad")),
                sql.SQL("{} AS {}").format(range_expr(c, typed), _id(c.name, "__oor"))]  # fmt: skip
    return out


def _range_layer(cols: list[ColumnRule]) -> list[sql.Composable]:
    """s5: the final column value (out-of-range values become NULL)."""
    return [sql.SQL("CASE WHEN {} THEN NULL ELSE {} END AS {}").format(_id(c.name, "__oor"), _id(c.name, "__t"), _id(c.name))
            for c in cols]  # fmt: skip


def _check_layer(rules: SilverRules) -> list[sql.Composable]:
    """s6: one boolean per row check (true = the check fires)."""
    return [
        sql.SQL("{} AS {}").format(row_check_failure(ch, rules), _id(f"_chk{i}"))
        for i, ch in enumerate(rules.row_checks)
    ]


def _layer(select: list[sql.Composable], inner: sql.Composable, alias: str) -> sql.Composable:
    items = sql.SQL(", ").join([sql.SQL("*"), *select]) if select else sql.SQL("*")
    return sql.SQL("SELECT {} FROM ({}) AS {}").format(items, inner, sql.Identifier(alias))


# --- the output columns ---------------------------------------------------------------------------


def _final_value(c: ColumnRule, plan: _RowCheckPlan) -> sql.Composable:
    drops = plan.nulled_by.get(c.name, [])
    if not drops:
        return _id(c.name)
    cond = sql.SQL(" OR ").join(_id(f"_chk{i}") for i in drops)
    return sql.SQL("CASE WHEN {} THEN NULL ELSE {} END AS {}").format(cond, _id(c.name), _id(c.name))


def _column_flags(c: ColumnRule, plan: _RowCheckPlan) -> sql.Composable:
    """silver.flags(...) for one column: every fix applied to its value, as labels."""
    nulled = [f"_chk{i}" for i in plan.nulled_by.get(c.name, [])]
    items = [
        sql.SQL("CASE WHEN {} IS DISTINCT FROM (payload ->> {}) AND {} IS NOT NULL THEN 'cleaned' END").format(
            _id(c.name, "__c"), sql.Literal(c.source), _id(c.name, "__c")),
        sql.SQL("CASE WHEN {} THEN 'null_token' END").format(_id(c.name, "__tok")),
        sql.SQL("CASE WHEN {} THEN 'sentinel:' || {} END").format(_id(c.name, "__sen"), _id(c.name, "__c")),
        sql.SQL("CASE WHEN {} IS DISTINCT FROM {} AND {} IS NOT NULL THEN 'mapped' END").format(
            _id(c.name, "__m"), _id(c.name, "__v"), _id(c.name, "__m")),
        sql.SQL("CASE WHEN {} THEN 'unmapped' END").format(_id(c.name, "__unm")),
        sql.SQL("CASE WHEN {} THEN 'unparseable' END").format(_id(c.name, "__bad")),
        sql.SQL("CASE WHEN {} THEN 'out_of_range' END").format(_id(c.name, "__oor")),
        *[sql.SQL("CASE WHEN {} THEN 'nulled_by_row_check' END").format(_id(n)) for n in nulled],
        *[sql.SQL("CASE WHEN {} THEN {} END").format(_id(f"_chk{i}"), sql.Literal(label)) for i, label in plan.swapped_in.get(c.name, [])],
        *[sql.SQL("CASE WHEN {} THEN 'declared_duplicate' END").format(_id(f"_chk{i}"))
          for i, ch in plan.declared if ch.params["source"] == c.name],
    ]  # fmt: skip
    return sql.SQL("silver.flags({})").format(sql.SQL(", ").join(items))


def _quality_flags(cols: list[ColumnRule], plan: _RowCheckPlan) -> sql.Composable:
    """{column: [labels]} as jsonb; built in chunks (jsonb_build_object has an argument limit)."""
    chunks = [cols[i : i + FLAG_CHUNK] for i in range(0, len(cols), FLAG_CHUNK)]
    return sql.SQL(" || ").join(
        sql.SQL("jsonb_build_object({})").format(
            sql.SQL(", ").join(sql.SQL("{}, {}").format(sql.Literal(c.name), _column_flags(c, plan)) for c in chunk)
        )
        for chunk in chunks
    )


def _column_errors(c: ColumnRule) -> list[tuple[sql.Composable, bool]]:
    """(error expression, rejects row?) for one column."""
    reject = c.invalid_action == "reject"
    found = [
        (sql.SQL("CASE WHEN {} THEN {} END").format(_id(c.name, "__bad"), sql.Literal(f"{c.name}:unparseable")), reject),
        (sql.SQL("CASE WHEN {} THEN {} END").format(_id(c.name, "__oor"), sql.Literal(f"{c.name}:out_of_range")), reject),
    ]  # fmt: skip
    if c.mapping and c.unmapped_action == "invalid":
        found.append((sql.SQL("CASE WHEN {} THEN {} END").format(_id(c.name, "__unm"), sql.Literal(f"{c.name}:unmapped")), reject))  # fmt: skip
    if c.required:
        found.append((sql.SQL("CASE WHEN {} IS NULL THEN {} END").format(_id(c.name), sql.Literal(f"{c.name}:required_missing")),
                      reject))  # fmt: skip
    return found


def _errors_and_rejection(rules: SilverRules) -> tuple[sql.Composable, sql.Composable]:
    """(_errors text[], _rejected bool). Swaps and duplicates are fixes, not errors."""
    errors = [e for c in rules.columns for e in _column_errors(c)]
    errors += [
        (sql.SQL("CASE WHEN {} THEN {} END").format(_id(f"_chk{i}"), sql.Literal(_check_label(ch))), ch.action == "reject")
        for i, ch in enumerate(rules.row_checks)
        if ch.action not in ("swap", "duplicate")
    ]  # fmt: skip
    error_array = sql.SQL("array_remove(ARRAY[{}]::text[], NULL)").format(
        sql.SQL(", ").join(e for e, _ in errors)) if errors else sql.SQL("ARRAY[]::text[]")  # fmt: skip
    rejecting = [e for e, rejects in errors if rejects]
    rejected = (sql.SQL("(array_length(array_remove(ARRAY[{}]::text[], NULL), 1) IS NOT NULL)").format(
        sql.SQL(", ").join(rejecting)) if rejecting else sql.SQL("false"))  # fmt: skip
    return error_array, rejected


def _duplicate_of(plan: _RowCheckPlan) -> sql.Composable:
    """The ticket a row says it duplicates (declared_duplicate checks), else NULL."""
    if not plan.declared:
        return sql.SQL("NULL::text")
    return sql.SQL("coalesce({})").format(sql.SQL(", ").join(
        sql.SQL("CASE WHEN {} THEN {} END").format(_id(f"_chk{i}"), duplicate_of_expr(ch)) for i, ch in plan.declared))  # fmt: skip


def _dup_rank(rules: SilverRules) -> sql.Composable:
    """row_number() within the dedup key (1 = the row kept); constant 1 without a dedup rule."""
    dedup = rules.dedup
    if not dedup:
        return sql.SQL("1")
    order = (sql.SQL("{} DESC NULLS LAST, bronze_id").format(sql.Identifier(dedup.order_by))
             if dedup.strategy == "keep_latest" and dedup.order_by else sql.SQL("bronze_id"))  # fmt: skip
    return sql.SQL("row_number() OVER (PARTITION BY {} ORDER BY {})").format(
        sql.SQL(", ").join(sql.Identifier(k) for k in dedup.key), order)  # fmt: skip


def compile_rules(rules: SilverRules, run_id: str, sample: int | None = None) -> CompiledSQL:
    """Pure: rules -> SELECT over the run's ok bronze rows (optionally a fixed hash sample).

    The query is a stack of layers, s1 (cleaning) to s6 (row checks); s7 picks the output columns
    with lineage (<col>_raw, _quality_flags, _errors, _rejected, _duplicate_of) and s8 ranks duplicates.
    """
    cols = rules.columns
    plan = _plan_row_checks(rules)
    base = sql.SQL("SELECT bronze_id, row_hash, payload FROM bronze.records WHERE {}").format(_source_filter(sample))
    inner = base
    for alias, select in [("s1", _clean_layer(cols, plan)), ("s2", _token_layer(cols)), ("s3", _mapping_layer(cols)),
                          ("s4", _typed_layer(cols)), ("s5", _range_layer(cols)), ("s6", _check_layer(rules))]:  # fmt: skip
        inner = _layer(select, inner, alias)

    raw_cols = [c for c in cols if has_raw_column(c) or c.name in plan.swapped_in]
    raw_select = [
        sql.SQL("(payload ->> {}) AS {}").format(sql.Literal(c.source), _id(c.name, "_raw")) for c in raw_cols
    ]
    error_array, rejected = _errors_and_rejection(rules)
    final = sql.SQL(", ").join([
        sql.SQL("bronze_id"), sql.SQL("row_hash"), *(_final_value(c, plan) for c in cols), *raw_select,
        sql.SQL("jsonb_strip_nulls({}) AS _quality_flags").format(_quality_flags(cols, plan)),
        sql.SQL("{} AS _errors").format(error_array),
        sql.SQL("{} AS _rejected").format(rejected), sql.SQL("{} AS _duplicate_of").format(_duplicate_of(plan)),
    ])  # fmt: skip
    ranked = sql.SQL("SELECT {} FROM ({}) AS s7").format(final, inner)
    query = sql.SQL("SELECT *, {} AS _dup_rank FROM ({}) AS s8").format(_dup_rank(rules), ranked)
    params: dict[str, Any] = {"run_id": run_id} | ({"sample": sample} if sample is not None else {})
    return CompiledSQL(query=query, text=query.as_string(None), params=params, rules_hash=rules_hash(rules),
                       columns=[c.name for c in cols], raw_columns=[f"{c.name}_raw" for c in raw_cols])  # fmt: skip


def write_compiled(compiled: CompiledSQL, run_id: str) -> Path:
    """build/compiled/<run>/<rules_hash>.sql, for review."""
    path = COMPILED_DIR / run_id / f"{compiled.rules_hash}.sql"
    path.parent.mkdir(parents=True, exist_ok=True)
    header = f"-- rules {compiled.rules_hash}, params: run_id, sample\n"
    path.write_text(header + compiled.text + "\n")
    return path


def sql_digest(compiled: CompiledSQL) -> str:
    """Short hash of the compiled SQL text (stored with each build)."""
    return hashlib.sha256(compiled.text.encode()).hexdigest()[:16]
