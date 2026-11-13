"""Plain-language wording for the UI: issue codes, cleaning steps, flags, tools and rules.

Pure functions over the pipeline's own facts (profile issues, rules, tool calls), so a business
reader sees "14 rows say 'n/a' instead of a ticket id" rather than `NULLTOK:ticket_id`.
"""

from typing import Any

SEVERITY = {
    "high": "Must be fixed before the data can be trusted for reporting.",
    "medium": "Distorts some numbers; a cleaning rule is expected.",
    "low": "Cosmetic; cleaned by default where possible.",
}

# code -> (title, why it matters)
ISSUES: dict[str, tuple[str, str]] = {
    "DATE_ORDER": ("Dates in the wrong order", "Negative durations break time-to-resolve and SLA metrics."),
    "MIXED_DATE_FORMATS": (
        "Dates written in many formats",
        "Dates can't be compared or sorted until they share one format.",
    ),
    "AMBIGUOUS_DATES": (
        "Dates that could be day-first or month-first",
        "05/07 can be 5 July or 7 May; a wrong guess shifts dates by months.",
    ),
    "NULL_LIKE_TOKENS": (
        "Placeholder text instead of blanks",
        "Words like 'n/a' or '???' look like real values and inflate counts.",
    ),
    "SENTINEL_VALUES": ("Fake placeholder numbers or dates", "Values like 999 or 0000-00-00 skew averages and ranges."),
    "UNPARSED_DATES": ("Text in a date column", "Values like 'asap' can't become dates and will be emptied."),
    "HIGH_EMPTY_RATE": ("Many missing values", "Totals and averages are computed on fewer rows than expected."),
    "NORMALIZATION_VARIANTS": ("Same value spelled several ways", "Counts per group are split across spellings."),
    "WHITESPACE": ("Extra spaces around values", "'Plumbing ' and 'Plumbing' are counted separately."),
    "MIXED_TYPES": ("Numbers mixed with text", "The column can't be summed until text is removed."),
    "NUMBERS_IN_TEXT": ("Numbers with symbols ($, %, units)", "Values need the symbols removed to become numbers."),
    "KEY_WITH_REPEATS": ("An ID appears more than once", "Records may be double-counted."),
    "INVALID_DATES": ("Impossible dates", "Dates such as 31 February can't be stored."),
    "FUTURE_DATES": ("Dates in the future", "Usually typos; they distort date ranges."),
    "IMPLAUSIBLY_OLD_DATES": ("Implausibly old dates", "Usually placeholders or typos."),
    "TWO_DIGIT_YEARS": ("Two-digit years", "'24' could be 1924 or 2024."),
    "TYPO_VARIANTS": ("Likely typos", "Near-identical spellings split counts."),
    "HIDDEN_CHARACTERS": ("Invisible characters", "Values that look equal don't match."),
    "MOJIBAKE": ("Garbled characters", "Text was decoded with the wrong encoding (e.g. 'Ã©' for 'é')."),
    "LEADING_ZEROS": ("Leading zeros", "Converting to numbers would lose the zeros (e.g. ZIP codes)."),
    "CONSTANT_COLUMN": ("Column has a single value", "Carries no information."),
    "PII_HINT": ("Possible personal data", "Handled as private: hidden from the AI."),
    "OUTLIERS": ("Unusual extreme values", "May be errors that skew averages."),
    "PARTIAL_ROWS": ("Rows missing most of their fields", "Mostly empty rows add noise to counts."),
}


def _pct(share: float | None) -> str:
    return "?" if share is None else f"{share * 100:.1f}%"


def _n(value: Any) -> str:
    return f"{value:,}" if isinstance(value, int) else str(value)


def _tokens(examples: dict[str, int]) -> str:
    return ", ".join(f"'{k}' ({_n(v)})" for k, v in examples.items())


def issue_sentence(issue: dict[str, Any]) -> str:
    """One sentence with the issue's own numbers."""
    code, col, ev = issue["code"], issue.get("column") or "the dataset", issue.get("evidence") or {}
    if code == "DATE_ORDER":
        a, _, b = col.partition(" > ")
        return (f"In {_n(ev.get('count'))} of {_n(ev.get('compared'))} rows ({_pct(ev.get('share'))}), "
                f"{a} is later than {b}.")  # fmt: skip
    if code == "MIXED_DATE_FORMATS":
        shown = "; ".join(f"{e['format']} ({_pct(e['share'])}, e.g. {e['example']})" for e in ev.get("examples", []))
        return f"{col} uses {ev.get('format_count')} different date formats. Most common: {shown}."
    if code == "AMBIGUOUS_DATES":
        return (f"{_n(ev.get('ambiguous'))} values in {col} could be read either way. The data itself shows "
                f"{_n(ev.get('mdy_evidence'))} month-first and {_n(ev.get('dmy_evidence'))} day-first values.")  # fmt: skip
    if code == "NULL_LIKE_TOKENS":
        return f"{_n(ev.get('count'))} values in {col} are placeholders: {_tokens(ev.get('examples', {}))}."
    if code == "SENTINEL_VALUES":
        shown = ", ".join(f"'{e['value']}' ({_n(e['count'])})" for e in ev.get("examples", []))
        return f"{_pct(ev.get('share'))} of {col} are placeholder values: {shown}."
    if code == "UNPARSED_DATES":
        return (
            f"{_n(ev.get('count'))} values in {col} are not dates, e.g. {', '.join(map(repr, ev.get('examples', [])))}."
        )
    if code == "HIGH_EMPTY_RATE":
        return f"{_n(ev.get('count'))} values ({_pct(ev.get('share'))}) in {col} are empty or placeholders."
    if code == "NORMALIZATION_VARIANTS" or code == "TYPO_VARIANTS":
        groups = ev.get("examples", [])
        shown = "; ".join(" / ".join(f"'{v}' ({_n(c)})" for v, c in g) for g in groups[:2] if isinstance(g, list))
        return f"{_n(ev.get('groups'))} values in {col} are spelled several ways, e.g. {shown}."
    if code == "WHITESPACE":
        return f"{_n(ev.get('leading_trailing'))} values in {col} have spaces at the start or end."
    if code == "MIXED_TYPES":
        examples = ev.get("examples", {})
        return f"{_pct(ev.get('text_share'))} of {col} is text, not a number (e.g. '{examples.get('text', '')}')."
    if code == "NUMBERS_IN_TEXT":
        return (f"{_n(ev.get('count'))} values in {col} carry symbols, e.g. {', '.join(ev.get('examples', []))}; "
                f"{_pct(ev.get('numeric_after_cleanup'))} become numbers once cleaned.")  # fmt: skip
    if code == "PARTIAL_ROWS":
        return f"{_n(ev.get('count'))} rows are missing at least {ev.get('min_missing_cells')} fields."
    if "count" in ev:
        share = f" ({_pct(ev['share'])})" if "share" in ev else ""
        return f"{_n(ev['count'])} values{share} in {col}."
    return f"Found in {col}."


def describe_issue(issue: dict[str, Any]) -> dict[str, Any]:
    title, why = ISSUES.get(issue["code"], (issue["code"].replace("_", " ").capitalize(), ""))
    return {**issue, "title": title, "plain": issue_sentence(issue), "why": why}


# --- cleaning rules ----------------------------------------------------------------------

STEPS = {
    "trim": "remove spaces at the start and end",
    "collapse_spaces": "turn repeated spaces into one",
    "remove_hidden_chars": "remove invisible characters",
    "lower": "make lowercase",
    "upper": "make uppercase",
    "title": "capitalise each word",
    "strip_currency": "remove currency symbols ($, €, USD …)",
    "strip_thousands": "remove thousands separators (1,000 → 1000)",
    "strip_percent": "remove the % sign",
    "strip_units": "remove units (12 kg → 12)",
    "european_decimal": "read 1.234,5 as 1234.5",
}

TYPES = {"text": "text", "integer": "whole number", "numeric": "number", "boolean": "yes/no",
         "date": "date", "timestamp": "date and time"}  # fmt: skip

FLAGS = {
    "cleaned": "Cleaned (spaces, symbols or case changed)",
    "null_token": "Placeholder text removed (now empty)",
    "sentinel": "Fake placeholder value removed (now empty)",
    "mapped": "Spelling standardised",
    "unmapped": "Not in the standard list (kept as is)",
    "unparseable": "Could not be read as the expected type (now empty)",
    "out_of_range": "Outside the allowed range (now empty)",
    "nulled_by_row_check": "Emptied by a row check",
    "swapped": "Values of two columns were swapped back",
    "declared_duplicate": "The ticket says it duplicates another",
    "ai_labelled": "Label recovered by AI from the ticket text (see the _ai column)",
    "ai_low_confidence": "AI was unsure; no label recovered",
}


def flag_label(flag: str) -> str:
    kind, _, value = flag.partition(":")
    label = FLAGS.get(kind, flag)
    return f"{label}: '{value}'" if value else label


def _range(rule: dict[str, Any]) -> str | None:
    r = rule.get("range") or {}
    if r.get("min") is not None and r.get("max") is not None:
        return f"between {r['min']} and {r['max']}"
    if r.get("min") is not None:
        return f"at least {r['min']}"
    if r.get("max") is not None:
        return f"at most {r['max']}"
    return None


def describe_column_rule(rule: dict[str, Any]) -> list[str]:
    """Ordered, plain steps the rule applies to a value."""
    out = []
    if rule.get("steps"):
        out.append("Tidy: " + "; ".join(STEPS.get(s, s) for s in rule["steps"]) + ".")
    if rule.get("null_tokens"):
        out.append("Treat " + ", ".join(f"'{t}'" for t in rule["null_tokens"]) + " as empty.")
    if rule.get("sentinels"):
        out.append("Treat placeholder values " + ", ".join(f"'{t}'" for t in rule["sentinels"]) + " as empty.")
    if rule.get("mapping"):
        variants = sum(len(v) for v in rule["mapping"].values())
        out.append(f"Merge {variants} spellings into {len(rule['mapping'])} standard values.")
        if rule.get("unmapped_action") == "null":
            out.append("Values outside the standard list become empty.")
        elif rule.get("unmapped_action") == "invalid":
            out.append("Values outside the standard list are marked invalid.")
    if rule.get("type") and rule["type"] != "text":
        fmt = f" (reading {len(rule['parse_formats'])} date formats)" if rule.get("parse_formats") else ""
        order = {"mdy": ", month before day", "dmy": ", day before month"}.get(rule.get("day_month_order") or "", "")
        out.append(f"Store as {TYPES.get(rule['type'], rule['type'])}{fmt}{order}.")
    if rng := _range(rule):
        out.append(f"Allowed values: {rng}; anything else becomes empty.")
    if rule.get("required"):
        action = "the row is rejected" if rule.get("invalid_action") == "reject" else "it is flagged"
        out.append(f"Required: if it is missing, {action}.")
    elif rule.get("invalid_action") == "reject":
        out.append("Rows with an unreadable value are rejected.")
    return out or ["Kept exactly as it is."]


ROW_CHECKS = {
    "swapped_columns": lambda p: (
        f"when {p['a']} holds a sentence and {p['b']} holds one of {p['a']}'s known values, "
        f"swap the two (columns that were entered the wrong way round)"
    ),
    "junk_row": lambda p: (
        f"rows with at least {float(p['min_empty_share']):.0%} of their cells empty or placeholders, "
        f"or whose {p['id']} does not match {p['id_pattern']}, are junk"
    ),
    "declared_duplicate": lambda p: (
        f"when {p['source']} says the ticket duplicates another (pattern {p['pattern']}), "
        f"record which one in _duplicate_of"
    ),
    "date_order": lambda p: f"{p['a']} must not be after {p['b']}",
    "required_if": lambda p: f"{p['column']} is required when {p['other']} is one of {', '.join(p['values'])}",
    "not_both_null": lambda p: f"{p['a']} and {p['b']} must not both be empty",
}
ROW_ACTIONS = {
    "reject": "the row is rejected",
    "null_second": "the second value is emptied",
    "flag": "the row is flagged",
    "swap": "the values are swapped and flagged",
    "duplicate": "the row is marked a duplicate (kept, not counted)",
}


def describe_row_check(check: dict[str, Any]) -> str:
    rule = ROW_CHECKS.get(check["kind"], lambda p: check["kind"])(check["params"])
    link = "; then" if check["action"] in ("swap", "duplicate") or check["kind"] == "junk_row" else "; otherwise"
    return f"{rule[0].upper()}{rule[1:]}{link} {ROW_ACTIONS.get(check['action'], check['action'])}."


def describe_dedup(dedup: dict[str, Any]) -> str:
    key = ", ".join(dedup["key"])
    if dedup["strategy"] == "keep_latest":
        return f"Rows with the same {key} are duplicates; keep the one with the latest {dedup['order_by']}."
    return f"Rows with the same {key} are duplicates; keep the first."


# --- agent tools -------------------------------------------------------------------------


def describe_tool_call(tool: str, args: dict[str, Any]) -> str:
    col = args.get("column")
    if tool == "list_issues":
        scope = " ".join(x for x in [args.get("severity"), "issues", f"for {col}" if col else ""] if x)
        return f"Listed {scope}"
    if tool == "get_column_profile":
        return f"Read the full health check of {col}"
    if tool == "get_value_counts":
        how = " grouped by spelling" if args.get("normalized") else ""
        mask = f" matching the shape {args['pattern_mask']}" if args.get("pattern_mask") else ""
        return f"Counted the values of {col}{how}{mask}"
    if tool == "sample_rows":
        return f"Looked at {args.get('n', 'a few')} sample row(s)"
    if tool == "check_regex":
        return f"Checked how many {col} values match the pattern {args.get('pattern')}"
    if tool == "test_rule":
        rule = args.get("column_rule") or {}
        return f"Tried a cleaning rule for {rule.get('name') or rule.get('source')} on 2,000 sample rows"
    if tool == "check_cross_column":
        return f"Compared {args.get('a')} with {args.get('b')} row by row"
    if tool == "submit_rules":
        return "Submitted the cleaning rules for validation"
    return f"Called {tool}"


ERRORS = {
    "unparseable": "{col} could not be read as {type}",
    "out_of_range": "{col} is outside the allowed range",
    "unmapped": "{col} is not one of the standard values",
    "required_missing": "{col} is missing but required",
}


def error_label(error: str, types: dict[str, str] | None = None) -> str:
    """'created_at:unparseable' -> 'created_at could not be read as date and time'."""
    if error.startswith("row:"):
        _, kind, cols = error.split(":", 2)
        a, _, b = cols.partition(",")
        return {"date_order": f"{a} is later than {b}", "not_both_null": f"{a} and {b} are both empty",
                "required_if": f"{a} is missing although {b} requires it",
                "junk_row": "junk row (mostly empty, or an invalid id)"}.get(kind, error)  # fmt: skip
    col, _, kind = error.rpartition(":")
    return ERRORS.get(kind, error).format(col=col, type=TYPES.get((types or {}).get(col, ""), "the expected type"))


# field -> (label, what it means); shown next to every setting of a rule on the review page
FIELD_HELP: dict[str, tuple[str, str]] = {
    "name": ("Name in the cleaned data", "Column name in Silver."),
    "source": ("Source column", "Column of the uploaded file this rule reads."),
    "type": ("Stored as", "text, integer, numeric, boolean, date or timestamp."),
    "steps": ("Tidying steps", "Applied in this order before anything else."),
    "null_tokens": ("Words meaning 'empty'", "Matched after tidying, ignoring case; become empty."),
    "sentinels": ("Placeholder values", "Fake values (like 999) that become empty and are flagged."),
    "mapping": ("Standard values", "Each standard value with the spellings merged into it."),
    "unmapped_action": ("Values not in the list", "keep: leave as is · null: make empty · invalid: mark as an error."),
    "boolean_true": ("Read as yes", "Values that mean yes."),
    "boolean_false": ("Read as no", "Values that mean no."),
    "parse_formats": ("Date formats read", "Formats tried in order to read a date."),
    "day_month_order": ("Day/month order", "mdy: month first (05/07 = 7 May) · dmy: day first."),
    "range": ("Allowed range", "Values outside become empty and are flagged."),
    "required": ("Required", "A missing value is an error."),
    "invalid_action": ("When a value is bad", "null: make it empty and flag it · reject: drop the whole row."),
    "confidence": ("Confidence", "How sure the author is (0–1)."),
    "why": ("Why", "The business reason for the rule."),
    "addresses": ("Fixes issues", "Health-check issues this rule resolves."),
    "generated_by": ("Written by", "agent: the AI · default: safe default · human: a person."),
    "kind": ("Check", "date_order, required_if or not_both_null."),
    "params": ("Columns checked", "The columns the check compares."),
    "action": (
        "When it fails",
        "reject: drop the row · null_second: empty the second value · flag: keep, with a warning.",
    ),
    "key": ("Duplicate key", "Rows with the same values here are duplicates."),
    "strategy": ("Which duplicate is kept", "exact: the first · keep_latest: the latest by 'order by'."),
    "order_by": ("Order by", "Column that decides which duplicate is the latest."),
}
