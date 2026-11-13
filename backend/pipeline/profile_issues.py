"""Deterministic issue flags derived from profiles. Each issue: {column, code, severity, evidence}.

Severity rules (high > medium > low); thresholds come from PROFILE_CONFIG:
| code                   | severity                                                             |
| MALFORMED_ROWS         | high if > high_share of records are malformed, else medium           |
| KEY_WITH_REPEATS       | high if any repeat group conflicts, medium if only time/status-like columns differ (update history), low if the repeats are exact duplicates |
| DATE_ORDER             | high if > high_share of compared rows have A > B, else medium        |
| MIXED_TYPES            | high (neither one type nor text reaches dominant_threshold)          |
| MIXED_DATE_FORMATS     | high if >= many_date_formats formats, else medium                    |
| INVALID_DATES          | high if > high_share of values are date-shaped but impossible        |
| AMBIGUOUS_DATES        | high if one format shows both DD/MM and MM/DD evidence, else medium  |
| HIGH_EMPTY_RATE        | high if > very_high_empty_rate empty/null-like, medium if > high_empty_rate |
| EXACT_DUPLICATES       | high if > duplicate_high_share of rows, else medium                  |
| SENTINEL_VALUES        | high if sentinels cover >= sentinel_high_share of values, else medium|
| PARTIAL_ROWS           | high if > partial_rows_high_share of rows, else medium               |
| MOJIBAKE               | medium (text decoded with the wrong encoding)                        |
| HIDDEN_CHARACTERS      | medium; low if only tabs/newlines                                    |
| NEAR_DUPLICATES        | medium                                                               |
| NULL_LIKE_TOKENS       | medium (tokens and punctuation-only values)                          |
| NUMBERS_IN_TEXT        | medium                                                               |
| FUTURE_DATES           | medium                                                               |
| IMPLAUSIBLY_OLD_DATES  | medium (before plausible_min_year, sentinels excluded)               |
| UNPARSED_DATES         | medium (values in a date column matching no known format)            |
| TWO_DIGIT_YEARS        | medium (century is a guess)                                          |
| TYPO_VARIANTS          | medium (rare value one typo away from a frequent one)                |
| LEADING_ZEROS          | medium if a minority of numbers are zero-padded; low when it's an identifier |
| NORMALIZATION_VARIANTS | medium if categorical or label-like (distinct ratio <= label_like_ratio), else low |
| OUTLIERS               | medium if negatives present, else low; judged without sentinels      |
| WHITESPACE             | low                                                                  |
| CONSTANT_COLUMN        | low                                                                  |
| PII_HINT               | low                                                                  |
"""

from typing import Any

from pipeline.profile_columns import mask
from pipeline.profile_config import PROFILE_CONFIG as CFG

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
# Short issue codes: used in issue ids and on the card (the profile keeps the full `code` too).
SHORT_CODES = {
    "DATE_ORDER": "ORDER",
    "MIXED_DATE_FORMATS": "DATEFMT",
    "AMBIGUOUS_DATES": "AMBIG",
    "NULL_LIKE_TOKENS": "NULLTOK",
    "SENTINEL_VALUES": "SENTINEL",
    "UNPARSED_DATES": "UNPARSED",
    "HIGH_EMPTY_RATE": "EMPTY",
    "NORMALIZATION_VARIANTS": "VARIANTS",
    "WHITESPACE": "WS",
    "MIXED_TYPES": "MIXED",
    "NUMBERS_IN_TEXT": "NUMTXT",
    "KEY_WITH_REPEATS": "KEYREP",
    "INVALID_DATES": "INVALID",
    "FUTURE_DATES": "FUTURE",
    "IMPLAUSIBLY_OLD_DATES": "OLD",
    "TWO_DIGIT_YEARS": "YY",
    "TYPO_VARIANTS": "TYPO",
    "HIDDEN_CHARACTERS": "HIDDEN",
    "MOJIBAKE": "MOJI",
    "LEADING_ZEROS": "LZERO",
    "CONSTANT_COLUMN": "CONST",
    "PII_HINT": "PII",
    "OUTLIERS": "OUTLIER",
}

Issue = dict[str, Any]


def _issue(column: str | None, code: str, severity: str, **evidence: Any) -> Issue:
    return {"column": column, "code": code, "severity": severity, "evidence": evidence}


def _ratio(count: int, total: int) -> float:
    return count / total if total else 0.0


def _missing_examples(comp: dict[str, Any]) -> list[list[Any]]:
    """The 3 most common ways a value is missing: [[form, count], ...]."""
    forms = {"<empty>": comp["empty"], "<whitespace>": comp["whitespace_only"], "<absent>": comp["absent"],
             **comp["null_like"], **comp["punctuation_examples"]}  # fmt: skip
    return [[k, v] for k, v in sorted(forms.items(), key=lambda kv: (-kv[1], kv[0])) if v][: CFG.examples]


def column_issues(p: dict[str, Any]) -> list[Issue]:
    """Issues visible in a single column profile. Every issue carries `examples`."""
    name, issues = p["column"], []
    comp, card, shape, types = p["completeness"], p["cardinality"], p["shape"], p["type_inference"]

    if comp["null_like"] or comp["punctuation_only"]:  # examples = {token: count}, punctuation-only included
        found = {**comp["null_like"], **comp["punctuation_examples"]}
        issues.append(_issue(name, "NULL_LIKE_TOKENS", "medium", count=sum(comp["null_like"].values())
                             + comp["punctuation_only"], examples=found))  # fmt: skip
    if comp["empty_or_null_like_share"] > CFG.high_empty_rate:
        severity = "high" if comp["empty_or_null_like_share"] > CFG.very_high_empty_rate else "medium"
        issues.append(_issue(name, "HIGH_EMPTY_RATE", severity, share=comp["empty_or_null_like_share"],
                             count=comp["total"] - comp["informative"], examples=_missing_examples(comp)))  # fmt: skip
    if shape["leading_trailing_ws"] or comp["whitespace_only"]:
        examples = shape.get("ws_examples") or ["<whitespace only>"]
        issues.append(_issue(name, "WHITESPACE", "low", leading_trailing=shape["leading_trailing_ws"],
                             whitespace_only=comp["whitespace_only"], examples=examples))  # fmt: skip
    if (var := p["variants"])["groups"]:
        label_like = card["role_hint"] == "categorical" or card["distinct_ratio"] <= CFG.label_like_ratio
        issues.append(_issue(name, "NORMALIZATION_VARIANTS", "medium" if label_like else "low",
                             groups=var["groups"], casing_only_groups=var["casing_only_groups"],
                             examples=[g["members"] for g in var["top_groups"][:2]]))  # fmt: skip
    if typos := p.get("typos"):
        issues.append(_issue(name, "TYPO_VARIANTS", "medium", count=len(typos), examples=typos))
    issues += _character_issues(name, p["characters"])
    if types["dominant_type"] == "mixed":
        issues.append(_issue(name, "MIXED_TYPES", "high", shares={t: s for t, s in types["shares"].items() if s},
                             text_share=types["text_share"], examples=types.get("examples", {})))  # fmt: skip
    if types["leading_zeros"]:
        identifier = types["dominant_type"] == "identifier"
        issues.append(_issue(name, "LEADING_ZEROS", "low" if identifier else "medium", count=types["leading_zeros"],
                             treated_as_identifier=identifier, examples=types.get("leading_zero_examples", [])))  # fmt: skip
    if card["role_hint"] == "constant":
        issues.append(_issue(name, "CONSTANT_COLUMN", "low", examples=p["frequency"]["top_values"][:1]))
    if (pii := p["pii_hints"])["email_share"] or pii["phone_share"] or pii["name_matches"]:
        masked = [mask(v) for v in p["samples"][:2]]  # shape only, never the value
        issues.append(_issue(name, "PII_HINT", "low", **pii, examples=masked))
    if found := p.get("sentinels"):
        covered = sum(s["share"] for s in found)
        issues.append(_issue(name, "SENTINEL_VALUES", "high" if covered >= CFG.sentinel_high_share else "medium",
                             share=round(covered, 4), examples=found))  # fmt: skip
    issues += _date_issues(name, p.get("dates"), comp["informative"])
    issues += _numeric_issues(name, p.get("numeric"))
    return issues


def _character_issues(name: str, chars: dict[str, Any]) -> list[Issue]:
    issues = []
    if chars["hidden_values"]:
        severity = "low" if chars["hidden_only_line_chars"] else "medium"
        issues.append(_issue(name, "HIDDEN_CHARACTERS", severity, values=chars["hidden_values"],
                             by_char=chars["hidden_by_char"], examples=chars["hidden_examples"]))  # fmt: skip
    if chars["mojibake_values"]:
        issues.append(_issue(name, "MOJIBAKE", "medium", values=chars["mojibake_values"],
                             examples=chars["mojibake_examples"]))  # fmt: skip
    return issues


def _date_issues(name: str, dates: dict[str, Any] | None, informative: int) -> list[Issue]:
    if not dates:
        return []
    issues = []
    examples = dates.get("examples", {})
    if dates["format_count"] > 1:
        severity = "high" if dates["format_count"] >= CFG.many_date_formats else "medium"
        issues.append(_issue(name, "MIXED_DATE_FORMATS", severity, format_count=dates["format_count"],
                             examples=dates["formats"][:5]))  # fmt: skip
    if dates["invalid_dates"]:
        severity = "high" if _ratio(dates["invalid_dates"], informative) > CFG.high_share else "medium"
        issues.append(_issue(name, "INVALID_DATES", severity, count=dates["invalid_dates"],
                             examples=examples.get("invalid", [])))  # fmt: skip
    if dates["ambiguous_count"]:
        severity = "high" if dates["conflicting_order_formats"] else "medium"
        issues.append(_issue(name, "AMBIGUOUS_DATES", severity, ambiguous=dates["ambiguous_count"],
                             dmy_evidence=dates["dmy_evidence"], mdy_evidence=dates["mdy_evidence"],
                             examples=examples.get("ambiguous", [])))  # fmt: skip
    if dates["unparsed"]:
        issues.append(_issue(name, "UNPARSED_DATES", "medium", count=dates["unparsed"],
                             examples=dates["unparsed_examples"]))  # fmt: skip
    if dates["two_digit_year"]:
        issues.append(_issue(name, "TWO_DIGIT_YEARS", "medium", count=dates["two_digit_year"],
                             examples=examples.get("two_digit_year", [])))  # fmt: skip
    if dates["future_count"]:
        issues.append(_issue(name, "FUTURE_DATES", "medium", count=dates["future_count"], max=dates["max"],
                             examples=examples.get("future", [])))  # fmt: skip
    if dates["implausibly_old_count"]:
        issues.append(_issue(name, "IMPLAUSIBLY_OLD_DATES", "medium", count=dates["implausibly_old_count"],
                             min=dates["min"], before_year=CFG.plausible_min_year,
                             examples=examples.get("implausibly_old", [])))  # fmt: skip
    return issues


def _numeric_issues(name: str, numeric: dict[str, Any] | None) -> list[Issue]:
    if not numeric:
        return []
    issues = []
    if numeric["numbers_in_text"]:
        issues.append(_issue(name, "NUMBERS_IN_TEXT", "medium", count=numeric["numbers_in_text"],
                             kinds=[k[:2] for k in numeric.get("number_kinds", [])],
                             numeric_after_cleanup=numeric.get("numeric_after_cleanup"),
                             examples=numeric["numbers_in_text_examples"]))  # fmt: skip
    # Sentinels are their own issue, so outliers are judged on the stats without them.
    stats = numeric.get("without_sentinels", numeric)
    if stats.get("iqr_outliers") or stats.get("negatives"):
        severity = "medium" if stats.get("negatives") else "low"
        issues.append(_issue(name, "OUTLIERS", severity, iqr_outliers=stats["iqr_outliers"], negatives=stats["negatives"],
                             min=stats["min"], max=stats["max"], excluding_sentinels="without_sentinels" in numeric,
                             examples=stats.get("outlier_examples") or [stats["min"]]))  # fmt: skip
    return issues


def dataset_issues(summary: dict[str, Any]) -> list[Issue]:
    """Issues from the dataset summary and cross-column checks. Every issue carries `examples`
    (source lines for row-level issues)."""
    issues = []
    rows, records = summary["rows_ok"], summary["rows_ok"] + summary["rows_malformed"]
    if summary["rows_malformed"]:
        severity = "high" if _ratio(summary["rows_malformed"], records) > CFG.high_share else "medium"
        issues.append(_issue(None, "MALFORMED_ROWS", severity, count=summary["rows_malformed"],
                             by_reason=summary["malformed_by_reason"], examples=summary["malformed_examples"]))  # fmt: skip
    if dup := summary["exact_duplicate_rows"]:
        severity = "high" if _ratio(dup, rows) > CFG.duplicate_high_share else "medium"
        issues.append(_issue(None, "EXACT_DUPLICATES", severity, extra_rows=dup, groups=summary["exact_duplicate_groups"],
                             examples=summary["exact_duplicate_examples"]))  # fmt: skip
    if near := summary["near_duplicate_rows"]:
        issues.append(_issue(None, "NEAR_DUPLICATES", "medium", extra_rows=near,
                             examples=summary["near_duplicate_examples"]))  # fmt: skip
    if (partial := summary["partial_rows"])["count"]:
        severity = "high" if _ratio(partial["count"], rows) > CFG.partial_rows_high_share else "medium"
        issues.append(_issue(None, "PARTIAL_ROWS", severity, count=partial["count"],
                             min_missing_cells=partial["min_missing_cells"], examples=partial["examples"]))  # fmt: skip
    for key in summary["key_with_repeats"]:
        if not key["repeated_values"]:
            continue
        severity = {"conflicting": "high", "update_history": "medium", "exact_duplicate": "low"}[key["hint"]]
        issues.append(_issue(key["column"], "KEY_WITH_REPEATS", severity, repeated_values=key["repeated_values"],
                             rows=key["rows"], hint=key["hint"], hint_counts=key["hint_counts"],
                             differing_columns=key["differing_columns"], examples=key["examples"]))  # fmt: skip
    for pair in summary["date_order"]:
        severity = "high" if pair["share"] > CFG.high_share else "medium"
        issues.append(_issue(f"{pair['a']} > {pair['b']}", "DATE_ORDER", severity, count=pair["count"],
                             share=pair["share"], compared=pair["compared"],
                             unambiguous_share=pair["unambiguous_share"],
                             unambiguous_compared=pair["unambiguous_compared"],
                             format_pairs=pair["format_pairs"], examples=pair["examples"]))  # fmt: skip
    return issues


def sort_issues(issues: list[Issue], column_order: list[str]) -> list[Issue]:
    """Severity first, then dataset-level before columns (in header order), then code."""
    position = {c: i for i, c in enumerate(column_order)}

    def key(issue: Issue) -> tuple[int, int, str]:
        first_column = (issue["column"] or "").split(" > ")[0]
        return SEVERITY_RANK[issue["severity"]], position.get(first_column, -1), issue["code"]

    return sorted(issues, key=key)


def issue_id(issue: Issue) -> str:
    """Stable id: <short code>:<column>, 'dataset' for dataset-level, `a>b` for column pairs."""
    column = (issue["column"] or "dataset").replace(" > ", ">")
    return f"{SHORT_CODES.get(issue['code'], issue['code'])}:{column}"


def assign_ids(issues: list[Issue]) -> list[Issue]:
    """Give every issue its id (in place order); a repeat of the same id gets ':<n>' appended."""
    seen: dict[str, int] = {}
    result = []
    for issue in issues:
        base = issue_id(issue)
        seen[base] = seen.get(base, 0) + 1
        result.append({"id": base if seen[base] == 1 else f"{base}:{seen[base]}", **issue})
    return result
