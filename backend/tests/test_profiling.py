"""Deterministic profiling of bronze runs. Expected counts are derived by hand from the fixtures."""

from typing import Any

import pytest

from pipeline.profile_columns import mask, profile_size
from pipeline.profile_config import PROFILE_CONFIG
from pipeline.profiling import load_profile, profile_run

MESSY = "profile_messy.csv"  # 22 ok rows + 1 malformed (line 16)


@pytest.fixture
def messy(ingest) -> dict[str, Any]:
    run_id = ingest(MESSY)
    profile_run(run_id)
    return load_profile(run_id)


def column(profile: dict[str, Any], name: str) -> dict[str, Any]:
    return next(c for c in profile["columns"] if c["column"] == name)


def issues(profile: dict[str, Any], code: str) -> list[dict[str, Any]]:
    return [i for i in profile["issues"] if i["code"] == code]


def test_mask_collapses_letters_keeps_digits() -> None:
    assert mask("TKT-0042") == "A-9999"
    assert mask("15/03/2024 10:00 AM") == "99/99/9999 99:99 A"


def test_null_like_tokens_and_whitespace(messy) -> None:
    priority = column(messy, "priority")
    assert priority["completeness"] | {} == {
        "total": 22, "informative": 18, "empty": 1, "whitespace_only": 1, "null_like": {"null": 1, "n/a": 1},
        "punctuation_only": 0, "punctuation_examples": {}, "absent": 0, "empty_or_null_like_share": round(4 / 22, 4),
    }  # fmt: skip
    assert priority["shape"]["leading_trailing_ws"] == 1
    assert {i["column"] for i in issues(messy, "NULL_LIKE_TOKENS")} == {"priority"}
    assert {i["column"] for i in issues(messy, "WHITESPACE")} == {"priority"}


def test_casing_variants_grouped(messy) -> None:
    variants = column(messy, "priority")["variants"]
    assert (variants["groups"], variants["casing_only_groups"]) == (3, 3)
    assert {"High", "HIGH", " high"} in [{m[0] for m in g["members"]} for g in variants["top_groups"]]


def test_mixed_date_formats_and_day_month_evidence(messy) -> None:
    dates = column(messy, "created_at")["dates"]
    assert [(f["format"], f["share"]) for f in dates["formats"]] == [
        ("YYYY-MM-DD", round(19 / 22, 4)), ("DD/MM/YYYY", round(2 / 22, 4)), ("YYYY-MM-DDTHH:MM:SS", round(1 / 22, 4))]
    assert dates["formats"][1]["example"] in ("15/03/2024", "16/03/2024", "03/04/2024")
    assert (dates["ambiguous_count"], dates["dmy_evidence"], dates["mdy_evidence"]) == (1, 1, 0)
    assert dates["future_count"] == 1
    assert issues(messy, "MIXED_DATE_FORMATS")[0]["column"] == "created_at"
    assert [i["column"] for i in issues(messy, "AMBIGUOUS_DATES")] == ["created_at"]


def test_numbers_in_text(messy) -> None:
    numeric = column(messy, "cost")["numeric"]
    assert numeric["numbers_in_text"] == 3
    assert set(numeric["numbers_in_text_examples"]) == {"3 hrs", "$40", "1,200"}
    assert column(messy, "cost")["type_inference"]["dominant_type"] == "mixed"


def test_key_with_repeats_and_duplicates(messy) -> None:
    (repeat,) = messy["summary"]["key_with_repeats"]
    # K-03: two different tickets (conflicting); K-10: exact duplicate rows; K-11: differs only in case.
    assert (repeat["column"], repeat["repeated_values"], repeat["rows"]) == ("id", 3, 6)
    assert repeat["hint"] == "conflicting"
    assert repeat["hint_counts"] == {"conflicting": 1, "exact_duplicate": 2}
    assert repeat["examples"][0]["value"] == "K-03" and repeat["examples"][0]["hint"] == "conflicting"
    assert {c for c, _ in repeat["differing_columns"]} >= {"priority", "note"}
    assert len(repeat["examples"][0]["differs"]) <= 3
    assert messy["summary"]["exact_duplicate_rows"] == 1
    assert messy["summary"]["near_duplicate_rows"] == 1
    assert messy["summary"]["exact_duplicate_examples"] == [[12, 13]]
    assert messy["summary"]["near_duplicate_examples"] == [[14, 15]]
    assert issues(messy, "KEY_WITH_REPEATS")[0]["severity"] == "high"


def test_date_order_flags_a_after_b(messy) -> None:
    (order,) = messy["summary"]["date_order"]
    assert (order["a"], order["b"], order["count"], order["compared"]) == ("created_at", "resolved_at", 2, 21)
    assert order["examples"] == [3, 6]
    assert issues(messy, "DATE_ORDER")[0]["column"] == "created_at > resolved_at"


def test_malformed_rows_excluded_from_column_stats(messy) -> None:
    assert all(c["completeness"]["total"] == 22 for c in messy["columns"])
    assert (messy["summary"]["rows_ok"], messy["summary"]["rows_malformed"]) == (22, 1)
    assert messy["summary"]["malformed_by_reason"] == {"too many fields": 1}
    assert issues(messy, "MALFORMED_ROWS")[0]["severity"] == "high"


def test_issues_sorted_by_severity(messy) -> None:
    ranks = [{"high": 0, "medium": 1, "low": 2}[i["severity"]] for i in messy["issues"]]
    assert ranks == sorted(ranks)


def test_profiling_twice_is_identical(ingest) -> None:
    run_id = ingest(MESSY)
    profile_run(run_id)
    first = load_profile(run_id)
    profile_run(run_id)
    assert load_profile(run_id) == first


def test_profile_size_within_budget_on_long_text(ingest) -> None:
    run_id = ingest("long_text.csv")
    profile_run(run_id)
    profile = load_profile(run_id)
    note = column(profile, "long_note")
    assert note["cardinality"]["role_hint"] == "free_text"
    assert all(profile_size(c) <= PROFILE_CONFIG.max_profile_bytes for c in profile["columns"])
    assert all(len(v) <= 100 for v in note["samples"])
