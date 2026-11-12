"""Profiler v1.1 upgrades. Unit tests call profile_column directly (no DB) with the audit's
probe values; the last tests ingest a fixture and profile it end to end."""

import ast
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pipeline.config import REPO_ROOT
from pipeline.context import TAGS
from pipeline.profile_columns import profile_column, profile_size
from pipeline.profile_config import PROFILE_CONFIG, PROFILER_VERSION, config_hash
from pipeline.profile_dates import parse_dates
from pipeline.profile_issues import column_issues
from pipeline.profile_values import is_adjacent_swap, normalization_key
from pipeline.profiling import load_profile, profile_run

REFERENCE = datetime(2026, 1, 1)


def profile(values: list[str], name: str = "col") -> dict[str, Any]:
    return profile_column(name, 1, pd.Series(values, dtype=object), seed=1, reference_time=REFERENCE)


def codes(p: dict[str, Any]) -> dict[str, str]:
    return {i["code"]: i["severity"] for i in column_issues(p)}


def test_null_like_tokens_and_punctuation_only() -> None:
    comp = profile(["a", "b", " NULL ", "Unknown", "TBD", "#N/A", "(blank)", "???", "...", "--", "**"])["completeness"]
    assert comp["null_like"] == {"#n/a": 1, "(blank)": 1, "null": 1, "tbd": 1, "unknown": 1}
    assert comp["punctuation_only"] == 4
    assert comp["informative"] == 2


def test_known_numeric_sentinels_and_stats_without_them() -> None:
    p = profile(["4", "8", "24", "48", "72"] * 40 + ["999"] * 15 + ["-999"] * 5)
    assert [(s["value"], s["count"], s["kind"]) for s in p["sentinels"]] == [("999", 15, "known"), ("-999", 5, "known")]
    assert p["numeric"]["max"] == 999 and p["numeric"]["without_sentinels"]["max"] == 72
    assert p["numeric"]["without_sentinels"]["count"] == 200
    assert codes(p)["SENTINEL_VALUES"] == "high"  # 20 of 220 values >= 5%
    assert "OUTLIERS" not in codes(p)  # judged without the sentinels


def test_zero_is_a_sentinel_only_when_it_spikes() -> None:
    assert "sentinels" not in profile(["0", "1", "2", "3", "4"] * 20)
    spiking = profile([str(n) for n in range(1, 41)] * 2 + ["0"] * 30)
    assert {"value": "0", "count": 30, "share": round(30 / 110, 4), "kind": "known"} in spiking["sentinels"]


def test_statistical_spike_in_high_cardinality_column() -> None:
    p = profile([f"user{n}@x.com" for n in range(200)] + ["noreply@x.com"] * 40)
    assert p["sentinels"] == [{"value": "noreply@x.com", "count": 40, "share": round(40 / 240, 4), "kind": "spike"}]
    templated = profile([f"note {n}" for n in range(200)] + ["fixed", "done", "ok"] * 40)
    assert "sentinels" not in templated  # three canned values spike: templates, not a placeholder


def test_date_sentinels_invalid_and_implausibly_old_dates() -> None:
    p = profile(["2024-03-01", "2024-03-02"] * 20 + ["1900-01-01", "0000-00-00", "9999-12-31", "1985-06-01", "2024-02-30"])
    assert {s["value"] for s in p["sentinels"]} == {"1900-01-01", "0000-00-00", "9999-12-31"}
    dates = p["dates"]
    assert (dates["invalid_dates"], dates["implausibly_old_count"], dates["future_count"]) == (1, 1, 0)
    assert dates["max"] == "2024-03-02T00:00:00"  # sentinels are not range facts
    assert {"SENTINEL_VALUES", "INVALID_DATES", "IMPLAUSIBLY_OLD_DATES"} <= codes(p).keys()
    assert "FUTURE_DATES" not in codes(p)


def test_normalization_variants() -> None:
    assert len({normalization_key(v) for v in ["In-Progress", "in progress", "in_progress", "InProgress", "IN PROGRESS"]}) == 1
    assert normalization_key("Café") == normalization_key("cafe")
    assert normalization_key("-999") != normalization_key("999")  # no letters: punctuation is kept
    p = profile(["In Progress"] * 5 + ["In-Progress", "in_progress", "InProgress", "High", "HIGH", " high"])
    groups = {g["key"]: g for g in p["variants"]["top_groups"]}
    assert {m[0] for m in groups["inprogress"]["members"]} == {"In Progress", "In-Progress", "in_progress", "InProgress"}
    assert groups["high"]["casing_only"] and not groups["inprogress"]["casing_only"]
    assert (p["variants"]["groups"], p["variants"]["casing_only_groups"]) == (2, 1)
    assert "NORMALIZATION_VARIANTS" in codes(p)


def test_typo_neighbours() -> None:
    assert is_adjacent_swap("hihg", "high") and not is_adjacent_swap("test", "pest")
    p = profile(["Open"] * 150 + ["Closed"] * 120 + ["High"] * 60 + ["Pest"] * 60 + ["Building 3"] * 30
                + ["Opne", "Clsoed", "Hihg", "Pending Vendr", "test", "Building 7"])  # fmt: skip
    pairs = {(t["rare"], t["likely_intended"]) for t in p["typos"]}
    # swaps are typos; "test"/"Pest" is a different word; digits-only differences are not typos
    assert pairs == {("Opne", "Open"), ("Clsoed", "Closed"), ("Hihg", "High")}
    assert codes(p)["TYPO_VARIANTS"] == "medium"


def test_hidden_characters_and_mojibake() -> None:
    p = profile(["ok"] * 5 + ["a\u00a0b", "a\u200bb", "a\tb", "x\x07y", "CafÃ©", "donâ€™t", "ï»¿hello"])
    chars = p["characters"]
    assert chars["hidden_by_char"] == {"U+0007": 1, "U+0009": 1, "U+00A0": 1, "U+200B": 1}
    assert "a[U+200B]b" in chars["hidden_examples"]
    assert chars["mojibake_values"] == 3
    assert codes(p)["HIDDEN_CHARACTERS"] == "medium" and codes(p)["MOJIBAKE"] == "medium"
    newline_only = profile(["line one\nline two", "plain"])
    assert codes(newline_only)["HIDDEN_CHARACTERS"] == "low"


def test_leading_zeros_make_an_identifier() -> None:
    p = profile(["00123", "00456", "07890", "12345"])
    assert p["type_inference"]["dominant_type"] == "identifier" and "numeric" not in p
    assert codes(p)["LEADING_ZEROS"] == "low"


def test_zero_one_column_is_a_boolean_candidate() -> None:
    for values in (["1", "0", "1", "0"], ["1", "0", "yes", "No"]):
        types = profile(values)["type_inference"]
        assert types["boolean_candidate"] and types["dominant_type"] == "boolean"
    assert not profile(["1", "0", "2"])["type_inference"]["boolean_candidate"]


def test_mixed_when_neither_a_type_nor_text_dominates() -> None:
    assert profile(["1", "2", "3", "4", "5", "a", "b", "c", "d", "e"])["type_inference"]["dominant_type"] == "mixed"
    assert profile(["alpha"] * 9 + ["1"])["type_inference"]["dominant_type"] == "text"


@pytest.mark.parametrize(
    ("value", "label", "parsed"),
    [
        ("2024-03-15T10:00:00Z", "YYYY-MM-DDTHH:MM:SSZ", datetime(2024, 3, 15, 10)),
        ("2024-03-15T10:00:00+05:30", "YYYY-MM-DDTHH:MM:SS±HH:MM", datetime(2024, 3, 15, 4, 30)),
        ("2024-03-15T10:00:00.123", "YYYY-MM-DDTHH:MM:SS.fff", datetime(2024, 3, 15, 10, 0, 0, 123000)),
        ("15 March 2024", "DD Month YYYY", datetime(2024, 3, 15)),
        ("March 15, 2024", "Month DD, YYYY", datetime(2024, 3, 15)),
        ("15/03/24", "DD/MM/YY", datetime(2024, 3, 15)),
        ("1710460800000", "epoch milliseconds", datetime(2024, 3, 15)),
        ("20240315", "YYYYMMDD", datetime(2024, 3, 15)),
        ("45000", "Excel serial", datetime(2023, 3, 15)),
    ],
)
def test_new_date_formats_in_a_date_column(value: str, label: str, parsed: datetime) -> None:
    result = parse_dates(["2024-03-01"] * 5 + [value])[-1]
    assert result is not None and (result.label, result.value) == (label, parsed)


def test_contextual_formats_only_in_date_like_columns() -> None:
    assert parse_dates(["20240315", "45000", "3", "4"]) == [None] * 4
    p = profile(["2024-03-01"] * 5 + ["15/03/24", "45000"])
    assert (p["dates"]["two_digit_year"], p["dates"]["excel_serial"]) == (1, 1)
    assert "TWO_DIGIT_YEARS" in codes(p)


# --- end to end ------------------------------------------------------------------


@pytest.fixture
def upgraded(ingest) -> tuple[Any, dict[str, Any]]:
    run_id = ingest("profile_upgrades.csv")
    profile_run(run_id)
    return run_id, load_profile(run_id)


def test_partial_rows_and_config_in_summary(upgraded) -> None:
    _, result = upgraded
    partial = result["summary"]["partial_rows"]
    assert (partial["min_missing_cells"], partial["count"], partial["examples"]) == (4, 2, [244, 245])
    assert [6, 2] in partial["histogram"]
    assert any(i["code"] == "PARTIAL_ROWS" for i in result["issues"])
    profiler = result["summary"]["profiler"]
    assert (profiler["version"], profiler["config_hash"]) == (PROFILER_VERSION, config_hash())
    assert profiler["config"]["dominant_threshold"] == PROFILE_CONFIG.dominant_threshold


def test_upgrade_fixture_issues_end_to_end(upgraded) -> None:
    _, result = upgraded
    found = {(i["column"], i["code"]) for i in result["issues"]}
    assert {
        ("state", "TYPO_VARIANTS"), ("state", "NORMALIZATION_VARIANTS"), ("opened", "SENTINEL_VALUES"),
        ("hours", "SENTINEL_VALUES"), ("code", "LEADING_ZEROS"), ("note", "HIDDEN_CHARACTERS"), ("note", "MOJIBAKE"),
    } <= found  # fmt: skip
    flag = next(c for c in result["columns"] if c["column"] == "flag")
    assert flag["type_inference"]["dominant_type"] == "boolean"


def test_profiling_twice_is_identical_and_within_budget(upgraded) -> None:
    run_id, first = upgraded
    profile_run(run_id)
    second = load_profile(run_id)
    assert json.dumps(second, sort_keys=True) == json.dumps(first, sort_keys=True)
    assert all(profile_size(c) <= PROFILE_CONFIG.max_profile_bytes for c in second["columns"])


# --- generic ---------------------------------------------------------------------

PROFILER_MODULES = ["profiling.py", "profile_columns.py", "profile_values.py", "profile_dates.py",
                    "profile_issues.py", "profile_config.py"]  # fmt: skip


def test_profiler_code_has_no_dataset_column_names() -> None:
    """No string literal in the profiler equals a column name of the real dataset."""
    raw = REPO_ROOT / "data" / "raw_tickets.csv"
    if not raw.exists():
        pytest.skip("data/raw_tickets.csv not present")
    with raw.open(encoding="utf-8-sig", newline="") as f:
        columns = {c.strip().lower() for c in next(csv.reader(f))}
    package = Path(__file__).parents[1] / "pipeline"
    literals = {
        node.value.strip().lower()
        for module in PROFILER_MODULES
        for node in ast.walk(ast.parse((package / module).read_text()))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    # context tags are a fixed vocabulary (pipeline/context.py), not dataset column names
    assert not columns & literals - set(TAGS)
