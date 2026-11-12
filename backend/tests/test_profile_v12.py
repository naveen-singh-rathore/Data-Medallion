"""Profiler v1.2: date-order evidence, duplicate stats, key-repeat hints, examples on every
issue, number kinds + numeric_after_cleanup, zero_share."""

from datetime import datetime
from typing import Any

import pandas as pd
import pytest

from pipeline.profile_columns import profile_column
from pipeline.profile_numbers import cleanup_number, number_kinds
from pipeline.profiling import load_profile, profile_run


def profile(values: list[str]) -> dict[str, Any]:
    return profile_column("col", 1, pd.Series(values, dtype=object), seed=1, reference_time=datetime(2026, 1, 1))


@pytest.fixture
def profiled(ingest):
    def _profiled(name: str) -> dict[str, Any]:
        run_id = ingest(name)
        profile_run(run_id)
        return load_profile(run_id)

    return _profiled


def test_date_order_has_format_pairs_and_unambiguous_share(profiled) -> None:
    (order,) = profiled("profile_messy.csv")["summary"]["date_order"]
    # K-02 (ISO with T on both sides) and K-05 (ISO vs ISO) violate; K-04 "03/04/2024" is ambiguous -> excluded
    assert (order["count"], order["compared"]) == (2, 21)
    assert (order["unambiguous_compared"], order["unambiguous_share"]) == (20, 0.1)
    assert order["format_pairs"] == [["YYYY-MM-DD", "YYYY-MM-DD", 1, 18], ["YYYY-MM-DDTHH:MM:SS", "YYYY-MM-DDTHH:MM:SS", 1, 1]]


def test_duplicate_stats_reported_even_when_zero(profiled) -> None:
    summary = profiled("key_history.csv")["summary"]  # no duplicate rows
    assert (summary["exact_duplicate_rows"], summary["near_duplicate_rows"]) == (0, 0)
    assert summary["exact_duplicate_examples"] == [] and summary["near_duplicate_examples"] == []
    assert isinstance(summary["key_with_repeats"], list)


def test_key_repeat_with_only_status_and_time_changes_is_update_history(profiled) -> None:
    result = profiled("key_history.csv")
    (key,) = [k for k in result["summary"]["key_with_repeats"] if k["column"] == "id"]
    assert (key["repeated_values"], key["hint"], key["hint_counts"]) == (1, "update_history", {"update_history": 1})
    assert key["differing_columns"] == [["state", 1.0], ["updated_at", 1.0]]
    assert key["examples"][0]["differs"] == {"state": ["Open", "Closed"], "updated_at": ["2024-01-01", "2024-01-05"]}
    (issue,) = [i for i in result["issues"] if i["code"] == "KEY_WITH_REPEATS" and i["column"] == "id"]
    assert issue["severity"] == "medium"


def test_every_issue_has_examples(profiled) -> None:
    for fixture in ("profile_messy.csv", "profile_upgrades.csv", "key_history.csv"):
        for issue in profiled(fixture)["issues"]:
            assert issue["evidence"].get("examples"), (fixture, issue["code"], issue["column"])


def test_number_kinds_and_cleanup() -> None:
    kinds = {k: (n, ex) for k, n, ex in number_kinds(["$40", "₹1,200", "45%", "1.234,56", "(40)", "3 hrs", "40 USD", "1e3"])}
    assert {k: n for k, (n, _) in kinds.items()} == {
        "currency_prefix": 2, "thousands_separator": 1, "percent": 1, "european_decimal": 1,
        "parentheses_negative": 1, "unit_suffix": 1, "currency_suffix": 1, "scientific": 1}  # fmt: skip
    assert kinds["unit_suffix"][1] == "3 hrs"
    assert [cleanup_number(v) for v in ["$1,067.53", "(40)", "€3,50", "45%", "3 hrs", "TBD"]] == [1067.53, -40, 3.5, 45, 3, None]
    p = profile(["10", "12", "$40", "1,200", "45%", "error", "7", "8", "9", "11"])
    assert p["numeric"]["numeric_after_cleanup"] == 0.9
    assert [k[0] for k in p["numeric"]["number_kinds"]] == ["currency_prefix", "percent", "thousands_separator"]


def test_zero_share_is_a_fact_on_numeric_columns() -> None:
    assert profile(["0", "0", "5", "7"])["numeric"]["zero_share"] == 0.5
    assert profile(["1", "2", "3"])["numeric"]["zero_share"] == 0.0


def test_examples_on_outliers_mixed_types_and_whitespace() -> None:
    outliers = profile([str(n) for n in range(1, 41)] + ["500", "-300"])
    assert outliers["numeric"]["outlier_examples"] == [500.0, -300.0]
    mixed = profile(["1", "2", "3", "4", "5", "6", "a", "b", "c", "d"])
    assert mixed["type_inference"]["examples"] == {"integer": "1", "text": "a"}
    padded = profile(["x", " x", "y "])
    assert padded["shape"]["ws_examples"] == ["' x'", "'y '"]
