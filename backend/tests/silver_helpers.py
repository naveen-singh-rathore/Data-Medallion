"""Shared rule builders for silver tests (fixture: tests/fixtures/silver_small.csv)."""

from typing import Any

WHY = "Test rule: keeps the fixture's values comparable for the assertions."
DATES = ["YYYY-MM-DD", "MM/DD/YYYY"]


def column(**fields: Any) -> dict[str, Any]:
    return {"steps": ["trim"], "confidence": 0.9, "why": WHY} | fields


def small_rules(run_id: str, order: str = "mdy", **extra: Any) -> dict[str, Any]:
    return {
        "source_run_id": run_id, "generated_by": "human",
        "columns": [
            column(name="id", source="id", type="text", required=True),
            column(name="state", source="state", type="text", mapping={"open": ["OPEN"], "in_progress": ["in progress"],
                                                                      "closed": []}, unmapped_action="invalid"),
            column(name="opened", source="opened", type="date", parse_formats=DATES, day_month_order=order),
            column(name="closed", source="closed", type="date", parse_formats=DATES, day_month_order=order),
            column(name="amount", source="amount", type="numeric", steps=["trim", "strip_currency"],
                   null_tokens=["n/a"], sentinels=["999"]),
            column(name="flag", source="flag", type="boolean", boolean_true=["yes"], boolean_false=["no"]),
        ],
    } | extra  # fmt: skip
