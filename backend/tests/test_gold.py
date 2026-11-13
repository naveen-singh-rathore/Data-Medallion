"""Gold: metric contracts on a fixture with hand-computed answers, reconciliation, atomic swap, rebuild."""

import json
from decimal import Decimal

import pytest

from pipeline.context import ensure_context
from pipeline.gold import build as gold
from pipeline.profiling import profile_run
from pipeline.silver import build as silver
from pipeline.silver.rules_schema import ColumnRule, RowCheck, SilverRules, to_yaml

SILVER = "silver.test_gold_clean"
SCHEMA = "test_gold"
WHY = "Needed for the gold metrics in this test."


def col(name: str, type_: str = "text", **kw) -> ColumnRule:
    return ColumnRule(name=name, source=name, type=type_, steps=["trim"], confidence=0.9, why=WHY, **kw)


@pytest.fixture
def gold_run(ingest, conn, tmp_path):
    run_id = str(ingest("gold_small.csv"))
    profile_run(run_id)
    tags = {"ticket_id": "id", "created_at": "timestamp", "resolved_at": "timestamp", "category": "category",
            "priority": "priority", "status": "status", "building": "location", "cost": "money", "sla_hours": "duration"}  # fmt: skip
    ensure_context(run_id, json.dumps({"domain": "tickets", "summary": "One row per ticket.", "column_tags": tags}))
    stamps = ["YYYY-MM-DD HH:MM:SS"]
    rules = SilverRules(source_run_id=run_id, generated_by="human", target_table=SILVER, columns=[
        col("ticket_id", required=True, invalid_action="reject"),
        col("created_at", "timestamp", parse_formats=stamps), col("resolved_at", "timestamp", parse_formats=stamps),
        col("category", mapping={"hvac": [], "plumbing": [], "it_network": ["Network"]}),
        col("priority").model_copy(update={"steps": ["trim", "lower"]}),
        col("status").model_copy(update={"steps": ["trim", "lower"]}), col("building"),
        col("cost", "numeric", null_tokens=["n/a"]).model_copy(update={"steps": ["trim", "strip_currency"]}),
        col("sla_hours", "integer"),
    ], row_checks=[RowCheck(kind="date_order", params={"a": "created_at", "b": "resolved_at"}, action="flag", why=WHY)])  # fmt: skip
    path = tmp_path / "silver_rules.yaml"
    path.write_text(to_yaml(rules))
    silver.build_silver(run_id, path)
    yield run_id, path
    for schema in (SCHEMA, f"{SCHEMA}_staging"):
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.execute(f"DROP VIEW IF EXISTS {SILVER}_valid")
    conn.execute(f"DROP TABLE IF EXISTS {SILVER}")


def rows(conn, query: str) -> list[dict]:
    return conn.execute(query.replace("G.", f"{SCHEMA}.")).fetchall()


def test_roles_come_from_tags_and_the_date_order_check(gold_run) -> None:
    run_id, path = gold_run
    rules = SilverRules.model_validate(__import__("yaml").safe_load(path.read_text()))
    from pipeline.context import get_context
    from pipeline.privacy import context_tags

    roles = gold.resolve_roles(rules, context_tags(get_context(__import__("uuid").UUID(run_id))), {})
    assert roles == {"id": "ticket_id", "opened": "created_at", "closed": "resolved_at", "category": "category",
                     "priority": "priority", "status": "status", "location": "building", "money": "cost", "sla": "sla_hours"}  # fmt: skip
    with pytest.raises(gold.GoldError, match="missing"):
        gold.resolve_roles(rules, {}, {})


def test_metric_contracts_match_hand_computed_answers(gold_run, conn) -> None:
    run_id, path = gold_run
    b = gold.build_gold(run_id, SCHEMA, approved_path=path)
    assert b["status"] == "succeeded" and all(c["ok"] for c in b["checks"].values())
    fact = {r["ticket_id"]: r for r in rows(conn, "SELECT * FROM G.fact_tickets")}
    assert len(fact) == 8 and str(fact["T1"]["silver_build_id"]) == str(silver.latest_build(run_id)["build_id"])
    assert {t: r["exclusion_reason"] for t, r in fact.items() if r["exclusion_reason"]} == {
        "T3": "not_resolved",
        "T5": "not_resolved",
        "T4": "resolved_before_created",
        "T7": "missing_created_at",
    }
    assert (
        fact["T7"]["category"] == "it_network"
        and fact["T2"]["category"] == "hvac"
        and fact["T1"]["category_source"] == "rule"
    )
    assert (fact["T2"]["resolution_hours"], fact["T2"]["sla_met"], fact["T1"]["sla_met"], fact["T6"]["sla_met"]) == (
        48,
        False,
        True,
        None,
    )
    assert fact["T4"]["sla_met"] is None and fact["T4"]["resolution_hours"] < 0  # negative duration kept, never used

    sla = {(str(r["month"])[:7] if r["month"] else None, r["priority"], r["category"]): r
           for r in rows(conn, "SELECT * FROM G.resolution_sla")}  # fmt: skip
    jan = sla[("2024-01", "high", "hvac")]
    assert (jan["tickets"], jan["resolved"], jan["usable"], jan["excluded_count"], jan["coverage_pct"]) == (
        3,
        3,
        3,
        0,
        Decimal("100.0"),
    )
    assert (jan["median_resolution_hours"], jan["avg_resolution_hours"], jan["p90_resolution_hours"]) == (
        Decimal("20.00"),
        Decimal("26.00"),
        Decimal("42.40"),
    )
    assert (jan["sla_measured"], jan["sla_met_rate_pct"], jan["escalation_rate_pct"]) == (
        3,
        Decimal("66.7"),
        Decimal("0.0"),
    )
    feb_low = sla[("2024-02", "low", "plumbing")]
    assert (feb_low["tickets"], feb_low["usable"], feb_low["median_resolution_hours"], feb_low["sla_met_rate_pct"],
            feb_low["escalation_rate_pct"]) == (2, 1, Decimal("6.00"), None, Decimal("50.0"))  # fmt: skip
    feb_high = sla[("2024-02", "high", "plumbing")]
    assert (feb_high["excluded_count"], feb_high["coverage_pct"], feb_high["median_resolution_hours"]) == (
        1,
        Decimal("0.0"),
        None,
    )
    assert sla[(None, "high", "it_network")]["excluded_count"] == 1

    vol = {(str(r["month"])[:7] if r["month"] else None, r["category"], r["building"]): r
           for r in rows(conn, "SELECT * FROM G.volume_backlog")}  # fmt: skip
    pick = lambda k: (vol[k]["opened"], vol[k]["closed"], vol[k]["closed_date_unknown"], vol[k]["open_at_month_end"])
    assert pick(("2024-01", "hvac", "B1")) == (3, 3, 0, 0)
    assert pick(("2024-01", "hvac", "B2")) == (1, 0, 0, 1) and pick(("2024-02", "hvac", "B2")) == (0, 0, 0, 1)
    assert pick(("2024-02", "plumbing", "B1")) == (1, 0, 1, 0)
    assert pick(("2024-02", "plumbing", "B2")) == (2, 1, 0, 1)
    assert pick((None, "it_network", "B1")) == (1, 0, 1, None)
    assert sum(r["opened"] for r in vol.values()) == 8

    cost = {(r["category"], r["building"]): r for r in rows(conn, "SELECT * FROM G.cost_summary")}
    h1 = cost[("hvac", "B1")]
    assert (
        h1["tickets"],
        h1["tickets_with_cost"],
        h1["total_cost"],
        h1["avg_cost"],
        h1["median_cost"],
        h1["cost_coverage_pct"],
    ) == (3, 3, Decimal(600), Decimal("200.00"), Decimal("200.00"), Decimal("100.0"))
    assert (cost[("plumbing", "B2")]["total_cost"], cost[("plumbing", "B2")]["cost_coverage_pct"]) == (
        Decimal(25),
        Decimal("50.0"),
    )
    assert (cost[("hvac", "B2")]["total_cost"], cost[("hvac", "B2")]["avg_cost"]) == (Decimal(0), None)

    findings = {r["finding"]: r for r in rows(conn, "SELECT * FROM G.data_quality_findings")}
    assert findings["cost_missing"]["rows_affected"] == 2 and findings["time_metric_exclusions"]["rows_affected"] == 2
    assert "resolution_dates_unreliable" not in findings  # too few pairs to judge


def test_failed_build_leaves_previous_gold_untouched_and_rebuild_is_identical(gold_run, conn, monkeypatch) -> None:
    run_id, path = gold_run
    first = gold.build_gold(run_id, SCHEMA, approved_path=path)
    before = rows(conn, "SELECT ticket_id, category, resolution_hours, sla_met FROM G.fact_tickets ORDER BY ticket_id")
    second = gold.build_gold(run_id, SCHEMA, approved_path=path)
    assert second["build_id"] != first["build_id"] and second["row_counts"] == first["row_counts"]
    assert (
        rows(conn, "SELECT ticket_id, category, resolution_hours, sla_met FROM G.fact_tickets ORDER BY ticket_id")
        == before
    )

    real = gold._checks
    monkeypatch.setattr(gold, "_checks", lambda c, s, n: real(c, s, n + 1))  # silver count off by one
    with pytest.raises(gold.GoldError, match="reconciliation failed: fact_rows = silver valid rows"):
        gold.build_gold(run_id, SCHEMA, approved_path=path)
    assert gold.latest_gold(run_id)["status"] == "failed"
    assert (
        rows(conn, "SELECT ticket_id, category, resolution_hours, sla_met FROM G.fact_tickets ORDER BY ticket_id")
        == before
    )
    assert conn.execute(f"SELECT to_regclass('{SCHEMA}_staging.fact_tickets') AS t").fetchone()["t"] is None


def test_gold_needs_a_current_silver_build(gold_run, tmp_path) -> None:
    run_id, path = gold_run
    stale = path.read_text().replace(
        "why: Needed for the gold metrics in this test.", "why: Needed for the gold metrics, edited.", 1
    )
    edited = tmp_path / "edited.yaml"
    edited.write_text(stale)
    with pytest.raises(gold.GoldError, match="no current silver build"):
        gold.build_gold(run_id, SCHEMA, approved_path=edited)


def test_values_outside_a_standard_list_are_unknown_in_gold(gold_run, conn) -> None:
    run_id, path = gold_run
    rules = SilverRules.model_validate(__import__("yaml").safe_load(path.read_text()))
    cols = [c.model_copy(update={"mapping": {"high": []}}) if c.name == "priority" else c for c in rules.columns]
    path.write_text(to_yaml(rules.model_copy(update={"columns": cols})))
    silver.build_silver(run_id, path)
    gold.build_gold(run_id, SCHEMA, approved_path=path)
    prio = {r["ticket_id"]: r["priority"] for r in rows(conn, "SELECT ticket_id, priority FROM G.fact_tickets")}
    assert prio["T1"] == "high" and prio["T3"] is None  # 'low' is not in the approved list
    assert {r["priority"] for r in rows(conn, "SELECT DISTINCT priority FROM G.resolution_sla")} == {
        "high",
        "(unknown)",
    }
