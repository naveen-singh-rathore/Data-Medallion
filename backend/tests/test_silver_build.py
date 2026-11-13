"""build-silver: approved rules -> a stored, reconciled silver table (own target table in tests)."""

import pytest

from pipeline.profiling import profile_run
from pipeline.silver import build
from pipeline.silver.defaults import default_rules
from pipeline.silver.rules_schema import ColumnRule, RowCheck, to_yaml

TARGET = "silver.test_records_clean"


@pytest.fixture
def approved(ingest, conn, tmp_path):
    """silver_small with approved rules (amount cleaned, a flagging row check) in tmp_path."""
    run_id = str(ingest("silver_small.csv"))
    profile_run(run_id)
    rules = default_rules(run_id)
    amount = ColumnRule(name="amount", source="amount", type="numeric", steps=["trim", "strip_currency"],
                        null_tokens=["n/a"], sentinels=["999"], confidence=0.8, generated_by="agent",
                        why="Amounts must be numbers to be summed.")  # fmt: skip
    opened = ColumnRule(name="opened", source="opened", type="date", steps=["trim"], confidence=0.8,
                        parse_formats=["YYYY-MM-DD", "MM/DD/YYYY"], day_month_order="mdy", why="Ticket open date for durations.")  # fmt: skip
    closed = ColumnRule(name="closed", source="closed", type="date", steps=["trim"], confidence=0.8,
                        parse_formats=["YYYY-MM-DD"], why="Ticket close date for durations.")  # fmt: skip
    check = RowCheck(kind="date_order", params={"a": "opened", "b": "closed"}, action="flag",
                     why="A ticket cannot close before it opens.")  # fmt: skip
    keep = [c for c in rules.columns if c.source not in ("amount", "opened", "closed")]
    rules = rules.model_copy(update={"columns": [*keep, amount, opened, closed], "row_checks": [check],
                                     "target_table": TARGET, "generated_by": "human"})  # fmt: skip
    path = tmp_path / "silver_rules.yaml"
    path.write_text(to_yaml(rules))
    yield run_id, path, rules
    conn.execute(f"DROP VIEW IF EXISTS {TARGET}_valid")
    conn.execute(f"DROP TABLE IF EXISTS {TARGET}, {TARGET}__build")


def test_build_stores_every_row_with_status_lineage_and_types(approved, conn) -> None:
    run_id, path, rules = approved
    b = build.build_silver(run_id, path)
    assert b["status"] == "succeeded" and b["target_table"] == TARGET and b["rules_yaml"] == path.read_text()
    assert b["rows_in"] == 12 == b["rows_kept"] + b["rows_flagged"] + b["rows_rejected"] + b["rows_duplicate"]
    assert b["rows_flagged"] >= 1  # S-02 closes before it opens
    rows = {r["_source_line"]: r for r in conn.execute(f"SELECT * FROM {TARGET}").fetchall()}
    assert len(rows) == 12
    s01 = rows[2]
    assert float(s01["amount"]) == 40.0 and s01["amount_raw"] == "$40" and s01["_status"] == "kept"
    assert str(s01["opened"]) == "2024-03-01" and str(s01["_build_id"]) == str(b["build_id"])
    assert "cleaned" in s01["_quality_flags"]["amount"]
    s02 = rows[3]
    assert s02["_status"] == "flagged" and s02["_errors"] == ["row:date_order:opened,closed"]
    valid = conn.execute(f"SELECT count(*) AS n FROM {TARGET}_valid").fetchone()["n"]
    assert valid == b["rows_kept"] + b["rows_flagged"]
    assert b["column_stats"]["amount"]["changed"] >= 3 and b["column_stats"]["amount"]["raw_kept"] is True
    assert build.is_current(b, rules)


def test_rebuild_replaces_the_table_and_a_failed_build_leaves_it_untouched(approved, conn, monkeypatch) -> None:
    run_id, path, rules = approved
    first = build.build_silver(run_id, path)
    second = build.build_silver(run_id, path)
    assert second["build_id"] != first["build_id"] and build.is_current(second, rules) and not build.is_current(first, rules)
    monkeypatch.setattr(build, "STATUS_SQL", "'surprise'")  # every row gets an unknown status -> reconciliation fails
    with pytest.raises(build.BuildError, match="reconciliation failed"):
        build.build_silver(run_id, path)
    failed = build.latest_build(run_id)
    assert failed["status"] == "failed" and "reconciliation" in failed["error_message"]
    holder = conn.execute(f"SELECT DISTINCT _build_id FROM {TARGET}").fetchall()
    assert [str(h["_build_id"]) for h in holder] == [str(second["build_id"])]  # previous table intact
    assert conn.execute(f"SELECT to_regclass('{TARGET}__build') AS t").fetchone()["t"] is None


def test_only_approved_rules_of_the_same_run_are_built(approved, tmp_path) -> None:
    run_id, path, _ = approved
    with pytest.raises(build.BuildError, match="finish the review"):
        build.build_silver(run_id, tmp_path / "missing.yaml")
    with pytest.raises(build.BuildError, match="belong to run"):
        build.build_silver("00000000-0000-0000-0000-000000000000", path)
