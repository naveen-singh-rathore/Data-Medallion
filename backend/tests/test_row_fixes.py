"""Row-level fixes: swapped columns, junk rows, self-declared duplicates (schema, compiler, build)."""

import pytest
from psycopg.rows import dict_row
from pydantic import ValidationError

from pipeline.profiling import profile_run
from pipeline.silver import build
from pipeline.silver.compiler import compile_rules
from pipeline.silver.rules_schema import ColumnRule, RowCheck, SilverRules, to_yaml
from pipeline.silver.validator import validate_rules

WHY = "Needed for the row fix tests in this file."
TARGET = "silver.test_row_fixes"


def text(name: str, **kw) -> ColumnRule:
    return ColumnRule(name=name, source=name, type="text", steps=["trim"], confidence=0.9, why=WHY, **kw)


def rules_for(run_id: str, checks: list[RowCheck]) -> SilverRules:
    return SilverRules(source_run_id=run_id, generated_by="human", target_table=TARGET, row_checks=checks, columns=[
        text("id"), text("category", mapping={"hvac": ["HVAC", "AC"], "plumbing": ["Plumbing"]}), text("description"),
        text("notes"), ColumnRule(name="amount", source="amount", type="numeric", confidence=0.9, why=WHY), text("owner"),
    ])  # fmt: skip


SWAP = RowCheck(kind="swapped_columns", params={"a": "category", "b": "description"}, action="swap", why=WHY)
JUNK = RowCheck(kind="junk_row", params={"id": "id", "id_pattern": "^T-[0-9]+$", "min_empty_share": "0.6"},
                action="reject", why=WHY)  # fmt: skip
DUP = RowCheck(kind="declared_duplicate", action="duplicate", why=WHY,
               params={"source": "notes", "pattern": "(?i)duplicate of ticket #?([0-9]+)", "id": "id", "prefix": "T-"})  # fmt: skip


@pytest.fixture
def run(ingest, conn):
    run_id = str(ingest("row_fixes.csv"))
    profile_run(run_id)
    yield run_id
    conn.execute(f"DROP VIEW IF EXISTS {TARGET}_valid")
    conn.execute(f"DROP TABLE IF EXISTS {TARGET}, {TARGET}__build")


def rows(conn, rules: SilverRules, run_id: str) -> dict[str, dict]:
    compiled = compile_rules(rules, run_id)
    out = conn.cursor(row_factory=dict_row).execute(compiled.query, compiled.params).fetchall()
    payload = {r["bronze_id"]: r["payload"] for r in conn.execute(
        "SELECT bronze_id, payload FROM bronze.records WHERE run_id = %s", (run_id,)).fetchall()}  # fmt: skip
    return {payload[r["bronze_id"]]["id"]: r for r in out}


def test_swapped_columns_are_swapped_back_only_when_the_label_is_known(run, conn) -> None:
    got = rows(conn, rules_for(run, [SWAP]), run)
    t2 = got["T-2"]
    assert (t2["category"], t2["description"]) == ("hvac", "The AC unit on floor 2 is not working at all today")
    assert "swapped:category,description" in t2["_quality_flags"]["category"]
    assert "swapped:category,description" in t2["_quality_flags"]["description"]
    assert t2["category_raw"].startswith("The AC unit") and t2["description_raw"] == "HVAC"  # lineage keeps the original
    assert t2["_errors"] == []  # a fix, not an error
    t6 = got["T-6"]  # long text, but the description is no known label: left alone
    assert t6["category"].startswith("Another long sentence") and "swapped" not in str(t6["_quality_flags"])
    assert got["T-1"]["category"] == "hvac" and "swapped" not in str(got["T-1"]["_quality_flags"])


def test_junk_rows_are_rejected_for_emptiness_or_a_bad_id(run, conn) -> None:
    got = rows(conn, rules_for(run, [JUNK]), run)
    assert got["T-4"]["_rejected"] and got["T-4"]["_errors"] == ["row:junk_row:id"]  # 5 of 6 cells empty
    assert got["X99"]["_rejected"]  # id does not match ^T-[0-9]+$
    assert not any(got[t]["_rejected"] for t in ("T-1", "T-2", "T-3", "T-6", "T-7"))


def test_declared_duplicates_point_at_their_ticket_and_leave_the_valid_rows(run, conn, tmp_path) -> None:
    rules = rules_for(run, [SWAP, JUNK, DUP])
    got = rows(conn, rules, run)
    assert (got["T-3"]["_duplicate_of"], got["T-7"]["_duplicate_of"]) == ("T-1", "T-3")  # "#1" and "Ticket 3"
    assert "declared_duplicate" in got["T-3"]["_quality_flags"]["notes"] and got["T-3"]["_errors"] == []
    assert got["T-1"]["_duplicate_of"] is None
    path = tmp_path / "rules.yaml"
    path.write_text(to_yaml(rules))
    b = build.build_silver(run, path)
    assert (b["rows_kept"], b["rows_rejected"], b["rows_duplicate"]) == (3, 2, 2)
    valid = {r["id"] for r in conn.execute(f"SELECT id FROM {TARGET}_valid").fetchall()}
    assert valid == {"T-1", "T-2", "T-6"}


def test_new_kinds_are_validated() -> None:
    with pytest.raises(ValidationError, match="use 'swap'"):
        RowCheck(kind="swapped_columns", params={"a": "x", "b": "y"}, action="flag", why=WHY)
    with pytest.raises(ValidationError, match="does not fit"):
        RowCheck(kind="date_order", params={"a": "x", "b": "y"}, action="duplicate", why=WHY)
    with pytest.raises(ValidationError, match="exactly one capture group"):
        RowCheck(kind="declared_duplicate", action="duplicate", why=WHY,
                 params={"source": "n", "pattern": "dup", "id": "id", "prefix": ""})  # fmt: skip
    with pytest.raises(ValidationError, match="min_empty_share"):
        RowCheck(kind="junk_row", params={"id": "id", "id_pattern": "^x$", "min_empty_share": "2"}, action="reject", why=WHY)


def test_swap_needs_a_mapping_on_the_label_column(run) -> None:
    rules = rules_for(run, [RowCheck(kind="swapped_columns", params={"a": "description", "b": "category"}, action="swap", why=WHY)])
    report = validate_rules(rules, run)
    assert not report.ok and "needs a standard-value mapping" in " ".join(report.errors)
