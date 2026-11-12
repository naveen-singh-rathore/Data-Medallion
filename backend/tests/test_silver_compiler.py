"""Compiler: golden SQL, literal quoting, dates, sentinels, mapping, safe casts."""

import datetime as dt
import os
from pathlib import Path

import pytest
from psycopg.rows import dict_row
from silver_helpers import column, small_rules

from pipeline.profile_config import PROFILE_CONFIG
from pipeline.profiling import profile_run
from pipeline.silver.compiler import compile_rules
from pipeline.silver.rules_schema import SilverRules

GOLDEN = Path(__file__).parent / "golden" / "compiled_small.sql"
FIXED_RUN = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def silver_run(ingest) -> str:
    run_id = ingest("silver_small.csv")
    profile_run(run_id)
    return str(run_id)


def run_rules(conn, raw: dict, run_id: str) -> dict[str, dict]:
    compiled = compile_rules(SilverRules.model_validate(raw), run_id)
    rows = conn.cursor(row_factory=dict_row).execute(compiled.query, compiled.params).fetchall()
    ids = conn.execute("SELECT bronze_id, payload ->> 'id' AS id FROM bronze.records WHERE run_id = %s", (run_id,)).fetchall()
    by_bronze = {r["bronze_id"]: r["id"] for r in ids}
    return {by_bronze[r["bronze_id"]]: r for r in rows}


def test_golden_sql() -> None:
    text = compile_rules(SilverRules.model_validate(small_rules(FIXED_RUN)), FIXED_RUN).text + "\n"
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.write_text(text)
    assert text == GOLDEN.read_text()


def test_rule_strings_are_always_quoted() -> None:
    evil = "x'); DROP TABLE bronze.records; --"
    raw = small_rules(FIXED_RUN)
    raw["columns"][4]["null_tokens"] = [evil]
    raw["columns"][4]["sentinels"] = [evil]
    raw["columns"][1]["mapping"]["open"].append(evil)
    text = compile_rules(SilverRules.model_validate(raw), FIXED_RUN).text
    assert evil not in text  # only the escaped form appears
    assert evil.replace("'", "''") in text


def test_values_sentinels_mapping_and_flags(conn, silver_run) -> None:
    rows = run_rules(conn, small_rules(silver_run), silver_run)
    assert rows["S-01"]["amount"] == 40 and rows["S-09"]["amount"] == 1200
    assert rows["S-03"]["amount"] is None and rows["S-03"]["_quality_flags"]["amount"] == ["sentinel:999"]
    assert rows["S-03"]["amount_raw"] == "999"
    assert rows["S-04"]["amount"] is None and rows["S-04"]["_quality_flags"]["amount"] == ["null_token"]
    assert rows["S-05"]["amount"] is None and "amount:unparseable" in rows["S-05"]["_errors"]
    assert {rows[k]["state"] for k in ("S-01", "S-02", "S-03")} == {"open"}
    assert rows["S-04"]["state"] == rows["S-05"]["state"] == "in_progress"  # In-Progress / in progress
    assert rows["S-04"]["_quality_flags"]["state"] == ["mapped"]
    assert rows["S-01"]["flag"] is True and rows["S-02"]["flag"] is False
    assert rows["S-12"]["closed"] is None and not rows["S-12"]["_errors"]


@pytest.mark.parametrize(("order", "expected"), [("mdy", dt.date(2024, 3, 4)), ("dmy", dt.date(2024, 4, 3))])
def test_day_month_order(conn, silver_run, order, expected) -> None:
    assert run_rules(conn, small_rules(silver_run, order=order), silver_run)["S-03"]["opened"] == expected


def test_row_checks_and_dedup(conn, silver_run) -> None:
    raw = small_rules(silver_run, row_checks=[{"kind": "date_order", "params": {"a": "opened", "b": "closed"},
                                               "action": "null_second", "why": "Closed before opened is impossible."}],
                      dedup={"key": ["state"], "strategy": "keep_latest", "order_by": "opened", "why": "x" * 25})  # fmt: skip
    rows = run_rules(conn, raw, silver_run)
    failed = sorted(k for k, r in rows.items() if "row:date_order:opened,closed" in r["_errors"])
    assert failed == ["S-02", "S-07"]
    assert rows["S-02"]["closed"] is None and "nulled_by_row_check" in rows["S-02"]["_quality_flags"]["closed"]
    assert sum(r["_dup_rank"] == 1 for r in rows.values()) == 3  # one row per state


def test_safe_casts_never_raise(conn) -> None:
    row = conn.execute(
        "SELECT silver.try_numeric('1e999999'), silver.try_int('99999999999999999999'), silver.try_numeric('abc'),"
        " silver.try_ts('2024-02-30', '\\d{4}-\\d{1,2}-\\d{1,2}', 'YYYY-MM-DD'),"
        " silver.try_ts('2024-00-10', '\\d{4}-\\d{1,2}-\\d{1,2}', 'YYYY-MM-DD'),"
        " silver.try_ts('0000-00-00', '\\d{4}-\\d{1,2}-\\d{1,2}', 'YYYY-MM-DD'), silver.try_bool(NULL, '{y}', '{n}')"
    ).fetchone()
    assert all(v is None for v in row.values())


def test_sql_numeric_date_ranges_match_profiler_config() -> None:
    text = (Path(__file__).parents[2] / "db" / "sql" / "007_silver_functions.sql").read_text()
    low, high = PROFILE_CONFIG.epoch_range
    assert f"BETWEEN {low} AND {high}" in text
    assert "BETWEEN {} AND {}".format(*PROFILE_CONFIG.excel_serial_range) in text


def test_sample_is_deterministic(conn, silver_run) -> None:
    compiled = compile_rules(SilverRules.model_validate(small_rules(silver_run)), silver_run, sample=5)
    first = [r["bronze_id"] for r in conn.execute(compiled.query, compiled.params)]
    assert len(first) == 5 and first == [r["bronze_id"] for r in conn.execute(compiled.query, compiled.params)]


def test_unknown_column_type_combination_compiles_text_only() -> None:
    raw = small_rules(FIXED_RUN)
    raw["columns"] = [column(name="id", source="id", type="text")]
    assert "silver.try_" not in compile_rules(SilverRules.model_validate(raw), FIXED_RUN).text
