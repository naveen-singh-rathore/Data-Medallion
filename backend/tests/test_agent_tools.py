"""Agent tools: bounded, masked, deterministic, logged, read-only."""

import json

import psycopg
import pytest

from pipeline.agent_tools import TOOLS, run_tool, tool_schemas
from pipeline.context import ensure_context
from pipeline.db import get_ro_conn
from pipeline.profiling import profile_run

PEOPLE = ["Alice Moreno", "Bilal Khan", "Chen Wei"]
CALLS = {
    "list_issues": {},
    "get_column_profile": {"column": "owner"},
    "get_value_counts": {"column": "owner"},
    "sample_rows": {"n": 10},
    "check_regex": {"column": "owner", "pattern": "^[A-Z][a-z]+ [A-Z][a-z]+$"},
    "test_rule": {"column_rule": {"name": "owner", "source": "owner", "type": "text", "steps": ["trim", "upper"],
                                  "confidence": 0.9, "why": "Owner names in one case for grouping."}},
    "check_cross_column": {"a": "opened", "b": "closed", "a_formats": ["YYYY-MM-DD", "MM/DD/YYYY"],
                           "b_formats": ["YYYY-MM-DD"], "day_month_order": "mdy"},
}  # fmt: skip


@pytest.fixture
def tagged_run(ingest) -> str:
    """silver_small with a human context that tags `owner` as a person."""
    run_id = ingest("silver_small.csv")
    profile_run(run_id)
    ensure_context(run_id, '{"domain": "test", "summary": "x", "column_tags": {"owner": "person"}}')
    return str(run_id)


def test_every_tool_is_registered_with_a_schema() -> None:
    assert set(CALLS) == set(TOOLS)
    assert all(s["input_schema"]["type"] == "object" for s in tool_schemas())


def test_outputs_are_bounded_masked_and_deterministic(tagged_run) -> None:
    for name, args in CALLS.items():
        first = run_tool(name, args, tagged_run)
        assert first["ok"], (name, first)
        text = json.dumps(first, ensure_ascii=False)
        assert len(text.encode()) <= 2048 + 200, name  # result <= 2 KB plus the envelope
        assert not [p for p in PEOPLE if p in text or p.upper() in text], name
        assert run_tool(name, args, tagged_run) == first, name


def test_errors_are_returned_and_calls_logged(conn, tagged_run) -> None:
    bad = run_tool("get_column_profile", {"column": "nope"}, tagged_run)
    assert not bad["ok"] and "unknown column 'nope'" in bad["error"]
    assert not run_tool("check_regex", {"column": "owner", "pattern": "("}, tagged_run)["ok"]
    assert "invalid arguments" in run_tool("sample_rows", {"n": 99}, tagged_run)["error"]
    rows = conn.execute(
        "SELECT tool, ok, result_bytes FROM meta.tool_calls WHERE run_id = %s", (tagged_run,)
    ).fetchall()
    assert len(rows) == 3 and [r["ok"] for r in rows] == [False, False, False] and all(r["result_bytes"] for r in rows)


@pytest.mark.parametrize("statement", [("INSERT INTO meta.tool_calls (call_id, tool, args, ok) VALUES"
                                       " (gen_random_uuid(), 'x', '{}', true)"), "UPDATE bronze.records SET row_hash = ''",
                                       "DELETE FROM meta.ingestion_runs"])  # fmt: skip
def test_read_only_role_cannot_write(statement) -> None:
    with pytest.raises(psycopg.errors.Error), get_ro_conn() as conn:
        conn.execute(statement)


def test_read_only_role_lacks_write_privileges(conn) -> None:
    row = conn.execute("SELECT has_table_privilege('pipeline_ro', 'bronze.records', 'INSERT') AS i,"
                       " has_table_privilege('pipeline_ro', 'meta.ingestion_runs', 'DELETE') AS d,"
                       " has_table_privilege('pipeline_ro', 'bronze.records', 'SELECT') AS s").fetchone()  # fmt: skip
    assert (row["i"], row["d"], row["s"]) == (False, False, True)
