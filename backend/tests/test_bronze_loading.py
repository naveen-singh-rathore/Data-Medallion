"""Idempotency, batching/fallback and row_hash integrity of bronze ingestion."""

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from pipeline import bronze

TEN = "ten_records.csv"  # records on lines 2, 3 and 10 are identical ("1,same")
TEN_PATH = Path(__file__).parent / "fixtures" / TEN


def _hashes(rows) -> set[tuple[int, str]]:
    return {(r["source_line"], r["row_hash"]) for r in rows}


# --- idempotency -------------------------------------------------------------


def test_same_file_twice_is_skipped(ingest, records) -> None:
    first = ingest(TEN)
    second = ingest(TEN)
    assert second == first
    assert len(records(first)) == 10


def test_force_reloads_without_duplicating(conn, ingest, records) -> None:
    first = ingest(TEN)
    second = ingest(TEN, force=True)
    assert second != first
    assert records(first) == []
    assert len(records(second)) == 10


def test_failure_mid_load_leaves_no_rows_and_retry_succeeds(conn, ingest, records, run_row, monkeypatch) -> None:
    real_load_batch = bronze.load_batch

    def failing_load_batch(conn, ctx, batch_no, rows):
        if batch_no == 2:
            raise RuntimeError("simulated crash")
        return real_load_batch(conn, ctx, batch_no, rows)

    monkeypatch.setattr(bronze, "load_batch", failing_load_batch)
    with pytest.raises(RuntimeError, match="simulated crash"):
        ingest(TEN, batch_size=3)

    failed = conn.execute(
        "SELECT run_id, status, error_message FROM meta.ingestion_runs WHERE file_sha256 = %s",
        (bronze.hash_file(TEN_PATH),),
    ).fetchone()
    assert failed["status"] == "failed"
    assert "simulated crash" in failed["error_message"]
    assert records(failed["run_id"]) == []

    monkeypatch.setattr(bronze, "load_batch", real_load_batch)
    retry = ingest(TEN, batch_size=3)
    assert run_row(retry)["status"] == "succeeded"
    assert run_row(failed["run_id"]) is None
    assert len(records(retry)) == 10


# --- batching ----------------------------------------------------------------


def test_batches_and_stats_sum_to_run_totals(conn, ingest, run_row) -> None:
    run_id = ingest(TEN, batch_size=3)
    batches = conn.execute(
        "SELECT * FROM meta.ingestion_batches WHERE run_id = %s ORDER BY batch_no", (run_id,)
    ).fetchall()
    run = run_row(run_id)
    assert [b["records"] for b in batches] == [3, 3, 3, 1]
    assert run["batches"] == 4
    assert sum(b["records"] for b in batches) == run["records_parsed"] == 10
    assert sum(b["loaded"] for b in batches) == run["rows_loaded_ok"] + run["rows_loaded_malformed"]
    assert sum(b["load_failed"] for b in batches) == run["load_failed"]
    assert {b["method"] for b in batches} == {"copy"}


def test_db_rejected_row_falls_back_and_is_kept_in_load_errors(conn, ingest, records, run_row, monkeypatch) -> None:
    real_build_row = bronze.build_row
    bad_line = 6  # record "5,name5", in batch 2 with batch_size=3

    def build_row_with_invalid_json(record, columns):
        row = real_build_row(record, columns)
        return replace(row, payload_json="{not json") if row.source_line == bad_line else row

    monkeypatch.setattr(bronze, "build_row", build_row_with_invalid_json)
    run_id = ingest(TEN, batch_size=3)

    run = run_row(run_id)
    assert (run["status"], run["load_failed"], run["batches_fallback"]) == ("succeeded", 1, 1)
    batch2 = conn.execute(
        "SELECT method, loaded, load_failed FROM meta.ingestion_batches WHERE run_id = %s AND batch_no = 2",
        (run_id,),
    ).fetchone()
    assert batch2 == {"method": "row_fallback", "loaded": 2, "load_failed": 1}

    (error,) = conn.execute("SELECT * FROM bronze.load_errors WHERE run_id = %s", (run_id,)).fetchall()
    assert (error["source_line"], error["raw_line"]) == (bad_line, "5,name5")
    assert error["row_hash"] == hashlib.sha256(b"5,name5").hexdigest()
    assert "json" in error["error_message"].lower()
    assert [r["source_line"] for r in records(run_id)] == [2, 3, 4, 5, 7, 8, 9, 10, 11]


# --- row_hash integrity ------------------------------------------------------


def test_identical_lines_kept_with_same_hash_across_batches(ingest, records, run_row) -> None:
    run_id = ingest(TEN, batch_size=3)
    same = [r for r in records(run_id) if r["raw_line"] == "1,same"]
    assert [(r["source_line"], r["batch_no"]) for r in same] == [(2, 1), (3, 1), (10, 3)]
    assert len({r["row_hash"] for r in same}) == 1
    assert run_row(run_id)["duplicate_hash_groups"] == 1


def test_row_hash_independent_of_batch_size_and_reload(ingest, records) -> None:
    small = _hashes(records(ingest(TEN, batch_size=3)))
    large = _hashes(records(ingest(TEN, batch_size=1000, force=True)))
    again = _hashes(records(ingest(TEN, force=True)))
    assert small == large == again
    assert len(small) == 10
