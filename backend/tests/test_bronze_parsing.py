"""Parsing and no-data-loss guarantees of bronze ingestion."""

from pipeline.bronze import iter_records, normalize_header


def test_iter_records_joins_quoted_newlines_and_keeps_raw_text() -> None:
    lines = ["a,b\r\n", '1,"x\r\n', 'y"\r\n', "2,z"]
    records = list(iter_records(lines))
    assert [(r.start_line, r.end_line, r.text) for r in records] == [
        (1, 1, "a,b"),
        (2, 3, '1,"x\r\ny"'),
        (4, 4, "2,z"),
    ]


def test_normalize_header_names_empty_and_duplicate_columns() -> None:
    assert normalize_header(["id", "", "name", "name", ""]) == ["id", "column_2", "name", "name_2", "column_5"]


def test_quoted_comma_and_embedded_newline(ingest, records) -> None:
    rows = records(ingest("quoted_newline.csv"))
    assert [r["source_line"] for r in rows] == [2, 3, 5]
    assert rows[0]["payload"] == {"id": "1", "note": "hello, world", "amount": "10"}
    assert rows[1]["payload"]["note"] == "line one\r\nline two"
    assert rows[1]["raw_line"] == '2,"line one\r\nline two",20'
    assert all(r["parse_status"] == "ok" for r in rows)


def test_too_many_and_too_few_fields_are_kept_as_malformed(ingest, records, run_row) -> None:
    run_id = ingest("field_counts.csv")
    ok, extra, short = records(run_id)
    assert ok["parse_status"] == "ok"
    assert extra["parse_status"] == "malformed"
    assert extra["parse_error"] == "expected 3 fields, got 5"
    assert extra["payload"] == {"id": "2", "name": "bob", "city": "rome", "_extra_fields": ["EXTRA", "MORE"]}
    assert short["parse_error"] == "expected 3 fields, got 2"
    assert short["payload"] == {"id": "3", "name": "cid"}
    assert run_row(run_id)["malformed_by_reason"] == {"too many fields": 1, "too few fields": 1}


def test_blank_line_is_kept_as_malformed(ingest, records) -> None:
    rows = records(ingest("blank_line.csv"))
    blank = rows[1]
    assert (blank["source_line"], blank["raw_line"], blank["parse_status"], blank["parse_error"]) == (
        3, "", "malformed", "blank line",
    )  # fmt: skip
    assert len(rows) == 3


def test_bom_file_is_decoded_and_bom_not_in_header(ingest, records, run_row) -> None:
    run_id = ingest("bom_utf8.csv")
    assert run_row(run_id)["encoding"] == "utf-8-sig"
    assert run_row(run_id)["header"] == ["id", "name"]
    assert records(run_id)[0]["payload"] == {"id": "1", "name": "José"}


def test_latin1_file_loads_and_encoding_recorded(ingest, records, run_row) -> None:
    run_id = ingest("latin1.csv")
    assert run_row(run_id)["encoding"] == "latin-1"
    assert records(run_id)[0]["payload"]["name"] == "José"


def test_original_header_stored_and_payload_uses_normalized_names(ingest, records, run_row) -> None:
    run_id = ingest("header_names.csv")
    assert run_row(run_id)["header"] == ["id", "", "name", "name", ""]
    assert records(run_id)[0]["payload"] == {"id": "1", "column_2": "a", "name": "b", "name_2": "c", "column_5": "d"}


def test_unclosed_quote_at_eof_becomes_one_malformed_record(ingest, records) -> None:
    rows = records(ingest("unclosed_quote.csv"))
    assert len(rows) == 2
    assert rows[1]["source_line"] == 3
    assert rows[1]["raw_line"] == '2,"never closed\n3,swallowed'
    assert rows[1]["parse_error"].startswith("unclosed quote")


def test_nul_bytes_replaced_and_noted(ingest, records) -> None:
    (row,) = records(ingest("nul_byte.csv"))
    assert row["raw_line"] == "1,bad\\x00byte"
    assert row["payload"]["note"] == "bad\\x00byte"
    assert "NUL" in row["parse_error"]


def test_reconciliation_accounts_for_every_record(ingest, run_row) -> None:
    for name in ["field_counts.csv", "blank_line.csv", "quoted_newline.csv", "ten_records.csv"]:
        run = run_row(ingest(name))
        assert run["status"] == "succeeded"
        assert run["rows_loaded_ok"] + run["rows_loaded_malformed"] + run["load_failed"] == run["records_parsed"]
