"""Bronze ingestion: load any CSV faithfully into bronze.records.

Schema-on-read: each record is stored as its exact text (raw_line) plus a
{column: raw string} payload, so nothing is lost, even for unparseable rows.
"""

import csv
import hashlib
import io
import json
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import batched
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.config import settings
from pipeline.db import column_list, transaction
from pipeline.errors import PipelineError
from pipeline.logging_setup import get_logger

ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")
EXTRA_FIELDS_KEY = "_extra_fields"
NUL_MARKER = "\\x00"  # Postgres text/jsonb cannot store NUL, so it becomes this literal text
CHUNK_SIZE = 1 << 20

RECORD_COLUMNS = (
    "run_id", "source_file", "source_line", "raw_line", "payload", "row_hash",
    "parse_status", "parse_error", "batch_no", "ingested_at",
)  # fmt: skip
COPY_RECORDS_SQL = sql.SQL("COPY bronze.records ({}) FROM STDIN").format(column_list(RECORD_COLUMNS))
INSERT_RECORD_SQL = sql.SQL(
    "INSERT INTO bronze.records ({}) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)"
).format(column_list(RECORD_COLUMNS))
INSERT_LOAD_ERROR_SQL = """
    INSERT INTO bronze.load_errors (run_id, batch_no, source_line, raw_line, row_hash, error_message)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


class ReconciliationError(PipelineError, RuntimeError):
    """Raised when loaded + malformed + failed does not account for every record."""


@dataclass(frozen=True)
class RawRecord:
    """One CSV record as text, possibly spanning several physical lines."""

    start_line: int
    end_line: int
    text: str  # without the final line terminator
    unclosed_quote: bool = False


@dataclass(frozen=True)
class Row:
    """A record fully prepared for loading; built once, reused by COPY and fallback."""

    source_line: int
    raw_line: str
    payload_json: str
    row_hash: str
    parse_status: str
    parse_error: str | None
    reason: str | None  # malformed category, for the funnel


@dataclass(frozen=True)
class RunContext:
    """Identity and lineage shared by every row of one ingestion run."""

    run_id: uuid.UUID
    source_file: str
    ingested_at: datetime


@dataclass
class BatchResult:
    """Outcome of loading one batch: COPY or row fallback, loaded / malformed / refused counts."""

    batch_no: int
    first_line: int
    last_line: int
    records: int
    method: str = "copy"
    loaded: int = 0
    load_failed: int = 0
    reasons: Counter[str] = field(default_factory=Counter)
    duration_ms: int = 0

    @property
    def malformed(self) -> int:
        """Rows loaded with parse_status = 'malformed' in this batch."""
        return sum(self.reasons.values())


@dataclass
class Funnel:
    """Run-level counts that must reconcile: physical lines -> records -> ok + malformed + refused."""

    physical_lines: int = 0
    records_parsed: int = 0
    ok: int = 0
    malformed: int = 0
    load_failed: int = 0
    malformed_by_reason: Counter[str] = field(default_factory=Counter)
    batches: int = 0
    batches_fallback: int = 0
    duplicate_hash_groups: int = 0

    def add(self, batch: BatchResult) -> None:
        """Add one batch's counts to the run totals."""
        self.ok += batch.loaded - batch.malformed
        self.malformed += batch.malformed
        self.load_failed += batch.load_failed
        self.malformed_by_reason.update(batch.reasons)
        self.batches += 1
        self.batches_fallback += batch.method == "row_fallback"


# --- reading -----------------------------------------------------------------


def hash_file(path: Path) -> str:
    """SHA-256 of the file bytes, streamed in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def detect_encoding(path: Path) -> str:
    """Return the first of ENCODINGS that decodes the whole file (latin-1 always does)."""
    for encoding in ENCODINGS:
        try:
            with path.open(encoding=encoding) as f:
                while f.read(CHUNK_SIZE):
                    pass
        except UnicodeDecodeError:
            continue
        else:
            return encoding
    raise AssertionError("unreachable: latin-1 decodes any byte sequence")


def _strip_terminator(text: str) -> str:
    for terminator in ("\r\n", "\n", "\r"):
        if text.endswith(terminator):
            return text[: -len(terminator)]
    return text


def iter_records(lines: Iterable[str]) -> Iterator[RawRecord]:
    """Group physical lines into records: a record ends once its quote count is even.

    `lines` must keep their terminators (file opened with newline='') so raw text
    is exact. An unclosed quote at EOF yields the remaining text as one record.
    """
    buffer: list[str] = []
    quotes = start = line_no = 0
    for line_no, line in enumerate(lines, start=1):
        if not buffer:
            start = line_no
        buffer.append(line)
        quotes += line.count('"')
        if quotes % 2 == 0:
            yield RawRecord(start, line_no, _strip_terminator("".join(buffer)))
            buffer, quotes = [], 0
    if buffer:
        yield RawRecord(start, line_no, _strip_terminator("".join(buffer)), unclosed_quote=True)


def normalize_header(names: list[str]) -> list[str]:
    """Empty names -> column_<n> (1-based); duplicates -> <name>_2, <name>_3, ..."""
    result: list[str] = []
    seen: set[str] = set()
    for position, name in enumerate(names, start=1):
        base = name or f"column_{position}"
        candidate, suffix = base, 1
        while candidate in seen:
            suffix += 1
            candidate = f"{base}_{suffix}"
        seen.add(candidate)
        result.append(candidate)
    return result


def _parse_header(record: RawRecord) -> list[str]:
    rows = list(csv.reader(io.StringIO(record.text)))
    if record.unclosed_quote or len(rows) != 1 or not rows[0]:
        raise ValueError(f"cannot parse header on line {record.start_line}: {record.text[:200]!r}")
    return rows[0]


# --- row building ------------------------------------------------------------


def _parse_payload(
    text: str, unclosed_quote: bool, columns: list[str]
) -> tuple[dict[str, Any], str | None, str | None]:
    """Return (payload, malformed reason, precise error); reason is None when ok."""
    if text == "":
        return {}, "blank line", "blank line"
    if unclosed_quote:
        return {}, "unclosed quote", "unclosed quote: record runs to end of file"
    try:
        rows = list(csv.reader(io.StringIO(text)))
    except csv.Error as exc:
        return {}, "csv error", f"csv error: {exc}"
    if len(rows) != 1:
        # A stray quote inside an unquoted field can balance across lines yet parse as several rows.
        return {}, "stray quote", f"quotes balance across lines but text parses as {len(rows)} records"

    fields = rows[0]
    payload: dict[str, Any] = dict(zip(columns, fields, strict=False))
    expected, got = len(columns), len(fields)
    if got > expected:
        payload[EXTRA_FIELDS_KEY] = fields[expected:]
        return payload, "too many fields", f"expected {expected} fields, got {got}"
    if got < expected:
        return payload, "too few fields", f"expected {expected} fields, got {got}"
    return payload, None, None


def build_row(record: RawRecord, columns: list[str]) -> Row:
    """Turn a raw record into a loadable Row. Values are kept exactly as read."""
    text, notes = record.text, []
    if "\x00" in text:
        text = text.replace("\x00", NUL_MARKER)
        notes.append(f"NUL bytes replaced with {NUL_MARKER}")
    payload, reason, error = _parse_payload(text, record.unclosed_quote, columns)
    if error:
        notes.insert(0, error)
    return Row(
        source_line=record.start_line,
        raw_line=text,
        payload_json=json.dumps(payload, ensure_ascii=False),
        row_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        parse_status="malformed" if reason else "ok",
        parse_error="; ".join(notes) or None,
        reason=reason,
    )


# --- loading -----------------------------------------------------------------


def iter_batches(rows: Iterable[Row], batch_size: int) -> Iterator[tuple[int, tuple[Row, ...]]]:
    """Yield (batch_no, rows) with 1-based batch numbers."""
    yield from enumerate(batched(rows, batch_size), start=1)


def _record_values(ctx: RunContext, batch_no: int, row: Row) -> tuple[Any, ...]:
    return (
        ctx.run_id, ctx.source_file, row.source_line, row.raw_line, row.payload_json,
        row.row_hash, row.parse_status, row.parse_error, batch_no, ctx.ingested_at,
    )  # fmt: skip


def load_batch(conn: psycopg.Connection, ctx: RunContext, batch_no: int, rows: tuple[Row, ...]) -> BatchResult:
    """COPY the batch inside a savepoint; on failure fall back to row-by-row."""
    log = get_logger("bronze", run_id=str(ctx.run_id))
    started = time.perf_counter()
    try:
        with conn.transaction(), conn.cursor() as cur, cur.copy(COPY_RECORDS_SQL) as copy:
            for row in rows:
                copy.write_row(_record_values(ctx, batch_no, row))
        result = BatchResult(batch_no, rows[0].source_line, rows[-1].source_line, len(rows))
        result.loaded = len(rows)
        result.reasons.update(r.reason for r in rows if r.reason)
    except psycopg.Error as exc:
        log.warning("copy failed, falling back to row-by-row", batch_no=batch_no, error=str(exc).splitlines()[0])
        result = load_batch_row_fallback(conn, ctx, batch_no, rows)
    result.duration_ms = int((time.perf_counter() - started) * 1000)
    _record_batch(conn, ctx.run_id, result)
    log.debug(
        "batch loaded", batch_no=batch_no, method=result.method, records=result.records,
        loaded=result.loaded, malformed=result.malformed, load_failed=result.load_failed,
        duration_ms=result.duration_ms,
    )  # fmt: skip
    return result


def load_batch_row_fallback(
    conn: psycopg.Connection, ctx: RunContext, batch_no: int, rows: tuple[Row, ...]
) -> BatchResult:
    """Insert rows one by one, each in its own savepoint; refused rows go to load_errors."""
    result = BatchResult(batch_no, rows[0].source_line, rows[-1].source_line, len(rows), method="row_fallback")
    for row in rows:
        try:
            with conn.transaction():
                conn.execute(INSERT_RECORD_SQL, _record_values(ctx, batch_no, row))
        except psycopg.Error as exc:
            conn.execute(
                INSERT_LOAD_ERROR_SQL,
                (ctx.run_id, batch_no, row.source_line, row.raw_line, row.row_hash, str(exc)),
            )
            result.load_failed += 1
        else:
            result.loaded += 1
            if row.reason:
                result.reasons[row.reason] += 1
    return result


def _record_batch(conn: psycopg.Connection, run_id: uuid.UUID, b: BatchResult) -> None:
    conn.execute(
        """
        INSERT INTO meta.ingestion_batches
            (run_id, batch_no, first_line, last_line, records, loaded, malformed, load_failed, method, duration_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, b.batch_no, b.first_line, b.last_line, b.records, b.loaded,
         b.malformed, b.load_failed, b.method, b.duration_ms),
    )  # fmt: skip


def _counted(records: Iterable[RawRecord], funnel: Funnel) -> Iterator[RawRecord]:
    for record in records:
        funnel.physical_lines = record.end_line
        funnel.records_parsed += 1
        yield record


def _load(
    conn: psycopg.Connection, ctx: RunContext, path: Path, encoding: str, batch_size: int
) -> tuple[Funnel, list[str]]:
    """Stream the file through parse -> build -> batch -> load. Returns (funnel, header)."""
    funnel = Funnel()
    with path.open(encoding=encoding, newline="") as f:
        records = iter_records(f)
        header_record = next(records, None)
        if header_record is None:
            raise ValueError("file is empty")
        header = _parse_header(header_record)
        columns = normalize_header(header)
        funnel.physical_lines = header_record.end_line
        rows = (build_row(r, columns) for r in _counted(records, funnel))
        for batch_no, batch in iter_batches(rows, batch_size):
            funnel.add(load_batch(conn, ctx, batch_no, batch))
    return funnel, header


# --- run bookkeeping ---------------------------------------------------------


def reconcile(conn: psycopg.Connection, run_id: uuid.UUID, funnel: Funnel) -> None:
    """Prove every parsed record is accounted for, in memory and in the database."""
    db_ok, db_malformed, db_failed = conn.execute(
        """
        SELECT count(*) FILTER (WHERE parse_status = 'ok'),
               count(*) FILTER (WHERE parse_status = 'malformed'),
               (SELECT count(*) FROM bronze.load_errors WHERE run_id = %(id)s)
        FROM bronze.records WHERE run_id = %(id)s
        """,
        {"id": run_id},
    ).fetchone()  # type: ignore[misc]
    counted = (funnel.ok, funnel.malformed, funnel.load_failed)
    if sum(counted) != funnel.records_parsed or counted != (db_ok, db_malformed, db_failed):
        raise ReconciliationError(
            f"records_parsed={funnel.records_parsed} ok/malformed/failed: "
            f"counted={counted} in_db={(db_ok, db_malformed, db_failed)}"
        )


def _duplicate_hash_groups(conn: psycopg.Connection, run_id: uuid.UUID) -> int:
    return conn.execute(
        """
        SELECT count(*) FROM (
            SELECT row_hash FROM (
                SELECT row_hash FROM bronze.records WHERE run_id = %(id)s
                UNION ALL
                SELECT row_hash FROM bronze.load_errors WHERE run_id = %(id)s
            ) h GROUP BY row_hash HAVING count(*) > 1
        ) g
        """,
        {"id": run_id},
    ).fetchone()[0]  # type: ignore[index]


def _elapsed_ms(started_at: datetime) -> int:
    return int((datetime.now(UTC) - started_at).total_seconds() * 1000)


def find_run(file_sha256: str) -> tuple[uuid.UUID, str] | None:
    """Return (run_id, status) of the run for this file hash, if any."""
    with transaction() as conn:
        return conn.execute(
            "SELECT run_id, status FROM meta.ingestion_runs WHERE file_sha256 = %s", (file_sha256,)
        ).fetchone()  # type: ignore[return-value]


def _open_run(ctx: RunContext, sha: str, size: int, encoding: str, replaces: uuid.UUID | None) -> None:
    """Insert the 'running' row in its own committed transaction so failures stay recorded."""
    with transaction() as conn:
        if replaces:
            conn.execute("DELETE FROM meta.ingestion_runs WHERE run_id = %s", (replaces,))
        conn.execute(
            """
            INSERT INTO meta.ingestion_runs
                (run_id, source_file, file_sha256, file_size_bytes, encoding, status, started_at)
            VALUES (%s, %s, %s, %s, %s, 'running', %s)
            """,
            (ctx.run_id, ctx.source_file, sha, size, encoding, ctx.ingested_at),
        )


def finalize_run(conn: psycopg.Connection, ctx: RunContext, funnel: Funnel, header: list[str]) -> None:
    """Mark the run succeeded with its funnel, in the same transaction as the data."""
    funnel.duplicate_hash_groups = _duplicate_hash_groups(conn, ctx.run_id)
    conn.execute(
        """
        UPDATE meta.ingestion_runs SET
            status = 'succeeded', finished_at = now(), duration_ms = %s, header = %s,
            physical_lines = %s, records_parsed = %s, rows_loaded_ok = %s,
            rows_loaded_malformed = %s, load_failed = %s, malformed_by_reason = %s,
            batches = %s, batches_fallback = %s, duplicate_hash_groups = %s
        WHERE run_id = %s
        """,
        (_elapsed_ms(ctx.ingested_at), Jsonb(header), funnel.physical_lines,
         funnel.records_parsed, funnel.ok, funnel.malformed, funnel.load_failed,
         Jsonb(dict(funnel.malformed_by_reason.most_common())), funnel.batches,
         funnel.batches_fallback, funnel.duplicate_hash_groups, ctx.run_id),
    )  # fmt: skip


def _mark_failed(ctx: RunContext, exc: BaseException) -> None:
    with transaction() as conn:
        conn.execute(
            """
            UPDATE meta.ingestion_runs
            SET status = 'failed', error_message = %s, finished_at = now(), duration_ms = %s
            WHERE run_id = %s
            """,
            (f"{type(exc).__name__}: {exc}", _elapsed_ms(ctx.ingested_at), ctx.run_id),
        )


def ingest_file(path: str | Path, force: bool = False, batch_size: int | None = None) -> uuid.UUID:
    """Load a CSV into bronze.records. Idempotent per file content; returns the run_id."""
    path = Path(path)
    batch_size = batch_size or settings.batch_size
    log = get_logger("bronze")
    size = path.stat().st_size
    log.info("file received", file=str(path), size_bytes=size, force=force)

    sha = hash_file(path)
    log.info("file fingerprinted", sha256=sha)
    existing = find_run(sha)
    if existing and existing[1] == "succeeded" and not force:
        log.info("already ingested, skipping", existing_run_id=existing[0])
        return existing[0]

    encoding = detect_encoding(path)
    ctx = RunContext(uuid.uuid4(), str(path), datetime.now(UTC))
    _open_run(ctx, sha, size, encoding, replaces=existing[0] if existing else None)
    log = get_logger("bronze", run_id=str(ctx.run_id))
    log.info("run opened", encoding=encoding, batch_size=batch_size, replaced_run=existing[0] if existing else "-")

    try:
        with transaction() as conn:
            funnel, header = _load(conn, ctx, path, encoding, batch_size)
            log.info("records parsed", records=funnel.records_parsed, physical_lines=funnel.physical_lines)
            log.info("batches done", batches=funnel.batches, fallback_batches=funnel.batches_fallback)
            reconcile(conn, ctx.run_id, funnel)
            log.info("reconciliation passed")
            finalize_run(conn, ctx, funnel, header)
    except Exception as exc:
        _mark_failed(ctx, exc)
        log.exception("ingestion failed", error=f"{type(exc).__name__}: {exc}")
        raise

    log.info(
        "funnel", physical_lines=funnel.physical_lines, records=funnel.records_parsed,
        ok=funnel.ok, malformed=funnel.malformed, load_failed=funnel.load_failed,
        batches=funnel.batches, fallback_batches=funnel.batches_fallback,
        duplicate_groups=funnel.duplicate_hash_groups, reconciliation="passed",
        duration_ms=_elapsed_ms(ctx.ingested_at),
    )  # fmt: skip
    return ctx.run_id


# --- reporting ---------------------------------------------------------------

RUN_COLUMNS = column_list((
    "run_id", "source_file", "status", "encoding", "started_at", "duration_ms", "physical_lines",
    "records_parsed", "rows_loaded_ok", "rows_loaded_malformed", "load_failed", "malformed_by_reason",
    "batches", "batches_fallback", "duplicate_hash_groups", "error_message",
))  # fmt: skip


def get_run(run_id: uuid.UUID) -> dict[str, Any]:
    """Return one run row as a dict."""
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(  # type: ignore[return-value]
            sql.SQL("SELECT {} FROM meta.ingestion_runs WHERE run_id = %s").format(RUN_COLUMNS), (run_id,)
        ).fetchone()


def recent_runs(limit: int = 20) -> list[dict[str, Any]]:
    """Return the most recent runs, newest first."""
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(
            sql.SQL("SELECT {} FROM meta.ingestion_runs ORDER BY started_at DESC LIMIT %s").format(RUN_COLUMNS),
            (limit,),
        ).fetchall()
