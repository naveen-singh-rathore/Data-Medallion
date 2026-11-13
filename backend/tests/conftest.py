from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
import pytest
from psycopg.rows import dict_row

from pipeline import bronze
from pipeline.db import get_conn
from pipeline.migrate import migrate

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:
    """An autocommit DB connection with dict rows; skips if Postgres isn't reachable."""
    try:
        connection = get_conn()
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable (run `make up`): {exc}")
    connection.autocommit = True
    connection.row_factory = dict_row  # type: ignore[assignment]
    with connection:
        yield connection


def _delete_runs_for(conn: psycopg.Connection, paths: set[Path]) -> None:
    """Delete the fixture files' runs and the LLM cache entries those runs created.

    The dataset card names a file by content, so two tests on the same fixture file send identical
    requests; without this, one test's scripted answers would be served from the cache to the next.
    """
    hashes = [bronze.hash_file(p) for p in paths]
    conn.execute(
        "DELETE FROM meta.llm_cache WHERE request_hash IN (SELECT c.request_hash FROM meta.llm_calls c"
        " JOIN meta.ingestion_runs r ON r.run_id = c.run_id WHERE r.file_sha256 = ANY(%s))",
        (hashes,),
    )
    conn.execute("DELETE FROM meta.ingestion_runs WHERE file_sha256 = ANY(%s)", (hashes,))


@pytest.fixture
def ingest(conn: psycopg.Connection) -> Iterator[Callable[..., UUID]]:
    """ingest("fixture.csv", **kwargs) -> run_id; deletes the fixture's runs before and after."""
    migrate()
    touched: set[Path] = set()

    def _ingest(name: str, **kwargs: Any) -> UUID:
        path = FIXTURES / name
        if path not in touched:
            touched.add(path)
            _delete_runs_for(conn, {path})
        return bronze.ingest_file(path, **kwargs)

    yield _ingest
    _delete_runs_for(conn, touched)


@pytest.fixture
def records(conn: psycopg.Connection) -> Callable[[UUID], list[dict[str, Any]]]:
    """records(run_id) -> bronze.records rows for the run, ordered by source_line."""

    def _records(run_id: UUID) -> list[dict[str, Any]]:
        return conn.execute("SELECT * FROM bronze.records WHERE run_id = %s ORDER BY source_line", (run_id,)).fetchall()

    return _records


@pytest.fixture
def run_row(conn: psycopg.Connection) -> Callable[[UUID], dict[str, Any]]:
    """run_row(run_id) -> the meta.ingestion_runs row."""

    def _run_row(run_id: UUID) -> dict[str, Any]:
        return conn.execute("SELECT * FROM meta.ingestion_runs WHERE run_id = %s", (run_id,)).fetchone()

    return _run_row
