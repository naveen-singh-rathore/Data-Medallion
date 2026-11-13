"""Postgres connection helpers (plain psycopg 3, no ORM)."""

from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager

import psycopg
from psycopg import sql

from pipeline.config import settings


def table(qualified: str) -> sql.Identifier:
    """`schema.table` as a quoted identifier."""
    return sql.Identifier(*qualified.split("."))


def column_list(names: Iterable[str]) -> sql.Composed:
    """A comma-separated list of quoted column names."""
    return sql.SQL(", ").join(map(sql.Identifier, names))


def insert_sql(qualified_table: str, names: Sequence[str], suffix: sql.Composable | None = None) -> sql.Composed:
    """`INSERT INTO t (cols) VALUES (%s, ...)` plus an optional suffix (e.g. ON CONFLICT ...)."""
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        table(qualified_table), column_list(names), sql.SQL(", ").join(sql.Placeholder() * len(names))
    )
    return query + suffix if suffix is not None else query


def get_conn() -> psycopg.Connection:
    """Open a new connection. Caller is responsible for closing it."""
    return psycopg.connect(settings.database_url)


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    """Yield a connection in one transaction: commit on success, roll back on error, close.

    It is an explicit psycopg transaction block, so any `conn.transaction()` nested
    inside is guaranteed to be a SAVEPOINT (in non-autocommit mode, the first one
    would instead BEGIN/COMMIT on its own when no transaction is open yet).
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn, conn.transaction():
        yield conn


@contextmanager
def get_ro_conn(timeout: str = "5s") -> Iterator[psycopg.Connection]:
    """A READ ONLY transaction as role pipeline_ro with a statement timeout; always rolled back.

    Used by agent tools and validator dry-runs: even a bug in generated SQL can't write.
    """
    with psycopg.connect(settings.database_url, autocommit=True) as conn, conn.transaction(force_rollback=True):
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL ROLE pipeline_ro")
        conn.execute(sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(timeout)))
        yield conn
