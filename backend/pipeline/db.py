"""Postgres connection helpers (plain psycopg 3, no ORM)."""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg

from pipeline.config import settings


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
