"""Apply db/sql/*.sql in filename order.

Every file is re-run on each invocation, so each file must itself be idempotent
(CREATE ... IF NOT EXISTS, etc.). No version table needed for that.
"""

from pathlib import Path

from pipeline.config import settings
from pipeline.db import transaction
from pipeline.logging_setup import get_logger

log = get_logger("migrate")


def sql_files(sql_dir: Path = settings.sql_dir) -> list[Path]:
    """Return migration files sorted by filename."""
    return sorted(sql_dir.glob("*.sql"))


def migrate(sql_dir: Path = settings.sql_dir) -> int:
    """Run every SQL file, each in its own transaction. Returns files applied."""
    files = sql_files(sql_dir)
    for path in files:
        with transaction() as conn:
            conn.execute(path.read_text())  # type: ignore[arg-type]  # trusted repo SQL
        log.info("applied sql file", file=path.name)
    from pipeline.demo import (
        load_fixtures,  # recorded LLM answers: replay works on a fresh database
    )

    with transaction() as conn:
        loaded = load_fixtures(conn)
    if loaded:
        log.info("fixtures loaded", **{k.split(".")[0]: v for k, v in loaded.items()})
    setup_checkpointer()
    log.info("migration complete", files=len(files))
    return len(files)


def setup_checkpointer() -> None:
    """Create/upgrade LangGraph's checkpoint tables (idempotent; the library owns their DDL)."""
    from langgraph.checkpoint.postgres import PostgresSaver

    with PostgresSaver.from_conn_string(settings.database_url) as saver:
        saver.setup()
    log.info("checkpointer ready")
