"""Build silver: apply the APPROVED rules to a bronze run and store the result.

- Only approved rules (rules/silver_rules.yaml for this run) are built; the exact YAML, its hash
  and the compiled SQL digest are recorded in meta.silver_builds.
- The compiled query (pipeline/silver/compiler.py, the only SQL writer) runs once into a new
  table, which replaces the rules' target table in the same transaction: a failed build leaves
  the previous table untouched, and the failure is recorded.
- Nothing is dropped. Every bronze ok row becomes one silver row with `_status`:
    kept       clean
    flagged    kept, with errors in `_errors` (e.g. a failed row check with action `flag`)
    rejected   a rule rejects the row (`_rejected`)
    duplicate  removed by the dedup rule (`_dup_rank` > 1)
  `<target>_valid` is a view of the usable rows (kept + flagged).
- Reconciliation: rows in == kept + flagged + rejected + duplicate == bronze ok rows, or the
  build fails. Lineage per row: bronze_id, _source_line, row_hash, <col>_raw, _quality_flags.
- One table per target: building another run with the same target replaces it.
"""

import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.db import transaction
from pipeline.logging_setup import get_logger
from pipeline.silver.compiler import (
    compile_rules,
    has_raw_column,
    sql_digest,
    write_compiled,
)
from pipeline.silver.rules_schema import (
    SilverRules,
    from_yaml,
    rules_hash,
)

STATUSES = ("kept", "flagged", "rejected", "duplicate")
STATUS_SQL = ("CASE WHEN c._rejected THEN 'rejected' WHEN c._dup_rank > 1 OR c._duplicate_of IS NOT NULL THEN 'duplicate'"
              " WHEN cardinality(c._errors) > 0 THEN 'flagged' ELSE 'kept' END")  # fmt: skip


class BuildError(RuntimeError):
    """The build can't start (no approved rules for the run) or failed reconciliation."""


def approved_rules(run_id: str, path: Path | None = None) -> tuple[SilverRules, str]:
    """(rules, yaml text) approved for this run; raises BuildError otherwise."""
    if path is None:
        from pipeline.demo import (
            approved_path_for,  # the repo file, or its binding to this run
        )

        path = approved_path_for(run_id)
    if not path.exists():
        raise BuildError("no approved rules yet: finish the review first")
    text = path.read_text()
    rules = SilverRules.model_validate(from_yaml(text))
    if rules.source_run_id != run_id:
        raise BuildError(f"the approved rules belong to run {rules.source_run_id}, not {run_id}")
    return rules, text


def _names(target: str) -> tuple[str, str]:
    schema, _, table = target.partition(".")
    return schema, table


def _column_stats(conn: Any, table: sql.Composable, rules: SilverRules) -> dict[str, Any]:
    """Per column: rows a rule changed (any quality flag except 'unmapped') and null counts."""
    parts = []
    for c in rules.columns:
        changed = sql.SQL(
            "count(*) FILTER (WHERE jsonb_path_exists(_quality_flags -> {}, '$[*] ? (@ != \"unmapped\")'))"
        )
        parts.append(sql.SQL("{} AS {}, count(*) FILTER (WHERE {} IS NULL) AS {}").format(
            changed.format(sql.Literal(c.name)), sql.Identifier(f"{c.name}__changed"),
            sql.Identifier(c.name), sql.Identifier(f"{c.name}__null")))  # fmt: skip
    row = conn.cursor(row_factory=dict_row).execute(sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(parts), table)).fetchone()  # fmt: skip
    return {c.name: {"changed": row[f"{c.name}__changed"], "null_after": row[f"{c.name}__null"],
                     "raw_kept": has_raw_column(c)} for c in rules.columns}  # fmt: skip


def build_silver(run_id: uuid.UUID | str, approved_path: Path | None = None) -> dict[str, Any]:
    """Build the run's silver table from its approved rules. Returns the meta.silver_builds row."""
    run_id = str(run_id)
    rules, text = approved_rules(run_id, approved_path)
    compiled = compile_rules(rules, run_id)
    write_compiled(compiled, run_id)
    build_id = uuid.uuid4()
    log = get_logger("silver.build", run_id=run_id)
    schema, table = _names(rules.target_table)
    target, staging = sql.Identifier(schema, table), sql.Identifier(schema, f"{table}__build")
    valid = sql.Identifier(schema, f"{table}_valid")
    started = time.perf_counter()
    with transaction() as conn:  # committed first, so a failed build stays visible
        conn.execute(
            "INSERT INTO meta.silver_builds (build_id, run_id, target_table, rules_hash, rules_yaml, sql_digest, status)"
            " VALUES (%s, %s, %s, %s, %s, %s, 'running')",
            (build_id, run_id, rules.target_table, rules_hash(rules), text, sql_digest(compiled)),
        )
    log.info("silver build started", build_id=str(build_id), target=rules.target_table, rules_hash=rules_hash(rules))
    try:
        with transaction() as conn:
            conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging))
            conn.execute(sql.SQL(
                "CREATE TABLE {} AS SELECT c.*, {} AS _status, b.source_line AS _source_line,"
                " %(build_id)s::uuid AS _build_id, %(run_id)s::uuid AS _run_id"
                " FROM ({}) AS c JOIN bronze.records b ON b.bronze_id = c.bronze_id"
            ).format(staging, sql.SQL(STATUS_SQL), compiled.query), compiled.params | {"build_id": build_id})  # fmt: skip
            from pipeline.silver.classify import (
                apply_ai_labels,  # AI labels survive every rebuild
            )

            apply_ai_labels(conn, staging, run_id, rules)
            counts = dict(
                conn.execute(sql.SQL("SELECT _status, count(*) FROM {} GROUP BY _status").format(staging)).fetchall()
            )
            rows_in = conn.execute("SELECT count(*) FROM bronze.records WHERE run_id = %s AND parse_status = 'ok'",
                                   (run_id,)).fetchone()[0]  # type: ignore[index]  # fmt: skip
            accounted = sum(counts.get(s, 0) for s in STATUSES)
            if accounted != rows_in or set(counts) - set(STATUSES):
                raise BuildError(f"reconciliation failed: {accounted} silver rows for {rows_in} bronze rows ({counts})")
            stats = _column_stats(conn, staging, rules)
            conn.execute(sql.SQL("DROP VIEW IF EXISTS {}").format(valid))
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
            conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(staging, sql.Identifier(table)))
            conn.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (bronze_id)").format(target))
            conn.execute(sql.SQL("CREATE INDEX ON {} (_status)").format(target))
            conn.execute(
                sql.SQL("CREATE VIEW {} AS SELECT * FROM {} WHERE _status IN ('kept', 'flagged')").format(valid, target)
            )
            comment = (f"Silver build {build_id} of run {run_id}, rules {rules_hash(rules)}. One row per bronze ok row;"
                       " _status kept|flagged|rejected|duplicate. Lineage: bronze_id, _source_line, <col>_raw, _quality_flags.")  # fmt: skip
            conn.execute(sql.SQL("COMMENT ON TABLE {} IS {}").format(target, sql.Literal(comment)))
            conn.execute(
                sql.SQL("COMMENT ON VIEW {} IS {}").format(
                    valid, sql.Literal(f"Usable rows (kept + flagged) of {rules.target_table}.")
                )
            )
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO pipeline_ro").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("GRANT SELECT ON {}, {} TO pipeline_ro").format(target, valid))
            duration = int((time.perf_counter() - started) * 1000)
            conn.execute(
                "UPDATE meta.silver_builds SET status = 'succeeded', finished_at = %s, duration_ms = %s, rows_in = %s,"
                " rows_kept = %s, rows_flagged = %s, rows_rejected = %s, rows_duplicate = %s, column_stats = %s"
                " WHERE build_id = %s",
                (datetime.now(UTC), duration, rows_in, *(counts.get(s, 0) for s in STATUSES), Jsonb(stats), build_id),
            )
    except Exception as exc:
        with transaction() as conn:
            conn.execute("UPDATE meta.silver_builds SET status = 'failed', error_message = %s, finished_at = %s,"
                         " duration_ms = %s WHERE build_id = %s",
                         (f"{type(exc).__name__}: {str(exc)[:1000]}", datetime.now(UTC),
                          int((time.perf_counter() - started) * 1000), build_id))  # fmt: skip
        log.exception("silver build failed", build_id=str(build_id), error=str(exc)[:300])
        raise
    result = latest_build(run_id)
    log.info("silver build done", build_id=str(build_id), target=rules.target_table,
             **{f"rows_{s}": counts.get(s, 0) for s in STATUSES}, rows_in=rows_in, duration_ms=duration)  # fmt: skip
    return result  # type: ignore[return-value]


def latest_build(run_id: uuid.UUID | str) -> dict[str, Any] | None:
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute("SELECT * FROM meta.silver_builds WHERE run_id = %s ORDER BY started_at DESC LIMIT 1",
                           (str(run_id),)).fetchone()  # fmt: skip


def is_current(build: dict[str, Any] | None, rules: SilverRules | None) -> bool:
    """The build succeeded, used these exact rules, and its table still holds that build."""
    if not build or not rules or build["status"] != "succeeded" or build["rules_hash"] != rules_hash(rules):
        return False
    schema, table = _names(build["target_table"])
    with transaction() as conn:
        exists = conn.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",)).fetchone()[0]  # type: ignore[index]
        if not exists:
            return False
        holder = conn.execute(
            sql.SQL("SELECT _build_id FROM {} LIMIT 1").format(sql.Identifier(schema, table))
        ).fetchone()
    return bool(holder) and holder[0] == build["build_id"]
