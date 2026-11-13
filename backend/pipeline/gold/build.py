"""Build gold from the run's current silver build.

The business model lives outside the pipeline (gold/model.yaml + gold/sql/*.sql); this engine is
generic. It resolves business roles to silver columns from the context tags and the approved rules
(opened/closed = the date_order row check's a/b), renders each SQL file with those columns as
identifiers, builds every table in `<schema>_staging`, computes the data-quality findings, runs the
reconciliation checks, and only then swaps all tables into `<schema>` in the SAME transaction: a
failed build leaves the previous gold untouched. Every build is logged in meta.gold_builds.
"""

import random
import statistics
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.config import REPO_ROOT
from pipeline.context import get_context
from pipeline.db import transaction
from pipeline.logging_setup import get_logger
from pipeline.privacy import context_tags
from pipeline.silver.build import approved_rules, is_current, latest_build
from pipeline.silver.rules_schema import SilverRules, rules_hash

GOLD_DIR = REPO_ROOT / "gold"
TABLES = ("fact_tickets", "dim_category", "dim_building", "dim_priority", "resolution_sla", "volume_backlog",
          "cost_summary", "data_quality_findings")  # fmt: skip
ROLE_TAGS = {"id": "id", "category": "category", "priority": "priority", "status": "status", "location": "location",
             "money": "money", "sla": "duration"}  # fmt: skip


class GoldError(RuntimeError):
    """Gold can't be built (no current silver build, a role has no column) or a check failed."""


def load_model(path: Path | None = None) -> dict[str, Any]:
    return yaml.safe_load((path or GOLD_DIR / "model.yaml").read_text()) or {}


def resolve_roles(rules: SilverRules, tags: dict[str, str], model: dict[str, Any]) -> dict[str, str]:
    """role -> silver (target) column name. Overrides in the model win; otherwise tags and the date_order check."""
    names = {c.name for c in rules.columns}
    roles: dict[str, str] = {}
    for role, tag in ROLE_TAGS.items():
        hit = next((c.name for c in rules.columns if tags.get(c.source) == tag), None)
        if hit:
            roles[role] = hit
    order = next((ch for ch in rules.row_checks if ch.kind == "date_order"), None)
    if order:
        roles["opened"], roles["closed"] = str(order.params["a"]), str(order.params["b"])
    else:
        stamps = [c.name for c in rules.columns if tags.get(c.source) == "timestamp"]
        if len(stamps) >= 2:
            roles["opened"], roles["closed"] = stamps[0], stamps[1]
    roles |= {k: v for k, v in (model.get("roles") or {}).items() if v}
    missing = sorted((set(ROLE_TAGS) | {"opened", "closed"}) - set(roles))
    unknown = sorted(r for r, c in roles.items() if c not in names)
    if missing or unknown:
        raise GoldError(
            f"cannot map gold roles: missing {missing}, unknown columns {unknown} (set them in gold/model.yaml)"
        )
    return roles


def _text_array(values: list[str]) -> sql.Composable:
    return sql.SQL("ARRAY[{}]::text[]").format(sql.SQL(", ").join(sql.Literal(v) for v in values)) if values \
        else sql.SQL("ARRAY[]::text[]")  # fmt: skip


def _category_exprs(conn: Any, silver_table: tuple[str, str], rules: SilverRules, col: str,
                    min_conf: float) -> tuple[sql.Composable, sql.Composable]:  # fmt: skip
    """(final category, category source) using the AI columns when the silver table has them."""
    have = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", silver_table)}  # fmt: skip
    c, src, ai, conf = (sql.Identifier(x) for x in (col, f"{col}_source", f"{col}_ai", f"{col}_ai_confidence"))
    if f"{col}_source" in have:
        final = sql.SQL("CASE WHEN s.{src} = 'rule' THEN s.{c} WHEN s.{src} = 'ai' AND s.{conf} >= {m} THEN s.{ai} END").format(
            src=src, c=c, conf=conf, ai=ai, m=sql.Literal(min_conf))  # fmt: skip
        return final, sql.SQL("CASE WHEN s.{src} = 'ai' AND s.{conf} < {m} THEN 'none' ELSE coalesce(s.{src}, 'none') END").format(
            src=src, conf=conf, m=sql.Literal(min_conf))  # fmt: skip
    rule = next((r for r in rules.columns if r.name == col), None)
    if rule and rule.mapping:
        canon = _text_array(list(rule.mapping))
        return (
            sql.SQL("CASE WHEN s.{c} = ANY({k}) THEN s.{c} END").format(c=c, k=canon),
            sql.SQL("CASE WHEN s.{c} = ANY({k}) THEN 'rule' ELSE 'none' END").format(c=c, k=canon),
        )
    return sql.SQL("s.{c}").format(c=c), sql.SQL("CASE WHEN s.{c} IS NULL THEN 'none' ELSE 'rule' END").format(c=c)


# --- findings: the business case next to the metrics it affects -------------------------------------


def _standard_only(rules: SilverRules, col: str) -> sql.Composable:
    """A role column with a standard-value list: values outside the list become NULL ((unknown) in marts)."""
    rule = next((r for r in rules.columns if r.name == col), None)
    if rule and rule.mapping:
        return sql.SQL("CASE WHEN s.{c} = ANY({k}) THEN s.{c} END").format(
            c=sql.Identifier(col), k=_text_array(list(rule.mapping))
        )
    return sql.SQL("s.{c}").format(c=sql.Identifier(col))


def _duplicate_finding(conn: Any, silver: sql.Identifier, id_col: str) -> dict[str, Any] | None:
    """Tickets whose own notes say they duplicate another (silver status duplicate, _duplicate_of set)."""
    q = conn.cursor(row_factory=dict_row)
    has = q.execute("SELECT 1 FROM information_schema.columns WHERE table_schema || '.' || table_name = %s"
                    " AND column_name = '_duplicate_of'", (silver.as_string(conn).replace('"', ""),)).fetchone()  # fmt: skip
    if not has:  # silver built before declared duplicates existed: nothing to report
        return None
    ids = sql.Identifier(id_col)
    r = q.execute(sql.SQL(
        "SELECT count(*) AS total, count(*) FILTER (WHERE {dup}) AS declared,"
        " count(*) FILTER (WHERE {dup} AND _duplicate_of IN (SELECT {i} FROM {t})) AS target_found,"
        " (SELECT count(*) FROM (SELECT {i} FROM {t} WHERE {i} IS NOT NULL GROUP BY 1 HAVING count(*) > 1) x) AS repeated_ids"
        " FROM {t}").format(dup=sql.SQL("_duplicate_of IS NOT NULL"), i=ids, t=silver)).fetchone()  # fmt: skip
    if not r["declared"]:
        return None
    return {"finding": "self_declared_duplicates", "title": "Tickets that say they duplicate another ticket",
            "severity": "medium" if r["declared"] / r["total"] > 0.02 else "low", "rows_affected": r["declared"],
            "denominator": r["total"], "share_pct": round(100 * r["declared"] / r["total"], 1),
            "metric_affected": "ticket volume, backlog, cost (they are not counted as tickets)",
            "impact": (f"{r['declared']:,} tickets are closed as duplicates in their own notes; {r['target_found']:,} of them point "
                       f"at a ticket that exists in this file. They are kept in silver with status duplicate and left out of "
                       f"every gold count. The id column itself never repeats ({r['repeated_ids']} repeated ids)."),
            "recommendation": "Link duplicates in the source system instead of a note, so they never count as new work.",
            "evidence": {"declared": r["declared"], "target_found": r["target_found"], "repeated_ids": r["repeated_ids"]}}  # fmt: skip


def _findings(conn: Any, schema: sql.Identifier) -> list[dict[str, Any]]:
    q = conn.cursor(row_factory=dict_row)
    fact = sql.SQL("{}.fact_tickets").format(schema)
    total = q.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(fact)).fetchone()["n"] or 1
    pairs = q.execute(sql.SQL("SELECT created_at AS a, resolved_at AS b FROM {} WHERE created_at IS NOT NULL AND"
                              " resolved_at IS NOT NULL").format(fact)).fetchall()  # fmt: skip
    out = []
    if len(pairs) >= 30:
        observed = sum(p["a"] > p["b"] for p in pairs) / len(pairs)
        corr = statistics.correlation([p["a"].timestamp() for p in pairs], [p["b"].timestamp() for p in pairs])
        rng, closes = random.Random(7), [p["b"] for p in pairs]
        shuffled = []
        for _ in range(20):
            rng.shuffle(closes)
            shuffled.append(sum(p["a"] > c for p, c in zip(pairs, closes, strict=True)) / len(pairs))
        expected = statistics.mean(shuffled)
        unrelated = abs(observed - expected) < 0.03 and abs(corr) < 0.1
        if observed > 0.01:
            out.append({
                "finding": "resolution_dates_unreliable" if unrelated else "resolution_dates_out_of_order",
                "title": "Resolution dates are unrelated to creation dates" if unrelated
                         else "Some tickets are resolved before they were created",
                "severity": "high" if unrelated or observed > 0.05 else "medium",
                "rows_affected": sum(p["a"] > p["b"] for p in pairs), "denominator": len(pairs),
                "share_pct": round(100 * observed, 1),
                "metric_affected": "resolution time, SLA compliance, backlog",
                "impact": (f"{observed:.1%} of tickets with both dates resolve before they open - the same share as pairing "
                           f"dates at random ({expected:.1%}), with a correlation of {corr:.2f}. Even the tickets in the "
                           "right order carry no information about how long work took: resolution and SLA figures "
                           "for this file are not meaningful.") if unrelated else
                          (f"{observed:.1%} of tickets with both dates resolve before they open; they are excluded "
                           "from time metrics."),
                "recommendation": "Fix how the source system records the resolution timestamp before using time metrics; "
                                  "until then report volume, backlog by creation and cost only." if unrelated else
                                  "Check the source for these tickets; time metrics exclude them.",
                "evidence": {"observed_share": round(observed, 4), "random_pairing_share": round(expected, 4),
                             "correlation": round(corr, 4), "pairs": len(pairs)},
            })  # fmt: skip
    rows = q.execute(sql.SQL(
        "SELECT count(*) FILTER (WHERE NOT is_resolved AND resolved_at IS NOT NULL) AS open_with_date,"
        " count(*) FILTER (WHERE NOT is_resolved) AS open_n, count(*) FILTER (WHERE cost IS NULL) AS no_cost,"
        " count(*) FILTER (WHERE category_source = 'ai') AS ai_cat, count(*) FILTER (WHERE category_source = 'none') AS no_cat,"
        " count(*) FILTER (WHERE is_resolved AND NOT usable_for_time_metrics) AS excluded, count(*) FILTER (WHERE is_resolved) AS res"
        " FROM {}").format(fact)).fetchone()  # fmt: skip
    if rows["open_with_date"]:
        out.append({"finding": "open_tickets_with_resolution_date", "title": "Open tickets already have a resolution date",
                    "severity": "medium" if rows["open_with_date"] / max(rows["open_n"], 1) > 0.05 else "low",
                    "rows_affected": rows["open_with_date"], "denominator": rows["open_n"],
                    "share_pct": round(100 * rows["open_with_date"] / max(rows["open_n"], 1), 1),
                    "metric_affected": "resolution time (ignored: only resolved tickets are timed)",
                    "impact": "The status and the resolution date disagree; the status is trusted, the date ignored.",
                    "recommendation": "Ask the source owners which field is authoritative for closing a ticket.",
                    "evidence": {}})  # fmt: skip
    if rows["no_cost"]:
        out.append({"finding": "cost_missing", "title": "Many tickets have no repair cost",
                    "severity": "medium" if rows["no_cost"] / total > 0.2 else "low",
                    "rows_affected": rows["no_cost"], "denominator": total, "share_pct": round(100 * rows["no_cost"] / total, 1),
                    "metric_affected": "repair cost totals and averages",
                    "impact": "Cost totals cover only tickets with a known cost (see cost_coverage_pct).",
                    "recommendation": "Record cost at closure, or report totals with their coverage.", "evidence": {}})  # fmt: skip
    if rows["ai_cat"] or rows["no_cat"]:
        out.append({"finding": "category_recovered_by_ai", "title": "Categories recovered by AI from ticket text",
                    "severity": "low", "rows_affected": rows["ai_cat"], "denominator": total,
                    "share_pct": round(100 * rows["ai_cat"] / total, 1), "metric_affected": "everything split by category",
                    "impact": f"{rows['ai_cat']:,} categories come from AI (confidence >= threshold); {rows['no_cat']:,} stay unknown.",
                    "recommendation": "Use a fixed category list at entry; check the AI accuracy figure in the README.",
                    "evidence": {"ai": rows["ai_cat"], "unknown": rows["no_cat"]}})  # fmt: skip
    if rows["excluded"]:
        out.append({"finding": "time_metric_exclusions", "title": "Resolved tickets left out of time metrics",
                    "severity": "medium" if rows["excluded"] / max(rows["res"], 1) > 0.1 else "low",
                    "rows_affected": rows["excluded"], "denominator": rows["res"],
                    "share_pct": round(100 * rows["excluded"] / max(rows["res"], 1), 1),
                    "metric_affected": "resolution time, SLA compliance",
                    "impact": "These resolved tickets have a missing or impossible date; see coverage_pct next to each metric.",
                    "recommendation": "Fix the dates at the source.", "evidence": {}})  # fmt: skip
    return out


def _store_findings(conn: Any, schema: sql.Identifier, findings: list[dict[str, Any]]) -> None:
    t = sql.SQL("{}.data_quality_findings").format(schema)
    conn.execute(sql.SQL(
        "CREATE TABLE {} (finding text PRIMARY KEY, title text NOT NULL, severity text NOT NULL, rows_affected integer,"
        " denominator integer, share_pct numeric, metric_affected text, impact text, recommendation text, evidence jsonb)"
    ).format(t))  # fmt: skip
    for f in findings:
        conn.execute(sql.SQL("INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").format(t),
                     (f["finding"], f["title"], f["severity"], f["rows_affected"], f["denominator"], f["share_pct"],
                      f["metric_affected"], f["impact"], f["recommendation"], Jsonb(f["evidence"])))  # fmt: skip
    conn.execute(sql.SQL("COMMENT ON TABLE {} IS {}").format(t, sql.Literal(
        "Data-quality findings that limit the gold metrics: what, how many, which metric, and what to do.")))  # fmt: skip


def _checks(conn: Any, schema: sql.Identifier, silver_rows: int) -> dict[str, dict[str, Any]]:
    def one(query: str) -> Any:
        return conn.execute(sql.SQL(query).format(s=schema)).fetchone()[0]

    fact = one("SELECT count(*) FROM {s}.fact_tickets")
    pairs = {
        "fact_rows = silver valid rows": (silver_rows, fact),
        "sum(volume_backlog.opened) = fact rows": (fact, one("SELECT coalesce(sum(opened), 0) FROM {s}.volume_backlog")),
        "sum(resolution_sla.tickets) = fact rows": (fact, one("SELECT coalesce(sum(tickets), 0) FROM {s}.resolution_sla")),
        "sum(cost_summary.tickets) = fact rows": (fact, one("SELECT coalesce(sum(tickets), 0) FROM {s}.cost_summary")),
        "sum(cost_summary.total_cost) = sum(fact cost)": (
            one("SELECT coalesce(sum(cost), 0) FROM {s}.fact_tickets"), one("SELECT coalesce(sum(total_cost), 0) FROM {s}.cost_summary")),
    }  # fmt: skip
    return {name: {"expected": str(e), "actual": str(a), "ok": e == a} for name, (e, a) in pairs.items()}


def build_gold(run_id: uuid.UUID | str, schema: str = "gold", approved_path: Path | None = None,
               model_path: Path | None = None) -> dict[str, Any]:  # fmt: skip
    run_id = str(run_id)
    log = get_logger("gold.build", run_id=run_id)
    rules, _ = approved_rules(run_id, approved_path)
    silver = latest_build(run_id)
    if not is_current(silver, rules):
        raise GoldError("no current silver build for the approved rules: run build-silver first")
    model = load_model(model_path)
    roles = resolve_roles(rules, context_tags(get_context(uuid.UUID(run_id))), model)
    build_id, started = uuid.uuid4(), time.perf_counter()
    with transaction() as conn:
        conn.execute("INSERT INTO meta.gold_builds (build_id, run_id, silver_build_id, rules_hash, target_schema, status,"
                     " roles) VALUES (%s, %s, %s, %s, %s, 'running', %s)",
                     (build_id, run_id, silver["build_id"], rules_hash(rules), schema, Jsonb(roles)))  # type: ignore[index]  # fmt: skip
    s_schema, _, s_table = silver["target_table"].partition(".")  # type: ignore[index]
    target, staging = sql.Identifier(schema), sql.Identifier(f"{schema}_staging")
    try:
        with transaction() as conn:
            final, source = _category_exprs(conn, (s_schema, s_table), rules, roles["category"],
                                            float(model.get("ai_min_confidence", 0.6)))  # fmt: skip
            statuses = {r[0] for r in conn.execute(sql.SQL("SELECT DISTINCT {} FROM {}").format(
                sql.Identifier(roles["status"]), sql.Identifier(s_schema, s_table))).fetchall()}  # fmt: skip
            escalated = [v for v in model.get("escalated_statuses") or [] if v in statuses]
            escalation = sql.SQL(", round(100.0 * count(*) FILTER (WHERE status = ANY({})) / count(*), 1) AS escalation_rate_pct").format(
                _text_array(escalated)) if escalated else sql.SQL("")  # fmt: skip
            params: dict[str, sql.Composable] = {r: sql.Identifier(c) for r, c in roles.items()} | {
                "schema": staging, "silver": sql.Identifier(s_schema, f"{s_table}_valid"),
                "silver_build_id": sql.Literal(str(silver["build_id"])),  # type: ignore[index]
                "resolved_statuses": _text_array(model.get("resolved_statuses") or []),
                "category_final": final, "category_source": source, "escalation_select": escalation,
                "priority_value": _standard_only(rules, roles["priority"]), "location_value": _standard_only(rules, roles["location"]),
            }  # fmt: skip
            conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(staging))
            for t in TABLES:
                conn.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(staging, sql.Identifier(t)))
            for path in sorted((GOLD_DIR / "sql").glob("*.sql")):
                conn.execute(
                    sql.SQL(path.read_text()).format(**params)
                )  # trusted repo SQL; values are Identifiers/Literals
            if escalated:
                conn.execute(sql.SQL("COMMENT ON COLUMN {}.resolution_sla.escalation_rate_pct IS {}").format(staging, sql.Literal(
                    f"100 * tickets whose CURRENT status is one of {escalated} / tickets. A snapshot: tickets escalated "
                    "earlier and since resolved are not counted (no status history in the source).")))  # fmt: skip
            findings = _findings(conn, staging)
            if dup := _duplicate_finding(conn, sql.Identifier(s_schema, s_table), roles["id"]):
                findings.append(dup)
            _store_findings(conn, staging, findings)
            silver_rows = conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(s_schema, f"{s_table}_valid"))
            ).fetchone()[0]  # type: ignore[index]
            checks = _checks(conn, staging, silver_rows)
            if not all(c["ok"] for c in checks.values()):
                raise GoldError("reconciliation failed: " + "; ".join(f"{k} ({v['expected']} vs {v['actual']})"
                                                                      for k, v in checks.items() if not v["ok"]))  # fmt: skip
            counts = {t: conn.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(staging, sql.Identifier(t))).fetchone()[0]  # type: ignore[index]
                      for t in TABLES}  # fmt: skip
            conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(target))
            for t in TABLES:  # the swap: same transaction as everything above
                conn.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(target, sql.Identifier(t)))
                conn.execute(sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(staging, sql.Identifier(t), target))
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO pipeline_ro").format(target))
            conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO pipeline_ro").format(target))
            duration = int((time.perf_counter() - started) * 1000)
            conn.execute("UPDATE meta.gold_builds SET status = 'succeeded', finished_at = %s, duration_ms = %s, row_counts = %s,"
                         " checks = %s WHERE build_id = %s",
                         (datetime.now(UTC), duration, Jsonb(counts | {"escalation_rate": bool(escalated)}), Jsonb(checks), build_id))  # fmt: skip
    except Exception as exc:
        with transaction() as conn:
            conn.execute("UPDATE meta.gold_builds SET status = 'failed', error_message = %s, finished_at = %s, duration_ms = %s"
                         " WHERE build_id = %s", (f"{type(exc).__name__}: {str(exc)[:1000]}", datetime.now(UTC),
                                                  int((time.perf_counter() - started) * 1000), build_id))  # fmt: skip
        log.exception("gold build failed", build_id=str(build_id), error=str(exc)[:300])
        raise
    log.info("gold build done", build_id=str(build_id), duration_ms=duration, **counts)
    return latest_gold(run_id)  # type: ignore[return-value]


def latest_gold(run_id: uuid.UUID | str) -> dict[str, Any] | None:
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute("SELECT * FROM meta.gold_builds WHERE run_id = %s ORDER BY started_at DESC LIMIT 1",
                           (str(run_id),)).fetchone()  # fmt: skip
