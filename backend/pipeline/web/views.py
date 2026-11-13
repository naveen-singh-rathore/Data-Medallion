"""Read-only views of one run for the UI, one function per checkpoint.

Everything here reads what the pipeline already stores (meta.*, bronze.*, rules/, the agent
checkpoint). The only computation is the silver preview: the compiled rules run read-only over
the run's bronze rows, cached per (run, rules hash), because build-silver does not exist yet.
"""

import json
import threading
import uuid
from collections import Counter, OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from psycopg import sql
from psycopg.rows import dict_row

from pipeline.card import latest_card
from pipeline.config import REPO_ROOT, settings
from pipeline.context import get_context
from pipeline.db import get_ro_conn, transaction
from pipeline.profile_values import normalization_key
from pipeline.profiling import load_profile
from pipeline.silver.build import is_current, latest_build
from pipeline.silver.compiler import _check_label, compile_rules
from pipeline.silver.mapping_assist import spellings_of
from pipeline.silver.rules_schema import (
    APPROVED_PATH,
    SilverRules,
    from_yaml,
    proposed_path,
    rules_hash,
)
from pipeline.web import explain

PREVIEW_TIMEOUT = "120s"
PREVIEW_CACHE_SIZE = 4


def _q(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(query, params).fetchall()  # type: ignore[arg-type]


def _one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    rows = _q(query, params)
    return rows[0] if rows else None


def jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    return value


# --- runs ----------------------------------------------------------------------------


def run_row(run_id: uuid.UUID) -> dict[str, Any] | None:
    return _one("SELECT * FROM meta.ingestion_runs WHERE run_id = %s", (run_id,))


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    return _q(
        """
        SELECT r.run_id, r.source_file, r.status, r.started_at, r.records_parsed, r.rows_loaded_ok,
               r.rows_loaded_malformed, r.file_size_bytes,
               (p.run_id IS NOT NULL) AS profiled,
               coalesce(jsonb_array_length(p.issues), 0) AS issues,
               (SELECT count(*) FROM jsonb_array_elements(p.issues) i WHERE i->>'severity' = 'high') AS high_issues,
               (SELECT a.status FROM meta.agent_runs a WHERE a.run_id = r.run_id ORDER BY a.updated_at DESC LIMIT 1)
                   AS agent_status
          FROM meta.ingestion_runs r LEFT JOIN meta.dataset_profiles p ON p.run_id = r.run_id
         ORDER BY r.started_at DESC LIMIT %s
        """,
        (limit,),
    )


def file_label(source_file: str) -> str:
    """Upload paths are <stamp>__<name>; show the original name."""
    name = Path(source_file).name
    return name.split("__", 1)[1] if "__" in name else name


# --- agent / rules state -------------------------------------------------------------


def agent_snapshot(run_id: uuid.UUID) -> dict[str, Any] | None:
    """Latest agent thread: status (incl. awaiting_context, which only the checkpoint knows)."""
    from pipeline.dq_agent import runner  # LangGraph only when needed

    thread_id = runner.latest_thread(str(run_id))
    if not thread_id:
        return None
    with runner.compiled() as graph:
        snap = runner.snapshot(graph, thread_id)
    snap["interrupted"] = bool(snap["next"]) and not snap["paused"]  # stopped mid-run (e.g. server restart)
    return snap


def _in_review(run_id: uuid.UUID) -> bool:
    row = _one("SELECT status FROM meta.agent_runs WHERE run_id = %s ORDER BY updated_at DESC LIMIT 1", (run_id,))
    return bool(row and row["status"] == "awaiting_approval" and proposed_path(str(run_id)).exists())


def current_rules(run_id: uuid.UUID) -> tuple[SilverRules | None, str | None, Path | None]:
    """(rules, 'approved' | 'proposed', path) for this run. While a review is open the proposal is shown,
    even when an earlier version is approved (and still builds silver)."""
    if _in_review(run_id):
        path = proposed_path(str(run_id))
        try:
            return SilverRules.model_validate(from_yaml(path.read_text())), "proposed", path
        except (ValueError, TypeError):
            return None, "proposed", path
    if APPROVED_PATH.exists():
        try:
            approved = SilverRules.model_validate(from_yaml(APPROVED_PATH.read_text()))
            if approved.source_run_id == str(run_id):
                return approved, "approved", APPROVED_PATH
        except (ValueError, TypeError):
            pass
    from pipeline.demo import bound_path  # approved rules bound to this run by the demo

    if bound_path(str(run_id)).exists():
        return SilverRules.model_validate(from_yaml(bound_path(str(run_id)).read_text())), "approved", bound_path(str(run_id))
    path = proposed_path(str(run_id))
    if path.exists():
        try:
            return SilverRules.model_validate(from_yaml(path.read_text())), "proposed", path
        except (ValueError, TypeError):
            return None, "proposed", path
    return None, None, None


# --- overview: one status per checkpoint ---------------------------------------------


def _stage(key: str, title: str, state: str, summary: str, **extra: Any) -> dict[str, Any]:
    return {"key": key, "title": title, "state": state, "summary": summary, **extra}


def overview(run_id: uuid.UUID, job: dict[str, Any] | None) -> dict[str, Any] | None:
    run = run_row(run_id)
    if not run:
        return None
    running_step = job["step"] if job and job["state"] == "running" else None
    failed_step = job["step"] if job and job["state"] == "error" else None
    profile = _one("SELECT summary, issues FROM meta.dataset_profiles WHERE run_id = %s", (run_id,))
    context = get_context(run_id)
    card = latest_card(run_id)
    agent = agent_snapshot(run_id)
    agent_metrics = _one("SELECT * FROM meta.agent_runs WHERE run_id = %s ORDER BY updated_at DESC LIMIT 1", (run_id,))
    rules, rules_state, _ = current_rules(run_id)

    def state_for(step: str, done: bool) -> str:
        if running_step == step:
            return "running"
        if failed_step == step:
            return "error"
        return "done" if done else "waiting"

    size = run["file_size_bytes"] or 0
    stages = [_stage("upload", "File received", "done",
                     f"{file_label(run['source_file'])} · {size / 1024:,.0f} KB · {run['encoding'] or '?'}")]  # fmt: skip

    loaded = (run["rows_loaded_ok"] or 0) + (run["rows_loaded_malformed"] or 0)
    bronze_state = {"succeeded": "done", "failed": "error", "running": "running"}[run["status"]]
    stages.append(_stage("bronze", "Raw data landed (Bronze)", bronze_state,
                         f"{loaded:,} of {run['records_parsed'] or 0:,} records stored, nothing changed"
                         if run["status"] == "succeeded" else (run["error_message"] or "loading")))  # fmt: skip

    issues = profile["issues"] if profile else []
    by_sev = Counter(i["severity"] for i in issues)
    stages.append(_stage("profile", "Health check (profiling)", state_for("profile", profile is not None),
                         f"{len(issues)} issues found: {by_sev['high']} high, {by_sev['medium']} medium, {by_sev['low']} low"
                         if profile else "not run yet"))  # fmt: skip

    # The AI decides the context; nobody has to approve it. A person may still overwrite it.
    if context:
        ctx_state = "done"
        ctx_summary = "Written by a person" if context["source"] == "human" else "Decided by AI"
    elif profile is not None and running_step is None:
        # The draft failed (logged, not fatal): the AI retries when the review starts, else runs without it.
        ctx_state, ctx_summary = "error", "AI could not write it yet; the review will retry"
    else:
        ctx_state, ctx_summary = "waiting", "AI writes it after the health check"
    if running_step == "context":
        ctx_state = "running"
    stages.append(_stage("context", "Business context", ctx_state, ctx_summary))

    stages.append(_stage("card", "AI briefing prepared", state_for("card", card is not None),
                         f"{card['issues_shown']}/{card['issues_total']} issues summarised in "
                         f"~{(card['exact_tokens'] or card['est_tokens']):,} tokens" if card else "not prepared yet"))  # fmt: skip

    status = (agent or {}).get("status")
    if running_step in ("agent", "resume"):
        agent_state, agent_summary = "running", "the AI is investigating the data"
    elif not agent and rules_state == "approved":
        agent_state, agent_summary = "done", "not run here: the approved rules come from the repository"
    elif not agent:
        agent_state, agent_summary = ("error", job["error"]) if failed_step == "agent" and job else ("waiting", "not started")
    elif status == "failed":
        agent_state, agent_summary = "error", "the AI could not produce valid rules"
    elif agent and agent.get("interrupted"):
        agent_state, agent_summary = "action", "stopped before finishing; resume it"
    else:
        m = agent_metrics or {}
        agent_state = "done"
        agent_summary = f"{m.get('turns', 0)} steps, {m.get('tool_calls', 0)} checks, ${float(m.get('cost_usd') or 0):.2f}"
    stages.append(_stage("agent", "AI quality review", agent_state, agent_summary))

    if status == "awaiting_approval":
        approval = ("action", "rules ready; waiting for your decision")
    elif status == "approved" or rules_state == "approved":
        approval = ("done", "approved")
    elif status == "rejected":
        approval = ("error", "rejected")
    else:
        approval = ("waiting", "no rules proposed yet")
    if running_step == "resume":
        approval = ("running", "applying your decision")
    elif running_step == "automap":
        approval = ("running", "the AI is deciding leftover values")
    stages.append(_stage("approval", "Your approval", *approval))

    build = latest_build(run_id)
    if running_step == "build":
        silver = ("running", "building the cleaned table")
    elif rules is None:
        silver = ("waiting", "available once rules are proposed")
    elif rules_state == "approved" and is_current(build, rules):
        usable = (build["rows_kept"] or 0) + (build["rows_flagged"] or 0)
        silver = ("done", f"{usable:,} usable rows stored in {build['target_table']}")
    elif rules_state == "approved" and build and build["status"] == "failed":
        silver = ("error", "build failed: " + (build["error_message"] or "")[:120])
    elif rules_state == "approved":
        silver = ("action", "rules approved; the cleaned table is not built yet")
    else:
        silver = ("preview", "preview of the proposed rules")
    stages.append(_stage("silver", "Cleaned data (Silver)", *silver))
    g, g_current = gold_state(run_id)
    if running_step == "gold":
        gold = ("running", "building the business tables")
    elif g and g_current:
        gold = ("done", f"{(g['row_counts'] or {}).get('fact_tickets', 0):,} tickets in 4 tables + findings")
    elif g and g["status"] == "failed":
        gold = ("error", "build failed: " + (g["error_message"] or "")[:120])
    elif build and is_current(build, rules) and rules_state == "approved":
        gold = ("action", "cleaned data changed; build the business tables" if g else "ready to build")
    else:
        gold = ("waiting", "available once the cleaned table is built")
    stages.append(_stage("gold", "Business tables (Gold)", *gold))

    return jsonable({
        "run_id": run_id, "file": file_label(run["source_file"]), "started_at": run["started_at"],
        "stages": stages, "job": job, "llm_mode": settings.llm_mode, "agent": agent and {
            k: agent.get(k) for k in ("status", "paused", "turns", "tool_calls", "interrupted")},
        "rules_state": rules_state,
        "build": build and {k: build[k] for k in ("build_id", "status", "target_table", "rules_hash", "started_at",
                                                  "duration_ms", "rows_in", "rows_kept", "rows_flagged",
                                                  "rows_rejected", "rows_duplicate", "error_message")},
    })  # fmt: skip


# --- bronze --------------------------------------------------------------------------


def bronze_view(run_id: uuid.UUID) -> dict[str, Any]:
    run = run_row(run_id) or {}
    batches = _q("SELECT * FROM meta.ingestion_batches WHERE run_id = %s ORDER BY batch_no", (run_id,))
    errors = _q("SELECT source_line, raw_line, error_message FROM bronze.load_errors WHERE run_id = %s"
                " ORDER BY source_line LIMIT 50", (run_id,))  # fmt: skip
    dupes = _q(
        """SELECT row_hash, count(*) AS n, array_agg(source_line ORDER BY source_line) AS lines
             FROM bronze.records WHERE run_id = %s GROUP BY row_hash HAVING count(*) > 1
            ORDER BY n DESC LIMIT 10""",
        (run_id,),
    )
    accounted = (run.get("rows_loaded_ok") or 0) + (run.get("rows_loaded_malformed") or 0) + (run.get("load_failed") or 0)
    return jsonable({
        "run": {k: v for k, v in run.items() if k != "file_sha256"} | {"file_sha256": run.get("file_sha256"),
                                                                     "file": file_label(run.get("source_file", ""))},
        "reconciled": run.get("status") == "succeeded" and accounted == run.get("records_parsed"),
        "accounted": accounted, "batches": batches, "load_errors": errors, "duplicate_groups": dupes,
    })  # fmt: skip


def bronze_rows(run_id: uuid.UUID, status: str | None, offset: int, limit: int, search: str | None) -> dict[str, Any]:
    where = [sql.SQL("run_id = %(run)s")]
    params: dict[str, Any] = {"run": run_id, "limit": min(limit, 200), "offset": max(offset, 0)}
    if status in ("ok", "malformed"):
        where.append(sql.SQL("parse_status = %(status)s"))
        params["status"] = status
    if search:
        where.append(sql.SQL("raw_line ILIKE %(search)s"))
        params["search"] = f"%{search}%"
    cond = sql.SQL(" AND ").join(where)
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        total = cur.execute(sql.SQL("SELECT count(*) AS n FROM bronze.records WHERE {}").format(cond), params).fetchone()
        rows = cur.execute(sql.SQL(
            "SELECT bronze_id, source_line, batch_no, parse_status, parse_error, row_hash, raw_line, payload"
            " FROM bronze.records WHERE {} ORDER BY source_line LIMIT %(limit)s OFFSET %(offset)s").format(cond),
            params).fetchall()  # fmt: skip
    header = (run_row(run_id) or {}).get("header") or []
    return jsonable({"total": total["n"] if total else 0, "rows": rows, "header": header})


# --- profile -------------------------------------------------------------------------


def profile_view(run_id: uuid.UUID) -> dict[str, Any] | None:
    try:
        profile = load_profile(run_id)
    except (LookupError, TypeError, ValueError):
        return None
    if not profile.get("columns"):
        return None
    issues = [explain.describe_issue(i) for i in profile["issues"]]
    by_column: dict[str, list[str]] = {}
    for i in issues:
        for col in (i.get("column") or "").split(" > "):
            if col:
                by_column.setdefault(col, []).append(i["severity"])
    columns = []
    for p in profile["columns"]:
        t, comp, card = p["type_inference"], p["completeness"], p["cardinality"]
        sev = by_column.get(p["column"], [])
        columns.append({
            "column": p["column"], "type": t["dominant_type"], "confidence": t["confidence"],
            "empty_share": comp["empty_or_null_like_share"], "distinct": card["distinct"], "role": card["role_hint"],
            "issues": len(sev), "worst": min(sev, key=["high", "medium", "low"].index) if sev else None,
            "top_values": (p.get("frequency") or {}).get("top_values", [])[:5],
        })  # fmt: skip
    summary = {k: v for k, v in profile["summary"].items() if k != "profiler"}
    return jsonable({"columns": columns, "issues": issues, "summary": summary, "severity_help": explain.SEVERITY,
                     "profiler_version": (profile["summary"].get("profiler") or {}).get("version")})  # fmt: skip


def column_profile(run_id: uuid.UUID, column: str) -> dict[str, Any] | None:
    row = _one("SELECT profile FROM meta.column_profiles WHERE run_id = %s AND column_name = %s", (run_id, column))
    return row["profile"] if row else None


# --- context / card ------------------------------------------------------------------


def context_view(run_id: uuid.UUID) -> dict[str, Any]:
    rows = _q(
        """SELECT c.source, c.approved, c.text, c.draft, c.created_at, l.model, l.input_tokens, l.output_tokens,
                  l.cost_usd, l.cache_hit
             FROM meta.dataset_context c LEFT JOIN meta.llm_calls l ON l.call_id = c.llm_call_id
            WHERE c.run_id = %s""",
        (run_id,),
    )
    by_source = {r["source"]: r for r in rows}
    used = by_source.get("human") or by_source.get("inferred")
    return jsonable({"used": used, "human": by_source.get("human"), "inferred": by_source.get("inferred")})


def card_view(run_id: uuid.UUID) -> dict[str, Any] | None:
    card = latest_card(run_id)
    if not card:
        return None
    return jsonable({k: card[k] for k in ("card_text", "budget_tokens", "est_tokens", "exact_tokens", "chars",
                                          "issues_total", "issues_shown", "columns_total", "columns_flagged",
                                          "over_budget", "card_hash")})  # fmt: skip


# --- agent ---------------------------------------------------------------------------


def _summarize_result(content: Any) -> str:
    text = content if isinstance(content, str) else json.dumps(content, default=str)
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return str(text)[:300]
    if isinstance(data, dict) and not data.get("ok", True):
        return f"Error: {str(data.get('error', ''))[:300]}"
    return text[:600]


def agent_transcript(run_id: uuid.UUID) -> list[dict[str, Any]]:
    """Agent turns from the checkpoint: what it said, which checks it ran, and what came back."""
    from pipeline.dq_agent import runner

    thread_id = runner.latest_thread(str(run_id))
    if not thread_id:
        return []
    with runner.compiled() as graph:
        messages = (graph.get_state(runner.config(thread_id)).values or {}).get("messages", [])
    results: dict[str, dict[str, Any]] = {}
    for m in messages:
        if m["role"] == "user" and isinstance(m["content"], list):
            for b in m["content"]:
                if b.get("type") == "tool_result":
                    content = b.get("content")
                    if isinstance(content, list):
                        content = " ".join(x.get("text", "") for x in content if isinstance(x, dict))
                    results[b["tool_use_id"]] = {"error": bool(b.get("is_error")), "text": _summarize_result(content)}
    turns = []
    for m in messages:
        if m["role"] != "assistant":
            continue
        blocks = m["content"] if isinstance(m["content"], list) else [{"type": "text", "text": m["content"]}]
        said = [b["text"] for b in blocks if b.get("type") == "text" and b.get("text")]
        calls = []
        for b in blocks:
            if b.get("type") != "tool_use":
                continue
            args = b.get("input") or {}
            calls.append({"tool": b["name"], "plain": explain.describe_tool_call(b["name"], args),
                          "args": args if b["name"] != "submit_rules" else {"columns": len(args.get("columns", []))},
                          "result": results.get(b["id"])})  # fmt: skip
        turns.append({"turn": len(turns) + 1, "said": said, "calls": calls})
    return turns


def agent_view(run_id: uuid.UUID) -> dict[str, Any]:
    metrics = _one("SELECT * FROM meta.agent_runs WHERE run_id = %s ORDER BY updated_at DESC LIMIT 1", (run_id,))
    tool_calls = _q("SELECT tool, args, ok, error, latency_ms, result_bytes, created_at FROM meta.tool_calls"
                    " WHERE run_id = %s ORDER BY created_at DESC LIMIT 40", (run_id,))  # fmt: skip
    for c in tool_calls:
        c["plain"] = explain.describe_tool_call(c["tool"], c["args"] or {})
    llm = _one("""SELECT count(*) AS calls, coalesce(sum(cost_usd), 0) AS cost, count(*) FILTER (WHERE cache_hit) AS hits,
                         count(*) FILTER (WHERE status = 'error') AS errors,
                         (array_agg(error ORDER BY created_at DESC) FILTER (WHERE status = 'error'))[1] AS last_error
                    FROM meta.llm_calls WHERE run_id = %s AND purpose = 'dq_agent'""", (run_id,))  # fmt: skip
    snap = agent_snapshot(run_id)
    return jsonable({"snapshot": snap and {k: v for k, v in snap.items() if k != "validation"},
                     "validation": snap and snap.get("validation"), "metrics": metrics, "recent_tool_calls": tool_calls,
                     "llm": llm, "transcript": agent_transcript(run_id), "model": settings.main_model})  # fmt: skip


# --- rules ---------------------------------------------------------------------------


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def rules_view(run_id: uuid.UUID) -> dict[str, Any] | None:
    rules, state, path = current_rules(run_id)
    if path is None:
        return None
    validation_path = proposed_path(str(run_id)).parent / "validation.json"
    validation = json.loads(validation_path.read_text()) if validation_path.exists() else None
    if rules is None:
        return {"state": state, "path": _rel(path), "invalid": True, "validation": validation}
    data = rules.model_dump(mode="json")
    try:
        issues = {i["id"]: explain.describe_issue(i) for i in load_profile(run_id)["issues"]}
    except (LookupError, TypeError, ValueError):
        issues = {}
    from pipeline.web import review
    from pipeline.web.rules_edit import edited_columns, mapping_decisions

    edited = set(edited_columns(run_id, data)) if state == "proposed" else set()
    decisions = review.status(run_id, data)
    ai_mapped = mapping_decisions(run_id, data)
    sample = ((validation or {}).get("stats") or {}).get("columns", {})

    def described(ids: list[str]) -> list[dict[str, Any]]:
        return [{"id": i, **({k: issues[i][k] for k in ("title", "plain", "severity", "why")} if i in issues else {})}
                for i in ids]  # fmt: skip

    def common(key: str, rule: dict[str, Any], model: Any) -> dict[str, Any]:
        """What every reviewable rule carries: its key, review state, issues, and the exact file text."""
        written = model.model_dump(mode="json", exclude_defaults=True, exclude_none=True)
        return {"key": key, "review": decisions.get(key, {"decision": "pending"}),
                "issues": described(rule.get("addresses", [])), "all_fields": rule,
                "yaml": yaml.safe_dump(written, sort_keys=False, allow_unicode=True, width=100)}  # fmt: skip

    columns = [{**c, **common(f"column:{c['name']}", c, m), "plain": explain.describe_column_rule(c),
                "type_label": explain.TYPES.get(c["type"], c["type"]), "stats": sample.get(c["name"]),
                "edited": c["name"] in edited, "ai_mapped": ai_mapped.get(c["name"], [])}
               for c, m in zip(data["columns"], rules.columns, strict=True)]  # fmt: skip
    row_checks = [{**c, **common(review.row_check_key(c), c, m), "plain": explain.describe_row_check(c),
                   "error_key": _check_label(m)}
                  for c, m in zip(data.get("row_checks", []), rules.row_checks, strict=True)]  # fmt: skip
    dedup = data.get("dedup") and {**data["dedup"], **common("dedup", data["dedup"], rules.dedup),
                                   "plain": explain.describe_dedup(data["dedup"])}  # fmt: skip
    resolved = {r["issue_id"]: r for r in data.get("addressed_issues", [])}
    cited = {i for c in data["columns"] for i in c.get("addresses", [])}
    cited |= {i for ch in data.get("row_checks", []) for i in ch.get("addresses", [])}
    if data.get("dedup"):
        cited |= set(data["dedup"].get("addresses", []))
    coverage = []
    for issue_id, issue in issues.items():
        res = resolved.get(issue_id)
        how = "rule" if issue_id in cited else (res or {}).get("resolution", "open")
        coverage.append({"id": issue_id, "title": issue["title"], "plain": issue["plain"], "severity": issue["severity"],
                         "resolution": how, "reason": (res or {}).get("reason")})  # fmt: skip
    agent = _one("SELECT status, warnings FROM meta.agent_runs WHERE run_id = %s ORDER BY updated_at DESC LIMIT 1",
                 (run_id,))  # fmt: skip
    return jsonable({
        "state": state, "agent_status": (agent or {}).get("status"), "path": _rel(path), "rules_hash": rules_hash(rules),
        "generated_by": data["generated_by"], "columns": columns, "row_checks": row_checks, "dedup": dedup,
        "needs_human": data.get("needs_human", []), "coverage": coverage, "validation": validation,
        "review": review.summary(decisions), "field_help": explain.FIELD_HELP,
    })  # fmt: skip


# --- silver preview ------------------------------------------------------------------


@dataclass
class Preview:
    rules: SilverRules
    state: str
    rules_hash: str
    rows: list[dict[str, Any]]  # compiled output + __payload, __line
    build: dict[str, Any] | None = None  # the stored build the rows were read from, if any


_cache: "OrderedDict[tuple[str, str], Preview]" = OrderedDict()
_cache_lock = threading.Lock()
_compute_locks: dict[tuple[str, str], threading.Lock] = {}


def silver_preview(run_id: uuid.UUID) -> Preview | None:
    """Run the compiled rules over every ok bronze row of the run (read-only), cached."""
    rules, state, _ = current_rules(run_id)
    if rules is None or state is None:
        return None
    build = latest_build(run_id) if state == "approved" else None
    stored = build if is_current(build, rules) else None
    key = (str(run_id), rules_hash(rules), str(stored["build_id"]) if stored else "")
    with _cache_lock:
        if key in _cache:
            _cache.move_to_end(key)
            return _cache[key]
        lock = _compute_locks.setdefault(key, threading.Lock())
    with lock:
        with _cache_lock:
            if key in _cache:
                return _cache[key]
        if stored:  # read the built table: what Gold will read, and fast
            schema, _, table = stored["target_table"].partition(".")
            query = sql.SQL("SELECT c.*, b.payload AS __payload, b.source_line AS __line FROM {} AS c"
                            " JOIN bronze.records b ON b.bronze_id = c.bronze_id ORDER BY b.source_line").format(
                sql.Identifier(schema, table))  # fmt: skip
            params: dict[str, Any] = {}
        else:
            compiled = compile_rules(rules, str(run_id))
            query = sql.SQL("SELECT c.*, b.payload AS __payload, b.source_line AS __line FROM ({}) AS c"
                            " JOIN bronze.records b ON b.bronze_id = c.bronze_id ORDER BY b.source_line").format(compiled.query)  # fmt: skip
            params = compiled.params
        with get_ro_conn(PREVIEW_TIMEOUT) as conn:
            rows = conn.cursor(row_factory=dict_row).execute(query, params).fetchall()
        preview = Preview(rules, state, key[1], rows, stored)
        with _cache_lock:
            _cache[key] = preview
            while len(_cache) > PREVIEW_CACHE_SIZE:
                _cache.popitem(last=False)
        return preview


def is_cached(run_id: uuid.UUID) -> bool:
    rules, _, _ = current_rules(run_id)
    return rules is not None and any(k[:2] == (str(run_id), rules_hash(rules)) for k in _cache)


def _shown(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Decimal):
        return format(value.normalize(), "f") if value == value.to_integral_value() else str(value)
    return str(value)


def _cell(rule: Any, row: dict[str, Any]) -> dict[str, Any]:
    before = row["__payload"].get(rule.source)
    after = _shown(row[rule.name])
    flags = (row["_quality_flags"] or {}).get(rule.name) or []
    blank_before = before is None or not str(before).strip()
    if any(f != "unmapped" for f in flags):
        change = "fixed"
    elif blank_before and after is None:
        change = "same"
    else:
        change = "same" if (before or "") == (after or "") else "reformatted"
    return {"before": before, "after": after, "flags": flags, "change": change}


def _row_status(row: dict[str, Any]) -> str:
    if row["_rejected"]:
        return "rejected"
    if row["_dup_rank"] > 1 or row.get("_duplicate_of"):
        return "duplicate"
    return "flagged" if row["_errors"] else "kept"


def silver_summary(run_id: uuid.UUID) -> dict[str, Any] | None:
    preview = silver_preview(run_id)
    if preview is None:
        return None
    rows, rules = preview.rows, preview.rules
    status = Counter(_row_status(r) for r in rows)
    errors = Counter(e for r in rows for e in r["_errors"])
    swapped = sum(any(f.startswith("swapped:") for fl in (r["_quality_flags"] or {}).values() for f in fl) for r in rows)
    declared = sum(bool(r.get("_duplicate_of")) for r in rows)
    types = {c.name: c.type for c in rules.columns}
    columns = []
    for c in rules.columns:
        cells = [_cell(c, r) for r in rows]
        flags = Counter(f.split(":")[0] for cell in cells for f in cell["flags"])
        examples = Counter((cell["before"], cell["after"]) for cell in cells if cell["change"] != "same")
        leftovers: dict[str, Counter[str]] = {}  # values outside the standard list, grouped like the mapping matches
        for cell in cells:
            if "unmapped" in cell["flags"] and cell["before"] is not None:
                raw = str(cell["before"]).strip()
                leftovers.setdefault(normalization_key(raw), Counter())[raw] += 1
        columns.append({
            "name": c.name, "source": c.source, "type": explain.TYPES.get(c.type, c.type),
            "empty_before": sum(cell["before"] is None or not str(cell["before"]).strip() for cell in cells),
            "empty_after": sum(cell["after"] is None for cell in cells),
            "fixed": sum(cell["change"] == "fixed" for cell in cells),
            "reformatted": sum(cell["change"] == "reformatted" for cell in cells),
            "flags": [{"flag": k, "label": explain.flag_label(k), "count": v} for k, v in flags.most_common()],
            "examples": [{"before": b, "after": a, "count": n} for (b, a), n in examples.most_common(6)],
            "leftovers": sorted(({"value": spellings_of(s)[0], "count": sum(s.values()), "spellings": spellings_of(s)}
                                 for s in leftovers.values()), key=lambda x: (-x["count"], x["value"])),
            "plain": explain.describe_column_rule(c.model_dump(mode="json")), "generated_by": c.generated_by,
        })  # fmt: skip
    return jsonable({
        "state": preview.state, "rules_hash": preview.rules_hash, "rows_in": len(rows), "status": dict(status),
        "build": preview.build and {k: preview.build[k] for k in ("build_id", "started_at", "duration_ms", "target_table")},
        "rows_out": status["kept"] + status["flagged"],
        "errors": [{"error": k, "label": explain.error_label(k, types), "count": v} for k, v in errors.most_common()],
        "columns": columns,
        "target_table": rules.target_table, "flag_help": explain.FLAGS,
        "row_fixes": {"swapped_columns": swapped, "declared_duplicate": declared},
    })  # fmt: skip


def silver_rows(run_id: uuid.UUID, show: str, column: str | None, offset: int, limit: int) -> dict[str, Any] | None:
    preview = silver_preview(run_id)
    if preview is None:
        return None
    rules = [c for c in preview.rules.columns if not column or c.name == column] or list(preview.rules.columns)
    types = {c.name: c.type for c in preview.rules.columns}
    picked = []
    for r in preview.rows:
        cells = {c.name: _cell(c, r) for c in rules}
        status = _row_status(r)
        if show == "changed" and all(x["change"] == "same" for x in cells.values()):
            continue
        if show in ("rejected", "flagged", "duplicate") and status != show:
            continue
        picked.append((r, cells, status))
    page = picked[max(offset, 0): max(offset, 0) + min(limit, 200)]
    return jsonable({
        "total": len(picked), "columns": [{"name": c.name, "source": c.source} for c in rules],
        "rows": [{"bronze_id": r["bronze_id"], "source_line": r["__line"], "status": s, "errors": r["_errors"],
                  "error_labels": [explain.error_label(e, types) for e in r["_errors"]], "cells": cells}
                 for r, cells, s in page],
    })  # fmt: skip


def silver_column(run_id: uuid.UUID, column: str) -> dict[str, Any] | None:
    """Every distinct raw value of one column and what it became, with counts."""
    preview = silver_preview(run_id)
    if preview is None:
        return None
    rule = next((c for c in preview.rules.columns if c.name == column), None)
    if rule is None:
        return None
    groups: Counter[tuple[Any, Any, tuple[str, ...], str]] = Counter()
    for r in preview.rows:
        cell = _cell(rule, r)
        groups[(cell["before"], cell["after"], tuple(cell["flags"]), cell["change"])] += 1
    items = [{"before": b, "after": a, "flags": [explain.flag_label(f) for f in fl], "change": ch, "count": n}
             for (b, a, fl, ch), n in groups.most_common(300)]  # fmt: skip
    return jsonable({"column": column, "source": rule.source, "distinct_before": len({k[0] for k in groups}),
                     "distinct_after": len({k[1] for k in groups}), "values": items,
                     "plain": explain.describe_column_rule(rule.model_dump(mode="json")), "why": rule.why})  # fmt: skip


def trace(run_id: uuid.UUID, bronze_id: int) -> dict[str, Any] | None:
    """One record's journey: file line -> bronze row -> silver row, value by value."""
    record = _one("SELECT * FROM bronze.records WHERE run_id = %s AND bronze_id = %s", (run_id, bronze_id))
    if not record:
        return None
    run = run_row(run_id) or {}
    out: dict[str, Any] = {
        "file": {"name": file_label(run.get("source_file", "")), "line": record["source_line"], "raw": record["raw_line"],
                 "encoding": run.get("encoding")},
        "bronze": {k: record[k] for k in ("bronze_id", "batch_no", "parse_status", "parse_error", "row_hash",
                                          "ingested_at", "payload")},
        "silver": None,
    }  # fmt: skip
    preview = silver_preview(run_id) if record["parse_status"] == "ok" else None
    if preview:
        row = next((r for r in preview.rows if r["bronze_id"] == bronze_id), None)
        if row:
            out["silver"] = {
                "state": preview.state, "status": _row_status(row), "errors": row["_errors"],
                "error_labels": [explain.error_label(e, {c.name: c.type for c in preview.rules.columns})
                                 for e in row["_errors"]],
                "cells": [{"name": c.name, "source": c.source, **_cell(c, row),
                           "flag_labels": [explain.flag_label(f) for f in _cell(c, row)["flags"]],
                           "plain": explain.describe_column_rule(c.model_dump(mode="json"))} for c in preview.rules.columns],
            }  # fmt: skip
    return jsonable(out)


# --- gold ----------------------------------------------------------------------------


def gold_state(run_id: uuid.UUID) -> tuple[dict[str, Any] | None, bool]:
    """(latest gold build, is it built from the run's current silver build)."""
    from pipeline.gold.build import latest_gold

    g = latest_gold(run_id)
    s = latest_build(run_id)
    current = bool(g and s and g["status"] == "succeeded" and g["silver_build_id"] == s["build_id"])
    return g, current


def gold_view(run_id: uuid.UUID) -> dict[str, Any] | None:
    g, current = gold_state(run_id)
    if not g:
        return None
    out: dict[str, Any] = {"build": {k: g[k] for k in ("build_id", "status", "started_at", "duration_ms", "error_message",
                                                       "row_counts", "checks", "roles", "silver_build_id", "target_schema")},
                           "current": current}  # fmt: skip
    last_ok = g if g["status"] == "succeeded" else None
    if not last_ok:
        from pipeline.db import transaction as _tx

        with _tx() as conn, conn.cursor(row_factory=dict_row) as cur:
            last_ok = cur.execute("SELECT * FROM meta.gold_builds WHERE run_id = %s AND status = 'succeeded'"
                                  " ORDER BY started_at DESC LIMIT 1", (run_id,)).fetchone()  # fmt: skip
    if not last_ok:
        return jsonable(out)
    schema = sql.Identifier(last_ok["target_schema"])
    from pipeline.gold.build import load_model

    escalated = sql.SQL(", ").join(sql.Literal(v) for v in load_model().get("escalated_statuses") or [""])
    q = lambda text: _q_sql(sql.SQL(text).format(g=schema, e=escalated))
    holder = q("SELECT silver_build_id FROM {g}.fact_tickets LIMIT 1")
    if not holder or str(holder[0]["silver_build_id"]) != str(last_ok["silver_build_id"]):
        return jsonable(out | {"note": "the gold tables now hold another run"})
    escalation = bool((last_ok["row_counts"] or {}).get("escalation_rate"))
    out |= {
        "shown_build": str(last_ok["build_id"]),
        "findings": q("SELECT * FROM {g}.data_quality_findings ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,"
                      " rows_affected DESC"),
        "kpis": q("SELECT count(*) AS tickets, count(*) FILTER (WHERE is_resolved) AS resolved,"
                  " count(*) FILTER (WHERE usable_for_time_metrics) AS usable,"
                  " round(percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE usable_for_time_metrics)::numeric, 1) AS median_hours,"
                  " count(*) FILTER (WHERE sla_met IS NOT NULL) AS sla_measured, count(*) FILTER (WHERE sla_met) AS sla_met,"
                  " sum(cost) AS total_cost, count(cost) AS with_cost"
                  + (", count(*) FILTER (WHERE status = ANY(ARRAY[{e}]::text[])) AS escalated" if escalation else "")
                  + " FROM {g}.fact_tickets")[0],
        "by_priority": q("SELECT coalesce(priority, '(unknown)') AS priority, count(*) AS tickets, count(*) FILTER (WHERE is_resolved) AS resolved,"
                         " count(*) FILTER (WHERE usable_for_time_metrics) AS usable,"
                         " count(*) FILTER (WHERE is_resolved AND NOT usable_for_time_metrics) AS excluded,"
                         " round(percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE usable_for_time_metrics)::numeric, 1) AS median_hours,"
                         " round(percentile_cont(0.9) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE usable_for_time_metrics)::numeric, 1) AS p90_hours,"
                         " count(*) FILTER (WHERE sla_met IS NOT NULL) AS sla_measured, count(*) FILTER (WHERE sla_met) AS sla_met"
                         " FROM {g}.fact_tickets GROUP BY 1 ORDER BY 2 DESC"),
        "monthly": q("SELECT month, sum(opened) AS opened, sum(closed) AS closed, sum(closed_date_unknown) AS closed_unknown,"
                     " sum(open_at_month_end) AS backlog FROM {g}.volume_backlog GROUP BY 1 ORDER BY 1 NULLS LAST"),
        "by_category": q("SELECT category, sum(tickets) AS tickets, sum(tickets_with_cost) AS with_cost, sum(total_cost) AS total_cost,"
                         " round(sum(total_cost) / nullif(sum(tickets_with_cost), 0), 2) AS avg_cost FROM {g}.cost_summary"
                         " GROUP BY 1 ORDER BY 4 DESC"),
        "category_source": q("SELECT category_source, sum(tickets) AS tickets FROM {g}.dim_category GROUP BY 1"),
        "escalation": escalation,
    }  # fmt: skip
    return jsonable(out)


def _q_sql(query: sql.Composable) -> list[dict[str, Any]]:
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(query).fetchall()
