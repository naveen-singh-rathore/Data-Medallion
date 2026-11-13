"""Reproducible end-to-end run without an API key: `python -m pipeline.cli demo` (or `make demo`).

What makes a fresh clone reproduce the reviewed result:
- fixtures/llm_cache.jsonl and fixtures/classification_cache.jsonl: every recorded LLM answer and
  every cached category label, loaded by init-db (replay mode answers from them, no network).
- rules/silver_rules.yaml: the approved rules; rules/silver_rules.provenance.json: the file they
  were approved for (sha256), the approved business context, and a summary of the review.

Run ids are random per database, and approved rules name the run they were approved on. Approval is
really about the FILE, so when the ingested file's sha256 matches the provenance, the demo binds the
approved rules (unchanged except `source_run_id`) and the approved context to the new run.
`export_fixtures` writes all of the above from the current database.
"""

import hashlib
import json
import shutil
import uuid
from pathlib import Path
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.bronze import get_run, ingest_file
from pipeline.config import REPO_ROOT
from pipeline.context import _save, get_context
from pipeline.db import column_list, insert_sql, table, transaction
from pipeline.logging_setup import get_logger
from pipeline.silver.rules_schema import (
    APPROVED_PATH,
    RULES_DIR,
    SilverRules,
    from_yaml,
    rules_hash,
    to_yaml,
)

FIXTURES = REPO_ROOT / "fixtures"
PROVENANCE = RULES_DIR / "silver_rules.provenance.json"
BOUND_DIR = REPO_ROOT / "build" / "approved"
DEMO_FILE = REPO_ROOT / "data" / "raw_tickets.csv"
CACHES = {  # file -> (table, key column, columns in file order)
    "llm_cache.jsonl": ("meta.llm_cache", "request_hash", ("request_hash", "model", "response")),
    "classification_cache.jsonl": ("meta.classification_cache", "text_hash",
                                   ("text_hash", "task", "prompt_version", "model", "label", "confidence")),
}  # fmt: skip


def _sha(run_id: str) -> str:
    with transaction() as conn:
        return conn.execute("SELECT file_sha256 FROM meta.ingestion_runs WHERE run_id = %s", (run_id,)).fetchone()[0]  # type: ignore[index]


# --- approved rules bound to a file, not a run --------------------------------------------------


def bound_path(run_id: str) -> Path:
    """Where the demo writes the approved rules bound to this run."""
    return BOUND_DIR / f"{run_id}.yaml"


def approved_path_for(run_id: str) -> Path:
    """Where this run's approved rules are: the repo file when it names the run, else a binding."""
    if APPROVED_PATH.exists():
        try:
            if SilverRules.model_validate(from_yaml(APPROVED_PATH.read_text())).source_run_id == run_id:
                return APPROVED_PATH
        except (ValueError, TypeError):
            pass
    return bound_path(run_id) if bound_path(run_id).exists() else APPROVED_PATH


def bind_approved(run_id: str) -> Path:
    """Bind the committed approved rules to `run_id` when the run's file is the one they were approved for."""
    rules = SilverRules.model_validate(from_yaml(APPROVED_PATH.read_text()))
    if rules.source_run_id == run_id:
        return APPROVED_PATH
    prov = json.loads(PROVENANCE.read_text())
    sha = _sha(run_id)
    if prov["file_sha256"] != sha:
        raise ValueError(f"the approved rules are for file {prov['file_sha256'][:12]}…, this run is {sha[:12]}…")
    path = bound_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(to_yaml(rules.model_copy(update={"source_run_id": run_id})))
    return path


def ensure_approved_context(run_id: str) -> dict[str, Any]:
    """The run's context; for a fresh database, the approved context recorded in the provenance."""
    if ctx := get_context(uuid.UUID(run_id)):
        return ctx
    prov = json.loads(PROVENANCE.read_text())
    c = prov["context"]
    _save(uuid.UUID(run_id), c["text"], c.get("draft"), c["source"], True, None)
    return get_context(uuid.UUID(run_id))  # type: ignore[return-value]


# --- fixtures -------------------------------------------------------------------------------------


def load_fixtures(conn: Any) -> dict[str, int]:
    """Insert recorded LLM answers and category labels that are missing (init-db calls this)."""
    loaded = {}
    for name, (target, key, cols) in CACHES.items():
        path = FIXTURES / name
        if not path.exists():
            continue
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if conn.execute(sql.SQL("SELECT count(*) FROM {}").format(table(target))).fetchone()[0] >= len(lines):
            continue  # already loaded (every init-db and test calls this)
        rows = [json.loads(line) for line in lines]
        on_conflict = sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(sql.Identifier(key))
        with conn.cursor() as cur:
            cur.executemany(
                insert_sql(target, cols, on_conflict),
                [tuple(Jsonb(r[c]) if c == "response" else r[c] for c in cols) for r in rows],
            )
        loaded[name] = len(rows)
    return loaded


def export_fixtures(run_id: str) -> dict[str, Any]:
    """Write the caches (sorted, so the files diff cleanly) and the provenance of the approved rules."""
    FIXTURES.mkdir(exist_ok=True)
    counts = {}
    with transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
        for name, (target, key, cols) in CACHES.items():
            query = sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
                column_list(cols), table(target), sql.Identifier(key)
            )
            rows = cur.execute(query).fetchall()
            with (FIXTURES / name).open("w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps({c: (float(r[c]) if c == "confidence" else r[c]) for c in cols},
                                       ensure_ascii=False, sort_keys=True) + "\n")  # fmt: skip
            counts[name] = len(rows)
    rules = SilverRules.model_validate(from_yaml(APPROVED_PATH.read_text()))
    if rules.source_run_id != run_id:
        raise ValueError("rules/silver_rules.yaml is not approved for this run")
    ctx = get_context(uuid.UUID(run_id)) or {}
    decisions_path = RULES_DIR / run_id / "decisions.json"
    decisions = json.loads(decisions_path.read_text()) if decisions_path.exists() else {}
    prov = {
        "file": "data/raw_tickets.csv", "file_sha256": _sha(run_id), "approved_run_id": run_id,
        "rules_hash": rules_hash(rules), "generated_by": rules.generated_by,
        "agent": _recorded_agent(run_id),
        # the seed the reviewed profile's value samples were drawn with (profiles made before the seed was
        # stored used their run id); the demo profiles with it, so the recorded review sees the same values
        "profile_sample_seed": _sample_seed(run_id),
        "context": {"source": ctx.get("source"), "text": ctx.get("text"), "draft": ctx.get("draft")},
        "review": {k: {"decision": v["decision"], **({"reason": v["reason"]} if v.get("reason") else {}),
                       **({"note": v["note"]} if v.get("note") else {})} for k, v in sorted(decisions.items())},
    }  # fmt: skip
    PROVENANCE.write_text(json.dumps(prov, indent=2, ensure_ascii=False, default=str) + "\n")
    return counts | {"provenance": str(PROVENANCE.relative_to(REPO_ROOT))}


# --- the recorded AI review: replayable on any database -------------------------------------------


def proposal_fingerprint(rules: dict[str, Any] | None) -> str | None:
    """Hash of a proposal's content, ignoring the run it was made for and the order of keys."""
    if not rules:
        return None
    body = {k: v for k, v in rules.items() if k != "source_run_id"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def _agent_proposal(thread_id: str | None) -> dict[str, Any] | None:
    """The rules exactly as the AI proposed them, from the thread's checkpoint."""
    from pipeline.dq_agent import runner

    if not thread_id:
        return None
    with runner.compiled(mode="replay") as graph:
        return (graph.get_state(runner.config(thread_id)).values or {}).get("agent_rules")


def _sample_seed(run_id: str) -> str:
    with transaction() as conn:
        row = conn.execute("SELECT summary -> 'profiler' ->> 'sample_seed' FROM meta.dataset_profiles WHERE run_id = %s",
                           (run_id,)).fetchone()  # fmt: skip
    return (row and row[0]) or run_id


def _recorded_agent(run_id: str) -> dict[str, Any] | None:
    """What export-fixtures records about the AI review: its size, cost and the proposal's fingerprint."""
    from pipeline.dq_agent import runner
    from pipeline.dq_agent.metrics import latest_agent_run

    row = latest_agent_run(run_id)
    if not row:
        return None
    proposal = _agent_proposal(runner.latest_thread(run_id))
    return {
        "model": row["model"], "prompt_version": row["prompt_version"], "turns": row["turns"],
        "tool_calls": row["tool_calls"], "validation_attempts": row["validation_attempts"],
        "cost_usd": float(row["cost_usd"] or 0), "duration_ms": row["duration_ms"],
        "proposal_fingerprint": proposal_fingerprint(proposal),
    }  # fmt: skip


def _discard_review(run_id: str, folder: Path, remove_folder: bool) -> None:
    """Remove the scratch agent thread of replay_agent, so the run keeps the approved rules from the repository."""
    with transaction() as conn:
        for name in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
            conn.execute(
                sql.SQL("DELETE FROM {} WHERE thread_id LIKE %s").format(sql.Identifier(name)), (f"dq:{run_id}:%",)
            )
        conn.execute("DELETE FROM meta.agent_runs WHERE run_id = %s", (run_id,))
    if remove_folder:
        shutil.rmtree(folder, ignore_errors=True)


def replay_agent(run_id: str) -> dict[str, Any]:
    """Replay the recorded AI review on this run with no key and compare it with the recorded proposal.

    Model answers come only from the recorded cache (mode="replay"); the tools and the validator run for
    real against this database. The scratch thread is discarded afterwards. A run that already has a
    review is left alone.
    """
    from pipeline.dq_agent import graph as dq_graph
    from pipeline.dq_agent import runner
    from pipeline.llm import LLMError
    from pipeline.silver import mapping_assist

    recorded = (json.loads(PROVENANCE.read_text()).get("agent") or {}).get("proposal_fingerprint")
    if runner.latest_thread(run_id):
        return {"replayed": False, "reason": "this run already has a review"}
    folder = dq_graph.run_dir(run_id)
    existed = folder.exists()
    try:
        result = runner.run(run_id, fresh=True, mode="replay")
        proposal = _agent_proposal(result["thread_id"])
        seen = [proposal_fingerprint(proposal)]
        if proposal and recorded not in seen:
            # the reviewer may have asked the AI to decide leftover values once more: replay that click too
            try:
                again = mapping_assist.complete(run_id, SilverRules.model_validate(proposal), mode="replay")
                seen.append(proposal_fingerprint(again["rules"].model_dump(mode="json")))
            except LLMError:
                pass
        return {"replayed": result["status"] == "awaiting_approval", "status": result["status"],
                "turns": result["turns"], "tool_calls": result["tool_calls"],
                "matches_recorded_proposal": (recorded in seen) if recorded else None}  # fmt: skip
    except LLMError as exc:
        return {"replayed": False, "reason": str(exc)[:200]}
    finally:
        _discard_review(run_id, folder, remove_folder=not existed)


# --- the demo -------------------------------------------------------------------------------------


def run_demo(path: Path = DEMO_FILE) -> dict[str, Any]:
    """init-db -> ingest -> profile -> context -> approved rules -> silver -> classification -> gold,
    then replay the recorded AI review to show that it reproduces (no key, nothing kept)."""
    from pipeline.gold.build import build_gold
    from pipeline.migrate import migrate
    from pipeline.profiling import profile_run
    from pipeline.silver.build import build_silver
    from pipeline.silver.classify import classify_run

    log = get_logger("demo")
    migrate()
    run_id = str(ingest_file(path))
    run = get_run(uuid.UUID(run_id))
    log.info("bronze ready", run_id=run_id, rows=run["rows_loaded_ok"])
    # Same value samples as the recorded review saw, so its tool results (and the replay) are identical.
    profile_run(uuid.UUID(run_id), sample_seed=json.loads(PROVENANCE.read_text()).get("profile_sample_seed"))
    ctx = ensure_approved_context(run_id)
    approved = bind_approved(run_id)
    silver = build_silver(run_id, approved)
    classified = classify_run(run_id, mode="replay", approved_path=approved)
    gold = build_gold(run_id, approved_path=approved)
    return {
        "run_id": run_id, "rows": run["rows_loaded_ok"], "context": ctx["source"], "approved_rules": str(approved),
        "silver": {k: silver[k] for k in ("rows_in", "rows_kept", "rows_flagged", "rows_rejected", "rows_duplicate")},
        "classification": {k: classified[k] for k in ("targets", "cache_hits", "llm_calls", "recovered", "low_confidence")},
        "gold": {"checks": f"{sum(c['ok'] for c in gold['checks'].values())}/{len(gold['checks'])}",
                 "fact_tickets": gold["row_counts"]["fact_tickets"]},
        "ai_review_replay": replay_agent(run_id),
    }  # fmt: skip
