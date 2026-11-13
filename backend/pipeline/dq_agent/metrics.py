"""meta.agent_runs: one row per agent thread, upserted at every pause and at the end."""

from typing import Any

from psycopg.types.json import Jsonb

from pipeline.config import settings
from pipeline.db import transaction
from pipeline.dq_agent.prompts import PROMPT_VERSION

COLUMNS = ("thread_id", "run_id", "status", "model", "prompt_version", "turns", "tool_calls", "tool_calls_by_name",
           "input_tokens", "output_tokens", "cached_tokens", "cost_usd", "duration_ms", "validation_attempts",
           "first_validation_errors", "issues_total", "issues_addressed", "issues_ignored", "needs_human_count",
           "columns_default", "columns_agent", "columns_human_edited", "warnings", "rules_hash")  # fmt: skip
JSON_COLUMNS = {"tool_calls_by_name", "first_validation_errors", "warnings"}


def rules_counts(rules: dict[str, Any] | None, required: list[str]) -> dict[str, Any]:
    """Coverage and provenance counts from a (merged) rules dict."""
    if not rules:
        return {"issues_total": len(required)}
    resolutions = {r["issue_id"]: r["resolution"] for r in rules.get("addressed_issues", [])}
    by = [c.get("generated_by") for c in rules.get("columns", [])]
    return {
        "issues_total": len(required),
        "issues_addressed": sum(1 for i in required if resolutions.get(i) == "rule"),
        "issues_ignored": sum(1 for i in required if resolutions.get(i) == "ignored"),
        "needs_human_count": len(rules.get("needs_human", [])),
        "columns_default": by.count("default"), "columns_agent": by.count("agent"), "columns_human_edited": by.count("human"),
    }  # fmt: skip


def write_agent_run(state: dict[str, Any], status: str, **extra: Any) -> None:
    row = {
        "thread_id": state["thread_id"], "run_id": state["run_id"], "status": status, "model": settings.main_model,
        "prompt_version": PROMPT_VERSION, "turns": state.get("turns", 0), "tool_calls": state.get("tool_calls", 0),
        "tool_calls_by_name": state.get("tool_calls_by_name", {}), "input_tokens": state.get("input_tokens", 0),
        "output_tokens": state.get("output_tokens", 0), "cached_tokens": state.get("cached_tokens", 0),
        "cost_usd": state.get("cost_usd", 0.0), "duration_ms": state.get("duration_ms"),
        "validation_attempts": state.get("attempts", 0), "first_validation_errors": state.get("first_validation_errors"),
        "warnings": (state.get("validation") or {}).get("warnings"),
    } | rules_counts(state.get("proposed_rules"), state.get("required_ids", [])) | extra  # fmt: skip
    values = [Jsonb(row.get(c)) if c in JSON_COLUMNS and row.get(c) is not None else row.get(c) for c in COLUMNS]
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "thread_id")
    with transaction() as conn:
        conn.execute(
            f"INSERT INTO meta.agent_runs ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))})"
            f" ON CONFLICT (thread_id) DO UPDATE SET {updates}, updated_at = now()",
            values,
        )


def latest_agent_run(run_id: str) -> dict[str, Any] | None:
    with transaction() as conn:
        cur = conn.execute(
            "SELECT * FROM meta.agent_runs WHERE run_id = %s ORDER BY updated_at DESC LIMIT 1", (run_id,)
        )
        row = cur.fetchone()
        return dict(zip([d.name for d in cur.description], row, strict=True)) if row else None  # type: ignore[union-attr]
