"""Size, token and cost metrics for a run: full profile vs dataset card, context call, LLM spend."""

import json
import math
import uuid
from typing import Any

from pipeline.card import latest_card
from pipeline.db import transaction
from pipeline.profiling import load_profile


def _chars(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False))


def run_metrics(run_id: uuid.UUID) -> list[tuple[str, str]]:
    """(label, value) rows; card values come from the most recently rendered card."""
    profile = load_profile(run_id)
    profile_chars = sum(map(_chars, profile["columns"])) + _chars(profile["summary"]) + _chars(profile["issues"])
    profile_tokens = math.ceil(profile_chars / 4)
    rows: list[tuple[str, str]] = [
        ("full profile size", f"{profile_chars / 1024:.1f} KB (~{profile_tokens:,} tokens est.)"),
    ]
    card = latest_card(run_id)
    if card:
        exact = (f", exact {card['exact_tokens']:,} ({card['chars_per_token']} chars/token)" if card["exact_tokens"]
                 else " (exact: run `card --exact`)")
        rows += [
            ("card tokens", (f"~{card['est_tokens']:,} est.{exact} | budget {card['budget_tokens']:,}"
             f"{' | OVER BUDGET' if card['over_budget'] else ''}")),
            ("card / profile", f"{card['est_tokens'] / profile_tokens:.1%}"),
            ("issues shown", f"{card['issues_shown']} / {card['issues_total']}"),
            ("columns flagged", f"{card['columns_flagged']} / {card['columns_total']}"),
            ("compressed", ", ".join(f"{k}={v}" for k, v in card["compressed_counts"].items())),
            ("tiers full", ", ".join(f"{k}={'yes' if v else 'no'}" for k, v in card["tiers_full"].items())),
        ]  # fmt: skip
    else:
        rows.append(("card", "not rendered yet (run `card`)"))
    with transaction() as conn:
        context = conn.execute(
            "SELECT c.source, c.approved, l.model, l.input_tokens, l.output_tokens, l.cost_usd, l.latency_ms,"
            " l.cache_hit FROM meta.dataset_context c LEFT JOIN meta.llm_calls l ON l.call_id = c.llm_call_id"
            " WHERE c.run_id = %s ORDER BY c.source = 'human' DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        spend = conn.execute(
            "SELECT count(*), coalesce(sum(cost_usd), 0), count(*) FILTER (WHERE cache_hit),"
            " count(*) FILTER (WHERE status = 'error') FROM meta.llm_calls WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    if context:
        source, approved, model, tokens_in, tokens_out, cost, latency, cache_hit = context
        rows.append(("context source", f"{source} ({'approved' if approved else 'unapproved'})"))
        if model:
            rows.append(("context call", (f"{model}: {tokens_in} in / {tokens_out} out tokens, ${cost:.6f},"
                         f" {latency} ms{' (cache hit)' if cache_hit else ''}")))  # fmt: skip
    else:
        rows.append(("context source", "none"))
    calls, total, hits, errors = spend  # type: ignore[misc]
    rows.append(("LLM spend (run)", f"${total:.6f} over {calls} calls ({hits} cache hits, {errors} errors)"))
    return rows + _agent_rows(run_id)


def _agent_rows(run_id: uuid.UUID) -> list[tuple[str, str]]:
    from pipeline.dq_agent.metrics import (
        latest_agent_run,  # local: keeps `metrics` usable without LangGraph state
    )

    agent = latest_agent_run(str(run_id))
    if not agent:
        return [("DQ agent", "not run yet (dq-run)")]
    by_name = ", ".join(f"{k} {v}" for k, v in sorted(agent["tool_calls_by_name"].items(), key=lambda kv: -kv[1]))
    return [
        ("DQ agent", f"{agent['status']} ({agent['thread_id']}, {agent['model']}, {agent['prompt_version']})"),
        ("agent effort", (f"{agent['turns']} turns, {agent['tool_calls']} tool calls ({by_name or '-'}),"
                         f" {agent['validation_attempts']} failed validations")),
        ("agent tokens", (f"{agent['input_tokens']:,} in ({agent['cached_tokens']:,} cached) / {agent['output_tokens']:,} out,"
                         f" ${agent['cost_usd']:.4f}, {(agent['duration_ms'] or 0) / 1000:.0f} s")),
        ("agent coverage", (f"{agent['issues_addressed']} fixed + {agent['issues_ignored']} ignored of {agent['issues_total']}"
                           f" required issues, {agent['needs_human_count']} questions for a human")),
        ("agent columns", (f"{agent['columns_agent']} agent, {agent['columns_default']} default,"
                          f" {agent['columns_human_edited']} human-edited")),
    ]  # fmt: skip


def format_rows(rows: list[tuple[str, str]]) -> str:
    width = max(len(label) for label, _ in rows)
    return "\n".join(f"  {label:<{width}}  {value}" for label, value in rows)
