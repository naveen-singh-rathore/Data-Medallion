"""The DQ agent graph.

START -> prepare -> agent ->(tool calls) tools -> agent
                         ->(budget / no tool call) force_submit -> agent
                         ->(submit_rules) validate ->(ok) map_leftovers -> approve -> save -> END
                                                   ->(errors, attempts < 3) agent
                                                   ->(errors, attempts >= 3) END (status failed)
Pause (LangGraph interrupt): approve waits for a human. The business context is the AI's draft
unless a human wrote one; if no draft can be made, the agent runs without context.
State is checkpointed in Postgres, so a pause survives the process (resume via the CLI).
"""

import json
import operator
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from pipeline.agent_tools import run_tool, tool_schemas
from pipeline.card import render_card
from pipeline.config import settings
from pipeline.context import ensure_context
from pipeline.dq_agent import metrics
from pipeline.dq_agent.messages import (
    clean_block,
    compact,
    tool_result,
    tool_uses,
    with_cache_points,
)
from pipeline.dq_agent.prompts import (
    PROMPT_VERSION,
    SUBMIT_TOOL,
    submit_rules_tool,
    system_prompt,
    task_message,
)
from pipeline.llm import LLMError, call_llm
from pipeline.logging_setup import get_logger
from pipeline.profiling import load_profile
from pipeline.silver import mapping_assist
from pipeline.silver.defaults import default_rules, flagged_columns, merge_rules
from pipeline.silver.rules_schema import (
    APPROVED_PATH,
    RULES_DIR,
    SilverRules,
    from_yaml,
    rules_hash,
    to_yaml,
)
from pipeline.silver.validator import check_schema, validate_rules

TOOL_BUDGET = 20
TURN_BUDGET = 25
HARD_TURN_LIMIT = 40  # safety net if the model keeps answering without calling submit_rules
MAX_ATTEMPTS = 3
MAX_TOKENS = 20_000  # non-streaming ceiling; thinking + one large submit_rules call
FEEDBACK_BYTES = 2048
FORCE_MESSAGE = ("Tool budget used. Call submit_rules now with your best rules; use needs_human for "
                 "anything unsure.")  # fmt: skip


class State(TypedDict, total=False):
    run_id: str
    thread_id: str
    card: str
    flagged_columns: list[str]
    default_rules: dict[str, Any]
    required_ids: list[str]
    messages: Annotated[list[dict[str, Any]], operator.add]
    forced: bool
    proposed_rules: dict[str, Any] | None
    agent_rules: dict[str, Any] | None  # merged rules exactly as the agent proposed them
    validation: dict[str, Any] | None
    first_validation_errors: list[str] | None
    approval_errors: list[str] | None
    attempts: int
    tool_calls: int
    tool_calls_by_name: dict[str, int]
    turns: int
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cost_usd: float
    duration_ms: int | None
    status: str
    started_at: str


def run_dir(run_id: str) -> Path:
    return RULES_DIR / run_id


def request_options(model: str) -> dict[str, Any]:
    """Extra request fields for the agent model.

    - thinking.block_binding=drop_block: compaction and the force_submit tool removal edit the
      history; on models with preserved thinking that would otherwise be a 400. Affected thinking
      blocks are dropped instead (beta thinking-binding-controls-2026-08-01).
    - fallbacks=default: server-side refusal fallback on Opus/Sonnet (beta server-side-fallback-2026-07-01).
    """
    betas = ["thinking-binding-controls-2026-08-01"]
    options: dict[str, Any] = {
        "max_tokens": MAX_TOKENS,
        "thinking": {"type": "adaptive", "block_binding": {"prefix_mismatch_behavior": "drop_block"}},
    }
    if model.startswith(("claude-opus", "claude-sonnet")):
        betas.append("server-side-fallback-2026-07-01")
        options["extra_body"] = {"fallbacks": "default"}
    options["extra_headers"] = {"anthropic-beta": ",".join(betas)}
    return options


def _elapsed_ms(state: State) -> int:
    started = datetime.fromisoformat(state["started_at"])
    return int((datetime.now(UTC) - started).total_seconds() * 1000)


def build_graph(checkpointer: Any = None, client: Any = None, mode: str | None = None) -> Any:
    """Compile the graph. `client`/`mode` are passed to every LLM call (tests inject a fake client)."""
    log = get_logger("dq_agent")

    # --- prepare --------------------------------------------------------------------------
    def prepare(state: State) -> dict[str, Any]:
        run_id = uuid.UUID(state["run_id"])
        try:
            context: dict[str, Any] | None = ensure_context(run_id, client=client)
        except LLMError as exc:  # no draft possible (e.g. replay without a recording): go on without one
            log.warning("context unavailable, continuing without it", run_id=state["run_id"], error=str(exc)[:200])
            context = None
        card = render_card(run_id, exact=settings.llm_mode != "replay" and mode != "replay").text
        profile = load_profile(run_id)
        flagged = sorted(flagged_columns(profile["issues"]), key=[p["column"] for p in profile["columns"]].index)
        defaults = default_rules(run_id)
        required = [i["id"] for i in profile["issues"] if i["severity"] in ("high", "medium")]
        task = task_message(context, card, flagged, [c.source for c in defaults.columns], required, TOOL_BUDGET)
        return {"card": card, "flagged_columns": flagged, "default_rules": defaults.model_dump(mode="json"),
                "required_ids": required, "messages": [{"role": "user", "content": task}], "status": "running"}  # fmt: skip

    # --- agent ----------------------------------------------------------------------------
    def agent(state: State) -> dict[str, Any]:
        tools = [submit_rules_tool()] if state.get("forced") else [*tool_schemas(), submit_rules_tool()]
        request = with_cache_points(compact(state["messages"]))
        system = [{"type": "text", "text": system_prompt(), "cache_control": {"type": "ephemeral"}}]
        result = call_llm("dq_agent", settings.main_model, request, PROMPT_VERSION, uuid.UUID(state["run_id"]),
                          tools=tools, system=system, effort="high", options=request_options(settings.main_model),
                          mode=mode, client=client)  # fmt: skip
        turns = state.get("turns", 0) + 1
        log.info("agent turn", run_id=state["run_id"], turn=turns, input_tokens=result.input_tokens,
                 cached_tokens=result.cached_tokens, output_tokens=result.output_tokens,
                 tool_uses=[b["name"] for b in result.content if b.get("type") == "tool_use"])  # fmt: skip
        return {
            "messages": [{"role": "assistant", "content": [clean_block(b) for b in result.content]}],
            "turns": turns,
            "input_tokens": state.get("input_tokens", 0) + result.input_tokens,
            "output_tokens": state.get("output_tokens", 0) + result.output_tokens,
            "cached_tokens": state.get("cached_tokens", 0) + result.cached_tokens,
            "cost_usd": round(state.get("cost_usd", 0.0) + float(result.cost_usd), 6),
        }

    def route_agent(state: State) -> str:
        uses = tool_uses(state["messages"][-1])
        if any(u["name"] == SUBMIT_TOOL for u in uses):
            return "validate"
        if state.get("turns", 0) >= HARD_TURN_LIMIT:
            return "give_up"
        over_budget = state.get("tool_calls", 0) >= TOOL_BUDGET or state.get("turns", 0) >= TURN_BUDGET
        if (over_budget and not state.get("forced")) or not uses:
            return "force_submit"
        return "tools"

    # --- tools ----------------------------------------------------------------------------
    def tools(state: State) -> dict[str, Any]:
        uses = tool_uses(state["messages"][-1])
        by_name = dict(state.get("tool_calls_by_name", {}))
        results = []
        for use in uses:
            response = run_tool(use["name"], use.get("input") or {}, state["run_id"])  # errors come back, not raised
            results.append(tool_result(use["id"], json.dumps(response, ensure_ascii=False, default=str),
                                       is_error=not response["ok"]))  # fmt: skip
            by_name[use["name"]] = by_name.get(use["name"], 0) + 1
        return {"messages": [{"role": "user", "content": results}], "tool_calls": state.get("tool_calls", 0) + len(uses),
                "tool_calls_by_name": by_name}  # fmt: skip

    # --- force_submit ---------------------------------------------------------------------
    def force_submit(state: State) -> dict[str, Any]:
        """Answer any pending calls without running them, then ask for submit_rules (only tool left)."""
        pending = [tool_result(u["id"], "Not run: tool budget used.", is_error=True)
                   for u in tool_uses(state["messages"][-1])]  # fmt: skip
        return {"messages": [{"role": "user", "content": [*pending, {"type": "text", "text": FORCE_MESSAGE}]}],
                "forced": True}  # fmt: skip

    # --- validate -------------------------------------------------------------------------
    def validate(state: State) -> dict[str, Any]:
        uses = tool_uses(state["messages"][-1])
        submit = next(u for u in uses if u["name"] == SUBMIT_TOOL)
        others = [tool_result(u["id"], "Not run: submit_rules ends the investigation.", is_error=True)
                  for u in uses if u is not submit]  # fmt: skip
        defaults = SilverRules.model_validate(state["default_rules"])
        # The pipeline owns the run id (the card shows only a short form); never trust the model's copy.
        submitted = {**(submit.get("input") or {}), "source_run_id": state["run_id"]}
        rules, schema_errors = check_schema(submitted)
        if rules is None:
            report_dict = {"ok": False, "layer": "schema", "errors": schema_errors, "warnings": [], "stats": {}}
            merged = None
        else:
            merged = merge_rules(defaults, rules.model_copy(update={"generated_by": "agent"}))
            report = validate_rules(merged, state["run_id"])
            report_dict = {"ok": report.ok, "layer": report.layer, "errors": report.errors, "warnings": report.warnings,
                           "stats": report.stats}  # fmt: skip
        update: dict[str, Any] = {"validation": report_dict}
        folder = run_dir(state["run_id"])
        folder.mkdir(parents=True, exist_ok=True)
        if report_dict["ok"] and merged is not None:
            (folder / "proposed.yaml").write_text(to_yaml(merged))
            (folder / "validation.json").write_text(json.dumps(report_dict, indent=2, default=str))
            done = tool_result(submit["id"], "VALID - rules are waiting for human approval.")
            update |= {"messages": [{"role": "user", "content": [*others, done]}], "status": "awaiting_approval",
                       "proposed_rules": merged.model_dump(mode="json"), "agent_rules": merged.model_dump(mode="json"),
                       "duration_ms": _elapsed_ms(state)}  # fmt: skip
            metrics.write_agent_run({**state, **update}, "awaiting_approval", rules_hash=rules_hash(merged))
            return update
        attempts = state.get("attempts", 0) + 1
        feedback = _feedback(report_dict)
        update |= {"attempts": attempts, "messages": [{"role": "user", "content": [*others, tool_result(
            submit["id"], feedback, is_error=True)]}]}  # fmt: skip
        if attempts == 1:
            update["first_validation_errors"] = report_dict["errors"]
        if attempts >= MAX_ATTEMPTS:
            if merged is not None:
                (folder / "proposed_failed.yaml").write_text(to_yaml(merged))
            (folder / "validation.json").write_text(json.dumps(report_dict, indent=2, default=str))
            update |= {"status": "failed", "duration_ms": _elapsed_ms(state),
                       "proposed_rules": merged.model_dump(mode="json") if merged else None}  # fmt: skip
            metrics.write_agent_run({**state, **update}, "failed")
        return update

    def route_validate(state: State) -> str:
        if state.get("status") == "awaiting_approval":
            return "map_leftovers"
        return END if state.get("status") == "failed" else "agent"

    # --- map_leftovers --------------------------------------------------------------------
    def map_leftovers(state: State) -> dict[str, Any]:
        """The cheap model decides values left outside the standard lists; never blocks the review."""
        rules = SilverRules.model_validate(state["proposed_rules"])
        try:
            done = mapping_assist.complete(state["run_id"], rules, client=client, mode=mode)
        except LLMError as exc:
            log.warning("leftover mapping skipped", run_id=state["run_id"], error=str(exc)[:200])
            return {}
        report_dict = mapping_assist.write(run_dir(state["run_id"]), done, settings.cheap_model)
        if report_dict is None:
            return {}
        completed = done["rules"]
        dumped = completed.model_dump(mode="json")
        update = {"proposed_rules": dumped, "agent_rules": dumped, "validation": report_dict,
                  "input_tokens": state.get("input_tokens", 0) + done["input_tokens"],
                  "output_tokens": state.get("output_tokens", 0) + done["output_tokens"],
                  "cost_usd": round(state.get("cost_usd", 0.0) + done["cost_usd"], 6)}  # fmt: skip
        metrics.write_agent_run({**state, **update}, "awaiting_approval", rules_hash=rules_hash(completed))
        return update

    def give_up(state: State) -> dict[str, Any]:
        update = {"status": "failed", "duration_ms": _elapsed_ms(state)}
        metrics.write_agent_run({**state, **update}, "failed")
        return update

    # --- approve / save -------------------------------------------------------------------
    def approve(state: State) -> dict[str, Any]:
        path = run_dir(state["run_id"]) / "proposed.yaml"
        decision = interrupt({
            "reason": "awaiting_approval", "proposed": str(path),
            "warnings": (state.get("validation") or {}).get("warnings", []),
            "needs_human": (state.get("proposed_rules") or {}).get("needs_human", []),
            "errors": state.get("approval_errors") or [],
        })  # fmt: skip
        if (decision or {}).get("action") == "reject":
            update = {"status": "rejected", "approval_errors": None}
            metrics.write_agent_run(
                {**state, **update}, "rejected", warnings=[f"rejected: {decision.get('reason', '')}"]
            )
            return update
        edited, errors = _load_edited(path, state)
        if errors:
            return {"approval_errors": errors, "status": "awaiting_approval"}
        return {"proposed_rules": edited, "approval_errors": None, "status": "approved"}

    def route_approve(state: State) -> str:
        return {"approved": "save", "rejected": END}.get(state.get("status", ""), "approve")

    def save(state: State) -> dict[str, Any]:
        rules = SilverRules.model_validate(state["proposed_rules"])
        APPROVED_PATH.parent.mkdir(parents=True, exist_ok=True)
        APPROVED_PATH.write_text(to_yaml(rules))
        metrics.write_agent_run(state, "approved", rules_hash=rules_hash(rules))
        return {"status": "approved"}

    graph = StateGraph(State)
    for name, fn in [("prepare", prepare), ("agent", agent), ("tools", tools), ("force_submit", force_submit),
                     ("validate", validate), ("map_leftovers", map_leftovers), ("give_up", give_up),
                     ("approve", approve), ("save", save)]:  # fmt: skip
        graph.add_node(name, fn)
    graph.add_edge(START, "prepare")
    graph.add_edge("prepare", "agent")
    graph.add_conditional_edges("agent", route_agent, ["tools", "validate", "force_submit", "give_up"])
    graph.add_edge("tools", "agent")
    graph.add_edge("force_submit", "agent")
    graph.add_conditional_edges("validate", route_validate, ["map_leftovers", "agent", END])
    graph.add_edge("map_leftovers", "approve")
    graph.add_edge("give_up", END)
    graph.add_conditional_edges("approve", route_approve, ["save", "approve", END])
    graph.add_edge("save", END)
    return graph.compile(checkpointer=checkpointer)


def _feedback(report: dict[str, Any]) -> str:
    """Errors only (they name the uncovered issue ids), <= 2 KB."""
    lines = [f"INVALID at layer {report['layer']}: fix exactly these and call submit_rules again.",
             *(f"ERROR {e}" for e in report["errors"])]  # fmt: skip
    text = ""
    for i, line in enumerate(lines):
        candidate = f"{text}\n{line}" if text else line
        if len(candidate.encode()) > FEEDBACK_BYTES - 40:
            return f"{text}\n... {len(lines) - i} more errors"
        text = candidate
    return text


def _load_edited(path: Path, state: State) -> tuple[dict[str, Any] | None, list[str]]:
    """Re-read proposed.yaml (the human may have edited it), mark edited columns, re-validate."""
    try:
        raw = from_yaml(path.read_text())
    except (OSError, TypeError, ValueError) as exc:
        return None, [f"cannot read {path}: {exc}"]
    rules, errors = check_schema(raw)
    if rules is None:
        return None, errors
    agent_columns = {c["source"]: c for c in (state.get("agent_rules") or {}).get("columns", [])}
    columns = []
    for column in rules.columns:
        mine = column.model_dump(mode="json", exclude={"generated_by"})
        theirs = {k: v for k, v in agent_columns.get(column.source, {}).items() if k != "generated_by"}
        columns.append(column if mine == theirs else column.model_copy(update={"generated_by": "human"}))
    edited = rules.model_copy(update={"columns": columns})
    if edited.model_dump(mode="json") != state.get("agent_rules"):
        edited = edited.model_copy(update={"generated_by": "human"})
    report = validate_rules(edited, state["run_id"])
    if not report.ok:
        return None, report.errors
    return edited.model_dump(mode="json"), []


def initial_state(run_id: str, thread_id: str) -> State:
    return State(run_id=run_id, thread_id=thread_id, messages=[], attempts=0, tool_calls=0, tool_calls_by_name={},
                 turns=0, input_tokens=0, output_tokens=0, cached_tokens=0, cost_usd=0.0, forced=False,
                 status="running", started_at=datetime.now(UTC).isoformat())  # fmt: skip
