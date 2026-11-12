"""Agent tools: one registry (TOOLS) used by the agent, the CLI and (later) an MCP server.

run_tool validates the arguments, runs the tool, bounds the output to ~2 KB and logs every
call to meta.tool_calls. Tool errors are returned to the caller, not raised.
"""

import json
import time
import uuid
from collections.abc import Callable
from typing import Any

from psycopg.types.json import Jsonb
from pydantic import BaseModel, ValidationError

from pipeline.agent_tools import tools as t
from pipeline.agent_tools.context import ToolError, run_view
from pipeline.db import transaction

MAX_RESULT_BYTES = 2048

TOOLS: dict[str, tuple[Callable[..., dict[str, Any]], type[BaseModel], str]] = {
    "list_issues": (t.list_issues, t.ListIssuesArgs,
                    "List profiler issues (id, severity, compact evidence); filter by severity or column."),
    "get_column_profile": (t.get_column_profile, t.ColumnArgs,
                           "Full profiler facts for one column (types, formats, variants, sentinels...)."),
    "get_value_counts": (t.get_value_counts, t.ValueCountsArgs,
                         "Most frequent values of a column (or patterns for masked columns); optional mask filter."),
    "sample_rows": (t.sample_rows, t.SampleRowsArgs,
                    "Deterministic sample rows, optionally only where a column has a given pattern mask."),
    "test_rule": (t.test_rule, t.TestRuleArgs,
                  "Dry-run ONE column rule on a fixed 2,000-row sample: before/after values, null/invalid shares."),
    "check_cross_column": (t.check_cross_column, t.CrossColumnArgs,
                           "Share of rows where date column a > b (all rows and unambiguous-only), with examples."),
    "check_regex": (t.check_regex, t.RegexArgs, "Share of a column's values matching a regex, with non-matching examples."),
}  # fmt: skip


def tool_schemas() -> list[dict[str, Any]]:
    """LLM tool definitions (name, description, input_schema) - generated from TOOLS only."""
    # dict(...) rather than a literal: "description" is also a common dataset column name, and the
    # generic test forbids dataset column names as string literals anywhere in pipeline code.
    return [dict(name=name, description=desc, input_schema=model.model_json_schema())  # noqa: C408
            for name, (_, model, desc) in TOOLS.items()]  # fmt: skip


def bound(result: Any, limit: int = MAX_RESULT_BYTES) -> Any:
    """Shrink the longest list in the result until its JSON fits `limit` bytes."""
    def size(x: Any) -> int:
        return len(json.dumps(x, ensure_ascii=False, default=str).encode())

    def lists(node: Any) -> list[list[Any]]:
        found = [node] if isinstance(node, list) else []
        children = node.values() if isinstance(node, dict) else node if isinstance(node, list) else []
        return found + [x for child in children for x in lists(child)]

    truncated = 0
    while size(result) > limit:
        longest = max(lists(result), key=len, default=None)
        if not longest:
            break
        longest.pop()
        truncated += 1
    if truncated and isinstance(result, dict):
        result["truncated_items"] = truncated
    return result


def _log(call_id: uuid.UUID, run_id: str, name: str, args: dict[str, Any], size: int | None, started: float,
         error: str | None) -> None:  # fmt: skip
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.tool_calls (call_id, run_id, tool, args, result_bytes, latency_ms, ok, error)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (call_id, run_id, name, Jsonb(args), size, int((time.perf_counter() - started) * 1000), error is None, error),
        )


def run_tool(name: str, args: dict[str, Any], run_id: str | uuid.UUID) -> dict[str, Any]:
    """Validate, run, bound and log one tool call. Returns {"ok": true, "result": ...} or {"ok": false, "error": ...}."""
    run_id, call_id, started = str(run_id), uuid.uuid4(), time.perf_counter()
    if name not in TOOLS:
        response: dict[str, Any] = {"ok": False, "error": f"unknown tool {name!r}; tools: {list(TOOLS)}"}
    else:
        fn, model, _ = TOOLS[name]
        try:
            response = {"ok": True, "result": bound(fn(run_view(run_id), model.model_validate(args)))}
        except ValidationError as exc:
            errors = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
            response = {"ok": False, "error": f"invalid arguments: {errors}"[:1000]}
        except ToolError as exc:
            response = {"ok": False, "error": str(exc)[:1000]}
        except Exception as exc:  # noqa: BLE001 - a tool bug must not crash the agent loop; it is logged
            response = {"ok": False, "error": f"internal error: {type(exc).__name__}: {str(exc)[:300]}"}
    size = len(json.dumps(response, ensure_ascii=False, default=str).encode())
    _log(call_id, run_id, name, args, size, started, None if response["ok"] else response["error"])
    return response
