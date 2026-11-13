"""Message handling for the agent loop: compaction of old tool results, cache breakpoints,
and cleaning provider content blocks before they are sent back.

Compaction runs on a copy just before each request; the graph state keeps every message.
The system prompt, the task message (card) and validation feedback are never compacted.
"""

import copy
import json
from typing import Any

from pipeline.dq_agent.prompts import SUBMIT_TOOL

KEEP_FULL = 6
STUB_PREFIX = "[compacted]"
CACHE = {"type": "ephemeral"}


def clean_block(block: dict[str, Any]) -> dict[str, Any]:
    """Provider block as input: drop null fields the response model adds (e.g. citations=None)."""
    return {k: v for k, v in block.items() if v is not None}


def tool_uses(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    return [b for b in content if b.get("type") == "tool_use"] if isinstance(content, list) else []


def summarize_result(tool: str, content: str) -> str:
    """One line from a tool result, by code: error text, or the top-level numbers and list sizes."""
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return str(content)[:100]
    if not isinstance(data, dict):
        return str(data)[:100]
    if not data.get("ok", True):
        return f"error: {str(data.get('error'))[:90]}"
    result = data.get("result", data)
    parts = []
    for key, value in result.items() if isinstance(result, dict) else []:
        if isinstance(value, bool | int | float | str) and len(str(value)) <= 40:
            parts.append(f"{key}={value}")
        elif isinstance(value, list | dict):
            parts.append(f"{key}: {len(value)} items")
    return ", ".join(parts)[:160] or "ok"


def compact(messages: list[dict[str, Any]], keep: int = KEEP_FULL) -> list[dict[str, Any]]:
    """Copy of `messages` where all but the last `keep` investigation tool results are one-line stubs."""
    calls = {b["id"]: (b["name"], b.get("input", {})) for m in messages for b in tool_uses(m)}
    positions = [(mi, bi) for mi, m in enumerate(messages) if isinstance(m.get("content"), list)
                 for bi, b in enumerate(m["content"])
                 if b.get("type") == "tool_result" and calls.get(b["tool_use_id"], ("",))[0] != SUBMIT_TOOL]  # fmt: skip
    old = positions[: max(len(positions) - keep, 0)]
    if not old:
        return messages
    result = copy.deepcopy(messages)
    for mi, bi in old:
        block = result[mi]["content"][bi]
        if isinstance(block.get("content"), str) and block["content"].startswith(STUB_PREFIX):
            continue
        name, args = calls.get(block["tool_use_id"], ("?", {}))
        # sort_keys: tool inputs come back from the jsonb cache with reordered keys in replay mode;
        # anything rendered from them must not depend on key order, or request hashes diverge.
        args_text = json.dumps(args, ensure_ascii=False, separators=(",", ":"), sort_keys=True)[:120]
        block["content"] = f"{STUB_PREFIX} {name}({args_text}) → {summarize_result(name, block.get('content', ''))}"
    return result


def with_cache_points(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Cache breakpoints: the task message (card) and the newest user message.
    Moving markers between requests doesn't count as a history edit."""
    result = copy.deepcopy(messages)
    for index in sorted({0, len(result) - 1}):
        message = result[index]
        if message["role"] != "user":
            continue
        if isinstance(message["content"], str):
            message["content"] = [{"type": "text", "text": message["content"]}]
        message["content"][-1]["cache_control"] = CACHE
    return result


def tool_result(tool_use_id: str, content: str, is_error: bool = False) -> dict[str, Any]:
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": tool_use_id, "content": content}
    if is_error:
        block["is_error"] = True
    return block
