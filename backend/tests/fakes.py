"""Fake LLM clients for tests: no network, ever."""

import json
from typing import Any


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        return self.payload


class FakeMessages:
    def __init__(self, reply: Any, usage: dict[str, int] | None = None) -> None:
        self.reply = reply
        self.usage = usage or {"input_tokens": 1000, "output_tokens": 200}
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> FakeResponse:
        """Replies in order when `reply` is a list (the last one repeats), else the same reply."""
        self.calls.append(kwargs)
        reply = self.reply[min(len(self.calls), len(self.reply)) - 1] if isinstance(self.reply, list) else self.reply
        text = reply if isinstance(reply, str) else json.dumps(reply)
        return FakeResponse({
            "id": "msg_fake", "type": "message", "role": "assistant", "model": kwargs["model"],
            "content": [{"type": "thinking", "thinking": ""}, {"type": "text", "text": text}],
            "stop_reason": "end_turn", "usage": self.usage,
        })  # fmt: skip


class FakeClient:
    def __init__(self, reply: Any, usage: dict[str, int] | None = None) -> None:
        self.messages = FakeMessages(reply, usage)

    def payloads(self) -> str:
        """Everything sent to the model so far, as one string (for leak assertions)."""
        return json.dumps(self.messages.calls, ensure_ascii=False)


class ExplodingClient:
    """Fails the test if anything tries to reach the network."""

    @property
    def messages(self) -> Any:
        raise AssertionError("network call attempted")


class ScriptedMessages:
    """Returns the scripted assistant turns in order; each turn is a list of content blocks."""

    def __init__(self, turns: list[list[dict[str, Any]]]) -> None:
        self.turns = turns
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> FakeResponse:
        self.calls.append(kwargs)
        if len(self.calls) > len(self.turns):
            raise AssertionError(f"script exhausted after {len(self.turns)} turns")
        content = self.turns[len(self.calls) - 1]
        stop = "tool_use" if any(b["type"] == "tool_use" for b in content) else "end_turn"
        return FakeResponse({"id": f"msg_{len(self.calls)}", "type": "message", "role": "assistant",
                             "model": kwargs["model"], "content": content, "stop_reason": stop,
                             "usage": {"input_tokens": 1000, "output_tokens": 100, "cache_read_input_tokens": 500}})


class ScriptedClient:
    def __init__(self, turns: list[list[dict[str, Any]]]) -> None:
        self.messages = ScriptedMessages(turns)


def tool_call(name: str, args: dict[str, Any], n: int) -> dict[str, Any]:
    return {"type": "tool_use", "id": f"toolu_{name}_{n}", "name": name, "input": args}
