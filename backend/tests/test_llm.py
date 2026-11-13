"""Thin LLM client: hashing, cost, replay (zero network), live caching, error logging."""

import uuid
from decimal import Decimal

import pytest
from fakes import ExplodingClient, FakeClient

from pipeline.llm import LLMCacheMiss, call_llm, cost_usd, request_hash

MESSAGES = [{"role": "user", "content": "hello"}]


@pytest.fixture
def version(conn):
    """A unique prompt_version per test; its cache entries and call logs are removed afterwards."""
    value = f"test-{uuid.uuid4()}"
    yield value
    conn.execute("DELETE FROM meta.llm_cache WHERE request_hash IN"
                 " (SELECT request_hash FROM meta.llm_calls WHERE prompt_version = %s)", (value,))  # fmt: skip
    conn.execute("DELETE FROM meta.llm_calls WHERE prompt_version = %s", (value,))


def test_request_hash_is_stable_and_canonical() -> None:
    schema_a = {"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]}
    schema_b = {"required": ["a"], "properties": {"a": {"type": "string"}}, "type": "object"}
    assert request_hash("m", "v1", MESSAGES, json_schema=schema_a) == request_hash(
        "m", "v1", MESSAGES, json_schema=schema_b
    )
    assert request_hash("m", "v1", MESSAGES) != request_hash("m", "v2", MESSAGES)
    assert request_hash("m", "v1", MESSAGES) != request_hash("other", "v1", MESSAGES)
    assert len(request_hash("m", "v1", MESSAGES)) == 64


def test_cost_is_computed_from_usage() -> None:
    usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000, "cache_read_input_tokens": 1_000_000}
    assert cost_usd("claude-haiku-5-5", usage) == Decimal("0.61")
    assert cost_usd("claude-sonnet-5-5", {"input_tokens": 1000, "output_tokens": 500}) == Decimal("0.007")
    assert cost_usd("unknown-model", usage) == Decimal(0)


def test_live_calls_once_then_serves_from_cache(conn, version) -> None:
    client = FakeClient({"answer": 42})
    schema = {"type": "object", "properties": {"answer": {"type": "integer"}}, "required": ["answer"]}
    first = call_llm(
        "test", "claude-haiku-5-5", MESSAGES, version, None, json_schema=schema, mode="live", client=client
    )
    assert (first.data, first.cache_hit, first.cost_usd) == ({"answer": 42}, False, Decimal("0.0002"))
    sent = client.messages.calls[0]
    assert sent["output_config"]["format"] == {"type": "json_schema", "schema": schema}
    assert "temperature" not in sent
    second = call_llm("test", "claude-haiku-5-5", MESSAGES, version, None, json_schema=schema, mode="live",
                      client=ExplodingClient())  # fmt: skip
    assert (second.data, second.cache_hit, second.cost_usd) == ({"answer": 42}, True, Decimal(0))
    logged = conn.execute(
        "SELECT cache_hit, input_tokens, cost_usd FROM meta.llm_calls WHERE prompt_version = %s ORDER BY created_at",
        (version,),
    ).fetchall()
    assert [(r["cache_hit"], r["input_tokens"], r["cost_usd"]) for r in logged] == [
        (False, 1000, Decimal("0.000200")), (True, 1000, Decimal("0.000000"))]  # fmt: skip


def test_replay_returns_cached_response_with_zero_network(version) -> None:
    call_llm("test", "claude-haiku-5-5", MESSAGES, version, None, mode="record", client=FakeClient("recorded"))
    replayed = call_llm("test", "claude-haiku-5-5", MESSAGES, version, None, mode="replay", client=ExplodingClient())
    assert (replayed.text, replayed.cache_hit) == ("recorded", True)


def test_replay_cache_miss_is_a_clear_error_and_is_logged(conn, version) -> None:
    with pytest.raises(LLMCacheMiss, match=r"not in the recorded cache .* LLM_MODE=live to run it fresh"):
        call_llm("test", "claude-haiku-5-5", MESSAGES, version, None, mode="replay", client=ExplodingClient())
    row = conn.execute("SELECT status, error FROM meta.llm_calls WHERE prompt_version = %s", (version,)).fetchone()
    assert row["status"] == "error" and "replay mode" in row["error"]
