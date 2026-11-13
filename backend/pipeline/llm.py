"""Thin LLM client: one function, full call logging, a response cache and offline replay.

Modes (LLM_MODE):
- replay: answer ONLY from meta.llm_cache; a missing entry is an error. No key, no network.
- live:   answer from the cache when the request is there, otherwise call the API and cache it.
- record: always call the API and overwrite the cache entry (refresh recordings).

Every call, including cache hits and failures, is logged to meta.llm_calls. A cache hit
logs the original usage but costs $0. Sampling parameters (temperature) are not sent:
the current API rejects them, so reproducibility comes from the cache and replay mode.
"""

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb

from pipeline.config import settings
from pipeline.db import insert_sql, transaction
from pipeline.logging_setup import get_logger

MAX_TOKENS = 16_000  # includes (adaptive) thinking tokens; non-streaming stays under SDK timeouts


@dataclass(frozen=True)
class Price:
    """USD per 1M tokens. Cache write/read are prompt-caching rates."""

    input: float
    output: float
    cache_write: float
    cache_read: float


# Anthropic first-party prices (prompts <= 100K tokens for Haiku 5.5). Check the pricing page
# before relying on these for billing.
PRICING: dict[str, Price] = {
    "claude-haiku-5-5": Price(input=0.10, output=0.50, cache_write=0.125, cache_read=0.01),
    "claude-sonnet-5-5": Price(input=2.00, output=10.00, cache_write=2.50, cache_read=0.10),
    "claude-opus-5-5": Price(input=4.00, output=20.00, cache_write=5.00, cache_read=0.20),
}


class LLMError(RuntimeError):
    """The model could not produce a usable answer (API error, refusal, truncation, bad JSON)."""


class LLMCacheMiss(LLMError):
    """Replay mode and the request is not in meta.llm_cache."""


@dataclass(frozen=True)
class LLMResult:
    call_id: uuid.UUID
    text: str
    data: Any  # parsed JSON when a json_schema was given, else None
    model: str
    request_hash: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cost_usd: Decimal
    latency_ms: int
    cache_hit: bool
    content: list[dict[str, Any]] = field(default_factory=list)  # raw content blocks (text, thinking, tool_use)
    stop_reason: str | None = None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def request_hash(
    model: str,
    prompt_version: str,
    messages: list[dict[str, Any]],
    system: str | list[dict[str, Any]] | None = None,
    json_schema: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    effort: str | None = None,
    options: dict[str, Any] | None = None,
) -> str:
    """sha256 over everything that changes the answer, as canonical JSON (key order irrelevant).

    `options` joins the hash only when given, so recordings made without it stay valid.
    """
    payload = {"model": model, "prompt_version": prompt_version, "system": system, "messages": messages,
               "json_schema": json_schema, "tools": tools, "effort": effort}  # fmt: skip
    if options:
        payload["options"] = options
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def cost_usd(model: str, usage: dict[str, Any]) -> Decimal:
    """Cost of one response from its usage block; unknown models cost 0 (and are logged)."""
    price = PRICING.get(model)
    if price is None:
        get_logger("llm").warning("no price for model", model=model)
        return Decimal(0)
    total = (
        (usage.get("input_tokens") or 0) * price.input
        + (usage.get("output_tokens") or 0) * price.output
        + (usage.get("cache_creation_input_tokens") or 0) * price.cache_write
        + (usage.get("cache_read_input_tokens") or 0) * price.cache_read
    )
    return Decimal(str(round(total / 1_000_000, 6)))


def make_client() -> Any:
    """Anthropic client; SDK retries connection errors, 408/409/429 and 5xx twice with backoff."""
    if settings.llm_provider != "anthropic":
        raise LLMError(f"LLM_PROVIDER={settings.llm_provider!r} is not supported (only 'anthropic')")
    import anthropic  # imported lazily: replay mode never needs the SDK

    return anthropic.Anthropic(api_key=settings.llm_api_key, max_retries=2)


def _text_of(response: dict[str, Any]) -> str:
    """Concatenated text blocks (thinking blocks are skipped)."""
    return "".join(b.get("text", "") for b in response.get("content", []) if b.get("type") == "text")


def _check_stop(response: dict[str, Any]) -> None:
    stop = response.get("stop_reason")
    if stop == "refusal":
        raise LLMError(f"model refused: {response.get('stop_details')}")
    if stop == "max_tokens":
        raise LLMError("response truncated at max_tokens")


def _cached(request_hash_: str) -> dict[str, Any] | None:
    with transaction() as conn:
        row = conn.execute("SELECT response FROM meta.llm_cache WHERE request_hash = %s", (request_hash_,)).fetchone()
    return row[0] if row else None


def _store_cache(request_hash_: str, model: str, response: dict[str, Any]) -> None:
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.llm_cache (request_hash, response, model) VALUES (%s, %s, %s)"
            " ON CONFLICT (request_hash) DO UPDATE SET response = EXCLUDED.response, created_at = now()",
            (request_hash_, Jsonb(response), model),
        )


def _log_call(row: dict[str, Any]) -> None:
    """Append one row to meta.llm_calls (every call, hit or miss, success or error)."""
    with transaction() as conn:
        conn.execute(insert_sql("meta.llm_calls", list(row)), list(row.values()))


def call_llm(
    purpose: str,
    model: str,
    messages: list[dict[str, Any]],
    prompt_version: str,
    run_id: uuid.UUID | None,
    json_schema: dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    *,
    system: str | list[dict[str, Any]] | None = None,
    effort: str | None = None,
    options: dict[str, Any] | None = None,
    mode: str | None = None,
    client: Any = None,
) -> LLMResult:
    """One logged, cached LLM request. With json_schema the answer is constrained to it and parsed.

    `options`: extra request fields (thinking, max_tokens, extra_headers for betas, extra_body).
    """
    mode = mode or settings.llm_mode
    log = get_logger("llm", run_id=str(run_id) if run_id else "-")
    key = request_hash(model, prompt_version, messages, system, json_schema, tools, effort, options)
    call_id = uuid.uuid4()
    started = time.perf_counter()
    base = {"call_id": call_id, "run_id": run_id, "purpose": purpose, "provider": settings.llm_provider,
            "model": model, "prompt_version": prompt_version, "request_hash": key}  # fmt: skip
    try:
        response = None if mode == "record" else _cached(key)
        cache_hit = response is not None
        if response is None:
            if mode == "replay":
                raise LLMCacheMiss(
                    f"replay mode: no cached response for {purpose} (request {key[:12]}). "
                    "Run once with LLM_MODE=live or record and an API key to record it."
                )
            response = _call_api(client or make_client(), model, messages, system, json_schema, tools, effort, options)
        _check_stop(response)
        text = _text_of(response)
        data = _parse_json(text) if json_schema else None
        if not cache_hit:
            _store_cache(key, model, response)
    except Exception as exc:
        _log_call(base | {"status": "error", "error": str(exc)[:1000], "cache_hit": False,
                          "latency_ms": int((time.perf_counter() - started) * 1000)})  # fmt: skip
        # Expected failures (cache miss, provider 5xx after retries): one line, no traceback; the caller decides.
        log.error("llm call failed", purpose=purpose, model=model, mode=mode, error=str(exc)[:200])  # noqa: TRY400
        raise
    usage = response.get("usage") or {}
    spent = Decimal(0) if cache_hit else cost_usd(model, usage)
    latency = int((time.perf_counter() - started) * 1000)
    result = LLMResult(
        call_id=call_id, text=text, data=data, model=model, request_hash=key,
        input_tokens=usage.get("input_tokens") or 0, output_tokens=usage.get("output_tokens") or 0,
        cached_tokens=usage.get("cache_read_input_tokens") or 0, cost_usd=spent, latency_ms=latency,
        cache_hit=cache_hit, content=response.get("content", []), stop_reason=response.get("stop_reason"),
    )  # fmt: skip
    _log_call(base | {"input_tokens": result.input_tokens, "output_tokens": result.output_tokens,
                      "cached_tokens": result.cached_tokens, "cost_usd": spent, "latency_ms": latency,
                      "cache_hit": cache_hit, "status": "ok"})  # fmt: skip
    log.info("llm call", purpose=purpose, model=model, mode=mode, cache_hit=cache_hit, input_tokens=result.input_tokens,
             output_tokens=result.output_tokens, cost_usd=str(spent), latency_ms=latency)  # fmt: skip
    return result


def _call_api(
    client: Any,
    model: str,
    messages: list[dict[str, Any]],
    system: str | list[dict[str, Any]] | None,
    json_schema: dict[str, Any] | None,
    tools: list[dict[str, Any]] | None,
    effort: str | None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output_config: dict[str, Any] = {}
    if effort:
        output_config["effort"] = effort
    if json_schema:
        output_config["format"] = {"type": "json_schema", "schema": json_schema}
    kwargs: dict[str, Any] = {"model": model, "max_tokens": MAX_TOKENS, "messages": messages}
    if system:
        kwargs["system"] = system
    if tools:
        kwargs["tools"] = tools
    if output_config:
        kwargs["output_config"] = output_config
    kwargs |= options or {}
    try:
        response = client.messages.create(**kwargs)
    except Exception as exc:  # SDK has already retried transient errors
        raise LLMError(f"{type(exc).__name__}: {exc}") from exc
    return response.model_dump(mode="json") if hasattr(response, "model_dump") else dict(response)


def _parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise LLMError(f"response is not valid JSON: {exc}") from exc


def count_tokens(text: str, model: str, client: Any = None) -> int:
    """Exact input-token count of `text` as one user message (free endpoint, needs an API key)."""
    client = client or make_client()
    return client.messages.count_tokens(model=model, messages=[{"role": "user", "content": text}]).input_tokens
