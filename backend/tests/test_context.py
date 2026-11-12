"""Business context: human path (no LLM), inferred path (fake client), validation, PII masking."""

import dataclasses
import json

import pytest
from fakes import ExplodingClient, FakeClient

from pipeline import llm
from pipeline.context import approve_context, ensure_context, validate_draft
from pipeline.profiling import profile_run


@pytest.fixture
def live(monkeypatch):
    """call_llm in live mode (the fake client stands in for the API)."""
    monkeypatch.setattr(llm, "settings", dataclasses.replace(llm.settings, llm_mode="live"))


@pytest.fixture
def profiled(ingest, conn):
    def _profiled(name: str):
        run_id = ingest(name)
        profile_run(run_id)
        return run_id

    yield _profiled
    conn.execute("DELETE FROM meta.llm_cache WHERE request_hash IN"
                 " (SELECT request_hash FROM meta.llm_calls WHERE purpose = 'context_draft' AND run_id IS NULL)")  # fmt: skip


def draft(**overrides):
    base = {"domain": "support tickets", "summary": "One row per ticket.", "likely_metrics": ["tickets per week"],
            "critical_columns": ["priority"], "column_tags": [{"column": "priority", "tag": "priority"}]}  # fmt: skip
    return base | overrides


def test_human_context_makes_no_llm_call(profiled, conn) -> None:
    run_id = profiled("profile_messy.csv")
    context = ensure_context(run_id, "Facilities tickets; priority drives the SLA.", client=ExplodingClient())
    assert (context["source"], context["approved"], context["llm_call_id"]) == ("human", True, None)
    assert context["text"] == "Facilities tickets; priority drives the SLA."
    assert conn.execute("SELECT count(*) AS n FROM meta.llm_calls WHERE run_id = %s", (run_id,)).fetchone()["n"] == 0


def test_inferred_context_validates_and_rejects_unknown_columns(profiled, live, conn) -> None:
    run_id = profiled("profile_messy.csv")
    reply = draft(critical_columns=["priority", "bogus", "cost"],
                  column_tags=[{"column": "priority", "tag": "priority"}, {"column": "ghost", "tag": "other"},
                               {"column": "cost", "tag": "money"}])  # fmt: skip
    client = FakeClient(reply)
    context = ensure_context(run_id, client=client)
    assert (context["source"], context["approved"]) == ("inferred", False)
    stored = context["draft"]
    assert stored["critical_columns"] == ["priority", "cost"]
    assert stored["column_tags"] == {"priority": "priority", "cost": "money"}
    assert stored["rejected_columns"] == ["bogus", "ghost"]
    sent = client.messages.calls[0]
    schema = sent["output_config"]["format"]["schema"]
    assert schema["properties"]["critical_columns"]["items"]["enum"] == ["id", "priority", "created_at", "resolved_at",
                                                                         "cost", "note"]  # fmt: skip
    assert sent["model"] == "claude-haiku-5-5" and sent["output_config"]["effort"] == "low"
    call = conn.execute("SELECT purpose, cost_usd FROM meta.llm_calls WHERE call_id = %s", (context["llm_call_id"],)).fetchone()
    assert call["purpose"] == "context_draft" and call["cost_usd"] > 0
    assert ensure_context(run_id, client=ExplodingClient())["draft"] == stored  # stored context is reused
    assert approve_context(run_id)


def test_prompt_masks_pii_and_truncates_samples(profiled, live) -> None:
    run_id = profiled("context_pii.csv")
    client = FakeClient(draft(critical_columns=[], column_tags=[]))
    ensure_context(run_id, client=client)
    prompt = client.messages.calls[0]["messages"][0]["content"]
    assert "@example.com" not in prompt and "<masked: A@A.A>" in prompt
    assert "- contact_email | candidate_key | email" in prompt
    assert prompt.count('{"id": "C-') == 3


def test_draft_limits_are_enforced() -> None:
    long = draft(summary=" ".join(["word"] * 120), likely_metrics=[f"m{i}" for i in range(8)])
    stored = validate_draft(long, ["priority"])
    assert len(stored["summary"].split()) == 80 and len(stored["likely_metrics"]) == 5
    assert len(stored["repairs"]) == 2
    with pytest.raises(llm.LLMError, match="does not match the schema"):
        validate_draft({"domain": "x"}, ["priority"])
    with pytest.raises(llm.LLMError):
        validate_draft(draft(column_tags=[{"column": "priority", "tag": "not-a-tag"}]), ["priority"])
    assert json.dumps(stored)  # storable
