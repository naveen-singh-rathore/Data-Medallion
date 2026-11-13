"""Business context: human path, inferred path (fake client), validation, data minimization,
summary limits with sentence cut + retry, human vs inferred comparison."""

import dataclasses

import pytest
from fakes import ExplodingClient, FakeClient

from pipeline import llm
from pipeline.card import build_card, load_inputs
from pipeline.context import (
    approve_context,
    compare_contexts,
    ensure_context,
    fit_summary,
    get_context,
    validate_draft,
)
from pipeline.profiling import profile_run

PEOPLE = ["Alice Moreno", "Bilal Khan", "Chen Wei"]


@pytest.fixture
def live(monkeypatch):
    """call_llm in record mode: the fake client stands in for the API and is always called.

    Not "live": context prompts don't contain the run id, so a cached response from an earlier
    test run can match and the fake would silently not be called (a flaky test).
    """
    monkeypatch.setattr(llm, "settings", dataclasses.replace(llm.settings, llm_mode="record"))


@pytest.fixture
def profiled(ingest):
    def _profiled(name: str):
        run_id = ingest(name)
        profile_run(run_id)
        return run_id

    return _profiled


def draft(**overrides):
    base = {"domain": "support tickets", "summary": "One row per ticket. Ops managers track backlog.",
            "likely_metrics": ["tickets per week"], "critical_columns": ["priority"],
            "column_tags": [{"column": "priority", "tag": "priority"}]}  # fmt: skip
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
    assert (context["source"], context["approved"]) == ("inferred", True)  # the AI decides; no sign-off
    stored = context["draft"]
    assert stored["critical_columns"] == ["priority", "cost"]
    assert stored["column_tags"] == {"priority": "priority", "cost": "money"}
    assert stored["rejected_columns"] == ["bogus", "ghost"]
    sent = client.messages.calls[0]
    schema = sent["output_config"]["format"]["schema"]
    assert schema["properties"]["critical_columns"]["items"]["enum"] == ["id", "priority", "created_at", "resolved_at",
                                                                         "cost", "note"]  # fmt: skip
    assert sent["model"] == "claude-haiku-5-5" and sent["output_config"]["effort"] == "low"
    call = conn.execute("SELECT purpose, cost_usd, prompt_version FROM meta.llm_calls WHERE call_id = %s",
                        (context["llm_call_id"],)).fetchone()  # fmt: skip
    assert (call["purpose"], call["prompt_version"]) == ("context_draft", "context_draft_v2") and call["cost_usd"] > 0
    assert ensure_context(run_id, client=ExplodingClient())["draft"] == stored  # stored context is reused
    assert approve_context(run_id)


def test_no_raw_values_from_person_or_high_cardinality_columns(profiled, live) -> None:
    """First draft: names are raw (nothing marks them as people yet), free text is masked.
    After the context tags `owner` as person, no request and no card contains a name."""
    run_id = profiled("context_people.csv")
    tagging = draft(critical_columns=["team"], column_tags=[{"column": "owner", "tag": "person"},
                                                            {"column": "description", "tag": "free_text"}])  # fmt: skip
    first = FakeClient(tagging)
    ensure_context(run_id, client=first)
    prompt = first.messages.calls[0]["messages"][0]["content"]
    assert "Boiler pressure" not in first.payloads() and '"description": "<A A A A A' in prompt
    assert '"North"' in prompt  # label-like, non-sensitive values may be shown

    second = FakeClient(tagging)
    ensure_context(run_id, client=second, redraft=True)
    sent = second.payloads()
    assert not [name for name in PEOPLE if name in sent]
    assert "Boiler pressure" not in sent and "W-01" not in sent
    card = build_card(load_inputs(run_id), 4000).text
    assert not [name for name in PEOPLE if name in card]


def test_summary_is_cut_at_a_sentence_end() -> None:
    text = (
        "Each row is one work order. " + "Facility teams use it to plan staff and budget across many sites. " * 6
    ).strip()
    assert fit_summary(text, 10) == "Each row is one work order."
    assert fit_summary(" ".join(["word"] * 70) + ".", 60) is None
    stored = validate_draft(draft(summary=text), ["priority"])
    assert stored["summary"].endswith(".") and len(stored["summary"].split()) <= 60
    assert stored["repairs"] == [
        f"summary cut from {len(text.split())} to {len(stored['summary'].split())} words at a sentence end"
    ]


def test_too_long_single_sentence_is_retried_once(profiled, live) -> None:
    run_id = profiled("profile_messy.csv")
    client = FakeClient([draft(summary=" ".join(["word"] * 70) + "."), draft(summary="Short business summary.")])
    context = ensure_context(run_id, client=client)
    assert len(client.messages.calls) == 2
    assert client.messages.calls[1]["messages"][-1]["content"].startswith("The summary has 70 words")
    assert context["draft"]["summary"] == "Short business summary."
    assert "summary too long: retried once" in context["draft"]["repairs"]


def test_draft_limits_and_schema_are_enforced() -> None:
    stored = validate_draft(draft(likely_metrics=[f"m{i}" for i in range(8)]), ["priority"])
    assert len(stored["likely_metrics"]) == 5 and stored["repairs"] == ["likely_metrics cut from 8 to 5"]
    with pytest.raises(llm.LLMError, match="does not match the schema"):
        validate_draft({"domain": "x"}, ["priority"])
    with pytest.raises(llm.LLMError):
        validate_draft(draft(column_tags=[{"column": "priority", "tag": "not-a-tag"}]), ["priority"])


def test_human_and_inferred_are_kept_and_compared(profiled, live) -> None:
    run_id = profiled("profile_messy.csv")
    ensure_context(run_id, client=FakeClient(draft(critical_columns=["priority", "cost"],
                                                   column_tags=[{"column": "priority", "tag": "priority"},
                                                                {"column": "cost", "tag": "money"}])))  # fmt: skip
    human = (
        "Facility work orders used for SLA reporting.\nDomain: facilities work orders\n"
        "Critical columns: priority, created_at\nTags: priority=priority, cost=quantity"
    )
    used = ensure_context(run_id, human, client=ExplodingClient())
    assert used["source"] == "human" and get_context(run_id)["source"] == "human"
    assert get_context(run_id, "inferred")["draft"]["domain"] == "support tickets"  # draft kept, not used
    assert used["draft"]["critical_columns"] == ["priority", "created_at"]
    lines = "\n".join(compare_contexts(run_id))
    assert "human: facilities work orders" in lines and "inferred: support tickets" in lines
    assert "human only: created_at" in lines and "inferred only: cost" in lines
    assert "cost: human=quantity inferred=money" in lines
