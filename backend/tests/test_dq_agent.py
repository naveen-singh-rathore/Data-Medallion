"""DQ agent graph with a scripted model (no network): tool loop, budget, validation retries,
approval resume, context pause, compaction, replay determinism, metrics."""

import json

import pytest
import yaml
from fakes import ExplodingClient, ScriptedClient, tool_call

from pipeline.context import _save, ensure_context
from pipeline.db import transaction
from pipeline.dq_agent import graph as dq_graph
from pipeline.dq_agent import runner
from pipeline.dq_agent.graph import FORCE_MESSAGE
from pipeline.dq_agent.messages import STUB_PREFIX, compact, tool_result
from pipeline.profiling import load_profile, profile_run
from pipeline.silver.defaults import default_rules

WHY = "Amounts feed spend reporting; the 999 placeholder and $ prefixes would distort totals."


@pytest.fixture
def dq_run(ingest, conn, tmp_path, monkeypatch):
    """silver_small, profiled, with an approved human context; rules files go to tmp_path."""
    run_id = ingest("silver_small.csv")
    profile_run(run_id)
    ensure_context(run_id, '{"domain": "test tickets", "summary": "One row per ticket.", '
                           '"column_tags": {"owner": "person"}}')  # fmt: skip
    monkeypatch.setattr(dq_graph, "RULES_DIR", tmp_path / "rules")
    monkeypatch.setattr(dq_graph, "APPROVED_PATH", tmp_path / "rules" / "silver_rules.yaml")
    yield str(run_id)
    for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
        conn.execute(f"DELETE FROM {table} WHERE thread_id LIKE %s", (f"dq:{run_id}:%",))


def required_ids(run_id: str) -> list[str]:
    return [i["id"] for i in load_profile(run_id)["issues"] if i["severity"] in ("high", "medium")]


def submission(run_id: str, skip: int = 0) -> dict:
    """A valid agent submission: one real rule for `amount`, every other required id ignored."""
    ids = required_ids(run_id)
    amount_ids = [i for i in ids if i.endswith(":amount")]
    resolved = [{"issue_id": i, "resolution": "rule"} for i in amount_ids]
    resolved += [{"issue_id": i, "resolution": "ignored", "reason": "Out of scope for this scripted test."}
                 for i in ids if i not in amount_ids]  # fmt: skip
    return {"source_run_id": run_id, "generated_by": "agent", "addressed_issues": resolved[skip:], "columns": [
        {"name": "amount", "source": "amount", "type": "numeric", "steps": ["trim", "strip_currency"],
         "null_tokens": ["n/a"], "sentinels": ["999"], "confidence": 0.8, "why": WHY, "addresses": amount_ids}]}  # fmt: skip


def submit(run_id: str, n: int, **kw) -> list[dict]:
    return [tool_call("submit_rules", submission(run_id, **kw), n)]


def run_script(run_id: str, turns: list[list[dict]], fresh: bool = True):
    client = ScriptedClient(turns)
    return runner.run(run_id, fresh=fresh, client=client, mode="live"), client


def test_tool_loop_then_pause_at_approval(dq_run, conn) -> None:
    turns = [[tool_call("list_issues", {"severity": "high"}, 1), tool_call("get_value_counts", {"column": "amount"}, 2)],
             submit(dq_run, 3)]  # fmt: skip
    result, client = run_script(dq_run, turns)
    assert result["status"] == "awaiting_approval" and result["paused"]["proposed"].endswith("proposed.yaml")
    assert (result["tool_calls"], result["turns"]) == (2, 2)
    second_request = client.messages.calls[1]["messages"]
    assert [b["type"] for b in second_request[-1]["content"]] == ["tool_result", "tool_result"]
    row = conn.execute("SELECT * FROM meta.agent_runs WHERE thread_id = %s", (result["thread_id"],)).fetchone()
    assert (row["status"], row["turns"], row["tool_calls"], row["validation_attempts"]) == ("awaiting_approval", 2, 2, 0)
    assert row["tool_calls_by_name"] == {"list_issues": 1, "get_value_counts": 1}
    amount_ids = [i for i in required_ids(dq_run) if i.endswith(":amount")]
    assert row["issues_total"] == len(required_ids(dq_run)) and row["issues_addressed"] == len(amount_ids) > 0
    assert row["issues_ignored"] == row["issues_total"] - len(amount_ids)
    assert (row["columns_agent"], row["columns_default"]) == (1, len(default_rules(dq_run).columns))
    assert row["input_tokens"] == 2000 and row["cached_tokens"] == 1000 and row["rules_hash"]


def test_budget_forces_submit_with_only_submit_tool(dq_run) -> None:
    seven = [tool_call("get_value_counts", {"column": "state", "limit": i + 1}, i) for i in range(7)]
    turns = [seven, seven, seven, [tool_call("list_issues", {}, 99)], submit(dq_run, 100)]
    result, client = run_script(dq_run, turns)
    assert result["status"] == "awaiting_approval" and result["tool_calls"] == 21
    last = client.messages.calls[-1]
    assert [t["name"] for t in last["tools"]] == ["submit_rules"]
    assert last["messages"][-1]["content"][-1]["text"] == FORCE_MESSAGE
    assert last["messages"][-1]["content"][0]["content"] == "Not run: tool budget used."
    assert len(client.messages.calls[0]["tools"]) == 8


def test_validation_feedback_lists_missing_ids_then_passes(dq_run) -> None:
    missing = required_ids(dq_run)[:2]
    partial = {**submission(dq_run), "addressed_issues": [
        r for r in submission(dq_run)["addressed_issues"] if r["issue_id"] not in missing]}  # fmt: skip
    turns = [[tool_call("submit_rules", partial, 1)], submit(dq_run, 2)]
    result, client = run_script(dq_run, turns)
    assert result["status"] == "awaiting_approval" and result["attempts"] == 1
    feedback = client.messages.calls[1]["messages"][-1]["content"][0]
    assert feedback["is_error"] and f"not in addressed_issues: {missing}" in feedback["content"]
    assert len(feedback["content"].encode()) <= 2048


def test_three_failed_attempts_end_as_failed(dq_run, conn, tmp_path) -> None:
    bad = [tool_call("submit_rules", {"source_run_id": dq_run, "generated_by": "agent", "columns": []}, n) for n in range(3)]
    result, _ = run_script(dq_run, [[b] for b in bad])
    assert (result["status"], result["attempts"], result["next"]) == ("failed", 3, [])
    report = json.loads((tmp_path / "rules" / dq_run / "validation.json").read_text())
    assert report["layer"] == "schema" and report["errors"]
    row = conn.execute("SELECT status, validation_attempts, first_validation_errors FROM meta.agent_runs"
                       " WHERE thread_id = %s", (result["thread_id"],)).fetchone()  # fmt: skip
    assert (row["status"], row["validation_attempts"]) == ("failed", 3) and row["first_validation_errors"]


def test_approve_revalidates_human_edits(dq_run, tmp_path) -> None:
    run_script(dq_run, [submit(dq_run, 1)])
    path = tmp_path / "rules" / dq_run / "proposed.yaml"
    rules = yaml.safe_load(path.read_text())
    original = path.read_text()
    rules["columns"][0]["type"] = "date"  # invalid: a date column without parse_formats
    path.write_text(yaml.safe_dump(rules))
    paused = runner.resume(dq_run, {"action": "approve"}, mode="live")
    assert paused["status"] == "awaiting_approval" and "parse_formats: required" in " ".join(paused["paused"]["errors"])
    rules = yaml.safe_load(original)
    rules["columns"][0]["why"] = WHY + " Reviewed by finance."
    path.write_text(yaml.safe_dump(rules))
    done = runner.resume(dq_run, {"action": "approve"}, mode="live")
    assert done["status"] == "approved" and done["next"] == []
    saved = yaml.safe_load((tmp_path / "rules" / "silver_rules.yaml").read_text())
    assert saved["generated_by"] == "human" and saved["columns"][0]["generated_by"] == "human"


def test_reject_ends_the_thread(dq_run) -> None:
    run_script(dq_run, [submit(dq_run, 1)])
    result = runner.resume(dq_run, {"action": "reject", "reason": "wrong currency"}, mode="live")
    assert result["status"] == "rejected" and result["next"] == []


def test_inferred_context_never_pauses_the_agent(dq_run) -> None:
    """Even a draft stored unapproved by an older version is used as is: no human gate on context."""
    import uuid

    with transaction() as conn:
        conn.execute("DELETE FROM meta.dataset_context WHERE run_id = %s", (dq_run,))
    _save(uuid.UUID(dq_run), "AI draft: test tickets", {"domain": "x", "column_tags": {}}, "inferred", False, None)
    result, client = run_script(dq_run, [submit(dq_run, 1)])
    assert result["status"] == "awaiting_approval" and len(client.messages.calls) == 1
    assert "AI draft: test tickets" in json.dumps(client.messages.calls[0]["messages"])


def test_agent_runs_without_context_when_no_draft_is_possible(dq_run, monkeypatch) -> None:
    from pipeline.llm import LLMCacheMiss

    with transaction() as conn:
        conn.execute("DELETE FROM meta.dataset_context WHERE run_id = %s", (dq_run,))

    def no_draft(*args, **kwargs):
        raise LLMCacheMiss("replay mode: no recorded response")

    monkeypatch.setattr(dq_graph, "ensure_context", no_draft)
    result, client = run_script(dq_run, [submit(dq_run, 1)])
    assert result["status"] == "awaiting_approval"
    assert "BUSINESS CONTEXT\\n(none provided)" in json.dumps(client.messages.calls[0]["messages"])


def test_compaction_keeps_recent_results_and_feedback() -> None:
    task = {"role": "user", "content": "TASK with the card"}
    messages = [task]
    for i in range(8):
        messages.append({"role": "assistant", "content": [tool_call("get_value_counts", {"column": f"c{i}"}, i)]})
        messages.append({"role": "user", "content": [tool_result(f"toolu_get_value_counts_{i}",
                         json.dumps({"ok": True, "result": {"column": f"c{i}", "values": [["a", 1]]}}))]})  # fmt: skip
    messages.append({"role": "assistant", "content": [tool_call("submit_rules", {}, 9)]})
    messages.append({"role": "user", "content": [tool_result("toolu_submit_rules_9", "INVALID ...", is_error=True)]})
    compacted = compact(messages)
    results = [m["content"][0]["content"] for m in compacted if m["role"] == "user" and isinstance(m["content"], list)]
    assert [r.startswith(STUB_PREFIX) for r in results[:8]] == [True, True] + [False] * 6
    assert results[0] == '[compacted] get_value_counts({"column":"c0"}) → column=c0, values: 1 items'
    assert results[-1] == "INVALID ..." and compacted[0] == task and messages[2]["content"][0]["content"] != results[0]


def test_replay_reproduces_identical_rules_with_zero_network(dq_run, tmp_path) -> None:
    # 8 tool results trigger compaction; "column" before "limit" is an order jsonb rewrites (limit first),
    # so this fails if anything sent to the model depends on argument key order.
    calls = [[tool_call("get_value_counts", {"column": "state", "limit": n + 1}, n)] for n in range(8)]
    run_script(dq_run, [*calls, submit(dq_run, 99)])
    path = tmp_path / "rules" / dq_run / "proposed.yaml"
    recorded = path.read_bytes()
    for _ in range(2):
        path.unlink()
        result = runner.run(dq_run, fresh=True, client=ExplodingClient(), mode="replay")
        assert result["status"] == "awaiting_approval"
        assert path.read_bytes() == recorded


def test_stub_text_does_not_depend_on_argument_key_order() -> None:
    def history(args: dict) -> list[dict]:
        messages = [{"role": "user", "content": "TASK"}]
        for i in range(7):
            messages.append({"role": "assistant", "content": [tool_call("get_value_counts", args, i)]})
            messages.append({"role": "user", "content": [tool_result(f"toolu_get_value_counts_{i}", '{"ok": true}')]})
        return messages

    first = compact(history({"column": "c", "limit": 50}))
    second = compact(history({"limit": 50, "column": "c"}))  # same input, keys reordered (as jsonb does)
    assert first[2]["content"][0]["content"] == second[2]["content"][0]["content"]


def test_pipeline_sets_the_run_id_not_the_model(dq_run) -> None:
    wrong = {**submission(dq_run), "source_run_id": dq_run[:8] + "-0000-0000-0000-000000000000"}
    result, _ = run_script(dq_run, [[tool_call("submit_rules", wrong, 1)]])
    assert result["status"] == "awaiting_approval" and result["attempts"] == 0


def test_leftover_mapping_failure_never_blocks_the_review(dq_run, monkeypatch) -> None:
    from pipeline.llm import LLMCacheMiss

    def no_answer(*args, **kwargs):
        raise LLMCacheMiss("replay mode: no recorded response")

    monkeypatch.setattr(dq_graph.mapping_assist, "complete", no_answer)
    result, _ = run_script(dq_run, [submit(dq_run, 1)])
    assert result["status"] == "awaiting_approval"
