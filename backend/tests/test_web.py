"""UI layer: plain-language wording, and the HTTP API end to end on a small fixture."""

import time

import pytest
from fastapi.testclient import TestClient

from pipeline.context import ensure_context as real_ensure_context
from pipeline.llm import LLMCacheMiss
from pipeline.silver.defaults import default_rules
from pipeline.silver.rules_schema import ColumnRule, to_yaml
from pipeline.web import app as web_app
from pipeline.web import explain, jobs, views

FIXTURE = "silver_small.csv"


# --- explain ---------------------------------------------------------------------------


def test_issue_sentences_use_the_evidence_numbers() -> None:
    issue = {"id": "NULLTOK:ticket_id", "code": "NULL_LIKE_TOKENS", "column": "ticket_id", "severity": "medium",
             "evidence": {"count": 14, "examples": {"n/a": 9, "null": 5}}}  # fmt: skip
    d = explain.describe_issue(issue)
    assert d["title"] == "Placeholder text instead of blanks"
    assert d["plain"] == "14 values in ticket_id are placeholders: 'n/a' (9), 'null' (5)."
    order = {"code": "DATE_ORDER", "column": "a > b", "evidence": {"count": 3, "compared": 10, "share": 0.3}}
    assert explain.issue_sentence(order) == "In 3 of 10 rows (30.0%), a is later than b."
    assert explain.describe_issue({"code": "SOMETHING_NEW", "evidence": {}})["title"] == "Something new"


def test_column_rule_is_described_in_order() -> None:
    rule = {"type": "numeric", "steps": ["trim", "strip_currency"], "null_tokens": ["n/a"], "sentinels": ["999"],
            "range": {"min": 0}, "required": True, "invalid_action": "reject"}  # fmt: skip
    assert explain.describe_column_rule(rule) == [
        "Tidy: remove spaces at the start and end; remove currency symbols ($, €, USD …).",
        "Treat 'n/a' as empty.",
        "Treat placeholder values '999' as empty.",
        "Store as number.",
        "Allowed values: at least 0; anything else becomes empty.",
        "Required: if it is missing, the row is rejected.",
    ]
    assert explain.describe_column_rule({"type": "text"}) == ["Kept exactly as it is."]


def test_error_and_flag_labels() -> None:
    assert explain.error_label("created_at:unparseable", {"created_at": "timestamp"}) == (
        "created_at could not be read as date and time")
    assert explain.error_label("row:date_order:opened,closed") == "opened is later than closed"
    assert explain.flag_label("sentinel:999") == "Fake placeholder value removed (now empty): '999'"
    assert explain.describe_tool_call("get_value_counts", {"column": "x", "normalized": True}) == (
        "Counted the values of x grouped by spelling")


# --- API end to end --------------------------------------------------------------------


@pytest.fixture
def client(conn, tmp_path, monkeypatch):
    """App with uploads and rules in tmp_path; the AI context draft is unavailable (as in replay
    mode without a recording), so the human-context path is exercised."""
    from conftest import FIXTURES

    from pipeline.bronze import hash_file
    from pipeline.migrate import migrate

    migrate()
    sha = hash_file(FIXTURES / FIXTURE)
    conn.execute("DELETE FROM meta.ingestion_runs WHERE file_sha256 = %s", (sha,))
    monkeypatch.setattr(web_app, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(views, "APPROVED_PATH", tmp_path / "rules" / "silver_rules.yaml")
    monkeypatch.setattr(views, "proposed_path", lambda run_id: tmp_path / "rules" / run_id / "proposed.yaml")

    def no_draft(run_id, human_text=None, **kw):
        if human_text is None and views.get_context(run_id) is None:
            raise LLMCacheMiss("replay mode: no recorded response")
        return real_ensure_context(run_id, human_text, **kw)

    monkeypatch.setattr(jobs, "ensure_context", no_draft)
    yield TestClient(web_app.app), tmp_path
    conn.execute("DELETE FROM meta.ingestion_runs WHERE file_sha256 = %s", (sha,))


def _upload(c: TestClient, name: str = FIXTURE) -> dict:
    from conftest import FIXTURES

    with (FIXTURES / name).open("rb") as f:
        res = c.post("/api/upload", files={"file": (name, f, "text/csv")})
    assert res.status_code == 200, res.text
    return res.json()


def _wait(c: TestClient, run_id: str, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ov = c.get(f"/api/runs/{run_id}").json()
        if not ov["job"] or ov["job"]["state"] != "running":
            return ov
        time.sleep(0.2)
    raise AssertionError("job did not finish")


def test_upload_lands_profiles_and_explains_every_checkpoint(client) -> None:
    c, tmp = client
    up = _upload(c)
    run_id = up["run_id"]
    assert up["duplicate"] is False
    ov = _wait(c, run_id)
    stages = {s["key"]: s for s in ov["stages"]}
    assert list(stages) == ["upload", "bronze", "profile", "context", "card", "agent", "approval", "silver", "gold"]
    assert stages["bronze"]["state"] == "done" and "12 of 12 records" in stages["bronze"]["summary"]
    assert stages["profile"]["state"] == "done"
    assert stages["card"]["state"] == "done"
    # the AI draft failed: shown as a problem the review retries, never as a task for the user
    assert stages["context"]["state"] == "error" and "retry" in stages["context"]["summary"]
    assert not any(s["state"] == "action" for s in ov["stages"])
    assert any(e["step"] == "context" and e["state"] == "error" for e in ov["job"]["events"])
    assert stages["agent"]["state"] == "waiting" and stages["silver"]["state"] == "waiting"

    # same bytes again: idempotent, the existing run is shown
    assert _upload(c) == {"run_id": run_id, "duplicate": True}
    assert list((tmp / "uploads").iterdir()).__len__() == 1

    bronze = c.get(f"/api/runs/{run_id}/bronze").json()
    assert bronze["reconciled"] is True and bronze["accounted"] == 12
    rows = c.get(f"/api/runs/{run_id}/bronze/rows", params={"limit": 5, "q": "S-0"}).json()
    assert rows["total"] >= 5 and len(rows["rows"]) == 5 and rows["header"][0] == "id"

    profile = c.get(f"/api/runs/{run_id}/profile").json()
    assert {col["column"] for col in profile["columns"]} >= {"id", "state", "amount"}
    assert all(i["title"] and i["plain"] for i in profile["issues"])
    assert c.get(f"/api/runs/{run_id}/profile/amount").json()["column"] == "amount"
    assert c.get(f"/api/runs/{run_id}/rules").status_code == 404
    assert c.get(f"/api/runs/{run_id}/silver").status_code == 404

    # a person describes the data: stored as human, approved, card re-rendered
    res = c.post(f"/api/runs/{run_id}/context", json={"text": "Domain: test tickets\nTags: owner=person\n\nOne row per ticket."})
    assert res.status_code == 200
    ov = _wait(c, run_id)
    assert {s["key"]: s["state"] for s in ov["stages"]}["context"] == "done"
    ctx = c.get(f"/api/runs/{run_id}/context").json()
    assert ctx["used"]["source"] == "human" and ctx["used"]["approved"] is True


def test_silver_preview_shows_before_after_and_traces_a_record(client) -> None:
    c, tmp = client
    run_id = _upload(c)["run_id"]
    _wait(c, run_id)
    rules = default_rules(run_id)
    amount = ColumnRule(name="amount", source="amount", type="numeric", steps=["trim", "strip_currency"],
                        null_tokens=["n/a"], sentinels=["999"], confidence=0.8,
                        why="Amounts must be numbers to be summed per state.", generated_by="agent")  # fmt: skip
    rules = rules.model_copy(update={"columns": [c_ for c_ in rules.columns if c_.source != "amount"] + [amount]})
    path = tmp / "rules" / run_id / "proposed.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(to_yaml(rules))

    r = c.get(f"/api/runs/{run_id}/rules").json()
    assert r["state"] == "proposed" and r["path"].endswith("proposed.yaml")
    assert next(x for x in r["columns"] if x["name"] == "amount")["plain"][1] == "Treat 'n/a' as empty."

    s = c.get(f"/api/runs/{run_id}/silver").json()
    assert s["state"] == "proposed" and s["rows_in"] == 12
    col = next(x for x in s["columns"] if x["name"] == "amount")
    assert col["fixed"] >= 3  # $40 cleaned, N/A and 999 emptied
    assert {"before": "999", "after": None, "count": 3} in col["examples"]

    values = c.get(f"/api/runs/{run_id}/silver/columns/amount").json()
    sentinel = next(v for v in values["values"] if v["before"] == "999")
    assert sentinel["after"] is None and sentinel["change"] == "fixed" and "'999'" in sentinel["flags"][0]
    assert next(v for v in values["values"] if v["before"] == "$40")["after"] == "40"

    changed = c.get(f"/api/runs/{run_id}/silver/rows", params={"show": "changed", "column": "amount"}).json()
    assert changed["total"] >= 3 and [x["name"] for x in changed["columns"]] == ["amount"]
    first = changed["rows"][0]
    trace = c.get(f"/api/runs/{run_id}/trace/{first['bronze_id']}").json()
    assert trace["file"]["line"] == first["source_line"] and trace["file"]["raw"].startswith("S-")
    cell = next(x for x in trace["silver"]["cells"] if x["name"] == "amount")
    assert cell["before"] == trace["bronze"]["payload"]["amount"]

    stages = {x["key"]: x["state"] for x in c.get(f"/api/runs/{run_id}").json()["stages"]}
    assert stages["silver"] == "preview"


def test_decisions_need_a_reason_and_unknown_runs_404(client) -> None:
    c, _ = client
    run_id = _upload(c)["run_id"]
    _wait(c, run_id)
    assert c.post(f"/api/runs/{run_id}/rules/decision", json={"approve": False}).status_code == 422
    assert c.get("/api/runs/00000000-0000-0000-0000-000000000000").status_code == 404
    assert c.post("/api/upload", files={"file": ("notes.pdf", b"x", "application/pdf")}).status_code == 415
    assert c.get("/run/whatever").status_code == 200  # SPA fallback serves the page


# --- editing the proposal -------------------------------------------------------------


@pytest.fixture
def proposed(ingest, conn, tmp_path, monkeypatch):
    """silver_small with a scripted agent proposal waiting for approval; all rule files in tmp_path."""
    from fakes import ScriptedClient
    from test_dq_agent import submit

    from pipeline.dq_agent import graph as dq_graph
    from pipeline.dq_agent import runner
    from pipeline.profiling import profile_run
    from pipeline.web import review, rules_edit

    rules_dir = tmp_path / "rules"
    monkeypatch.setattr(dq_graph, "RULES_DIR", rules_dir)
    monkeypatch.setattr(dq_graph, "APPROVED_PATH", rules_dir / "silver_rules.yaml")
    monkeypatch.setattr(views, "APPROVED_PATH", rules_dir / "silver_rules.yaml")
    for module in (views, rules_edit, review):
        monkeypatch.setattr(module, "proposed_path", lambda run_id: rules_dir / run_id / "proposed.yaml")
    # never build into the real silver.records_clean from a test: record the call instead
    from pipeline.silver import build as silver_build

    builds: list[str] = []
    monkeypatch.setattr(silver_build, "build_silver",
                        lambda run, path=None: builds.append(str(run)) or {"target_table": "x", "rows_kept": 0,
                        "rows_flagged": 0, "rows_rejected": 0, "rows_duplicate": 0})  # fmt: skip
    from pipeline.gold import build as gold_build  # nor into the real gold schema

    monkeypatch.setattr(gold_build, "build_gold", lambda run, schema="gold", approved_path=None, model_path=None:
                        builds.append(f"gold:{run}") or {"duration_ms": 0, "checks": {}})  # fmt: skip
    run_id = str(ingest(FIXTURE))
    profile_run(run_id)
    real_ensure_context(run_id, "Domain: test tickets\nTags: owner=person\n\nOne row per ticket.")
    paused = runner.run(run_id, fresh=True, client=ScriptedClient([submit(run_id, 1)]), mode="live")
    assert paused["status"] == "awaiting_approval"
    yield TestClient(web_app.app), run_id, rules_dir / run_id / "proposed.yaml"
    for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
        conn.execute(f"DELETE FROM {table} WHERE thread_id LIKE %s", (f"dq:{run_id}:%",))


def test_rules_are_edited_checked_saved_reset_and_approved(proposed) -> None:
    from pipeline.dq_agent import runner

    c, run_id, path = proposed
    url = f"/api/runs/{run_id}/rules"
    d = c.get(f"{url}/yaml").json()
    assert d["editable"] is True and d["text"] == path.read_text() == d["ai_text"]

    broken = c.post(f"{url}/check", json={"text": "columns: [\n  - x"}).json()
    assert broken["ok"] is False and broken["layer"] == "yaml" and broken["errors"][0].startswith("line ")

    original = path.read_text()
    bad = d["text"].replace("- strip_currency", "- strip_everything")
    report = c.put(f"{url}/yaml", json={"text": bad}).json()
    assert report["ok"] is False and report["layer"] == "schema" and "(amount).steps[1]" in report["errors"][0]
    assert path.read_text() == original  # an invalid edit is never written

    good = d["text"].replace("  - n/a\n", "  - n/a\n  - tbd\n", 1)
    assert good != d["text"]
    assert c.post(f"{url}/check", json={"text": good}).json()["ok"] is True
    assert path.read_text() == original  # check does not save
    assert c.put(f"{url}/yaml", json={"text": good}).json()["ok"] is True
    assert "- tbd" in path.read_text()
    amount = next(col for col in c.get(url).json()["columns"] if col["name"] == "amount")
    assert amount["edited"] is True and "tbd" in amount["null_tokens"]

    assert c.post(f"{url}/reset").json()["ok"] is True
    assert path.read_text() == original
    assert not any(col["edited"] for col in c.get(url).json()["columns"])

    assert c.put(f"{url}/yaml", json={"text": good}).json()["ok"] is True
    done = runner.resume(run_id, {"action": "approve"}, mode="live")  # the approve step re-reads the edit
    assert done["status"] == "approved"
    approved = c.get(url).json()
    assert approved["state"] == "approved"
    assert next(col for col in approved["columns"] if col["name"] == "amount")["generated_by"] == "human"
    assert c.get(f"{url}/yaml").json()["editable"] is False
    assert c.put(f"{url}/yaml", json={"text": good}).status_code == 409


def test_each_rule_is_approved_or_rejected_on_its_own(proposed) -> None:
    from pipeline.web import review

    c, run_id, path = proposed
    url = f"/api/runs/{run_id}/rules"
    r = c.get(url).json()
    keys = [x["key"] for x in r["columns"] + r["row_checks"]] + ([r["dedup"]["key"]] if r["dedup"] else [])
    assert r["review"] == {"pending": len(keys), "approved": 0, "rejected": 0, "stale": 0, "total": len(keys),
                           "can_finish": False}  # fmt: skip
    amount = next(x for x in r["columns"] if x["name"] == "amount")
    assert amount["all_fields"]["unmapped_action"] == "keep"  # every setting, defaults included
    assert "strip_currency" in amount["yaml"] and amount["issues"][0]["title"]
    amount_ids = amount["addresses"]
    assert c.post(f"{url}/finish").status_code == 409  # not everything decided

    original = path.read_text()
    short = c.post(f"{url}/review", json={"key": "column:amount", "decision": "reject", "reason": "no"})
    assert short.status_code == 422 and path.read_text() == original
    rej = c.post(f"{url}/review", json={"key": "column:amount", "decision": "reject",
                                        "reason": "Amounts are fixed upstream next month."})  # fmt: skip
    assert rej.status_code == 200, rej.text
    r = c.get(url).json()
    amount = next(x for x in r["columns"] if x["name"] == "amount")
    assert amount["steps"] == ["trim", "remove_hidden_chars"] and amount["generated_by"] == "human"
    assert amount["review"]["decision"] == "rejected" and "strip_currency" in amount["review"]["original"]["steps"]
    handled = {x["id"]: x for x in r["coverage"]}
    assert all(handled[i]["resolution"] == "ignored" and "fixed upstream" in handled[i]["reason"] for i in amount_ids)

    assert c.post(f"{url}/review", json={"key": "column:amount", "decision": "undo"}).status_code == 200
    assert path.read_text() == original and not review.load(run_id)

    for key in keys:
        assert c.post(f"{url}/review", json={"key": key, "decision": "approve"}).status_code == 200
    assert c.get(url).json()["review"]["can_finish"] is True

    edited = original.replace("  - n/a\n", "  - n/a\n  - tbd\n", 1)
    assert c.put(f"{url}/yaml", json={"text": edited}).json()["ok"] is True
    r = c.get(url).json()
    assert r["review"]["stale"] == 1 and r["review"]["can_finish"] is False
    assert c.post(f"{url}/review", json={"key": "column:amount", "decision": "approve"}).status_code == 200

    assert c.post(f"{url}/finish").status_code == 200
    ov = _wait(c, run_id)
    steps = [e["step"] for e in ov["job"]["events"]]
    assert steps[-4:] == ["build", "build", "gold", "gold"]  # finishing builds the cleaned table, then gold
    r = c.get(url).json()
    assert r["state"] == "approved" and r["review"]["approved"] == len(keys)  # the decisions stay as a record
    assert c.post(f"{url}/review", json={"key": "column:amount", "decision": "undo"}).status_code in (409, 422)


def test_leftover_values_are_chosen_in_the_ui_and_a_finished_review_reopens(proposed) -> None:
    from pipeline.dq_agent import runner

    c, run_id, path = proposed
    url = f"/api/runs/{run_id}/rules"

    def values(choices: list[dict]) -> dict:
        res = c.post(f"{url}/values", json={"column": "state", "choices": choices})
        assert res.status_code == 200, res.text
        return res.json()

    def state_column() -> dict:
        return next(x for x in c.get(f"/api/runs/{run_id}/silver").json()["columns"] if x["name"] == "state")

    # the scripted proposal has no rule for `state`: add a plain one through the editor first
    import yaml as _yaml

    data = _yaml.safe_load(c.get(f"{url}/yaml").json()["text"])
    data["columns"].append({"name": "state", "source": "state", "type": "text", "steps": ["trim"], "confidence": 0.8,
                            "why": "Ticket state drives the open/closed counts."})  # fmt: skip
    assert c.put(f"{url}/yaml", json={"text": _yaml.safe_dump(data, sort_keys=False)}).json()["ok"] is True

    values([{"value": "open", "action": "new"}])  # a standard list with one value: Open/OPEN/open match it
    left = {x["value"]: x for x in state_column()["leftovers"]}
    assert left["Closed"]["count"] == 4
    assert left["In-Progress"]["count"] == 2 and set(left["In-Progress"]["spellings"]) == {"In-Progress", "in progress"}

    values([{"value": "Closed", "action": "new"}, {"value": "In-Progress", "action": "merge", "target": "open"}])
    assert state_column()["leftovers"] == []
    mapping = next(x for x in c.get(url).json()["columns"] if x["name"] == "state")["mapping"]
    assert mapping == {"open": ["In-Progress"], "Closed": []}

    values([{"value": "In-Progress", "action": "remove", "target": "open"}])  # the × on a merged spelling
    assert [x["value"] for x in state_column()["leftovers"]] == ["In-Progress"]
    values([{"value": "In-Progress", "action": "empty", "spellings": ["In-Progress", "in progress"]}])
    rule = next(x for x in c.get(url).json()["columns"] if x["name"] == "state")
    assert "In-Progress" in rule["null_tokens"] and state_column()["leftovers"] == []

    bad = c.post(f"{url}/values", json={"column": "state", "choices": [{"value": "x", "action": "merge", "target": "nope"}]})
    assert bad.status_code == 422

    # finish, then reopen: the approval is withdrawn, the rules are editable again, nothing calls the AI
    assert runner.resume(run_id, {"action": "approve"}, mode="live")["status"] == "approved"
    approved_file = path.parent.parent / "silver_rules.yaml"
    assert approved_file.exists() and c.get(url).json()["state"] == "approved"
    assert c.post(f"{url}/values", json={"column": "state", "choices": [{"value": "open", "action": "new"}]}).status_code == 409
    assert c.post(f"{url}/reopen").json()["ok"] is True
    assert approved_file.exists() and "Done" not in approved_file.read_text()  # still approved until finished
    r = c.get(url).json()
    assert r["state"] == "proposed" and r["agent_status"] == "awaiting_approval"
    assert c.get(f"{url}/yaml").json()["editable"] is True
    values([{"value": "Done", "action": "merge", "target": "Closed"}])
    assert runner.resume(run_id, {"action": "approve"}, mode="live")["status"] == "approved"
    assert "- Done" in approved_file.read_text()
    assert c.post(f"{url}/reopen").status_code == 200 and c.post(f"{url}/reopen").status_code == 409


def test_ai_decides_leftover_values_and_the_reviewer_only_changes_what_is_wrong(proposed, monkeypatch) -> None:
    import types
    import uuid as _uuid
    from decimal import Decimal

    import yaml as _yaml

    from pipeline.silver import mapping_assist
    from pipeline.web import rules_edit

    c, run_id, path = proposed
    url = f"/api/runs/{run_id}/rules"
    data = _yaml.safe_load(c.get(f"{url}/yaml").json()["text"])
    data["columns"].append({"name": "state", "source": "state", "type": "text", "steps": ["trim"], "confidence": 0.8,
                            "why": "Ticket state drives the open/closed counts.", "mapping": {"open": ["Open"]}})  # fmt: skip
    assert c.put(f"{url}/yaml", json={"text": _yaml.safe_dump(data, sort_keys=False)}).json()["ok"] is True

    found = mapping_assist.leftovers(mapping_assist.SilverRules.model_validate(_yaml.safe_load(path.read_text())), run_id)
    assert {x["value"]: x["count"] for x in found["state"]} == {"Closed": 4, "In-Progress": 2}

    sent = {}

    def fake_llm(purpose, model, messages, prompt_version, run, **kw):
        sent.update(purpose=purpose, prompt=messages[0]["content"], schema=kw["json_schema"])
        decisions = [
            {"value": "Closed", "action": "new", "target": "closed", "reason": "A finished ticket."},
            {"value": "in progress", "action": "merge", "target": "open", "reason": "Still being worked on."},
            {"value": "Nope", "action": "keep", "target": "", "reason": "not in the data"},
        ]  # fmt: skip
        return types.SimpleNamespace(data={"columns": [{"column": "state", "decisions": decisions}]}, call_id=_uuid.uuid4(),
                                     cost_usd=Decimal("0.0003"), input_tokens=300, output_tokens=80)  # fmt: skip

    monkeypatch.setattr(mapping_assist, "call_llm", fake_llm)
    result = rules_edit.auto_map(_uuid.UUID(run_id))
    assert result["decided"] == 2 and result["dropped"] == 1
    assert sent["purpose"] == "mapping_assist" and "Closed × 4" in sent["prompt"] and "open: Open" in sent["prompt"]

    r = c.get(url).json()
    state_rule = next(x for x in r["columns"] if x["name"] == "state")
    assert state_rule["mapping"] == {"open": ["Open", "In-Progress"], "closed": ["Closed"]}
    assert state_rule["edited"] is False  # the AI's decisions are the AI's proposal, not the reviewer's edits
    assert [(d["value"], d["now"], d["changed"]) for d in state_rule["ai_mapped"]] == [
        ("Closed", {"action": "new", "target": "closed"}, False),
        ("In-Progress", {"action": "merge", "target": "open"}, False),
    ]
    assert next(x for x in c.get(f"/api/runs/{run_id}/silver").json()["columns"] if x["name"] == "state")["leftovers"] == []
    assert rules_edit.editable(_uuid.UUID(run_id))  # still paused at approval

    # the reviewer disagrees with two decisions
    res = c.post(f"{url}/values", json={"column": "state", "choices": [
        {"value": "Closed", "action": "empty", "spellings": ["Closed"]},
        {"value": "In-Progress", "action": "remove", "target": "open", "spellings": ["In-Progress", "in progress"]}]})
    assert res.status_code == 200, res.text
    state_rule = next(x for x in c.get(url).json()["columns"] if x["name"] == "state")
    assert state_rule["mapping"] == {"open": ["Open"]} and "Closed" in state_rule["null_tokens"]
    assert [(d["value"], d["now"]["action"], d["changed"]) for d in state_rule["ai_mapped"]] == [
        ("Closed", "empty", True), ("In-Progress", "keep", True)]
    res = c.post(f"{url}/values", json={"column": "state", "choices": [{"value": "Closed", "action": "merge", "target": "open"}]})
    assert res.status_code == 200 and "Closed" not in next(x for x in c.get(url).json()["columns"]
                                                          if x["name"] == "state")["null_tokens"]  # fmt: skip


def test_only_label_like_leftovers_are_sent() -> None:
    from pipeline.silver.mapping_assist import label_like

    assert label_like("Lift") and label_like("air conditioning") and label_like("IT/Network")
    assert not label_like("Breaker keeps tripping in server room 696.")
    assert not label_like("x" * 41)
