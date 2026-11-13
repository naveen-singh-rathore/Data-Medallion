"""Semantic classification: targets, closed list, confidence threshold, cache, silver columns, eval."""

import csv
import dataclasses
import json
import threading
import time
import uuid
from types import SimpleNamespace

import pytest
from fakes import ExplodingClient, FakeResponse

from pipeline.context import ensure_context
from pipeline.privacy import name_like, scrub_text
from pipeline.profiling import profile_run
from pipeline.silver import build, classify
from pipeline.silver.defaults import default_rules
from pipeline.silver.rules_schema import ColumnRule, to_yaml

TARGET = "silver.test_classify_clean"


class LabelMessages:
    """Answers every ticket in the prompt: 'In-Progress' -> open (sure), 'in progress' -> open (unsure),
    plus one answer outside the closed list and one for an id that does not exist (both dropped)."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        prompt = kwargs["messages"][0]["content"]
        tickets = [json.loads(line) for line in prompt.splitlines() if line.startswith('{"id"')]
        items = [{"id": t["id"], "category": "open", "confidence": 0.9 if "In-Progress" in t["ticket"] else 0.3}
                 for t in tickets]  # fmt: skip
        items += [{"id": 99, "category": "open", "confidence": 1.0}]
        return FakeResponse({"id": "m", "type": "message", "role": "assistant", "model": kwargs["model"],
                             "content": [{"type": "text", "text": json.dumps({"items": items})}],
                             "stop_reason": "end_turn", "usage": {"input_tokens": 500, "output_tokens": 50}})  # fmt: skip


class LabelClient:
    def __init__(self) -> None:
        self.messages = LabelMessages()


@pytest.fixture
def built(ingest, conn, tmp_path):
    run_id = str(ingest("silver_small.csv"))
    profile_run(run_id)
    ensure_context(run_id, '{"domain": "tickets", "summary": "One row per ticket.", '
                           '"column_tags": {"state": "category", "owner": "person"}}')  # fmt: skip
    rules = default_rules(run_id)
    state = ColumnRule(name="state", source="state", type="text", steps=["trim"], confidence=0.8, generated_by="agent",
                       mapping={"closed": ["Closed"], "open": ["Open"]}, why="Ticket state drives open/closed counts.")  # fmt: skip
    rules = rules.model_copy(update={"columns": [c for c in rules.columns if c.source != "state"] + [state],
                                     "target_table": TARGET, "generated_by": "human"})  # fmt: skip
    path = tmp_path / "silver_rules.yaml"
    path.write_text(to_yaml(rules))
    build.build_silver(run_id, path)
    yield run_id, path
    conn.execute(f"DROP VIEW IF EXISTS {TARGET}_valid")
    conn.execute(f"DROP TABLE IF EXISTS {TARGET}, {TARGET}__build")
    conn.execute("DELETE FROM meta.classification_cache WHERE task = %s AND text_hash IN (SELECT text_hash FROM"
                 " silver.ai_labels WHERE run_id = %s)", (classify.TASK, run_id))  # fmt: skip


def test_names_are_scrubbed_but_generic_words_are_kept() -> None:
    assert (
        name_like("Jane Doe") and name_like("D. Kim") and not name_like("System") and not name_like("CityWide Electric")
    )
    assert not name_like("maintenance team") and not name_like("ABC Mechanical")
    text = scrub_text("Call Jane Doe at 555-123-4567 or jane@x.com; System says HVAC down", ["Jane Doe"])
    assert text == "Call [PERSON] at [PHONE] or [EMAIL]; System says HVAC down"


def test_targets_are_labelled_with_a_closed_list_and_never_overwrite_the_rule_value(built, conn) -> None:
    run_id, path = built
    client = LabelClient()
    report = classify.classify_run(run_id, mode="record", client=client, approved_path=path)
    assert (report["column"], report["targets"], report["unique_texts"], report["llm_calls"]) == ("state", 2, 2, 1)
    assert (report["recovered"], report["low_confidence"]) == (1, 1) and report["distribution"] == {"open": 1}
    schema = client.messages.calls[0]["output_config"]["format"]["schema"]
    assert schema["properties"]["items"]["items"]["properties"]["category"]["enum"] == ["closed", "open", "other"]
    rows = {r["state_raw"] if "state_raw" in r else r["state"]: r for r in
            conn.execute(f"SELECT * FROM {TARGET} WHERE state_source <> 'rule'").fetchall()}  # fmt: skip
    sure, unsure = rows["In-Progress"], rows["in progress"]
    assert (sure["state"], sure["state_ai"], float(sure["state_ai_confidence"]), sure["state_source"]) == (
        "In-Progress",
        "open",
        0.9,
        "ai",
    )
    assert (unsure["state_ai"], float(unsure["state_ai_confidence"]), unsure["state_source"]) == (None, 0.3, "none")
    assert "ai_labelled" in sure["_quality_flags"]["state"] and "ai_low_confidence" in unsure["_quality_flags"]["state"]
    sources = {r["state_source"]: r["n"] for r in
               conn.execute(f"SELECT state_source, count(*) AS n FROM {TARGET}_valid GROUP BY 1").fetchall()}  # fmt: skip
    assert sources == {"rule": 10, "ai": 1, "none": 1}

    again = classify.classify_run(run_id, client=ExplodingClient(), approved_path=path)  # rerun: cache only
    assert (again["cache_hits"], again["llm_calls"], again["recovered"]) == (2, 0, 1)
    flags = conn.execute(f"SELECT _quality_flags -> 'state' AS f FROM {TARGET} WHERE state_source = 'ai'").fetchone()[
        "f"
    ]
    assert flags.count("ai_labelled") == 1  # idempotent

    build.build_silver(run_id, path)  # a rebuild keeps the AI columns
    assert conn.execute(f"SELECT count(*) AS n FROM {TARGET}_valid WHERE state_source = 'ai'").fetchone()["n"] == 1


def test_eval_sheet_and_scoring(built, tmp_path) -> None:
    run_id, path = built
    classify.classify_run(run_id, mode="record", client=LabelClient(), approved_path=path)
    sheet = classify.make_eval_sheet(run_id, 40, tmp_path / "eval.csv", approved_path=path)
    with sheet.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2 and all(r["text_shown_to_ai"].startswith("state: ") for r in rows)
    assert "open" not in "".join(
        r["text_shown_to_ai"] + r["human_label"] for r in rows if "progress" not in r["text_shown_to_ai"].lower()
    )
    for r in rows:
        r["human_label"] = "open"
    with sheet.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    result = classify.evaluate(sheet, run_id)
    assert (
        result["labelled"],
        result["answered"],
        result["accuracy_all"],
        result["coverage"],
        result["accuracy_confident"],
    ) == (2, 2, 1.0, 0.5, 1.0)


class SlowLabelMessages(LabelMessages):
    """LabelMessages that takes a moment and records how many calls were in flight at once."""

    def __init__(self) -> None:
        super().__init__()
        self.lock, self.active, self.peak = threading.Lock(), 0, 0

    def create(self, **kwargs):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(0.2)
        try:
            return super().create(**kwargs)
        finally:
            with self.lock:
                self.active -= 1


def test_batches_run_in_parallel_with_the_same_results(built, conn, monkeypatch) -> None:
    run_id, _ = built
    rule = ColumnRule(name="state", source="state", type="text", confidence=0.8, why="State drives counts.",
                      mapping={"closed": ["Closed"], "open": ["Open"]})  # fmt: skip
    salt = uuid.uuid4().hex[:8]  # texts unique to this test, so nothing is cached yet
    texts = [f"In-Progress ticket {salt} #{i}" for i in range(2 * classify.BATCH + 7)]  # three batches
    client = SimpleNamespace(messages=SlowLabelMessages())
    monkeypatch.setattr(classify, "settings", dataclasses.replace(classify.settings, classify_concurrency=3))
    try:
        results, usage = classify.classify_texts(texts, rule, run_id, None, mode="live", client=client)
        assert client.messages.peak > 1  # the batches overlapped
        assert usage["llm_calls"] == 3 and usage["missing"] == 0
        assert results == {t: ("open", 0.9) for t in texts}  # every text, in the right place
        again, usage = classify.classify_texts(texts, rule, run_id, None, mode="live", client=client)
        assert again == results and usage["llm_calls"] == 0 and usage["cache_hits"] == len(texts)
    finally:
        hashes = [classify.text_hash(t, classify.labels_for(rule), classify.settings.cheap_model) for t in texts]
        conn.execute("DELETE FROM meta.classification_cache WHERE text_hash = ANY(%s)", (hashes,))
        conn.execute("DELETE FROM meta.llm_cache WHERE request_hash IN (SELECT request_hash FROM meta.llm_calls"
                     " WHERE run_id = %s AND purpose = %s)", (run_id, classify.TASK))  # fmt: skip
