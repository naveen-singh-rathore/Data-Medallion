"""Fresh-clone path: approved rules and context are bound to the FILE, recorded answers load on init-db."""

import json

import pytest

from pipeline import demo
from pipeline.migrate import migrate
from pipeline.silver.defaults import default_rules
from pipeline.silver.rules_schema import SilverRules, from_yaml, rules_hash, to_yaml


@pytest.fixture
def repo(tmp_path, monkeypatch, ingest):
    """A run plus committed-style approved rules + provenance written for ANOTHER run of the same file."""
    run_id = str(ingest("silver_small.csv"))
    from pipeline.profiling import profile_run

    profile_run(run_id)
    rules = default_rules(run_id).model_copy(update={"source_run_id": "00000000-0000-0000-0000-000000000001",
                                                     "generated_by": "human"})  # fmt: skip
    approved, prov = tmp_path / "silver_rules.yaml", tmp_path / "silver_rules.provenance.json"
    approved.write_text(to_yaml(rules))
    prov.write_text(json.dumps({"file_sha256": demo._sha(run_id), "rules_hash": rules_hash(rules),
                                "context": {"source": "inferred", "text": "Test tickets.", "draft": {"domain": "t"}}}))  # fmt: skip
    monkeypatch.setattr(demo, "APPROVED_PATH", approved)
    monkeypatch.setattr(demo, "PROVENANCE", prov)
    monkeypatch.setattr(demo, "BOUND_DIR", tmp_path / "bound")
    return run_id, approved, prov


def test_approved_rules_bind_to_a_new_run_of_the_same_file(repo) -> None:
    run_id, approved, _ = repo
    assert demo.approved_path_for(run_id) == approved  # not bound yet
    path = demo.bind_approved(run_id)
    bound = SilverRules.model_validate(from_yaml(path.read_text()))
    original = SilverRules.model_validate(from_yaml(approved.read_text()))
    assert bound.source_run_id == run_id
    assert bound.model_copy(update={"source_run_id": original.source_run_id}) == original  # nothing else changes
    assert demo.approved_path_for(run_id) == path


def test_binding_refuses_a_different_file(repo) -> None:
    run_id, _, prov = repo
    prov.write_text(json.dumps({"file_sha256": "0" * 64, "context": {}}))
    with pytest.raises(ValueError, match="approved rules are for file"):
        demo.bind_approved(run_id)


def test_approved_context_is_restored_only_when_missing(repo, conn) -> None:
    run_id, _, _ = repo
    conn.execute("DELETE FROM meta.dataset_context WHERE run_id = %s", (run_id,))
    ctx = demo.ensure_approved_context(run_id)
    assert (ctx["source"], ctx["approved"], ctx["text"]) == ("inferred", True, "Test tickets.")


def test_init_db_loads_the_recorded_answers(conn) -> None:
    migrate()
    for name, (table, _, _) in demo.CACHES.items():
        lines = sum(1 for line in (demo.FIXTURES / name).read_text().splitlines() if line.strip())
        assert conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] >= lines
