"""Dataset card: determinism, budget, coverage guarantee, Tier 0, 150 columns, storage, run-all."""

import re

import pytest

from pipeline import cli
from pipeline.card import CardInputs, build_card, estimate_tokens, render_card

SEVERITIES = ("high", "medium", "low")


def column(name: str, distinct: int = 10) -> dict:
    return {
        "column": name,
        "type_inference": {"dominant_type": "text", "confidence": 1.0},
        "completeness": {"empty_or_null_like_share": 0.05},
        "cardinality": {"distinct": distinct, "role_hint": "categorical"},
    }


def synthetic(n_columns: int, issue_every: int = 2) -> CardInputs:
    """n columns; every `issue_every`-th column gets one high, one medium and one low issue."""
    names = [f"column_with_a_long_name_{i:03d}" for i in range(n_columns)]
    issues = [
        {"column": name, "code": f"{sev.upper()}_CODE", "severity": sev,
         "evidence": {"count": i, "share": 0.1234, "examples": ["a", "b", "c"]}}
        for i, name in enumerate(names) if i % issue_every == 0 for sev in SEVERITIES
    ]  # fmt: skip
    summary = {"rows_ok": 1000, "rows_malformed": 2, "exact_duplicate_rows": 0, "near_duplicate_rows": 1,
               "partial_rows": {"count": 3}, "columns": n_columns}  # fmt: skip
    context = {"source": "inferred", "approved": False, "text": "Synthetic data.",
               "draft": {"critical_columns": [names[1]], "column_tags": {names[1]: "id"}}}  # fmt: skip
    return CardInputs("run-x", "synthetic.csv", context, summary, issues, [column(n) for n in names])


def pointer_counts(text: str) -> dict[str, int]:
    found = {"columns": 0, "issues": 0}
    for n, kind in re.findall(r"^- (\d+) (?:more )?\w+ (columns|issues)", text, re.MULTILINE):
        found[kind] += int(n)
    return found


def assert_full_coverage(inputs: CardInputs, text: str) -> None:
    """Every column and issue is shown on its own line or counted in a pointer line."""
    shown_columns = sum(1 for c in inputs.columns if re.search(rf"^- {c['column']} \|", text, re.MULTILINE))
    shown_issues = sum(1 for line in text.splitlines() if re.match(r"^- [A-Z_]+ \S", line) and "→" not in line)
    pointers = pointer_counts(text)
    assert shown_columns + pointers["columns"] == len(inputs.columns)
    assert shown_issues + pointers["issues"] == len(inputs.issues)


def test_card_is_deterministic() -> None:
    first, second = build_card(synthetic(20), 1000), build_card(synthetic(20), 1000)
    assert (first.text, first.card_hash) == (second.text, second.card_hash)


@pytest.mark.parametrize("budget", [700, 900, 1500, 4000])  # tier 0 alone is ~541 tokens
def test_budget_respected_and_coverage_holds(budget: int) -> None:
    inputs = synthetic(40)
    card = build_card(inputs, budget)
    assert not card.metrics["over_budget"]
    assert estimate_tokens(card.text) == card.metrics["est_tokens"] <= budget
    assert_full_coverage(inputs, card.text)
    m = card.metrics
    assert m["columns_shown"] + m["compressed_counts"]["flagged_columns"] + m["compressed_counts"]["clean_columns"] == 40
    assert m["issues_shown"] + m["compressed_counts"]["medium_issues"] + m["compressed_counts"]["low_issues"] == 60


def test_tiny_budget_keeps_tier0_and_counts_everything() -> None:
    inputs = synthetic(40)
    card = build_card(inputs, 50)
    assert card.metrics["over_budget"]
    high = [i for i in inputs.issues if i["severity"] == "high"]
    assert all(f"- HIGH_CODE {i['column']}:" in card.text for i in high)  # Tier 0 never dropped
    assert "Context (inferred, UNAPPROVED" in card.text and "## Critical columns" in card.text
    assert_full_coverage(inputs, card.text)
    assert card.metrics["tiers_full"] == {"tier0": True, "tier1": False, "tier2": False, "tier3": False}


def test_150_columns_fit_the_default_budget() -> None:
    inputs = synthetic(150, issue_every=5)
    card = build_card(inputs, 4000)
    assert not card.metrics["over_budget"] and card.metrics["est_tokens"] <= 4000
    assert_full_coverage(inputs, card.text)
    assert "→ get_column_profile(name)" in card.text


def test_render_stores_one_row_per_card_hash(ingest, conn) -> None:
    run_id = ingest("profile_messy.csv")
    cli.main(["profile", "--run-id", str(run_id)])
    first, second = render_card(run_id, 4000), render_card(run_id, 4000)
    assert first.card_hash == second.card_hash
    rows = conn.execute("SELECT est_tokens, issues_shown, issues_total FROM meta.dataset_cards WHERE run_id = %s",
                        (run_id,)).fetchall()  # fmt: skip
    assert len(rows) == 1 and rows[0]["issues_shown"] == rows[0]["issues_total"]


def test_run_all_ingests_profiles_adds_context_and_renders_card(ingest, conn, tmp_path, capsys) -> None:
    run_id = ingest("profile_messy.csv")  # registers the fixture for cleanup; run-all reloads it
    context_file = tmp_path / "context.txt"
    context_file.write_text("Facility tickets used for SLA reporting.")
    path = str(conn.execute("SELECT source_file FROM meta.ingestion_runs WHERE run_id = %s", (run_id,)).fetchone()["source_file"])
    assert cli.main(["run-all", path, "--force", "--context-file", str(context_file)]) == 0
    latest = conn.execute("SELECT run_id FROM meta.ingestion_runs WHERE source_file = %s ORDER BY started_at DESC LIMIT 1",
                          (path,)).fetchone()["run_id"]  # fmt: skip
    context = conn.execute("SELECT source, approved FROM meta.dataset_context WHERE run_id = %s", (latest,)).fetchone()
    assert (context["source"], context["approved"]) == ("human", True)
    assert conn.execute("SELECT count(*) AS n FROM meta.dataset_cards WHERE run_id = %s", (latest,)).fetchone()["n"] == 1
    assert "Context (human):" in capsys.readouterr().out
