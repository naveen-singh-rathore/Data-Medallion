"""Validator: each layer catches its errors, coverage lists exactly the unaddressed ids,
dry-run changes nothing, warnings fire at thresholds, feedback stays under 2 KB."""

import pytest
from silver_helpers import column, small_rules

from pipeline.profiling import load_profile, profile_run
from pipeline.silver.defaults import default_rules, flagged_columns, merge_rules
from pipeline.silver.rules_schema import SilverRules
from pipeline.silver.validator import validate_rules


@pytest.fixture
def silver_run(ingest) -> str:
    run_id = ingest("silver_small.csv")
    profile_run(run_id)
    return str(run_id)


def issue_ids(run_id: str, severities=("high", "medium")) -> list[str]:
    return [i["id"] for i in load_profile(run_id)["issues"] if i["severity"] in severities]


def covered(run_id: str, **extra) -> dict:
    """Small rules that address every high/medium issue (by ignoring them, with a reason)."""
    resolved = [{"issue_id": i, "resolution": "ignored", "reason": "Out of scope for this unit test."}
                for i in issue_ids(run_id)]  # fmt: skip
    return small_rules(run_id, addressed_issues=resolved, **extra)


def test_schema_layer_names_the_field(silver_run) -> None:
    raw = small_rules(silver_run)
    raw["columns"][4]["why"] = "short"
    report = validate_rules(raw, silver_run)
    assert (report.ok, report.layer) == (False, "schema")
    assert report.errors == ["columns[4] (amount).why: String should have at least 20 characters"]


def test_semantic_layer(silver_run) -> None:
    raw = small_rules(silver_run)
    raw["columns"][0]["source"] = "nope"
    raw["columns"][2]["parse_formats"] = ["YYYY-MM-DD", "DD.MM.YYYY"]
    raw["columns"][3].pop("day_month_order")
    raw["columns"][4]["steps"] = ["lower"]
    raw["columns"][5]["addresses"] = ["FAKE:flag"]
    report = validate_rules(raw, silver_run)
    assert report.layer == "semantic"
    text = "\n".join(report.errors)
    for expected in ("(id).source: 'nope' is not a column", "unknown format id(s) ['DD.MM.YYYY']",
                     "(closed).day_month_order: required", "(amount).steps: ['lower'] only apply to text",
                     "unknown issue id 'FAKE:flag'"):  # fmt: skip
        assert expected in text


def test_coverage_lists_exactly_the_unaddressed_ids(silver_run) -> None:
    raw = covered(silver_run)
    dropped = raw["addressed_issues"][:2]
    raw["addressed_issues"] = raw["addressed_issues"][2:]
    report = validate_rules(raw, silver_run)
    assert (report.ok, report.layer) == (False, "coverage")
    assert report.errors == [f"coverage: 2 high/medium issue(s) not in addressed_issues: {[d['issue_id'] for d in dropped]}"]
    assert validate_rules(covered(silver_run), silver_run).ok


def test_rule_resolution_needs_a_citing_rule(silver_run) -> None:
    raw = covered(silver_run)
    raw["addressed_issues"][0] = {"issue_id": raw["addressed_issues"][0]["issue_id"], "resolution": "rule"}
    report = validate_rules(raw, silver_run)
    assert report.errors[0].startswith("coverage: resolution=rule but no rule lists it")


def test_dry_run_changes_nothing(conn, silver_run) -> None:
    counts = "SELECT (SELECT count(*) FROM bronze.records) + (SELECT count(*) FROM meta.dataset_profiles) AS n"
    before = conn.execute(counts).fetchone()["n"]
    report = validate_rules(covered(silver_run), silver_run)
    assert report.stats["rows"]["sampled"] == 12
    assert conn.execute(counts).fetchone()["n"] == before


def test_warnings_fire_at_thresholds(silver_run) -> None:
    raw = covered(silver_run)
    raw["columns"].append(column(name="state_as_int", source="state", type="integer", confidence=0.4))
    raw["columns"].append(column(name="flag_dropped", source="flag", type="text", null_tokens=["yes", "no"]))
    report = validate_rules(raw, silver_run)
    text = "\n".join(report.warnings)
    assert report.ok
    assert "state_as_int: 100.0% of sampled values are invalid" in text
    assert "flag_dropped: rules null 100% of previously non-null values" in text
    assert "rows: 100.0% of sampled rows have errors" in text
    assert "state_as_int: confidence 0.4 < 0.6 without a needs_human question" in text


def test_feedback_is_compact(silver_run) -> None:
    raw = small_rules(silver_run)
    raw["columns"] = [column(name=f"c{i}", source=f"missing_{i}", type="text") for i in range(80)]
    report = validate_rules(raw, silver_run)
    assert len(report.errors) == 80
    assert len(report.feedback().encode()) <= 2048 and "more lines" in report.feedback()


def test_default_rules_cover_only_issue_free_columns(silver_run) -> None:
    defaults = default_rules(silver_run)
    flagged = flagged_columns(load_profile(silver_run)["issues"])
    assert {c.source for c in defaults.columns}.isdisjoint(flagged)
    assert defaults.generated_by == "default" and all(c.steps == ["trim", "remove_hidden_chars"] for c in defaults.columns)
    agent = SilverRules.model_validate(small_rules(silver_run) | {"generated_by": "agent"})
    merged = merge_rules(defaults, agent)
    assert merged.generated_by == "agent"
    assert {c.source for c in merged.columns} == {c.source for c in defaults.columns} | {c.source for c in agent.columns}
    assert next(c for c in merged.columns if c.source == "amount").sentinels == ["999"]  # agent rule wins
