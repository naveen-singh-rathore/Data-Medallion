"""Rule schema: strict models, field paths in errors, JSON schema for the agent."""

import json

import pytest
from pydantic import ValidationError
from silver_helpers import small_rules

from pipeline.silver.rules_schema import (
    SilverRules,
    from_yaml,
    rules_json_schema,
    to_yaml,
)

RUN = "00000000-0000-0000-0000-000000000000"


def test_valid_example_passes_and_round_trips_yaml() -> None:
    rules = SilverRules.model_validate(small_rules(RUN))
    assert SilverRules.model_validate(from_yaml(to_yaml(rules))) == rules


@pytest.mark.parametrize(("change", "message"), [
    (lambda r: r["columns"][0].update(steps=["shout"]), "steps.0"),
    (lambda r: r["columns"][1].update(why="too short"), "why"),
    (lambda r: r["columns"][0].update(colour="red"), "Extra inputs are not permitted"),
    (lambda r: r.update(dedup={"key": ["id"], "strategy": "keep_latest", "why": "x" * 25}), "order_by is required"),
    (lambda r: r.update(addressed_issues=[{"issue_id": "X:y", "resolution": "ignored"}]), "reason"),
])  # fmt: skip
def test_invalid_rules_are_rejected(change, message) -> None:
    raw = small_rules(RUN)
    change(raw)
    with pytest.raises(ValidationError, match=message):
        SilverRules.model_validate(raw)


def test_json_schema_for_the_agent_is_generated() -> None:
    schema = rules_json_schema()
    assert schema["additionalProperties"] is False
    assert "trim" in json.dumps(schema) and "day_month_order" in json.dumps(schema)
