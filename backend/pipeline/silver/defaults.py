"""Default rules: a safe, boring rule for every column the profiler found no issue in.

The same per-column default is the starting point the agent edits for flagged columns.
"""

import re
import uuid
from typing import Any

from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profiling import load_profile
from pipeline.silver.rules_schema import ColumnRule, SilverRules

TYPE_MAP = {"integer": "integer", "float": "numeric", "boolean": "boolean", "date": "date", "datetime": "timestamp"}
DEFAULT_CONFIDENCE = 0.9
SAFE_TYPE_CONFIDENCE = 0.99
DEFAULT_WHY = "Default for a column with no profiler issues: trims whitespace and removes hidden characters only."


def target_name(source: str) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", source.lower()).strip("_") or "column"
    return name if name[0].isalpha() else f"c_{name}"


def default_column_rule(profile: dict[str, Any]) -> ColumnRule:
    t = profile["type_inference"]
    col_type = TYPE_MAP.get(t["dominant_type"], "text") if t["confidence"] >= SAFE_TYPE_CONFIDENCE else "text"
    extra: dict[str, Any] = {}
    if col_type in ("date", "timestamp"):
        dates = profile.get("dates") or {}
        extra["parse_formats"] = [f["format"] for f in dates.get("formats", [])]
        if dates.get("dmy_evidence", 0) or dates.get("mdy_evidence", 0) or dates.get("ambiguous_count", 0):
            extra["day_month_order"] = "dmy" if dates.get("dmy_evidence", 0) > dates.get("mdy_evidence", 0) else "mdy"
        if not extra["parse_formats"]:
            col_type, extra = "text", {}
    if col_type == "boolean":
        tokens = list(CFG.boolean_tokens) + list(CFG.boolean_digits)
        extra |= {"boolean_true": [x for x in tokens if x in ("true", "yes", "y", "t", "1")],
                  "boolean_false": [x for x in tokens if x in ("false", "no", "n", "f", "0")]}  # fmt: skip
    missing = profile["completeness"]["empty_or_null_like_share"]
    return ColumnRule(
        name=target_name(profile["column"]), source=profile["column"], type=col_type,  # type: ignore[arg-type]
        steps=["trim", "remove_hidden_chars"],
        required=missing == 0 and profile["cardinality"]["role_hint"] == "candidate_key",
        confidence=DEFAULT_CONFIDENCE, why=DEFAULT_WHY, generated_by="default", **extra,
    )  # fmt: skip


def flagged_columns(issues: list[dict[str, Any]]) -> set[str]:
    return {c for i in issues if i["column"] for c in i["column"].split(" > ")}


def default_rules(run_id: uuid.UUID | str) -> SilverRules:
    """Defaults for issue-free columns only (generated_by='default')."""
    profile = load_profile(uuid.UUID(str(run_id)))
    flagged = flagged_columns(profile["issues"])
    columns = [default_column_rule(p) for p in profile["columns"] if p["column"] not in flagged]
    return SilverRules(source_run_id=str(run_id), generated_by="default", columns=columns)


def merge_rules(default: SilverRules, agent: SilverRules) -> SilverRules:
    """Agent column rules replace defaults with the same source; everything else comes from the agent."""
    by_source = {c.source: c for c in default.columns}
    by_source.update({c.source: c.model_copy(update={"generated_by": c.generated_by or agent.generated_by})
                      for c in agent.columns})  # fmt: skip
    order = [c.source for c in default.columns] + [c.source for c in agent.columns if c.source not in
                                                   {d.source for d in default.columns}]  # fmt: skip
    return agent.model_copy(update={"columns": [by_source[s] for s in order]})
