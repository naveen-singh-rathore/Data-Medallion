"""Silver rules: the only thing the data-quality agent produces. It picks from menus
(steps, date format ids, actions); it never writes SQL. The compiler owns the SQL.

Strict Pydantic v2 models with extra='forbid', so unknown fields and typos are errors.
"""

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from pipeline.config import REPO_ROOT

RULES_DIR = REPO_ROOT / "rules"
SNAKE = r"^[a-z][a-z0-9_]*$"

Step = Literal["trim", "collapse_spaces", "remove_hidden_chars", "lower", "upper", "title", "strip_currency",
               "strip_thousands", "strip_percent", "strip_units", "european_decimal"]  # fmt: skip
ColumnType = Literal["text", "integer", "numeric", "boolean", "date", "timestamp"]
TEXT_STEPS = frozenset({"lower", "upper", "title"})
NUMBER_STEPS = frozenset({"strip_currency", "strip_thousands", "strip_percent", "strip_units", "european_decimal"})


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Range(_Strict):
    min: float | str | None = None  # number, or ISO date/timestamp for date/timestamp columns
    max: float | str | None = None


class ColumnRule(_Strict):
    name: str = Field(pattern=SNAKE, max_length=60, description="snake_case target column name")
    source: str = Field(description="bronze payload key (a column of the run header)")
    type: ColumnType
    steps: list[Step] = Field(default_factory=list, description="cleaning steps, applied in order")
    null_tokens: list[str] = Field(default_factory=list, description="values -> NULL (after steps, case-insensitive)")
    sentinels: list[str] = Field(default_factory=list, description="placeholder values -> NULL + flagged")
    mapping: dict[str, list[str]] | None = Field(
        default=None, description="target value -> variants; matched on the profiler normalization key"
    )
    unmapped_action: Literal["keep", "null", "invalid"] = "keep"
    boolean_true: list[str] = Field(default_factory=list)
    boolean_false: list[str] = Field(default_factory=list)
    parse_formats: list[str] = Field(default_factory=list, description="date format ids from the profiler registry")
    day_month_order: Literal["dmy", "mdy"] | None = None
    range: Range | None = None
    required: bool = False
    invalid_action: Literal["null", "reject"] = "null"
    confidence: float = Field(ge=0, le=1)
    why: str = Field(min_length=20, description="business impact of this rule")
    addresses: list[str] = Field(default_factory=list, description="issue ids this rule resolves")
    generated_by: Literal["agent", "default", "human"] | None = Field(
        default=None, description="who wrote this column rule (set by the pipeline; agents may omit it)"
    )


class DedupRule(_Strict):
    key: list[str] = Field(min_length=1, description="target columns")
    strategy: Literal["exact", "keep_latest"]
    order_by: str | None = None
    why: str = Field(min_length=20)
    addresses: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _order_by_for_latest(self) -> "DedupRule":
        if self.strategy == "keep_latest" and not self.order_by:
            raise ValueError("order_by is required when strategy is keep_latest")
        return self


ROW_CHECK_PARAMS = {"date_order": {"a", "b"}, "required_if": {"column", "other", "values"}, "not_both_null": {"a", "b"},
                    "swapped_columns": {"a", "b"}, "junk_row": {"id", "id_pattern", "min_empty_share"},
                    "declared_duplicate": {"source", "pattern", "id", "prefix"}}  # fmt: skip
# which params name target columns (the rest are values)
ROW_CHECK_COLUMNS = {"date_order": ("a", "b"), "required_if": ("column", "other"), "not_both_null": ("a", "b"),
                     "swapped_columns": ("a", "b"), "junk_row": ("id",), "declared_duplicate": ("source", "id")}  # fmt: skip
ONLY_ACTION = {"swapped_columns": "swap", "declared_duplicate": "duplicate"}


class RowCheck(_Strict):
    kind: Literal["date_order", "required_if", "not_both_null", "swapped_columns", "junk_row", "declared_duplicate"]
    params: dict[str, str | list[str]] = Field(
        description="date_order/not_both_null: {a, b}; required_if: {column, other, values: [...]}; "
        "swapped_columns: {a: column with a standard list, b: free-text column} - exchange their raw values when a "
        "holds long text and b holds one of a's known values; junk_row: {id, id_pattern (regex), min_empty_share "
        "(e.g. '0.6')}; declared_duplicate: {source: text column, pattern: regex with ONE group capturing the "
        "referenced id, id: id column, prefix: text put before the captured value}"
    )
    action: Literal["reject", "null_second", "flag", "swap", "duplicate"]
    why: str = Field(min_length=20)
    addresses: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _params_match_kind(self) -> "RowCheck":
        expected = ROW_CHECK_PARAMS[self.kind]
        if set(self.params) != expected:
            raise ValueError(f"params for {self.kind} must be exactly {sorted(expected)}")
        if self.kind == "required_if" and not isinstance(self.params["values"], list):
            raise ValueError("required_if params.values must be a list")
        if self.action == "null_second" and self.kind not in ("date_order", "not_both_null"):
            raise ValueError("null_second applies to date_order / not_both_null only")
        only = ONLY_ACTION.get(self.kind)
        if (only and self.action != only) or (not only and self.action in ONLY_ACTION.values()):
            raise ValueError(f"action {self.action!r} does not fit {self.kind}" + (f" (use {only!r})" if only else ""))
        if self.kind == "junk_row":
            try:
                share = float(str(self.params["min_empty_share"]))
            except ValueError:
                share = -1
            if not 0 < share <= 1:
                raise ValueError("junk_row params.min_empty_share must be a number in (0, 1]")
        if self.kind in ("junk_row", "declared_duplicate"):
            key = "id_pattern" if self.kind == "junk_row" else "pattern"
            try:
                groups = re.compile(str(self.params[key])).groups
            except re.error as exc:
                raise ValueError(f"{self.kind} params.{key} is not a valid regex: {exc}") from exc
            if self.kind == "declared_duplicate" and groups != 1:
                raise ValueError("declared_duplicate params.pattern needs exactly one capture group")
        return self


class IssueResolution(_Strict):
    issue_id: str
    resolution: Literal["rule", "ignored"]
    reason: str | None = None

    @model_validator(mode="after")
    def _reason_when_ignored(self) -> "IssueResolution":
        if self.resolution == "ignored" and not (self.reason and len(self.reason) >= 10):
            raise ValueError("reason (>= 10 chars) is required when resolution is 'ignored'")
        return self


class HumanQuestion(_Strict):
    issue_ids: list[str] = Field(min_length=1)
    question: str = Field(min_length=10)
    options: list[str] = Field(min_length=2)
    agent_recommendation: str


class SilverRules(_Strict):
    schema_version: Literal["1"] = "1"
    source_run_id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    target_table: str = Field(default="silver.records_clean", pattern=r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
    generated_by: Literal["agent", "default", "human"]
    prompt_version: str | None = None
    columns: list[ColumnRule] = Field(min_length=1)
    dedup: DedupRule | None = None
    row_checks: list[RowCheck] = Field(default_factory=list)
    addressed_issues: list[IssueResolution] = Field(default_factory=list)
    needs_human: list[HumanQuestion] = Field(default_factory=list)


def rules_json_schema() -> dict[str, Any]:
    """JSON schema for the agent's submit_rules tool (generated, never hand-written)."""
    return SilverRules.model_json_schema()


def rules_hash(rules: SilverRules) -> str:
    payload = json.dumps(rules.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def to_yaml(rules: SilverRules) -> str:
    """Only non-default fields (plus version and target), so files stay short to review.

    Free-form maps (mapping targets, row-check params) are written in sorted order: the same
    rules must give the same bytes whether they came from the API or from the jsonb cache,
    which reorders object keys.
    """
    data = {"schema_version": rules.schema_version, "target_table": rules.target_table,
            **rules.model_dump(mode="json", exclude_none=True, exclude_defaults=True)}  # fmt: skip
    for column in data.get("columns", []):
        if isinstance(column.get("mapping"), dict):
            column["mapping"] = dict(sorted(column["mapping"].items()))
    for check in data.get("row_checks", []):
        check["params"] = dict(sorted(check["params"].items()))
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=110)


def from_yaml(text: str) -> dict[str, Any]:
    """Parse YAML into a plain dict; validation is the validator's job (it reports field paths)."""
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise TypeError("rules file must contain a mapping at the top level")
    return data


def proposed_path(run_id: str) -> Path:
    return RULES_DIR / run_id / "proposed.yaml"


APPROVED_PATH = RULES_DIR / "silver_rules.yaml"
