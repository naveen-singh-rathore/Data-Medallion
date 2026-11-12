"""Business context for a run: written by a human, or drafted by ONE cheap LLM call.

Human context is approved on entry and costs nothing. Otherwise the cheap model sees only
column names, role hints, dominant types, the row count and 3 sample rows (truncated, with
PII-hinted columns masked) and returns a structured draft that waits for approval.
"""

import json
import string
import uuid
from typing import Any, Literal

from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, ValidationError

from pipeline.config import settings
from pipeline.db import transaction
from pipeline.llm import LLMError, call_llm
from pipeline.logging_setup import get_logger
from pipeline.profile_columns import mask
from pipeline.profiling import load_profile

PROMPT_VERSION = "context_draft_v1"
SAMPLE_ROWS = 3
SAMPLE_CHARS = 60
SUMMARY_WORDS = 80
MAX_METRICS = 5
TAGS = ("id", "timestamp", "category", "status", "priority", "money", "quantity", "duration",
        "free_text", "person", "contact_pii", "location", "other")  # fmt: skip

Tag = Literal["id", "timestamp", "category", "status", "priority", "money", "quantity", "duration",
              "free_text", "person", "contact_pii", "location", "other"]  # fmt: skip


class ColumnTag(BaseModel):
    column: str
    tag: Tag


class ContextDraft(BaseModel):
    """What the model returns (column_tags as a list: strict JSON schemas can't have free keys)."""

    domain: str
    summary: str
    likely_metrics: list[str] = Field(default_factory=list)
    critical_columns: list[str] = Field(default_factory=list)
    column_tags: list[ColumnTag] = Field(default_factory=list)


def draft_schema(columns: list[str]) -> dict[str, Any]:
    """JSON schema for structured output; column names are enums so the model can't invent any."""
    column = {"type": "string", "enum": columns}
    return {
        "type": "object",
        "properties": {
            "domain": {"type": "string"},
            "summary": {"type": "string"},
            "likely_metrics": {"type": "array", "items": {"type": "string"}},
            "critical_columns": {"type": "array", "items": column},
            "column_tags": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"column": column, "tag": {"type": "string", "enum": list(TAGS)}},
                    "required": ["column", "tag"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["domain", "summary", "likely_metrics", "critical_columns", "column_tags"],
        "additionalProperties": False,
    }


def validate_draft(raw: Any, columns: list[str]) -> dict[str, Any]:
    """Pydantic-validate the model output, drop unknown columns, enforce the size limits.

    Returns the stored draft: column_tags as {column: tag}, plus `rejected_columns` and
    `repairs` so nothing the profiler dropped goes unnoticed.
    """
    try:
        draft = ContextDraft.model_validate(raw)
    except ValidationError as exc:
        raise LLMError(f"context draft does not match the schema: {exc}") from exc
    known = set(columns)
    rejected = sorted({c for c in draft.critical_columns if c not in known}
                      | {t.column for t in draft.column_tags if t.column not in known})  # fmt: skip
    repairs = []
    words = draft.summary.split()
    if len(words) > SUMMARY_WORDS:
        repairs.append(f"summary cut from {len(words)} to {SUMMARY_WORDS} words")
    if len(draft.likely_metrics) > MAX_METRICS:
        repairs.append(f"likely_metrics cut from {len(draft.likely_metrics)} to {MAX_METRICS}")
    critical = [c for c in dict.fromkeys(draft.critical_columns) if c in known]
    tags = {t.column: t.tag for t in draft.column_tags if t.column in known}
    return {
        "domain": draft.domain.strip(),
        "summary": " ".join(words[:SUMMARY_WORDS]),
        "likely_metrics": draft.likely_metrics[:MAX_METRICS],
        "critical_columns": critical,
        "column_tags": {c: tags[c] for c in columns if c in tags},  # header order
        "rejected_columns": rejected,
        "repairs": repairs,
    }


def render_text(draft: dict[str, Any]) -> str:
    metrics = "; ".join(draft["likely_metrics"]) or "-"
    critical = ", ".join(draft["critical_columns"]) or "-"
    return f"{draft['domain']}. {draft['summary']}\nLikely metrics: {metrics}\nCritical columns: {critical}"


# --- prompt inputs -----------------------------------------------------------------


def _pii_columns(profiles: list[dict[str, Any]]) -> set[str]:
    return {
        p["column"] for p in profiles
        if p["pii_hints"]["email_share"] or p["pii_hints"]["phone_share"] or p["pii_hints"]["name_matches"]
    }  # fmt: skip


def sample_rows(run_id: uuid.UUID, columns: list[str], pii: set[str]) -> list[dict[str, str]]:
    """3 deterministic ok rows (ordered by a hash, not file position); PII columns masked to patterns."""
    with transaction() as conn:
        rows = conn.execute(
            "SELECT payload FROM bronze.records WHERE run_id = %s AND parse_status = 'ok'"
            " ORDER BY md5(row_hash || run_id::text) LIMIT %s",
            (run_id, SAMPLE_ROWS),
        ).fetchall()

    def show(column: str, value: Any) -> str:
        text = "" if value is None else str(value)[:SAMPLE_CHARS]
        return f"<masked: {mask(text)}>" if column in pii and text.strip() else text

    return [{c: show(c, payload.get(c)) for c in columns} for (payload,) in rows]


def load_prompt(version: str) -> tuple[str, string.Template]:
    """Versioned prompt file: system text, a `<!-- user -->` line, then the user template."""
    system, user = (settings.prompts_dir / f"{version}.md").read_text().split("<!-- user -->\n", 1)
    return system.strip(), string.Template(user)


def build_messages(run_id: uuid.UUID) -> tuple[str, list[dict[str, Any]], list[str]]:
    """System prompt, messages and the run's column names, from the stored profile only."""
    profile = load_profile(run_id)
    profiles = profile["columns"]
    columns = [p["column"] for p in profiles]
    lines = [
        f"- {p['column']} | {p['cardinality']['role_hint']} | {p['type_inference']['dominant_type']}"
        for p in profiles
    ]
    samples = sample_rows(run_id, columns, _pii_columns(profiles))
    system, template = load_prompt(PROMPT_VERSION)
    user = template.substitute(
        row_count=profile["summary"]["rows_ok"],
        column_count=len(columns),
        columns="\n".join(lines),
        samples="\n".join(json.dumps(row, ensure_ascii=False) for row in samples),
    )
    return system, [{"role": "user", "content": user}], columns


# --- storage + entry point ----------------------------------------------------------


def get_context(run_id: uuid.UUID) -> dict[str, Any] | None:
    with transaction() as conn:
        row = conn.execute(
            "SELECT text, draft, source, approved, llm_call_id FROM meta.dataset_context WHERE run_id = %s", (run_id,)
        ).fetchone()
    if not row:
        return None
    return dict(zip(("text", "draft", "source", "approved", "llm_call_id"), row, strict=True))


def _save(run_id: uuid.UUID, text: str, draft: dict[str, Any] | None, source: str, approved: bool,
          llm_call_id: uuid.UUID | None) -> None:  # fmt: skip
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.dataset_context (run_id, text, draft, source, approved, llm_call_id)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id) DO UPDATE SET text = EXCLUDED.text, draft = EXCLUDED.draft,"
            " source = EXCLUDED.source, approved = EXCLUDED.approved, llm_call_id = EXCLUDED.llm_call_id,"
            " created_at = now()",
            (run_id, text, Jsonb(draft) if draft is not None else None, source, approved, llm_call_id),
        )


def _human_draft(text: str, columns: list[str]) -> dict[str, Any] | None:
    """A human file may also be JSON in the draft shape; then it gets the same validation."""
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(raw, dict) and isinstance(raw.get("column_tags"), dict):
        raw = raw | {"column_tags": [{"column": c, "tag": t} for c, t in raw["column_tags"].items()]}
    return validate_draft(raw, columns)


def ensure_context(run_id: uuid.UUID, human_text: str | None = None, client: Any = None) -> dict[str, Any]:
    """Human text -> stored as approved (no LLM call). Otherwise reuse a stored context, or draft one."""
    log = get_logger("context", run_id=str(run_id))
    if human_text is not None:
        columns = [p["column"] for p in load_profile(run_id)["columns"]]
        draft = _human_draft(human_text, columns)
        _save(run_id, render_text(draft) if draft else human_text.strip(), draft, "human", True, None)
        log.info("context stored", source="human", structured=draft is not None)
        return get_context(run_id)  # type: ignore[return-value]
    if existing := get_context(run_id):
        return existing
    system, messages, columns = build_messages(run_id)
    result = call_llm("context_draft", settings.cheap_model, messages, PROMPT_VERSION, run_id,
                      json_schema=draft_schema(columns), system=system, effort="low", client=client)  # fmt: skip
    draft = validate_draft(result.data, columns)
    if draft["rejected_columns"] or draft["repairs"]:
        log.warning("context draft repaired", rejected=draft["rejected_columns"], repairs=draft["repairs"])
    _save(run_id, render_text(draft), draft, "inferred", False, result.call_id)
    log.info("context stored", source="inferred", cost_usd=str(result.cost_usd), cache_hit=result.cache_hit)
    return get_context(run_id)  # type: ignore[return-value]


def approve_context(run_id: uuid.UUID) -> bool:
    with transaction() as conn:
        updated = conn.execute("UPDATE meta.dataset_context SET approved = true WHERE run_id = %s", (run_id,)).rowcount
    return bool(updated)
