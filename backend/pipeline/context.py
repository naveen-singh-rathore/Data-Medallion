"""Business context for a run: written by a human, or drafted by ONE cheap LLM call.

Both can be stored side by side (one row per source); the human row is the one used.
Human context costs nothing. Otherwise the cheap model sees column names, roles, types and
the row count, plus values only for label-like, non-sensitive columns (pipeline.privacy);
every other column is a pattern + length range. Both are in use as soon as they are stored
(`approved = true`): nobody has to sign off the AI's draft, and a human text, when given,
still wins. `approve_context` only remains for drafts stored unapproved by older versions.
"""

import json
import re
import string
import uuid
from typing import Any, Literal

from psycopg import sql
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, ValidationError

from pipeline.config import settings
from pipeline.db import column_list, transaction
from pipeline.llm import LLMError, call_llm
from pipeline.logging_setup import get_logger
from pipeline.privacy import column_policies, minimize
from pipeline.profile_values import is_missing
from pipeline.profiling import load_profile

PROMPT_VERSION = "context_draft_v2"
SAMPLE_ROWS = 3
SAMPLE_CHARS = 60
TOP_VALUES = 5
SUMMARY_WORDS = 60
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


def fit_summary(text: str, limit: int = SUMMARY_WORDS) -> str | None:
    """The longest run of whole sentences within `limit` words; None if even the first is too long."""
    kept: list[str] = []
    for sentence in re.split(r"(?<=[.!?])\s+", text.strip()):
        if len(" ".join([*kept, sentence]).split()) > limit:
            break
        kept.append(sentence)
    return " ".join(kept) or None


def validate_draft(raw: Any, columns: list[str]) -> dict[str, Any]:
    """Pydantic-validate the model output, drop unknown columns, enforce the size limits.

    Returns the stored draft: column_tags as {column: tag}, plus `rejected_columns` and
    `repairs` so nothing that was changed goes unnoticed. A summary over the word limit is
    cut at the last full sentence; if no sentence fits, `summary` is None (caller retries).
    """
    try:
        draft = ContextDraft.model_validate(raw)
    except ValidationError as exc:
        raise LLMError(f"context draft does not match the schema: {exc}") from exc
    known = set(columns)
    rejected = sorted({c for c in draft.critical_columns if c not in known}
                      | {t.column for t in draft.column_tags if t.column not in known})  # fmt: skip
    repairs = []
    words = len(draft.summary.split())
    summary: str | None = draft.summary.strip()
    if words > SUMMARY_WORDS:
        summary = fit_summary(draft.summary)
        if summary:
            repairs.append(f"summary cut from {words} to {len(summary.split())} words at a sentence end")
    if len(draft.likely_metrics) > MAX_METRICS:
        repairs.append(f"likely_metrics cut from {len(draft.likely_metrics)} to {MAX_METRICS}")
    critical = [c for c in dict.fromkeys(draft.critical_columns) if c in known]
    tags = {t.column: t.tag for t in draft.column_tags if t.column in known}
    return {
        "domain": draft.domain.strip(),
        "summary": summary,
        "likely_metrics": draft.likely_metrics[:MAX_METRICS],
        "critical_columns": critical,
        "column_tags": {c: tags[c] for c in columns if c in tags},  # header order
        "rejected_columns": rejected,
        "repairs": repairs,
    }


def render_text(draft: dict[str, Any]) -> str:
    """Prose shown on the card: domain, summary, metrics (critical columns have their own section)."""
    lines = [f"{draft['domain']}. {draft.get('summary') or ''}".strip()]
    if draft.get("likely_metrics"):
        lines.append("Likely metrics: " + "; ".join(draft["likely_metrics"]))
    return "\n".join(lines)


# --- prompt inputs (minimized) -------------------------------------------------------


def _column_line(p: dict[str, Any], policy: str) -> str:
    head = f"- {p['column']} | {p['cardinality']['role_hint']} | {p['type_inference']['dominant_type']}"
    if policy == "raw":
        values = [v for v, _ in p["frequency"]["top_values"] if not is_missing(v)][:TOP_VALUES]
        return f"{head} | values: " + ", ".join(json.dumps(v[:SAMPLE_CHARS], ensure_ascii=False) for v in values)
    shape = p["shape"]
    masks = [m for m, _ in p["patterns"]["top_masks"][:2]]
    return f"{head} | pattern: {' or '.join(masks) or '-'} (length {shape['min_length']}-{shape['max_length']})"


def sample_rows(run_id: uuid.UUID, columns: list[str], policies: dict[str, str]) -> list[dict[str, str]]:
    """3 deterministic ok rows (ordered by a hash, not file position), minimized per column."""
    with transaction() as conn:
        rows = conn.execute(
            "SELECT payload FROM bronze.records WHERE run_id = %s AND parse_status = 'ok'"
            " ORDER BY md5(row_hash || run_id::text) LIMIT %s",
            (run_id, SAMPLE_ROWS),
        ).fetchall()

    def show(column: str, value: Any) -> str:
        text = "" if value is None else str(value)[:SAMPLE_CHARS]
        return minimize(text, policies[column])

    return [{c: show(c, payload.get(c)) for c in columns} for (payload,) in rows]


def load_prompt(version: str) -> tuple[str, string.Template]:
    """Versioned prompt file: system text, a `<!-- user -->` line, then the user template."""
    system, user = (settings.prompts_dir / f"{version}.md").read_text().split("<!-- user -->\n", 1)
    return system.strip(), string.Template(user)


def build_messages(run_id: uuid.UUID) -> tuple[str, list[dict[str, Any]], list[str]]:
    """System prompt, messages and column names. Uses any stored context's tags for masking."""
    profile = load_profile(run_id)
    profiles = profile["columns"]
    columns = [p["column"] for p in profiles]
    policies = column_policies(profiles, get_context(run_id))
    system, template = load_prompt(PROMPT_VERSION)
    user = template.substitute(
        row_count=profile["summary"]["rows_ok"],
        column_count=len(columns),
        columns="\n".join(_column_line(p, policies[p["column"]]) for p in profiles),
        samples="\n".join(json.dumps(row, ensure_ascii=False) for row in sample_rows(run_id, columns, policies)),
    )
    return system, [{"role": "user", "content": user}], columns


# --- human context file ---------------------------------------------------------------

_LABEL = re.compile(r"^\s*(domain|critical columns|tags)\s*:\s*(.*)$", re.IGNORECASE)


def parse_human(text: str, columns: list[str]) -> dict[str, Any] | None:
    """Optional structure in a human file: JSON in the draft shape, or `Domain:`,
    `Critical columns: a, b` and `Tags: col=tag, ...` lines among free text."""
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        raw = None
    if isinstance(raw, dict):
        if isinstance(raw.get("column_tags"), dict):
            raw = raw | {"column_tags": [{"column": c, "tag": t} for c, t in raw["column_tags"].items()]}
        return validate_draft(raw, columns)
    fields: dict[str, str] = {}
    prose = []
    for line in text.splitlines():
        if match := _LABEL.match(line):
            fields[match.group(1).lower()] = match.group(2).strip()
        elif line.strip() and not line.lstrip().startswith("#"):
            prose.append(line.strip())
    if not fields:
        return None
    split = [s.strip() for s in fields.get("critical columns", "").split(",") if s.strip()]
    tag_pairs = [t.split("=", 1) for t in fields.get("tags", "").split(",") if "=" in t]
    draft = validate_draft({"domain": fields.get("domain", ""), "summary": " ".join(prose), "likely_metrics": [],
                            "critical_columns": split,
                            "column_tags": [{"column": c.strip(), "tag": t.strip()} for c, t in tag_pairs]},
                           columns)  # fmt: skip
    draft["summary"] = " ".join(prose)  # a human's words are not cut
    return draft


# --- storage + entry point ----------------------------------------------------------

_FIELDS = ("text", "draft", "source", "approved", "llm_call_id")


def get_context(run_id: uuid.UUID, source: str | None = None) -> dict[str, Any] | None:
    """The context to use (human over inferred), or the row for one `source`."""
    with transaction() as conn:
        query = sql.SQL(
            "SELECT {} FROM meta.dataset_context WHERE run_id = %s"
            " AND (%s::text IS NULL OR source = %s) ORDER BY source = 'human' DESC LIMIT 1"
        ).format(column_list(_FIELDS))
        row = conn.execute(query, (run_id, source, source)).fetchone()
    return dict(zip(_FIELDS, row, strict=True)) if row else None


def _save(run_id: uuid.UUID, text: str, draft: dict[str, Any] | None, source: str, approved: bool,
          llm_call_id: uuid.UUID | None) -> None:  # fmt: skip
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.dataset_context (run_id, text, draft, source, approved, llm_call_id)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id, source) DO UPDATE SET text = EXCLUDED.text, draft = EXCLUDED.draft,"
            " approved = EXCLUDED.approved, llm_call_id = EXCLUDED.llm_call_id, created_at = now()",
            (run_id, text, Jsonb(draft) if draft is not None else None, source, approved, llm_call_id),
        )


def _draft_with_llm(run_id: uuid.UUID, client: Any) -> tuple[dict[str, Any], uuid.UUID]:
    """One cheap-model call; if no whole sentence of the summary fits, retry once with a reminder."""
    system, messages, columns = build_messages(run_id)
    schema = draft_schema(columns)

    def ask(msgs: list[dict[str, Any]]) -> Any:
        return call_llm("context_draft", settings.cheap_model, msgs, PROMPT_VERSION, run_id, json_schema=schema,
                        system=system, effort="low", client=client)  # fmt: skip

    result = ask(messages)
    draft = validate_draft(result.data, columns)
    if draft["summary"] is None:
        words = len(result.data["summary"].split())
        retry = [*messages, {"role": "assistant", "content": result.text},
                 {"role": "user", "content": f"The summary has {words} words. Return the same JSON with a summary "
                  f"of at most {SUMMARY_WORDS} words in complete sentences."}]  # fmt: skip
        result = ask(retry)
        draft = validate_draft(result.data, columns)
        draft["repairs"].append("summary too long: retried once")
        if draft["summary"] is None:  # still no whole sentence fits: last resort, cut words
            draft["summary"] = " ".join(result.data["summary"].split()[:SUMMARY_WORDS]) + " …"
            draft["repairs"].append("summary cut mid-sentence after retry")
    return draft, result.call_id


def ensure_context(
    run_id: uuid.UUID, human_text: str | None = None, client: Any = None, redraft: bool = False
) -> dict[str, Any]:
    """Human text -> stored as approved (no LLM call; an inferred draft is kept for comparison).
    Otherwise reuse a stored context, or draft one (`redraft=True` replaces the inferred row)."""
    log = get_logger("context", run_id=str(run_id))
    if human_text is not None:
        columns = [p["column"] for p in load_profile(run_id)["columns"]]
        draft = parse_human(human_text, columns)
        _save(run_id, human_text.strip(), draft, "human", True, None)
        log.info("context stored", source="human", structured=draft is not None)
        return get_context(run_id)  # type: ignore[return-value]
    if not redraft and (existing := get_context(run_id)):
        return existing
    draft, call_id = _draft_with_llm(run_id, client)
    if draft["rejected_columns"] or draft["repairs"]:
        log.warning("context draft repaired", rejected=draft["rejected_columns"], repairs=draft["repairs"])
    _save(run_id, render_text(draft), draft, "inferred", True, call_id)
    log.info("context stored", source="inferred")
    return get_context(run_id)  # type: ignore[return-value]


def approve_context(run_id: uuid.UUID) -> bool:
    with transaction() as conn:
        updated = conn.execute(
            "UPDATE meta.dataset_context SET approved = true WHERE run_id = %s AND source = 'inferred'", (run_id,)
        ).rowcount
    return bool(updated)


def compare_contexts(run_id: uuid.UUID) -> list[str]:
    """Human vs inferred: domain, critical columns and tag differences, as printable lines."""
    human, inferred = get_context(run_id, "human"), get_context(run_id, "inferred")
    if not human or not inferred:
        return [
            f"need both contexts to compare (human: {'yes' if human else 'no'}, inferred: {'yes' if inferred else 'no'})"
        ]
    h, i = human["draft"] or {}, inferred["draft"] or {}
    hc, ic = h.get("critical_columns", []), i.get("critical_columns", [])
    ht, it = h.get("column_tags", {}), i.get("column_tags", {})
    lines = [f"domain      human: {h.get('domain') or '(not given)'}", f"            inferred: {i.get('domain')}",
             f"critical    both: {', '.join(c for c in hc if c in ic) or '-'}",
             f"            human only: {', '.join(c for c in hc if c not in ic) or '-'}",
             f"            inferred only: {', '.join(c for c in ic if c not in hc) or '-'}"]  # fmt: skip
    differing = [f"{c}: human={ht.get(c, '-')} inferred={it.get(c, '-')}" for c in dict.fromkeys([*ht, *it])
                 if ht.get(c) != it.get(c) and c in ht]  # fmt: skip
    lines.append("tags        " + ("; ".join(differing) if differing else
                                   ("same where both tag a column" if ht else "(human gave no tags)")))  # fmt: skip
    if ht:
        lines.append(f"            tagged by inferred only: {', '.join(c for c in it if c not in ht) or '-'}")
    return lines
