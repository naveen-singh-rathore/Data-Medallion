"""Dataset card: a deterministic, budget-aware text index of a profiled run (0 LLM tokens).

The card is what the agent reads first on every turn; details stay behind tools
(get_column_profile, list_issues). Tiers are filled in order until the token budget:

- Tier 0 (never dropped): context, dataset header, ALL high issues, critical columns
- Tier 1: one line per flagged column
- Tier 2: medium issues
- Tier 3: low issues, then clean columns

A tier that doesn't fit is compressed: as many lines as fit, then one pointer line that
counts the rest. Room for every later tier's pointer is reserved up front, so every
column and every issue is either shown or counted - never silently omitted.
Tokens are estimated as ceil(chars / 4).
"""

import hashlib
import math
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from pipeline.config import settings
from pipeline.db import transaction
from pipeline.logging_setup import get_logger
from pipeline.profile_issues import SEVERITY_RANK

RENDERER_VERSION = "1.0"
EVIDENCE_CHARS = 140
POINTER_NAMES = 3


@dataclass(frozen=True)
class CardInputs:
    run_id: str
    source_file: str
    context: dict[str, Any] | None  # meta.dataset_context row
    summary: dict[str, Any]
    issues: list[dict[str, Any]]
    columns: list[dict[str, Any]]  # column profiles in header order


@dataclass(frozen=True)
class CardResult:
    text: str
    card_hash: str
    metrics: dict[str, Any]


def estimate_tokens(text: str) -> int:
    return math.ceil(len(text) / 4)


# --- line formatting -----------------------------------------------------------------


def _value(v: Any) -> str:
    if isinstance(v, dict):  # up to 4 keys: jsonb reorders keys, so a smaller cut can hide the key fact
        items = list(v.items())
        more = ", …" if len(items) > 4 else ""
        return "{" + ", ".join(f"{k}: {_value(x)}" for k, x in items[:4]) + more + "}"
    if isinstance(v, list):
        return "[" + ", ".join(_value(x) for x in v[:2]) + (", …" if len(v) > 2 else "") + "]"
    if isinstance(v, float):
        return f"{v:.4g}"
    if isinstance(v, str):
        return repr(v[:40] + ("…" if len(v) > 40 else ""))
    return str(v)


def short_evidence(evidence: dict[str, Any]) -> str:
    text = " ".join(f"{k}={_value(v)}" for k, v in evidence.items() if v not in ({}, [], None))
    return text if len(text) <= EVIDENCE_CHARS else text[: EVIDENCE_CHARS - 1] + "…"


def issue_line(issue: dict[str, Any]) -> str:
    return f"- {issue['code']} {issue['column'] or '(dataset)'}: {short_evidence(issue['evidence'])}"


def _issue_columns(issue: dict[str, Any]) -> list[str]:
    return issue["column"].split(" > ") if issue["column"] else []


def column_line(p: dict[str, Any], codes: list[str], tag: str | None) -> str:
    t = p["type_inference"]
    parts = [
        p["column"],
        f"{t['dominant_type']}@{t['confidence']:.0%}",
        f"missing {p['completeness']['empty_or_null_like_share']:.1%}",
        f"{p['cardinality']['distinct']} distinct",
        p["cardinality"]["role_hint"],
    ]
    if tag:
        parts.append(f"tag={tag}")
    parts.append(",".join(codes) if codes else "clean")
    return "- " + " | ".join(parts)


def context_lines(context: dict[str, Any] | None) -> list[str]:
    if not context:
        return ["Context: none (run `context` to add or draft one)"]
    if context["source"] == "human":
        label = "human"
    else:
        label = "inferred, approved" if context["approved"] else "inferred, UNAPPROVED - verify before relying on it"
    return [f"Context ({label}):", *(f"  {line}" for line in context["text"].splitlines() if line.strip())]


def header_lines(inputs: CardInputs) -> list[str]:
    s = inputs.summary
    partial = s.get("partial_rows", {}).get("count", 0)
    return [
        f"# Dataset card: {inputs.source_file} (run {inputs.run_id})",
        (f"Rows: {s['rows_ok']} ok, {s['rows_malformed']} malformed | duplicates: {s['exact_duplicate_rows']} exact,"
        f" {s['near_duplicate_rows']} near | {partial} partial rows | {s['columns']} columns |"
        f" {len(inputs.issues)} issues"),
        "Details on demand: get_column_profile(name), list_issues(severity=...)",
    ]


# --- pointer lines ---------------------------------------------------------------------


def _names(names: list[str]) -> str:
    return ", ".join(names[:POINTER_NAMES]) + ("…" if len(names) > POINTER_NAMES else "")


def column_pointer(kind: str) -> Callable[[list[Any]], str]:
    def pointer(rest: list[Any]) -> str:
        names = [p["column"] for p, _ in rest]
        more = "more " if kind == "flagged" else ""
        return f"- {len(rest)} {more}{kind} columns: {_names(names)} → get_column_profile(name)"

    return pointer


def issue_pointer(severity: str) -> Callable[[list[Any]], str]:
    def pointer(rest: list[Any]) -> str:
        columns = {c for issue in rest for c in _issue_columns(issue)}
        return f"- {len(rest)} more {severity} issues across {len(columns)} columns → list_issues(severity='{severity}')"

    return pointer


# --- assembly --------------------------------------------------------------------------


@dataclass
class _Section:
    key: str  # metrics name for the compressed count
    tier: str
    heading: str
    items: list[Any]
    render: Callable[[Any], str]
    pointer: Callable[[list[Any]], str]

    def min_chars(self) -> int:
        """Cost when fully compressed: heading + one pointer line."""
        return len(self.heading) + 1 + len(self.pointer(self.items)) + 1 if self.items else 0


def _fill(section: _Section, room: int) -> tuple[list[str], int]:
    """Lines for a section within `room` chars: as many items as fit, then a pointer for the rest."""
    lines = [section.heading]
    used = len(section.heading) + 1
    for i, item in enumerate(section.items):
        line = section.render(item)
        rest = section.items[i + 1 :]
        tail = len(section.pointer(rest)) + 1 if rest else 0
        if used + len(line) + 1 + tail > room:
            return [*lines, section.pointer(section.items[i:])], len(section.items) - i
        lines.append(line)
        used += len(line) + 1
    return lines, 0


def build_card(inputs: CardInputs, budget_tokens: int) -> CardResult:
    """Render the card for `inputs` within `budget_tokens` (pure; same input, same text)."""
    draft = (inputs.context or {}).get("draft") or {}
    tags = draft.get("column_tags", {})
    critical = [c for c in draft.get("critical_columns", []) if any(p["column"] == c for p in inputs.columns)]
    codes: dict[str, list[str]] = {p["column"]: [] for p in inputs.columns}
    for issue in sorted(inputs.issues, key=lambda i: (SEVERITY_RANK[i["severity"]], i["code"])):
        for column in _issue_columns(issue):
            if column in codes and issue["code"] not in codes[column]:
                codes[column].append(issue["code"])
    by_severity = {s: [i for i in inputs.issues if i["severity"] == s] for s in SEVERITY_RANK}
    critical_profiles = [(p, codes[p["column"]]) for c in critical for p in inputs.columns if p["column"] == c]
    others = [(p, codes[p["column"]]) for p in inputs.columns if p["column"] not in critical]
    flagged = [(p, c) for p, c in others if c]
    clean = [(p, c) for p, c in others if not c]

    def render_column(item: Any) -> str:
        return column_line(item[0], item[1], tags.get(item[0]["column"]))

    tier0 = [*header_lines(inputs), *context_lines(inputs.context)]
    if by_severity["high"]:
        tier0 += [f"## High issues ({len(by_severity['high'])})", *map(issue_line, by_severity["high"])]
    if critical_profiles:
        tier0 += ["## Critical columns (from context)", *map(render_column, critical_profiles)]
    sections = [
        _Section("flagged_columns", "tier1", f"## Flagged columns ({len(flagged)}): name | type@confidence | missing |"
                 " distinct | role | issues", flagged, render_column, column_pointer("flagged")),
        _Section("medium_issues", "tier2", f"## Medium issues ({len(by_severity['medium'])})", by_severity["medium"],
                 issue_line, issue_pointer("medium")),
        _Section("low_issues", "tier3", f"## Low issues ({len(by_severity['low'])})", by_severity["low"],
                 issue_line, issue_pointer("low")),
        _Section("clean_columns", "tier3", f"## Clean columns ({len(clean)})", clean, render_column,
                 column_pointer("clean")),
    ]  # fmt: skip

    budget_chars = budget_tokens * 4
    lines = list(tier0)
    used = sum(len(line) + 1 for line in lines)
    over_budget = used + sum(s.min_chars() for s in sections) > budget_chars
    if over_budget:
        get_logger("card", run_id=inputs.run_id).warning(
            "tier 0 and pointers exceed the budget; rendering them anyway", budget_tokens=budget_tokens,
            tier0_tokens=math.ceil(used / 4),
        )  # fmt: skip
    compressed: dict[str, int] = {}
    tiers_full = {"tier0": True, "tier1": True, "tier2": True, "tier3": True}
    for i, section in enumerate(sections):
        if not section.items:
            compressed[section.key] = 0
            continue
        reserve = sum(s.min_chars() for s in sections[i + 1 :])
        room = max(budget_chars - used - reserve, 0)
        section_lines, rest = _fill(section, room)
        compressed[section.key] = rest
        tiers_full[section.tier] = tiers_full[section.tier] and rest == 0
        lines += section_lines
        used += sum(len(line) + 1 for line in section_lines)

    text = "\n".join(lines) + "\n"
    shown_issues = len(inputs.issues) - compressed["medium_issues"] - compressed["low_issues"]
    metrics = {
        "renderer_version": RENDERER_VERSION,
        "budget_tokens": budget_tokens,
        "est_tokens": estimate_tokens(text),
        "chars": len(text),
        "over_budget": over_budget,
        "tiers_full": tiers_full,
        "compressed_counts": compressed,
        "columns_total": len(inputs.columns),
        "columns_flagged": sum(1 for c in codes.values() if c),
        "columns_shown": len(inputs.columns) - compressed["flagged_columns"] - compressed["clean_columns"],
        "issues_total": len(inputs.issues),
        "issues_shown": shown_issues,
    }
    card_hash = hashlib.sha256(f"{RENDERER_VERSION}\n{text}".encode()).hexdigest()[:16]
    return CardResult(text=text, card_hash=card_hash, metrics=metrics)


# --- DB entry point --------------------------------------------------------------------


def load_inputs(run_id: uuid.UUID) -> CardInputs:
    from pipeline.context import get_context  # local: context imports the LLM client
    from pipeline.profiling import load_profile

    profile = load_profile(run_id)
    with transaction() as conn:
        (source_file,) = conn.execute(
            "SELECT source_file FROM meta.ingestion_runs WHERE run_id = %s", (run_id,)
        ).fetchone()  # type: ignore[misc]
    return CardInputs(str(run_id), source_file, get_context(run_id), profile["summary"], profile["issues"],
                      profile["columns"])  # fmt: skip


def render_card(run_id: uuid.UUID, budget_tokens: int | None = None, exact: bool = False) -> CardResult:
    """Render, (optionally) count exact tokens, and store the card. Same text -> same row."""
    budget = budget_tokens or settings.card_budget_tokens
    card = build_card(load_inputs(run_id), budget)
    if exact:
        card.metrics["exact_tokens"] = _exact_tokens(card.text, run_id)
    m = card.metrics
    with transaction() as conn:
        conn.execute(
            "INSERT INTO meta.dataset_cards (run_id, card_hash, renderer_version, budget_tokens, est_tokens,"
            " exact_tokens, chars, tiers_full, compressed_counts, columns_total, columns_flagged, issues_total,"
            " issues_shown, over_budget, card_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_id, card_hash) DO UPDATE SET"
            " exact_tokens = COALESCE(EXCLUDED.exact_tokens, meta.dataset_cards.exact_tokens)",
            (run_id, card.card_hash, RENDERER_VERSION, budget, m["est_tokens"], m.get("exact_tokens"), m["chars"],
             Jsonb(m["tiers_full"]), Jsonb(m["compressed_counts"]), m["columns_total"], m["columns_flagged"],
             m["issues_total"], m["issues_shown"], m["over_budget"], card.text),
        )  # fmt: skip
    get_logger("card", run_id=str(run_id)).info(
        "card stored", card_hash=card.card_hash, est_tokens=m["est_tokens"], budget_tokens=budget,
        issues_shown=m["issues_shown"], issues_total=m["issues_total"], over_budget=m["over_budget"],
    )  # fmt: skip
    return card


def _exact_tokens(text: str, run_id: uuid.UUID) -> int | None:
    """Provider count for the card as the main model would see it; None if no key/offline."""
    from pipeline.llm import count_tokens

    try:
        return count_tokens(text, settings.main_model)
    except Exception as exc:  # noqa: BLE001 - optional metric, never fails the card
        get_logger("card", run_id=str(run_id)).warning("exact token count unavailable", error=str(exc)[:200])
        return None


def latest_card(run_id: uuid.UUID) -> dict[str, Any] | None:
    with transaction() as conn:
        cur = conn.execute(
            "SELECT * FROM meta.dataset_cards WHERE run_id = %s ORDER BY created_at DESC LIMIT 1", (run_id,)
        )
        row = cur.fetchone()
        return dict(zip([d.name for d in cur.description], row, strict=True)) if row else None  # type: ignore[union-attr]
