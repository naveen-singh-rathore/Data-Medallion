"""Data minimization for everything sent to an LLM: prompts, the dataset card, agent tool output.

One policy per column, decided from the profile (and the business context, when it exists):
- "raw":  label-like and not sensitive - categorical role, constant, boolean-like, or few
          distinct values relative to rows (<= label_like_ratio). Values may be shown.
- "mask": everything else. String values are replaced by their pattern, e.g. "<A A>",
          "<9999-99-99>" (A = letter run, 9 = digit). Aggregates (counts, shares, min/max)
          and numbers are kept.
Sensitive columns are always masked: tagged person/contact_pii in the context, or with
profiler PII hints (email/phone values, or a PII word in the column name).

Known vocabulary is never masked: null-like tokens, punctuation-only placeholders, known
sentinels, date-format labels, type names and the profiler's own markers ("<empty>").
"""

from typing import Any

from pipeline.profile_columns import TYPES, mask
from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profile_dates import DATE_FORMATS
from pipeline.profile_numbers import KIND_PATTERNS
from pipeline.profile_values import NULL_LIKE, is_punctuation_only, number_label

SENSITIVE_TAGS = frozenset({"person", "contact_pii"})
LABEL_ROLES = frozenset({"categorical", "constant"})
VOCABULARY = frozenset(
    {*NULL_LIKE, *CFG.date_sentinels, *(number_label(float(n)) for n in CFG.numeric_sentinels), "0",
     *(f.label for f in DATE_FORMATS), *(f.day_first_label for f in DATE_FORMATS if f.day_first_label),
     *TYPES, "text", "mixed", "identifier", "known", "spike",
     "exact_duplicate", "update_history", "conflicting", *KIND_PATTERNS}
)  # fmt: skip


def context_tags(context: dict[str, Any] | None) -> dict[str, str]:
    return ((context or {}).get("draft") or {}).get("column_tags", {})


def is_sensitive(profile: dict[str, Any], tags: dict[str, str]) -> bool:
    pii = profile["pii_hints"]
    return tags.get(profile["column"]) in SENSITIVE_TAGS or bool(pii["email_share"] or pii["phone_share"] or pii["name_matches"])


def is_label_like(profile: dict[str, Any]) -> bool:
    card = profile["cardinality"]
    return (card["role_hint"] in LABEL_ROLES or profile["type_inference"].get("boolean_candidate", False)
            or card["distinct_ratio"] <= CFG.label_like_ratio)  # fmt: skip


def column_policy(profile: dict[str, Any], tags: dict[str, str]) -> str:
    return "raw" if is_label_like(profile) and not is_sensitive(profile, tags) else "mask"


def column_policies(profiles: list[dict[str, Any]], context: dict[str, Any] | None) -> dict[str, str]:
    tags = context_tags(context)
    return {p["column"]: column_policy(p, tags) for p in profiles}


def is_vocabulary(value: str) -> bool:
    stripped = value.strip()
    return (stripped.lower() in VOCABULARY or stripped in VOCABULARY or is_punctuation_only(stripped)
            or (stripped.startswith("<") and stripped.endswith(">")) or stripped == "")  # fmt: skip


def mask_value(value: str) -> str:
    return f"<{mask(value.strip())}>"


def minimize(value: Any, policy: str) -> Any:
    """One value as it may be shown under `policy` (strings only; other types pass through)."""
    if not isinstance(value, str) or policy == "raw" or is_vocabulary(value):
        return value
    return mask_value(value)


def redact(obj: Any, policy: str, policies: dict[str, str]) -> Any:
    """Apply `policy` to every string leaf; a dict key that is a column name switches to that
    column's policy for its subtree (e.g. the `differs` map of a key-repeat example)."""
    if isinstance(obj, dict):
        return {k: redact(v, policies.get(k, policy) if isinstance(k, str) else policy, policies) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [redact(v, policy, policies) for v in obj]
    return minimize(obj, policy)


def issue_policy(issue: dict[str, Any], policies: dict[str, str]) -> str:
    """Masked if any column the issue is about is masked; dataset-level issues hold no raw values."""
    columns = issue["column"].split(" > ") if issue["column"] else []
    return "mask" if any(policies.get(c, "mask") == "mask" for c in columns) else "raw"


def redact_issue(issue: dict[str, Any], policies: dict[str, str]) -> dict[str, Any]:
    return issue | {"evidence": redact(issue["evidence"], issue_policy(issue, policies), policies)}
