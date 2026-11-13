"""The one masking function every tool output goes through (policy lives in pipeline.privacy)."""

from typing import Any

from pipeline.privacy import column_policy, is_vocabulary
from pipeline.profile_columns import mask


def mask_value(value: Any, context_tags: dict[str, str], profile: dict[str, Any]) -> Any:
    """`value` as a tool may show it. Masked columns (person/contact_pii tags, PII hints, or
    non-label-like high-cardinality data) become `<pattern>(length)`; profiler vocabulary
    (null tokens, sentinels, ...) and non-strings pass through."""
    if not isinstance(value, str) or is_vocabulary(value):
        return value
    if column_policy(profile, context_tags) == "raw":
        return value[:100]
    return f"<{mask(value.strip())}>({len(value)})"
