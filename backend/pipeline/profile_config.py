"""Every profiler threshold and token list, in one place.

The whole config (and a short hash of it) is written into each dataset summary, so two
profiles can only be compared when they were produced with the same rules.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

PROFILER_VERSION = "1.3"


@dataclass(frozen=True)
class ProfileConfig:
    # --- tokens ---------------------------------------------------------------
    # Matched case-insensitively after strip(). Values made only of punctuation/symbols
    # ("???", "--", "**") are null-like too, by rule rather than by list.
    null_like_tokens: tuple[str, ...] = (
        "null", "none", "nil", "nan", "na", "n/a", "n.a.", "n.a", "n/a.", "#n/a", "<na>",
        "(null)", "<null>", "[null]", "(none)", "missing", "undefined", "unknown",
        "tbd", "tba", "not available", "not applicable", "empty", "(empty)", "blank", "(blank)",
    )  # fmt: skip
    boolean_tokens: tuple[str, ...] = ("true", "false", "yes", "no", "y", "n", "t", "f")
    boolean_digits: tuple[str, ...] = ("0", "1")  # boolean only when the whole column fits the tokens
    pii_name_words: tuple[str, ...] = ("email", "phone", "name", "address", "ip")

    # --- output size ----------------------------------------------------------
    max_chars: int = 100  # every example/sample value
    max_profile_bytes: int = 2048  # per column profile
    mask_max_chars: int = 40
    top_values: int = 10
    top_masks: int = 5
    samples: int = 5
    examples: int = 3  # examples per finding

    # --- types and sections ---------------------------------------------------
    dominant_threshold: float = 0.9  # a type (or text) must reach this share to dominate
    section_threshold: float = 0.3  # date/numeric section when that type's share reaches this
    leading_zero_identifier_share: float = 0.1  # of numeric values -> "identifier", not a number
    epoch_range: tuple[int, int] = (631_152_000, 4_102_444_800)  # 1990-01-01 .. 2100-01-01 (seconds)
    excel_serial_range: tuple[int, int] = (20_000, 60_000)  # ~1954 .. ~2064, date-like columns only
    plausible_min_year: int = 1990

    # --- roles ----------------------------------------------------------------
    free_text_min_words: float = 5
    free_text_min_ratio: float = 0.5
    candidate_key_ratio: float = 0.95
    categorical_max_distinct: int = 50
    iqr_multiplier: float = 1.5

    # --- sentinels and spikes -------------------------------------------------
    date_sentinels: tuple[str, ...] = ("1900-01-01", "1970-01-01", "0000-00-00", "9999-12-31", "2099-12-31")
    numeric_sentinels: tuple[float, ...] = (-1, -99, -999, -9999, 99, 999, 9999, 99999)
    spike_zero: bool = True  # 0 is a sentinel only when it also passes the spike test
    spike_min_distinct: int = 20
    spike_max_distinct_ratio: float = 0.05  # below this the column is label-like: repeats are normal
    spike_min_share: float = 0.05
    spike_median_multiple: float = 10
    spike_max_values: int = 2  # more spiking values = a templated column, not a placeholder
    sentinel_high_share: float = 0.05
    sentinels_reported: int = 5

    # --- normalization variants and typos -------------------------------------
    variant_groups_reported: int = 5
    variant_members_reported: int = 5
    typo_max_distinct: int = 200  # normalized distinct values; above this the column isn't label-like
    typo_rare_share: float = 0.01
    typo_min_ratio: float = 0.85  # difflib SequenceMatcher
    typo_min_length: int = 4
    typo_max_pairs: int = 10

    # --- cross-column and dataset ---------------------------------------------
    key_ratio: float = 0.8
    key_pattern_share: float = 0.9
    partial_row_missing_share: float = 0.5
    # KEY_WITH_REPEATS hint: without a context tag, a categorical column with at most this many
    # values counts as status-like (its changes look like ticket/order history, not conflicts)
    status_like_max_distinct: int = 10
    currency_symbols: str = "$₹€£¥"
    currency_codes: tuple[str, ...] = ("usd", "eur", "inr", "gbp", "jpy")

    # --- issue severities -----------------------------------------------------
    high_empty_rate: float = 0.2
    very_high_empty_rate: float = 0.5
    label_like_ratio: float = 0.05
    high_share: float = 0.01  # malformed rows, date order, invalid dates: "high" above this
    duplicate_high_share: float = 0.05
    partial_rows_high_share: float = 0.05
    many_date_formats: int = 3


PROFILE_CONFIG = ProfileConfig()


def config_dict(config: ProfileConfig = PROFILE_CONFIG) -> dict[str, Any]:
    return asdict(config)


def config_hash(config: ProfileConfig = PROFILE_CONFIG) -> str:
    """Short, stable fingerprint of the rules (version included)."""
    payload = json.dumps({"version": PROFILER_VERSION, **config_dict(config)}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:12]
