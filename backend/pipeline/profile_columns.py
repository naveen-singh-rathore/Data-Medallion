"""Per-column facts for the profiler: pure functions over one column's raw string values.

Conventions:
- "present" values exist in the payload (absent keys are None and only counted).
- "informative" values are present, not blank and not null-like (token or punctuation-only);
  type, pattern, date and numeric facts are computed on them (whitespace-stripped).
- Type shares are nested: float includes integers, datetime includes date-only values.
- Ordered facts are lists, not objects, because jsonb does not keep key order.
- Every threshold lives in profile_config.PROFILE_CONFIG.
"""

import hashlib
import json
import random
import re
import statistics
from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd

from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profile_dates import (
    ParsedDate,
    date_profile,
    date_sentinel_counts,
    looks_like_date,
    parse_dates,
)
from pipeline.profile_numbers import number_kinds, numeric_after_cleanup
from pipeline.profile_values import (
    characters,
    group_by_key,
    null_like_kind,
    ranked,
    sentinels,
    share,
    truncate,
    typo_pairs,
    variants,
)

TYPES = ("boolean", "integer", "float", "date", "datetime", "email", "url", "phone")  # tie-break order
BOOLEAN_VALUES = frozenset(CFG.boolean_tokens)
BOOLEAN_CANDIDATE_VALUES = BOOLEAN_VALUES | frozenset(CFG.boolean_digits)

_INTEGER = re.compile(r"[+-]?\d+")
_FLOAT = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")
_LEADING_ZERO = re.compile(r"[+-]?0\d+(\.\d*)?")
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}")
_URL = re.compile(r"(https?://|www\.)\S+", re.IGNORECASE)
_PHONE = re.compile(r"(\+\d{1,3}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]\d{4}")
_LETTER_RUN = re.compile(r"[^\W\d_]+")
_DIGIT = re.compile(r"\d")
_DIGIT_RUN = re.compile(r"\d+")


def mask(value: str) -> str:
    """Generic format signature: letter runs -> 'A', each digit -> '9', punctuation kept.

    Letter runs collapse (words vary in length) but digits don't (lengths matter for
    ids and dates): "TKT-0042" -> "A-9999", "15/03/2024" -> "99/99/9999".
    """
    return _DIGIT.sub("9", _LETTER_RUN.sub("A", value))[: CFG.mask_max_chars]


def coarse_mask(value: str) -> str:
    """Like mask() but digit runs collapse too: "TKT-42" and "TKT-12345" -> "A-9"."""
    return _DIGIT_RUN.sub("9", _LETTER_RUN.sub("A", value))


def column_seed(run_id: object, column: str) -> int:
    """Stable per-column seed, so samples are the same on every run of the same data."""
    return int(hashlib.sha256(f"{run_id}:{column}".encode()).hexdigest()[:16], 16)


def profile_size(profile: dict[str, Any]) -> int:
    """Size of a column profile in bytes (UTF-8 JSON)."""
    return len(json.dumps(profile, ensure_ascii=False).encode())


# --- sections ----------------------------------------------------------------


def completeness(values: pd.Series, present: pd.Series, stripped: pd.Series, kinds: pd.Series) -> dict[str, Any]:
    """Absent, empty, whitespace-only and null-like counts and shares."""
    total = len(values)
    absent = int(values.isna().sum())
    empty = int((present == "").sum())
    whitespace_only = int(((present != "") & (stripped == "")).sum())
    tokens = Counter(stripped[kinds == "token"].str.lower())
    punctuation = Counter(stripped[kinds == "punctuation"])
    missing = absent + empty + whitespace_only + tokens.total() + punctuation.total()
    return {
        "total": total,
        "informative": total - missing,
        "empty": empty,
        "whitespace_only": whitespace_only,
        "null_like": dict(ranked(tokens)),
        "punctuation_only": punctuation.total(),
        "punctuation_examples": dict(ranked(punctuation)[: CFG.examples]),
        "absent": absent,
        "empty_or_null_like_share": share(missing, total),
    }


def role_hint(distinct: int, ratio: float, avg_words: float) -> str:
    """constant -> free_text -> candidate_key -> categorical -> other (first match wins).

    free_text is checked before candidate_key: unique sentences are not keys.
    """
    if distinct == 1:
        return "constant"
    if avg_words >= CFG.free_text_min_words and ratio >= CFG.free_text_min_ratio:
        return "free_text"
    if ratio >= CFG.candidate_key_ratio:
        return "candidate_key"
    if 1 < distinct <= CFG.categorical_max_distinct:
        return "categorical"
    return "other"


def cardinality(raw_informative: list[str]) -> dict[str, Any]:
    """Distinct count, ratio and a role hint (id, label, free text ...)."""
    distinct = len(set(raw_informative))
    ratio = share(distinct, len(raw_informative))
    avg_words = round(statistics.fmean(len(v.split()) for v in raw_informative), 2) if raw_informative else 0.0
    return {"distinct": distinct, "distinct_ratio": ratio, "avg_words": avg_words,
            "role_hint": role_hint(distinct, ratio, avg_words)}  # fmt: skip


def frequency(present: list[str]) -> dict[str, Any]:
    """Top values with counts, and how much of the column they cover."""
    counts = Counter(present)
    return {
        "top_values": [[truncate(v), c] for v, c in ranked(counts)[: CFG.top_values]],
        "singleton_count": sum(1 for c in counts.values() if c == 1),
    }


def shape(present: pd.Series, stripped: pd.Series) -> dict[str, Any]:
    """Lengths, leading/trailing spaces and hidden characters."""
    lengths = present.str.len()
    padded = present[(present != stripped) & (stripped != "")]
    result: dict[str, Any] = {
        "min_length": int(lengths.min()) if len(lengths) else None,
        "max_length": int(lengths.max()) if len(lengths) else None,
        "avg_length": round(float(lengths.mean()), 1) if len(lengths) else None,
        "leading_trailing_ws": len(padded),
    }
    if len(padded):  # repr() makes the surrounding whitespace visible
        result["ws_examples"] = [repr(truncate(v)) for v, _ in ranked(Counter(padded.tolist()))[: CFG.examples]]
    return result


def patterns(informative: list[str]) -> dict[str, Any]:
    """Value shape masks (A = letter, 9 = digit) and their counts."""
    masks = Counter(mask(v) for v in informative)
    return {
        "distinct_masks": len(masks),
        "top_masks": [[m, share(c, len(informative))] for m, c in ranked(masks)[: CFG.top_masks]],
    }


def _type_matches(value: str, parsed: ParsedDate | None) -> dict[str, bool]:
    return {
        "boolean": value.lower() in BOOLEAN_VALUES,
        "integer": bool(_INTEGER.fullmatch(value)),
        "float": bool(_FLOAT.fullmatch(value)),
        "date": bool(parsed and parsed.value and not parsed.fmt.has_time),
        "datetime": bool(parsed and parsed.value),
        "email": bool(_EMAIL.fullmatch(value)),
        "url": bool(_URL.fullmatch(value)),
        "phone": bool(_PHONE.fullmatch(value)),
    }


def type_inference(informative: list[str], parsed: list[ParsedDate | None]) -> dict[str, Any]:
    """Share of informative values parsing as each type; dominant type + confidence.

    - boolean candidate: every distinct value is a boolean token or 0/1 (at least two kinds)
    - identifier: numbers where >= leading_zero_identifier_share keep leading zeros ("00123")
    - text needs >= dominant_threshold too; otherwise the column is "mixed"
    """
    counts = Counter[str]()
    by_primary: dict[str, Counter[str]] = {}
    untyped = 0
    for value, p in zip(informative, parsed, strict=True):
        matched = _type_matches(value, p)
        counts.update(t for t, ok in matched.items() if ok)
        untyped += not any(matched.values())
        primary = next((t for t in TYPES if matched[t]), "text")  # most specific type of this value
        by_primary.setdefault(primary, Counter())[value] += 1
    total = len(informative)
    distinct_lower = {v.lower() for v in informative}
    boolean_candidate = len(distinct_lower) >= 2 and distinct_lower <= BOOLEAN_CANDIDATE_VALUES
    if boolean_candidate:
        counts["boolean"] = total
    zero_padded = Counter(v for v in informative if _LEADING_ZERO.fullmatch(v))
    leading_zeros = zero_padded.total()
    shares = {t: share(counts[t], total) for t in TYPES}
    text_share = share(untyped, total)
    best = max(TYPES, key=lambda t: shares[t])  # first max wins: the more specific type
    if total == 0:
        dominant, confidence = "empty", 0.0
    elif shares[best] >= CFG.dominant_threshold:
        dominant, confidence = best, shares[best]
    elif text_share >= CFG.dominant_threshold:
        dominant, confidence = "text", text_share
    else:
        dominant, confidence = "mixed", max(shares[best], text_share)
    if dominant in ("integer", "float") and leading_zeros >= CFG.leading_zero_identifier_share * counts["float"]:
        dominant = "identifier"
    result = {"shares": shares, "text_share": text_share, "dominant_type": dominant, "confidence": confidence,
              "boolean_candidate": boolean_candidate, "leading_zeros": leading_zeros}  # fmt: skip
    if dominant == "mixed":  # one example per value type, largest groups first
        groups = sorted(by_primary.items(), key=lambda kv: (-kv[1].total(), kv[0]))[: CFG.examples]
        result["examples"] = {t: truncate(ranked(c)[0][0]) for t, c in groups}
    if leading_zeros:
        result["leading_zero_examples"] = [truncate(v) for v, _ in ranked(zero_padded)[: CFG.examples]]
    return result


def _number_stats(numbers: list[float]) -> dict[str, Any]:
    q1, _, q3 = statistics.quantiles(numbers, n=4) if len(numbers) > 1 else (numbers[0],) * 3
    spread = CFG.iqr_multiplier * (q3 - q1)
    outliers = [n for n in numbers if n < q1 - spread or n > q3 + spread]
    median = statistics.median(numbers)
    result = {
        "min": round(min(numbers), 4),
        "max": round(max(numbers), 4),
        "mean": round(statistics.fmean(numbers), 4),
        "median": round(median, 4),
        "negatives": sum(n < 0 for n in numbers),
        "iqr_outliers": len(outliers),
    }
    if outliers:  # the most extreme distinct values (furthest from the median)
        extreme = sorted(set(outliers), key=lambda n: (-abs(n - median), n))[: CFG.examples]
        result["outlier_examples"] = [round(n, 4) for n in extreme]
    return result


def numeric_profile(informative: list[str], numbers: list[float], excluded: set[float]) -> dict[str, Any]:
    """Range and outliers (with and without suspected sentinels), plus numbers hidden in text."""
    in_text = Counter(v for v in informative if not _FLOAT.fullmatch(v) and _DIGIT.search(v) and not looks_like_date(v))
    result: dict[str, Any] = {"count": len(numbers)}
    if numbers:
        result |= _number_stats(numbers)
    kept = [n for n in numbers if n not in excluded]
    if excluded and kept and len(kept) < len(numbers):
        result["without_sentinels"] = {"count": len(kept)} | _number_stats(kept)
    result["numbers_in_text"] = in_text.total()
    result["numbers_in_text_examples"] = [truncate(v) for v, _ in ranked(in_text)[: CFG.examples]]
    result["number_kinds"] = number_kinds([v for v in informative if _DIGIT.search(v) and not looks_like_date(v)])
    result["numeric_after_cleanup"] = numeric_after_cleanup(informative)
    result["zero_share"] = share(sum(1 for n in numbers if n == 0), len(informative))  # a fact, not an issue
    return result


def text_profile(informative: list[str]) -> dict[str, Any]:
    """Word and length statistics for free-text columns."""
    return {
        "avg_words": round(statistics.fmean(len(v.split()) for v in informative), 2) if informative else 0.0,
        "max_length": max((len(v) for v in informative), default=0),
    }


def pii_hints(column: str, shares: dict[str, float]) -> dict[str, Any]:
    """Shares of email/phone/name-like values and PII words in the column name."""
    name_words = set(re.split(r"[^a-z0-9]+", column.lower()))
    return {
        "email_share": shares["email"],
        "phone_share": shares["phone"],
        "name_matches": sorted(name_words & set(CFG.pii_name_words)),
    }


def samples(raw_informative: list[str], seed: int) -> list[str]:
    """Values picked with a fixed seed, so the same run always gives the same samples."""
    picked = random.Random(seed).sample(raw_informative, min(CFG.samples, len(raw_informative)))
    return [truncate(v) for v in picked]


# --- size budget ---------------------------------------------------------------

# (path, keep): shorten that list to `keep` items, in order, until the profile fits.
# "*" applies the step to every element of a list.
_TRIM_STEPS: tuple[tuple[tuple[str, ...], int], ...] = (
    (("variants", "top_groups", "*", "members"), 3),
    (("variants", "top_groups"), 3),
    (("frequency", "top_values"), 5),
    (("samples",), 3),
    (("typos",), 5),
    (("frequency", "top_values"), 3),
    (("numeric", "numbers_in_text_examples"), 1),
    (("numeric", "number_kinds"), 4),
    (("dates", "examples", "*"), 1),
    (("characters", "hidden_examples"), 1),
    (("characters", "mojibake_examples"), 1),
    (("dates", "formats"), 6),
    (("dates", "unparsed_examples"), 1),
    (("sentinels",), 3),
    (("variants", "top_groups", "*", "members"), 2),
    (("variants", "top_groups"), 1),
    (("typos",), 2),
    (("samples",), 1),
    (("frequency", "top_values"), 1),
    (("shape", "ws_examples"), 1),
    (("patterns", "top_masks"), 3),
    (("sentinels",), 2),
    (("dates", "formats"), 2),
    (("patterns", "top_masks"), 1),
    (("dates", "formats"), 3),
)


def _trim(node: Any, path: tuple[str, ...], keep: int) -> bool:
    """Shorten the list at `path` inside node; True if anything was cut."""
    if not path:
        return False
    head, *rest = path
    if head == "*":  # every element of a list, or every value of a dict
        items = node if isinstance(node, list) else list(node.values()) if isinstance(node, dict) else []
        if not rest:
            cut = [_trim_list(node, key, keep) for key in (node if isinstance(node, dict) else [])]
        else:
            cut = [_trim(item, tuple(rest), keep) for item in items]  # trim every item, no short-circuit
        return any(cut)
    if not isinstance(node, dict) or head not in node:
        return False
    if rest:
        return _trim(node[head], tuple(rest), keep)
    if isinstance(node[head], list) and len(node[head]) > keep:
        node[head] = node[head][:keep]
        return True
    return False


def _trim_list(node: dict[str, Any], key: str, keep: int) -> bool:
    if isinstance(node.get(key), list) and len(node[key]) > keep:
        node[key] = node[key][:keep]
        return True
    return False


def fit_to_budget(profile: dict[str, Any]) -> dict[str, Any]:
    """Shorten example lists until the profile is at most max_profile_bytes."""
    for path, keep in _TRIM_STEPS:
        if profile_size(profile) <= CFG.max_profile_bytes:
            break
        if _trim(profile, path, keep):
            profile["trimmed"] = True
    return profile


# --- assembly ------------------------------------------------------------------


def profile_column(name: str, position: int, values: pd.Series, seed: int, reference_time: datetime) -> dict[str, Any]:
    """All facts for one column. `values` holds raw strings, None where the key is absent."""
    present = values.dropna()
    stripped = present.str.strip()
    kinds = stripped.map(lambda v: null_like_kind(v) if v else None)
    keep = (stripped != "") & kinds.isna()
    raw_informative = present[keep].tolist()
    informative = stripped[keep].tolist()
    parsed = parse_dates(informative)
    groups = group_by_key(Counter(raw_informative))

    types = type_inference(informative, parsed)
    profile: dict[str, Any] = {
        "column": name,
        "position": position,
        "completeness": completeness(values, present, stripped, kinds),
        "cardinality": cardinality(raw_informative),
        "frequency": frequency(present.tolist()),
        "shape": shape(present, stripped),
        "variants": variants(groups),
        "characters": characters(Counter(present.tolist())),
        "patterns": patterns(informative),
        "type_inference": types,
        "pii_hints": pii_hints(name, types["shares"]),
        "samples": samples(raw_informative, seed),
    }
    if typos := typo_pairs(groups, len(informative)):
        profile["typos"] = typos
    date_counts = None
    if types["shares"]["datetime"] >= CFG.section_threshold:
        profile["dates"] = date_profile(informative, parsed, reference_time)
        date_counts = date_sentinel_counts(informative, parsed)
    numeric = types["shares"]["float"] >= CFG.section_threshold and types["dominant_type"] != "identifier"
    numbers = [float(v) for v in informative if _FLOAT.fullmatch(v)] if numeric else None
    found, excluded = sentinels(informative, numbers, date_counts)
    if found:
        profile["sentinels"] = found
    if numbers is not None:
        profile["numeric"] = numeric_profile(informative, numbers, excluded)
    if profile["cardinality"]["role_hint"] == "free_text":
        profile["text"] = text_profile(informative)
    return fit_to_budget(profile)
