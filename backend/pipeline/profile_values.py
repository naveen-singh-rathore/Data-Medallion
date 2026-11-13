"""Value-quality checks for one column: null-like values, normalization variants, typo
neighbours, hidden characters, mojibake, sentinels and spikes.

All of these are *candidates*: the profiler reports what it sees, and the data-quality
agent decides what (if anything) to map or clean.
"""

import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from contextlib import suppress
from difflib import SequenceMatcher
from typing import Any

from pipeline.profile_config import PROFILE_CONFIG as CFG

NULL_LIKE = frozenset(CFG.null_like_tokens)
_SEPARATORS = re.compile(r"[\s\-_./\\]+")
_MOJIBAKE = re.compile("Ã[\x80-\xbf]|â€|Â[\x80-\xbf ]|ï»¿")
_ZERO_WIDTH = frozenset("\u200b\u200c\u200d\u2060\ufeff")
_LINE_CHARS = frozenset("\t\n\r")


def truncate(value: str) -> str:
    """A value cut to the profiler's display length."""
    return value[: CFG.max_chars]


def ranked(counts: Counter[Any]) -> list[tuple[Any, int]]:
    """Most common first; ties broken by value so output is deterministic."""
    return sorted(counts.items(), key=lambda kv: (-kv[1], str(kv[0])))


def share(count: int, total: int) -> float:
    """count / total rounded to 4 places (0.0 for an empty total)."""
    return round(count / total, 4) if total else 0.0


# --- null-like ---------------------------------------------------------------


def is_punctuation_only(value: str) -> bool:
    """'???', '--', '**', '. .': only punctuation/symbols (and inner spaces)."""
    return any(not ch.isspace() for ch in value) and all(
        ch.isspace() or unicodedata.category(ch)[0] in "PS" for ch in value
    )


def null_like_kind(stripped: str) -> str | None:
    """'token' or 'punctuation' when a stripped, non-empty value stands for "no value"."""
    if stripped.lower() in NULL_LIKE:
        return "token"
    return "punctuation" if is_punctuation_only(stripped) else None


def is_missing(value: str | None) -> bool:
    """Absent, empty, whitespace-only or null-like: the one definition used everywhere."""
    if value is None:
        return True
    stripped = value.strip()
    return not stripped or null_like_kind(stripped) is not None


# --- normalization variants and typos -----------------------------------------


def normalization_key(value: str) -> str:
    """NFKD without accents, lowercased, with separators [space - _ . / \\] removed.

    Removing separators also merges camelCase ("InProgress") with "in progress" and
    "in_progress", so no separate camelCase split is needed. Values without letters only
    get lower+strip: in "-999", "1.5" or "2024-03-01" the punctuation carries meaning.
    """
    decomposed = unicodedata.normalize("NFKD", value)
    plain = "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()
    return _SEPARATORS.sub("", plain) if _letters(plain) else plain.strip()


def group_by_key(raw_counts: Counter[str]) -> dict[str, Counter[str]]:
    """Raw values grouped by normalization key (case, spacing and punctuation folded)."""
    groups: dict[str, Counter[str]] = defaultdict(Counter)
    for raw, count in raw_counts.items():
        groups[normalization_key(raw)][raw] += count
    return groups


def _casing_only(group: Counter[str]) -> bool:
    return len({v.strip().lower() for v in group}) == 1


def variants(groups: dict[str, Counter[str]]) -> dict[str, Any]:
    """Raw spellings that share a normalization key ("In-Progress", "in_progress", ...)."""
    multi = sorted(((k, g) for k, g in groups.items() if len(g) > 1), key=lambda kg: (-kg[1].total(), kg[0]))
    return {
        "groups": len(multi),
        "casing_only_groups": sum(_casing_only(g) for _, g in multi),
        "top_groups": [
            {"key": truncate(k), "casing_only": _casing_only(g),
             "members": [[truncate(v), c] for v, c in ranked(g)[: CFG.variant_members_reported]]}
            for k, g in multi[: CFG.variant_groups_reported]
        ],
    }  # fmt: skip


def _letters(value: str) -> str:
    return "".join(ch for ch in value if ch.isalpha())


def is_adjacent_swap(a: str, b: str) -> bool:
    """True if b is a with exactly two neighbouring characters swapped ("hihg" / "high")."""
    if len(a) != len(b):
        return False
    diff = [i for i, (x, y) in enumerate(zip(a, b, strict=True)) if x != y]
    return len(diff) == 2 and diff[1] == diff[0] + 1 and a[diff[0]] == b[diff[1]] and a[diff[1]] == b[diff[0]]


def _similar(rare: str, target: str) -> tuple[bool, float]:
    """SequenceMatcher ratio >= threshold, or one adjacent swap.

    The ratio alone misses swapped letters in short words ("hihg" vs "high" is 0.75).
    Other single edits in short words are not accepted: "test" vs "pest" is a different word.
    """
    matcher = SequenceMatcher(None, rare, target)
    swap = is_adjacent_swap(rare, target)
    if not swap and matcher.quick_ratio() < CFG.typo_min_ratio:
        return False, 0.0
    ratio = round(matcher.ratio(), 3)
    return swap or ratio >= CFG.typo_min_ratio, ratio


def typo_pairs(groups: dict[str, Counter[str]], total: int) -> list[dict[str, Any]]:
    """Rare normalized values that look like a misspelling of a more frequent one.

    Only for label-like columns (<= typo_max_distinct keys). Values that differ only in
    digits/symbols ("building 3" vs "building 7") are never paired.
    """
    if not total or len(groups) > CFG.typo_max_distinct:
        return []
    counts = {k: g.total() for k, g in groups.items()}
    by_frequency = sorted(counts, key=lambda k: (-counts[k], k))
    representative = {k: ranked(g)[0][0] for k, g in groups.items()}
    pairs = []
    for rare in sorted(counts, key=lambda k: (counts[k], k)):
        if counts[rare] >= CFG.typo_rare_share * total or len(rare) < CFG.typo_min_length or not _letters(rare):
            continue
        for target in by_frequency:
            if counts[target] <= counts[rare]:
                break
            if len(target) < CFG.typo_min_length or _letters(target) == _letters(rare):
                continue
            similar, ratio = _similar(rare, target)
            if similar:
                pairs.append({"rare": truncate(representative[rare]), "likely_intended": truncate(representative[target]),
                              "rare_count": counts[rare], "ratio": ratio})  # fmt: skip
                break
    return sorted(pairs, key=lambda p: (-p["rare_count"], p["rare"]))[: CFG.typo_max_pairs]


# --- characters ----------------------------------------------------------------


def _is_hidden(ch: str) -> bool:
    return ch in _ZERO_WIDTH or ch == "\u00a0" or unicodedata.category(ch) == "Cc"


def _escape_hidden(value: str) -> str:
    return "".join(f"[U+{ord(ch):04X}]" if _is_hidden(ch) else ch for ch in value)


def characters(raw_counts: Counter[str]) -> dict[str, Any]:
    """Zero-width, control (incl. tab/newline) and NBSP characters; UTF-8-read-as-Latin-1 text."""
    by_char = Counter[str]()
    seen: set[str] = set()
    hidden = Counter[str]()
    mojibake = Counter[str]()
    for value, count in raw_counts.items():
        chars = {ch for ch in value if _is_hidden(ch)}
        seen |= chars
        for ch in chars:
            by_char[f"U+{ord(ch):04X}"] += count
        if chars:
            hidden[value] += count
        if _MOJIBAKE.search(value):
            mojibake[value] += count
    result: dict[str, Any] = {"hidden_values": hidden.total(), "mojibake_values": mojibake.total()}
    if hidden:
        result["hidden_by_char"] = dict(sorted(by_char.items()))
        result["hidden_only_line_chars"] = seen <= _LINE_CHARS
        result["hidden_examples"] = [truncate(_escape_hidden(v)) for v, _ in ranked(hidden)[: CFG.examples]]
    if mojibake:
        result["mojibake_examples"] = [truncate(v) for v, _ in ranked(mojibake)[: CFG.examples]]
    return result


# --- sentinels and spikes -------------------------------------------------------


def _is_spike(counts: Counter[Any], value: Any, total: int) -> bool:
    """value's share >= spike_min_share and its count >= N x the median count of the others."""
    count = counts[value]
    if not total or count / total < CFG.spike_min_share:
        return False
    others = [c for v, c in counts.items() if v != value]
    return bool(others) and count >= CFG.spike_median_multiple * statistics.median(others)


def number_label(number: float) -> str:
    """A float as text without a trailing .0."""
    return str(int(number)) if number.is_integer() else str(number)


def sentinels(
    informative: list[str], numbers: list[float] | None, date_counts: Counter[str] | None
) -> tuple[list[dict[str, Any]], set[float]]:
    """Known placeholder values plus statistical spikes.

    Returns the findings and the numeric values to leave out of "without sentinels" stats.
    - dates: known placeholder dates (counted by the date parser)
    - numbers: known sentinels (-1, 999, ...) whenever present; 0 only if it spikes
    - any column with >= spike_min_distinct values that is not label-like: one value with
      share >= spike_min_share and >= spike_median_multiple x the median frequency of the rest
      (only if at most spike_max_values qualify; more means canned/templated values)
    """
    total = len(informative)
    found: dict[str, tuple[int, str]] = {label: (c, "known") for label, c in (date_counts or {}).items()}
    excluded: set[float] = set()
    if numbers:
        num_counts = Counter(numbers)
        candidates = [float(s) for s in CFG.numeric_sentinels]
        if CFG.spike_zero and _is_spike(num_counts, 0.0, total):
            candidates.append(0.0)
        for number in candidates:
            if num_counts[number]:
                found[number_label(number)] = (num_counts[number], "known")
                excluded.add(number)
    counts = Counter(informative)
    if len(counts) >= CFG.spike_min_distinct and share(len(counts), total) > CFG.spike_max_distinct_ratio:
        spikes = [v for v in counts if v not in found and _is_spike(counts, v, total)]
        for value in spikes if len(spikes) <= CFG.spike_max_values else []:
            found[value] = (counts[value], "spike")
            with suppress(ValueError):  # non-numeric spikes are still reported, just not excluded
                excluded.add(float(value))
    findings = [
        {"value": truncate(v), "count": c, "share": share(c, total), "kind": kind}
        for v, (c, kind) in sorted(found.items(), key=lambda kv: (-kv[1][0], kv[0]))
    ]
    return findings[: CFG.sentinels_reported], excluded
