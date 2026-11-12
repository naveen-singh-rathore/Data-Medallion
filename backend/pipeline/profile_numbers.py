"""Numbers written as text: which formatting kinds occur, and how many values a simple
cleanup would turn into numbers (so the agent can judge whether a numeric rule is viable).

Kinds (a value can have several, e.g. "$1,200" is currency_prefix + thousands_separator):
currency_prefix/suffix ($ ₹ € £ ¥ or a currency code), percent, thousands_separator (1,200),
european_decimal (1.234,56 / 3,50), parentheses_negative ((40)), unit_suffix (3 hrs),
scientific (1e3 - parses as a float already, reported for completeness).
"""

import re
from collections import Counter, defaultdict
from typing import Any

from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profile_values import share, truncate

_SYMBOLS = re.escape(CFG.currency_symbols)
_CODES = "|".join(CFG.currency_codes)
_NUM = r"\d[\d,.]*"
KIND_PATTERNS = {
    "currency_prefix": re.compile(rf"^[(+-]?\s*(?:[{_SYMBOLS}]|(?:{_CODES})\s)\s*-?{_NUM}", re.IGNORECASE),
    "currency_suffix": re.compile(rf"{_NUM}\s*(?:[{_SYMBOLS}]|(?:{_CODES}))\)?$", re.IGNORECASE),
    "percent": re.compile(rf"{_NUM}\s*%$"),
    "thousands_separator": re.compile(r"(?<![\d.,])\d{1,3}(?:,\d{3})+(?:\.\d+)?(?![\d,])"),
    "european_decimal": re.compile(r"(?<![\d.,])(?:\d{1,3}(?:\.\d{3})+,\d+|\d+,\d{1,2})(?![\d,.])"),
    "parentheses_negative": re.compile(rf"^\(\s*(?:[{_SYMBOLS}]\s*)?{_NUM}\s*\)$"),
    "unit_suffix": re.compile(rf"^[+-]?\d+(?:\.\d+)?\s*(?!(?:{_CODES})$)[A-Za-z]+\.?$", re.IGNORECASE),
    "scientific": re.compile(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$"),
}
_FLOAT = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")
_STRIP_CURRENCY = re.compile(rf"[{_SYMBOLS}]|\b(?:{_CODES})\b", re.IGNORECASE)
_UNIT = re.compile(r"(?<=\d)\s*[A-Za-z]+\.?$")


def number_kinds(values: list[str]) -> list[list[Any]]:
    """[[kind, count, example], ...] most frequent first, for values containing a digit."""
    counts: Counter[str] = Counter()
    examples: dict[str, Counter[str]] = defaultdict(Counter)
    for value in values:
        for kind, pattern in KIND_PATTERNS.items():
            if pattern.search(value):
                counts[kind] += 1
                examples[kind][value] += 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [[k, c, truncate(min(examples[k].items(), key=lambda kv: (-kv[1], kv[0]))[0])] for k, c in ranked]


def cleanup_number(value: str) -> float | None:
    """The number a value would become after removing the formatting kinds above, else None."""
    text = value.strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    text = _STRIP_CURRENCY.sub("", text).strip()
    text = text.removesuffix("%").strip()
    text = _UNIT.sub("", text).strip()
    if re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?", text):
        text = text.replace(",", "")
    elif re.fullmatch(r"[+-]?(?:\d{1,3}(?:\.\d{3})+,\d+|\d+,\d{1,2})", text):
        text = text.replace(".", "").replace(",", ".")
    if not _FLOAT.fullmatch(text):
        return None
    number = float(text)
    return -number if negative else number


def numeric_after_cleanup(values: list[str]) -> float:
    """Share of values that are numbers as-is or after cleanup."""
    return share(sum(1 for v in values if cleanup_number(v) is not None), len(values))
