"""Date detection for the profiler: an explicit list of formats, parsed once per column.

- Each value is matched against DATE_FORMATS by regex shape, then parsed (strptime,
  epoch or Excel serial). Shape matches that fail to parse are "invalid" dates.
- DD/MM vs MM/DD order is decided per format by majority of unambiguous evidence.
- Contextual formats (bare YYYYMMDD and Excel serial numbers) are only tried when the
  column is already date-like, so plain integer columns are never read as dates.
- Timezone-aware values are converted to naive UTC so every value compares.
"""

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pipeline.profile_config import PROFILE_CONFIG as CFG

EXCEL_EPOCH = datetime(1899, 12, 30)  # Excel's day 0 (accounts for its 1900 leap-year bug)


@dataclass(frozen=True)
class DateFormat:
    """One registry date format: id, detection regex, strptime pattern and Postgres pattern."""

    label: str
    regex: re.Pattern[str]
    strptime: str | None  # None: numeric `kind`
    has_time: bool
    # Set for NN/NN/YY(YY) formats where day/month order is ambiguous; regex groups 1 and 2
    # are the two leading parts, and `label` is the month-first variant.
    day_first_label: str | None = None
    day_first_strptime: str | None = None
    kind: str = "strptime"  # strptime | epoch_s | epoch_ms | excel
    contextual: bool = False  # only tried when the column is date-like
    two_digit_year: bool = False


def _fmt(label: str, regex: str, strptime: str | None, has_time: bool, *day_first: str, **kw: Any) -> DateFormat:
    return DateFormat(label, re.compile(regex), strptime, has_time, *day_first, **kw)


# strptime directive -> Postgres to_timestamp pattern (the silver compiler parses with these)
_PG_TOKENS = {"%Y": "YYYY", "%m": "MM", "%d": "DD", "%H": "HH24", "%M": "MI", "%S": "SS", "%I": "HH12",
              "%p": "AM", "%b": "Mon", "%B": "FMMonth", "%y": "YY", "%f": "US", "%z": "TZH:TZM"}  # fmt: skip


def pg_format(strptime: str, label: str) -> str:
    """Postgres pattern for a strptime pattern: letters that aren't directives are quoted;
    a literal trailing Z (UTC) becomes "Z" (Postgres can't read Z as an offset)."""
    if label.endswith("Z"):
        strptime = strptime.replace("%z", "Z")
    out, i = [], 0
    while i < len(strptime):
        token = strptime[i : i + 2]
        if token in _PG_TOKENS:
            out.append(_PG_TOKENS[token])
            i += 2
            continue
        out.append(f'"{strptime[i]}"' if strptime[i].isalpha() else strptime[i])
        i += 1
    return "".join(out)


def format_by_id(format_id: str) -> DateFormat | None:
    """Registry lookup by label; a day-first label (DD/MM/...) finds its shared-shape entry."""
    return next((f for f in DATE_FORMATS if format_id in (f.label, f.day_first_label)), None)


def format_ids() -> list[str]:
    """Every registry format id, including the day-first variants."""
    return [label for f in DATE_FORMATS for label in (f.label, f.day_first_label) if label]


def is_ambiguous_shape(fmt: DateFormat) -> bool:
    """NN/NN/YYYY-style: the same text reads differently day-first and month-first."""
    return fmt.day_first_label is not None


def pg_pattern(fmt: DateFormat, day_first: bool) -> str:
    """Postgres pattern (or EPOCH / EPOCH_MS / EXCEL for numeric kinds) for one format."""
    if fmt.kind != "strptime":
        return {"epoch_s": "EPOCH", "epoch_ms": "EPOCH_MS", "excel": "EXCEL"}[fmt.kind]
    if day_first and fmt.day_first_strptime and fmt.day_first_label:
        return pg_format(fmt.day_first_strptime, fmt.day_first_label)
    return pg_format(fmt.strptime or "", fmt.label)


_DM = r"(\d{1,2})"
_ISO = r"\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{2}:\d{2}"
_OFFSET = r"[+-]\d{2}:?\d{2}"
DATE_FORMATS = (
    _fmt("YYYY-MM-DD", r"\d{4}-\d{1,2}-\d{1,2}", "%Y-%m-%d", False),
    _fmt("YYYY-MM-DDTHH:MM:SS", _ISO, "%Y-%m-%dT%H:%M:%S", True),
    _fmt("YYYY-MM-DDTHH:MM:SSZ", _ISO + "Z", "%Y-%m-%dT%H:%M:%S%z", True),
    _fmt("YYYY-MM-DDTHH:MM:SS±HH:MM", _ISO + _OFFSET, "%Y-%m-%dT%H:%M:%S%z", True),
    _fmt("YYYY-MM-DDTHH:MM:SS.fff", _ISO + r"\.\d{1,6}", "%Y-%m-%dT%H:%M:%S.%f", True),
    _fmt("YYYY-MM-DDTHH:MM:SS.fffZ", _ISO + r"\.\d{1,6}Z", "%Y-%m-%dT%H:%M:%S.%f%z", True),
    _fmt("YYYY-MM-DDTHH:MM:SS.fff±HH:MM", _ISO + r"\.\d{1,6}" + _OFFSET, "%Y-%m-%dT%H:%M:%S.%f%z", True),
    _fmt("YYYY-MM-DD HH:MM:SS", r"\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S", True),
    _fmt("YYYY-MM-DD HH:MM", r"\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{2}", "%Y-%m-%d %H:%M", True),
    _fmt("YYYY/MM/DD", r"\d{4}/\d{1,2}/\d{1,2}", "%Y/%m/%d", False),
    _fmt("YYYY/MM/DD HH:MM:SS", r"\d{4}/\d{1,2}/\d{1,2} \d{1,2}:\d{2}:\d{2}", "%Y/%m/%d %H:%M:%S", True),
    _fmt("MM/DD/YYYY", rf"{_DM}/{_DM}/\d{{4}}", "%m/%d/%Y", False, "DD/MM/YYYY", "%d/%m/%Y"),
    _fmt("MM/DD/YYYY HH:MM", rf"{_DM}/{_DM}/\d{{4}} \d{{1,2}}:\d{{2}}", "%m/%d/%Y %H:%M", True,
         "DD/MM/YYYY HH:MM", "%d/%m/%Y %H:%M"),
    _fmt("MM/DD/YYYY HH:MM:SS", rf"{_DM}/{_DM}/\d{{4}} \d{{1,2}}:\d{{2}}:\d{{2}}", "%m/%d/%Y %H:%M:%S", True,
         "DD/MM/YYYY HH:MM:SS", "%d/%m/%Y %H:%M:%S"),
    _fmt("MM/DD/YYYY HH:MM AM/PM", rf"{_DM}/{_DM}/\d{{4}} \d{{1,2}}:\d{{2}} [AaPp][Mm]", "%m/%d/%Y %I:%M %p", True,
         "DD/MM/YYYY HH:MM AM/PM", "%d/%m/%Y %I:%M %p"),
    _fmt("MM/DD/YY", rf"{_DM}/{_DM}/\d{{2}}", "%m/%d/%y", False, "DD/MM/YY", "%d/%m/%y", two_digit_year=True),
    _fmt("MM-DD-YYYY", rf"{_DM}-{_DM}-\d{{4}}", "%m-%d-%Y", False, "DD-MM-YYYY", "%d-%m-%Y"),
    _fmt("MM-DD-YYYY HH:MM", rf"{_DM}-{_DM}-\d{{4}} \d{{1,2}}:\d{{2}}", "%m-%d-%Y %H:%M", True,
         "DD-MM-YYYY HH:MM", "%d-%m-%Y %H:%M"),
    _fmt("MM-DD-YYYY HH:MM:SS", rf"{_DM}-{_DM}-\d{{4}} \d{{1,2}}:\d{{2}}:\d{{2}}", "%m-%d-%Y %H:%M:%S", True,
         "DD-MM-YYYY HH:MM:SS", "%d-%m-%Y %H:%M:%S"),
    _fmt("MM-DD-YY", rf"{_DM}-{_DM}-\d{{2}}", "%m-%d-%y", False, "DD-MM-YY", "%d-%m-%y", two_digit_year=True),
    _fmt("DD Mon YYYY", r"\d{1,2} [A-Za-z]{3} \d{4}", "%d %b %Y", False),
    _fmt("DD Month YYYY", r"\d{1,2} [A-Za-z]{4,9} \d{4}", "%d %B %Y", False),
    _fmt("DD-Mon-YYYY", r"\d{1,2}-[A-Za-z]{3}-\d{4}", "%d-%b-%Y", False),
    _fmt("DD-Mon-YYYY HH:MM", r"\d{1,2}-[A-Za-z]{3}-\d{4} \d{1,2}:\d{2}", "%d-%b-%Y %H:%M", True),
    _fmt("DD-Mon-YYYY HH:MM:SS", r"\d{1,2}-[A-Za-z]{3}-\d{4} \d{1,2}:\d{2}:\d{2}", "%d-%b-%Y %H:%M:%S", True),
    _fmt("Mon DD, YYYY", r"[A-Za-z]{3} \d{1,2}, \d{4}", "%b %d, %Y", False),
    _fmt("Month DD, YYYY", r"[A-Za-z]{4,9} \d{1,2}, \d{4}", "%B %d, %Y", False),
    _fmt("epoch seconds", r"\d{9,10}", None, True, kind="epoch_s"),
    _fmt("epoch milliseconds", r"\d{13}", None, True, kind="epoch_ms"),
    _fmt("YYYYMMDD", r"\d{8}", "%Y%m%d", False, contextual=True),
    _fmt("Excel serial", r"\d{5}", None, False, kind="excel", contextual=True),
)  # fmt: skip


@dataclass(frozen=True)
class ParsedDate:
    """A value parsed with a registry format (or why it could not be)."""

    fmt: DateFormat  # matched format; fmt.label is the family (groups DD/MM and MM/DD)
    label: str  # the format the value actually parsed with
    value: datetime | None  # None: shaped like a date but not a valid one
    evidence: str | None  # 'dmy' | 'mdy' | 'ambiguous' for day/month formats


def _match_format(value: str, contextual: bool) -> DateFormat | None:
    return next((f for f in DATE_FORMATS if (contextual or not f.contextual) and f.regex.fullmatch(value)), None)


def looks_like_date(value: str) -> bool:
    """Shape matches a non-contextual date format (used to keep dates out of numbers-in-text)."""
    return _match_format(value, contextual=False) is not None


def _day_month_evidence(fmt: DateFormat, value: str) -> str | None:
    if not fmt.day_first_strptime:
        return None
    match = fmt.regex.fullmatch(value)
    first, second = int(match.group(1)), int(match.group(2))  # type: ignore[union-attr]
    if first > 12:
        return "dmy"
    return "mdy" if second > 12 else "ambiguous"


def _numeric_date(value: str, kind: str) -> datetime | None:
    number = int(value)
    if kind == "excel":
        low, high = CFG.excel_serial_range
        return EXCEL_EPOCH + timedelta(days=number) if low <= number <= high else None
    seconds = number / 1000 if kind == "epoch_ms" else number
    low, high = CFG.epoch_range
    return datetime.fromtimestamp(seconds, UTC).replace(tzinfo=None) if low <= seconds <= high else None


def _parse(value: str, fmt: DateFormat, day_first: bool) -> ParsedDate:
    evidence = _day_month_evidence(fmt, value)
    if fmt.strptime is None:
        return ParsedDate(fmt, fmt.label, _numeric_date(value, fmt.kind), None)
    attempts = [(fmt.label, fmt.strptime)]
    if fmt.day_first_label and fmt.day_first_strptime:
        attempts.insert(0 if day_first else 1, (fmt.day_first_label, fmt.day_first_strptime))
    for label, pattern in attempts:
        try:
            parsed = datetime.strptime(value, pattern)
        except ValueError:
            continue
        if parsed.tzinfo:
            parsed = parsed.astimezone(UTC).replace(tzinfo=None)
        return ParsedDate(fmt, label, parsed, evidence)
    return ParsedDate(fmt, fmt.label, None, evidence)


def _parse_all(values: list[str], contextual: bool) -> list[ParsedDate | None]:
    matched = [(v, _match_format(v, contextual)) for v in values]
    votes = Counter((f.label, _day_month_evidence(f, v)) for v, f in matched if f and f.day_first_strptime)
    return [
        _parse(v, f, day_first=votes[(f.label, "dmy")] > votes[(f.label, "mdy")]) if f else None for v, f in matched
    ]


def parse_dates(values: list[str]) -> list[ParsedDate | None]:
    """Parse each (stripped) value; None if no date format matches its shape.

    Ambiguous day/month values follow the majority of unambiguous evidence for the same
    format in this column (month-first on a tie). If at least `section_threshold` of the
    non-empty values are valid dates, contextual formats (YYYYMMDD, Excel serial) are
    tried on the rest.
    """
    parsed = _parse_all(values, contextual=False)
    non_empty = sum(1 for v in values if v)
    valid = sum(1 for p in parsed if p and p.value)
    if not non_empty or valid / non_empty < CFG.section_threshold:
        return parsed
    unmatched = [i for i, p in enumerate(parsed) if p is None and values[i]]
    for i, p in zip(unmatched, _parse_all([values[i] for i in unmatched], contextual=True), strict=True):
        parsed[i] = p
    return parsed


def _sentinel_dates() -> set[datetime]:
    return {datetime.strptime(s, "%Y-%m-%d") for s in CFG.date_sentinels if not s.startswith("0000")}


def _is_zero_date(value: str) -> bool:
    return not set(re.sub(r"\D", "", value)) - {"0"}


def _midnight(value: datetime) -> datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def date_sentinel_counts(values: list[str], parsed: list[ParsedDate | None]) -> Counter[str]:
    """Known placeholder dates (1900-01-01, 9999-12-31, ...) and all-zero dates like 0000-00-00."""
    known = _sentinel_dates()
    counts = Counter[str]()
    for value, p in zip(values, parsed, strict=True):
        if p and p.value and _midnight(p.value) in known:
            counts[p.value.strftime("%Y-%m-%d")] += 1
        elif p and not p.value and _is_zero_date(value):
            counts["0000-00-00"] += 1
    return counts


def _share(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _first(values: list[str], limit: int = 2) -> list[str]:
    """Most frequent distinct values first (ties by value), truncated."""
    counts = Counter(values)
    return [v[: CFG.max_chars] for v, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]


def is_unambiguous(p: ParsedDate) -> bool:
    """Day/month order is certain: no day/month ambiguity in the format, or a part > 12."""
    return p.evidence != "ambiguous"


def date_profile(values: list[str], parsed: list[ParsedDate | None], reference_time: datetime) -> dict[str, Any]:
    """Formats with shares (+ one raw example each), day/month evidence, range, future and old counts.

    Range facts (min/max/future/old) exclude known sentinel dates, and invalid_dates excludes
    all-zero dates: those are reported as sentinels, so a 9999-12-31 placeholder is not
    also a "future date". `examples` holds up to 2 raw values per finding that has a count.
    """
    total = len(values)
    pairs = list(zip(values, parsed, strict=True))
    valid = [(v, p) for v, p in pairs if p and p.value]
    known = _sentinel_dates()
    real = [(v, p.value) for v, p in valid if p.value and _midnight(p.value) not in known]
    evidence = Counter(p.evidence for _, p in valid if p.evidence)
    family_evidence: dict[str, set[str]] = defaultdict(set)
    for _, p in valid:
        if p.evidence in ("dmy", "mdy"):
            family_evidence[p.fmt.label].add(p.evidence)
    unmatched = [v for v, p in pairs if p is None]
    by_format: dict[str, list[str]] = defaultdict(list)
    for v, p in valid:
        by_format[p.label].append(v)
    findings = {
        "ambiguous": [v for v, p in valid if p.evidence == "ambiguous"],
        "invalid": [v for v, p in pairs if p and not p.value and not _is_zero_date(v)],
        "two_digit_year": [v for v, p in valid if p.fmt.two_digit_year],
        "future": [v for v, d in real if d > reference_time],
        "implausibly_old": [v for v, d in real if d.year < CFG.plausible_min_year],
    }
    dates = [d for _, d in real]
    return {
        "formats": [{"format": f, "share": _share(len(vs), total), "example": _first(vs, 1)[0]}
                    for f, vs in sorted(by_format.items(), key=lambda kv: (-len(kv[1]), kv[0]))],
        "format_count": len(by_format),  # survives trimming of the formats list
        # every format id seen, most frequent first: the exact ids rules must use (never trimmed)
        "format_ids": [f for f, _ in sorted(by_format.items(), key=lambda kv: (-len(kv[1]), kv[0]))],
        "unparsed": len(unmatched),
        "unparsed_examples": _first(unmatched, CFG.examples),
        "invalid_dates": len(findings["invalid"]),
        "ambiguous_count": evidence["ambiguous"],
        "dmy_evidence": evidence["dmy"],
        "mdy_evidence": evidence["mdy"],
        "conflicting_order_formats": sorted(f for f, ev in family_evidence.items() if len(ev) == 2),
        "two_digit_year": len(findings["two_digit_year"]),
        "excel_serial": sum(1 for _, p in valid if p.fmt.kind == "excel"),
        "min": min(dates).isoformat() if dates else None,
        "max": max(dates).isoformat() if dates else None,
        "future_count": len(findings["future"]),
        "implausibly_old_count": len(findings["implausibly_old"]),
        "examples": {k: _first(vs) for k, vs in findings.items() if vs},
    }  # fmt: skip
