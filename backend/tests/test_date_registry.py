"""Single source of truth for dates: every profiler registry format compiles to silver SQL and
parses to the same timestamp in Postgres as in Python (both day/month orders where they exist)."""

from datetime import UTC, datetime

import pytest

from pipeline.profile_dates import DATE_FORMATS, parse_dates, pg_pattern

MOMENT = datetime(2024, 3, 25, 14, 7, 9, 123456)  # day 25 > 12: no day/month ambiguity


def sample(fmt, day_first: bool) -> str:
    """A value in this format for MOMENT."""
    if fmt.kind == "epoch_s":
        return str(int(MOMENT.replace(tzinfo=UTC).timestamp()))
    if fmt.kind == "epoch_ms":
        return str(int(MOMENT.replace(tzinfo=UTC).timestamp() * 1000))
    if fmt.kind == "excel":
        return str((MOMENT.date() - datetime(1899, 12, 30).date()).days)
    pattern = fmt.day_first_strptime if day_first else fmt.strptime
    moment = MOMENT.replace(tzinfo=UTC) if "%z" in pattern else MOMENT
    text = moment.strftime(pattern)
    if fmt.label.endswith("Z"):
        text = text.replace("+0000", "Z")
    elif "%z" in pattern:  # registry shape is ±HH:MM
        text = text[:-2] + ":" + text[-2:]
    return text


CASES = [(f, df) for f in DATE_FORMATS for df in ((False, True) if f.day_first_strptime else (False,))]


@pytest.mark.parametrize("fmt,day_first", CASES, ids=[f"{f.label}{' (day first)' if d else ''}" for f, d in CASES])
def test_every_registry_format_round_trips_in_postgres(conn, fmt, day_first) -> None:
    value = sample(fmt, day_first)
    assert fmt.regex.fullmatch(value), f"sample {value!r} does not match the registry shape"
    expected = parse_dates(["2024-01-01"] * 3 + [value])[-1]  # contextual formats need a date-like column
    assert expected is not None and expected.value is not None, f"python cannot parse {value!r}"
    got = conn.execute("SELECT silver.try_ts(%s, %s, %s) AS ts",
                       (value, fmt.regex.pattern, pg_pattern(fmt, day_first))).fetchone()["ts"]  # fmt: skip
    want = expected.value if not day_first else datetime.strptime(value, fmt.day_first_strptime)
    assert got == want.replace(tzinfo=None), f"{fmt.label}: {value!r} -> postgres {got}, python {want}"
