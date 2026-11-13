"""Deterministic profiling of a bronze run (no LLM): column facts, dataset summary, issue flags.

Only parse_status = 'ok' rows feed column stats; malformed rows are summarised from the run.
Results go to meta.column_profiles / meta.dataset_profiles and are the input for the
data-quality agent, so they are compact and identical for identical input.
"""

import math
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.bronze import normalize_header
from pipeline.db import transaction
from pipeline.errors import MissingRunError
from pipeline.logging_setup import get_logger
from pipeline.profile_columns import coarse_mask, column_seed, profile_column
from pipeline.profile_config import PROFILE_CONFIG as CFG
from pipeline.profile_config import PROFILER_VERSION, config_dict, config_hash
from pipeline.profile_dates import ParsedDate, is_unambiguous, parse_dates
from pipeline.profile_issues import (
    assign_ids,
    column_issues,
    dataset_issues,
    sort_issues,
)
from pipeline.profile_values import is_missing, normalization_key, truncate

Profile = dict[str, Any]


@dataclass(frozen=True)
class RunData:
    """A run's bronze rows as a string DataFrame, plus column names and counts."""

    run_id: uuid.UUID
    columns: list[str]  # normalized names, in header order
    frame: pd.DataFrame  # object dtype: raw strings, None = key absent
    source_lines: list[int]
    row_hashes: list[str]
    rows_malformed: int
    malformed_by_reason: dict[str, int]
    malformed_examples: list[dict[str, Any]]  # first few malformed records: source_line + reason
    reference_time: datetime  # run start, naive UTC: "future" means after ingestion
    column_tags: dict[str, str]  # from stored business context, if any (used only for hints)


def load_run_data(conn: psycopg.Connection, run_id: uuid.UUID) -> RunData:
    """Load the run's ok rows into a DataFrame of raw strings (no NaN, no type coercion)."""
    cur = conn.cursor(row_factory=dict_row)
    run = cur.execute(
        "SELECT header, started_at, rows_loaded_malformed, malformed_by_reason"
        " FROM meta.ingestion_runs WHERE run_id = %s AND status = 'succeeded'",
        (run_id,),
    ).fetchone()
    if not run:
        raise MissingRunError(f"run {run_id} not found or not succeeded")
    columns = normalize_header(run["header"])
    rows = cur.execute(
        "SELECT source_line, row_hash, payload FROM bronze.records"
        " WHERE run_id = %s AND parse_status = 'ok' ORDER BY source_line",
        (run_id,),
    ).fetchall()
    frame = pd.DataFrame({c: [r["payload"].get(c) for r in rows] for c in columns}, dtype=object)
    malformed = cur.execute(
        "SELECT source_line, parse_error FROM bronze.records WHERE run_id = %s AND parse_status = 'malformed'"
        " ORDER BY source_line LIMIT %s",
        (run_id, CFG.examples),
    ).fetchall()
    context = cur.execute("SELECT draft FROM meta.dataset_context WHERE run_id = %s", (run_id,)).fetchone()
    return RunData(
        run_id=run_id,
        columns=columns,
        frame=frame,
        source_lines=[r["source_line"] for r in rows],
        row_hashes=[r["row_hash"] for r in rows],
        rows_malformed=run["rows_loaded_malformed"],
        malformed_by_reason=run["malformed_by_reason"] or {},
        malformed_examples=[{"source_line": m["source_line"], "reason": m["parse_error"]} for m in malformed],
        column_tags=((context or {}).get("draft") or {}).get("column_tags", {}),
        reference_time=run["started_at"].astimezone(UTC).replace(tzinfo=None),
    )


# --- cross-column checks -----------------------------------------------------


def _cell(value: str | None) -> str | None:
    """Comparable cell: all missing forms are equal; case, accents and separators ignored."""
    return None if is_missing(value) else normalization_key(value)  # type: ignore[arg-type]


def history_like_columns(data: RunData, profiles: list[Profile]) -> set[str]:
    """Columns whose changes look like a record being updated: time-like (date/datetime type,
    or tagged timestamp/status in the context) or status-like (small categorical)."""
    found = set()
    for p in profiles:
        tag = data.column_tags.get(p["column"])
        small_categorical = (p["cardinality"]["role_hint"] == "categorical"
                             and p["cardinality"]["distinct"] <= CFG.status_like_max_distinct)  # fmt: skip
        if (
            p["type_inference"]["dominant_type"] in ("date", "datetime")
            or tag in ("timestamp", "status")
            or small_categorical
        ):
            found.add(p["column"])
    return found


def _repeat_group(data: RunData, key: str, rows: list[int], history: set[str]) -> tuple[str, dict[str, list[Any]]]:
    """(hint, {differing column: distinct values}) for one group of rows sharing a key value."""
    differs = {}
    for column in data.columns:
        if column == key:
            continue
        if len({_cell(data.frame.at[i, column]) for i in rows}) > 1:  # compare normalized, show raw
            differs[column] = list(dict.fromkeys(data.frame.at[i, column] for i in rows))
    if not differs:
        return "exact_duplicate", differs
    return ("update_history" if set(differs) <= history else "conflicting"), differs


HINT_RANK = {"conflicting": 0, "update_history": 1, "exact_duplicate": 2}


def key_with_repeats(data: RunData, profiles: list[Profile]) -> list[dict[str, Any]]:
    """Every key-like column (distinct ratio >= key_ratio, one coarse pattern >= key_pattern_share)
    with its repeated values - reported even when there are none.

    Each repeat group is classified: exact_duplicate (no other column differs), update_history
    (only time/status-like columns differ) or conflicting (anything else differs). The finding's
    hint is the most serious class present. Date/datetime columns are never keys.
    """
    history = history_like_columns(data, profiles)
    findings = []
    for p in profiles:
        if p["cardinality"]["distinct_ratio"] < CFG.key_ratio or p["type_inference"]["dominant_type"] in (
            "date",
            "datetime",
        ):
            continue
        rows_by_value: dict[str, list[int]] = defaultdict(list)
        for i, value in enumerate(data.frame[p["column"]]):
            if not is_missing(value):
                rows_by_value[value.strip()].append(i)
        masks = Counter(coarse_mask(v) for v, rows in rows_by_value.items() for _ in rows)
        total = masks.total()
        if not total or masks.most_common(1)[0][1] / total < CFG.key_pattern_share:
            continue
        repeats = sorted(((v, rows) for v, rows in rows_by_value.items() if len(rows) > 1),
                         key=lambda kv: (-len(kv[1]), kv[0]))  # fmt: skip
        finding: dict[str, Any] = {"column": p["column"], "repeated_values": len(repeats),
                                   "rows": sum(len(rows) for _, rows in repeats)}  # fmt: skip
        if repeats:
            groups = [(v, rows, *_repeat_group(data, p["column"], rows, history)) for v, rows in repeats]
            hints = Counter(hint for *_, hint, _ in groups)
            differing = Counter(c for *_, differs in groups for c in differs)
            shown = sorted(groups, key=lambda g: (HINT_RANK[g[2]], -len(g[1]), g[0]))[:2]
            finding |= {
                "hint": min(hints, key=HINT_RANK.__getitem__),
                "hint_counts": dict(sorted(hints.items())),
                "differing_columns": [[c, round(n / len(groups), 4)] for c, n in
                                      sorted(differing.items(), key=lambda kv: (-kv[1], kv[0]))[:5]],
                "examples": [
                    {"value": truncate(v), "hint": hint, "source_lines": [data.source_lines[i] for i in rows[:5]],
                     "differs": {c: [truncate(x)[:40] if x else x for x in vals[:3]] for c, vals in list(differs.items())[:3]}}
                    for v, rows, hint, differs in shown
                ],
            }  # fmt: skip
        findings.append(finding)
    return findings


def column_dates(values: pd.Series) -> list[ParsedDate | None]:
    """Parsed date per row (None if missing or unparseable), same rules as the column profile."""
    return parse_dates(["" if is_missing(v) else v.strip() for v in values])


def date_order(data: RunData, profiles: list[Profile]) -> list[dict[str, Any]]:
    """For date-like columns A before B in the header: rows where A > B (reported if any).

    Evidence for "real data or parsing?": the violation rate per (format A, format B) pair and
    the rate among rows where both values are unambiguous (no day/month guess involved).
    """
    date_columns = [p["column"] for p in profiles if "dates" in p]
    parsed = {c: column_dates(data.frame[c]) for c in date_columns}
    findings = []
    for i, a in enumerate(date_columns):
        for b in date_columns[i + 1 :]:
            compared = [(k, x, y) for k, (x, y) in enumerate(zip(parsed[a], parsed[b], strict=True))
                        if x and y and x.value and y.value]  # fmt: skip
            later = [(k, x, y) for k, x, y in compared if x.value > y.value]  # type: ignore[operator]
            if not later:
                continue
            certain = [(x, y) for _, x, y in compared if is_unambiguous(x) and is_unambiguous(y)]
            certain_later = sum(1 for x, y in certain if x.value > y.value)  # type: ignore[operator]
            pair_all = Counter((x.label, y.label) for _, x, y in compared)
            pair_later = Counter((x.label, y.label) for _, x, y in later)
            findings.append({
                "a": a, "b": b, "compared": len(compared), "count": len(later),
                "share": round(len(later) / len(compared), 4),
                "unambiguous_compared": len(certain),
                "unambiguous_share": round(certain_later / len(certain), 4) if certain else None,
                "format_pairs": [[fa, fb, n, pair_all[(fa, fb)]] for (fa, fb), n in
                                 sorted(pair_later.items(), key=lambda kv: (-kv[1], kv[0]))[: CFG.examples]],
                "examples": [data.source_lines[k] for k, _, _ in later[: CFG.examples]],
            })  # fmt: skip
    return findings


# --- dataset summary ---------------------------------------------------------


def _missing_cells(p: Profile) -> int:
    return p["completeness"]["total"] - p["completeness"]["informative"]


def partial_rows(data: RunData) -> dict[str, Any]:
    """Histogram of missing cells per row, and rows missing >= partial_row_missing_share of columns."""
    per_row = data.frame.map(is_missing).sum(axis=1).tolist() if len(data.frame) else []
    threshold = max(1, math.ceil(CFG.partial_row_missing_share * len(data.columns)))
    flagged = [i for i, n in enumerate(per_row) if n >= threshold]
    return {
        "histogram": [[n, c] for n, c in sorted(Counter(per_row).items())],
        "min_missing_cells": threshold,
        "count": len(flagged),
        "examples": [data.source_lines[i] for i in flagged[: CFG.examples]],
    }


def _groups(keys: list[Any]) -> list[list[int]]:
    """Row indexes of keys that occur more than once, in first-occurrence order."""
    rows: dict[Any, list[int]] = defaultdict(list)
    for i, key in enumerate(keys):
        rows[key].append(i)
    return [g for g in rows.values() if len(g) > 1]


def dataset_summary(data: RunData, profiles: list[Profile]) -> dict[str, Any]:
    """Row counts, duplicates, overall emptiness and cross-column check results.

    exact duplicates = extra rows sharing a row_hash; near duplicates = extra rows that become
    identical under the normalization key (accents, case, separators; all missing forms equal),
    minus the exact ones. Both are always reported, with source lines of up to 3 groups.
    """
    rows = len(data.frame)
    exact_groups = _groups(data.row_hashes)
    exact_extra = sum(len(g) - 1 for g in exact_groups)
    normalized = [
        tuple(None if is_missing(v) else normalization_key(v) for v in row)
        for row in data.frame.itertuples(index=False, name=None)
    ]
    near_groups = [g for g in _groups(normalized) if len({data.row_hashes[i] for i in g}) > 1]
    normalized_extra = sum(len(g) - 1 for g in _groups(normalized))
    cells = rows * len(data.columns)
    return {
        "profiler": {"version": PROFILER_VERSION, "config_hash": config_hash(), "config": config_dict()},
        "rows_ok": rows,
        "rows_malformed": data.rows_malformed,
        "malformed_by_reason": data.malformed_by_reason,
        "columns": len(data.columns),
        "malformed_examples": data.malformed_examples,
        "exact_duplicate_rows": exact_extra,
        "exact_duplicate_groups": len(exact_groups),
        "exact_duplicate_examples": [[data.source_lines[i] for i in g[:3]] for g in exact_groups[: CFG.examples]],
        "near_duplicate_rows": normalized_extra - exact_extra,
        "near_duplicate_examples": [[data.source_lines[i] for i in g[:3]] for g in near_groups[: CFG.examples]],
        "overall_empty_or_null_like_share": round(sum(map(_missing_cells, profiles)) / cells, 4) if cells else 0.0,
        "partial_rows": partial_rows(data),
        "key_with_repeats": key_with_repeats(data, profiles),
        "date_order": date_order(data, profiles),
    }


# --- run + storage -----------------------------------------------------------


def _store(
    conn: psycopg.Connection, run_id: uuid.UUID, profiles: list[Profile], summary: Profile, issues: list
) -> None:
    """Replace the run's profiles (same transaction, so re-profiling is idempotent)."""
    conn.execute("DELETE FROM meta.column_profiles WHERE run_id = %s", (run_id,))
    conn.execute("DELETE FROM meta.dataset_profiles WHERE run_id = %s", (run_id,))
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO meta.column_profiles (run_id, column_name, column_position, profile, profiler_version)"
            " VALUES (%s, %s, %s, %s, %s)",
            [(run_id, p["column"], p["position"], Jsonb(p), PROFILER_VERSION) for p in profiles],
        )
    conn.execute(
        "INSERT INTO meta.dataset_profiles (run_id, summary, issues, profiler_version) VALUES (%s, %s, %s, %s)",
        (run_id, Jsonb(summary), Jsonb(issues), PROFILER_VERSION),
    )


def profile_run(run_id: uuid.UUID) -> None:
    """Profile every column of a succeeded bronze run and store profiles + issues."""
    log = get_logger("profile", run_id=str(run_id))
    started = time.perf_counter()
    with transaction() as conn:
        data = load_run_data(conn, run_id)
        log.info("rows loaded", rows_ok=len(data.frame), columns=len(data.columns))
        profiles = [
            profile_column(c, i, data.frame[c], column_seed(run_id, c), data.reference_time)
            for i, c in enumerate(data.columns, start=1)
        ]
        summary = dataset_summary(data, profiles)
        issues = assign_ids(sort_issues([*dataset_issues(summary), *(i for p in profiles for i in column_issues(p))],
                                        data.columns))  # fmt: skip
        _store(conn, run_id, profiles, summary, issues)
    severity = Counter(i["severity"] for i in issues)
    log.info(
        "profile stored", columns=len(profiles), issues=len(issues), high=severity["high"],
        medium=severity["medium"], low=severity["low"], duration_ms=int((time.perf_counter() - started) * 1000),
    )  # fmt: skip


def latest_succeeded_run() -> uuid.UUID:
    """The most recent succeeded ingestion run (raises MissingRunError when there is none)."""
    with transaction() as conn:
        row = conn.execute(
            "SELECT run_id FROM meta.ingestion_runs WHERE status = 'succeeded' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    if not row:
        raise MissingRunError("no succeeded ingestion run found; run `ingest` first")
    return row[0]


def load_profile(run_id: uuid.UUID) -> dict[str, Any]:
    """Return {"summary", "issues", "columns": [profile, ...]} for a profiled run."""
    with transaction() as conn:
        dataset = conn.execute(
            "SELECT summary, issues FROM meta.dataset_profiles WHERE run_id = %s", (run_id,)
        ).fetchone()
        if not dataset:
            raise MissingRunError(f"run {run_id} has not been profiled; run `profile` first")
        columns = conn.execute(
            "SELECT profile FROM meta.column_profiles WHERE run_id = %s ORDER BY column_position", (run_id,)
        ).fetchall()
    return {"summary": dataset[0], "issues": dataset[1], "columns": [c[0] for c in columns]}
