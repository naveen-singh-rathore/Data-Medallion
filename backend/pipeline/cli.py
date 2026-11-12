"""Command-line entry point. Run from backend/: python -m pipeline.cli <command>"""

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from pipeline.agent_tools import TOOLS, run_tool
from pipeline.bronze import get_run, ingest_file, recent_runs
from pipeline.card import render_card
from pipeline.config import settings
from pipeline.context import approve_context, compare_contexts, ensure_context
from pipeline.llm import LLMError
from pipeline.logging_setup import get_logger
from pipeline.metrics import format_rows, run_metrics
from pipeline.migrate import migrate
from pipeline.profiling import latest_succeeded_run, load_profile, profile_run
from pipeline.silver.defaults import default_rules
from pipeline.silver.rules_schema import from_yaml, to_yaml
from pipeline.silver.validator import validate_rules

STUB_COMMANDS = ["build-silver", "build-gold"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description="Medallion data pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="create layer schemas and tables (idempotent)")
    ingest = sub.add_parser("ingest", help="load a CSV into bronze")
    ingest.add_argument("path", help="path to the CSV file")
    ingest.add_argument("--force", action="store_true", help="reload even if already ingested")
    runs = sub.add_parser("runs", help="show recent ingestion runs")
    runs.add_argument("--limit", type=int, default=20)
    profile = sub.add_parser("profile", help="profile a bronze run (default: latest succeeded)")
    profile.add_argument("--run-id", type=uuid.UUID)
    context = sub.add_parser("context", help="store human business context, or draft it with one cheap LLM call")
    context.add_argument("--run-id", type=uuid.UUID)
    context.add_argument("--context-file", type=Path, help="human-written context (plain text or draft-shaped JSON)")
    context.add_argument("--approve", action="store_true", help="approve the stored (inferred) context")
    context.add_argument("--redraft", action="store_true", help="draft the inferred context again (LLM call)")
    context.add_argument("--compare", action="store_true", help="show human vs inferred differences")
    card = sub.add_parser("card", help="render the dataset card within a token budget")
    card.add_argument("--run-id", type=uuid.UUID)
    card.add_argument("--budget", type=int, help=f"token budget (default {settings.card_budget_tokens})")
    card.add_argument("--show", action="store_true", help="print the card text")
    card.add_argument("--exact", action="store_true", help="also count exact tokens with the provider (needs a key)")
    metrics = sub.add_parser("metrics", help="profile vs card size, context call cost, LLM spend")
    metrics.add_argument("--run-id", type=uuid.UUID)
    tool = sub.add_parser("tool", help="run one agent tool by hand (logged like an agent call)")
    tool.add_argument("name", help="tool name (see `tool list`)")
    tool.add_argument("--run-id", type=uuid.UUID)
    tool.add_argument("--args", default="{}", help="JSON arguments")
    rules = sub.add_parser("validate-rules", help="validate a silver rules YAML file against a run")
    rules.add_argument("path", type=Path)
    rules.add_argument("--run-id", type=uuid.UUID)
    defaults = sub.add_parser("default-rules", help="write default rules for issue-free columns (YAML to stdout)")
    defaults.add_argument("--run-id", type=uuid.UUID)
    run_all = sub.add_parser("run-all", help="ingest a CSV, profile it, add context, render the card")
    run_all.add_argument("path", help="path to the CSV file")
    run_all.add_argument("--force", action="store_true", help="reload even if already ingested")
    run_all.add_argument("--context-file", type=Path, help="human-written context (skips the LLM draft)")
    for name in STUB_COMMANDS:
        sub.add_parser(name)
    return parser


def _reconciled(run: dict[str, Any]) -> bool:
    accounted = (run["rows_loaded_ok"] or 0) + (run["rows_loaded_malformed"] or 0) + (run["load_failed"] or 0)
    return run["status"] == "succeeded" and accounted == run["records_parsed"]


def format_funnel(run: dict[str, Any]) -> str:
    """Render one run's load funnel as a small table."""
    reasons = ", ".join(f"{k}: {v}" for k, v in (run["malformed_by_reason"] or {}).items()) or "-"
    batches, fallback = run["batches"] or 0, run["batches_fallback"] or 0
    lines = [
        ("run_id", run["run_id"]),
        ("status", run["status"]),
        ("encoding", run["encoding"]),
        ("physical lines", run["physical_lines"]),
        ("records parsed", run["records_parsed"]),
        ("loaded ok", run["rows_loaded_ok"]),
        ("loaded malformed", f"{run['rows_loaded_malformed']} ({reasons})"),
        ("load failed", run["load_failed"]),
        ("batches", f"{batches} (copy {batches - fallback}, fallback {fallback})"),
        ("duplicate hash groups", run["duplicate_hash_groups"]),
        ("duration", f"{run['duration_ms']} ms"),
        ("reconciliation", "✓" if _reconciled(run) else "✗"),
    ]
    if run["error_message"]:
        lines.append(("error", run["error_message"]))
    width = max(len(label) for label, _ in lines)
    return "\n".join(f"  {label:<{width}}  {value}" for label, value in lines)


def format_runs(runs: list[dict[str, Any]]) -> str:
    """Render recent runs as one line each."""
    header = f"{'started_at':<20} {'status':<9} {'records':>8} {'ok':>7} {'malf':>5} {'fail':>5} {'dup':>5}  run_id / file"
    rows = [
        f"{r['started_at']:%Y-%m-%d %H:%M:%S} {r['status']:<9} {r['records_parsed'] or 0:>8} "
        f"{r['rows_loaded_ok'] or 0:>7} {r['rows_loaded_malformed'] or 0:>5} {r['load_failed'] or 0:>5} "
        f"{r['duplicate_hash_groups'] or 0:>5}  {r['run_id']}  {r['source_file']}"
        for r in runs
    ]
    return "\n".join([header, *rows]) if rows else "no runs yet"


def _pct(share: float) -> str:
    return f"{share * 100:.1f}%"


def format_profile(profile: dict[str, Any]) -> str:
    """Columns table, then issues sorted by severity."""
    header = f"{'column':<20} {'type (confidence)':<22} {'empty/null':>10} {'distinct':>9}  role"
    lines = ["COLUMNS", header]
    for p in profile["columns"]:
        t = p["type_inference"]
        lines.append(
            f"{p['column']:<20} {t['dominant_type'] + ' (' + _pct(t['confidence']) + ')':<22} "
            f"{_pct(p['completeness']['empty_or_null_like_share']):>10} {p['cardinality']['distinct']:>9}  "
            f"{p['cardinality']['role_hint']}"
        )
    lines += ["", f"ISSUES ({len(profile['issues'])})"]
    for i in profile["issues"]:
        evidence = json.dumps(i["evidence"], ensure_ascii=False)
        evidence = evidence if len(evidence) <= 140 else evidence[:137] + "..."
        lines.append(f"{i['severity'].upper():<6} {i['code']:<18} {i['column'] or '(dataset)':<26} {evidence}")
    return "\n".join(lines)


def _profile(run_id: uuid.UUID) -> None:
    profile_run(run_id)
    print(format_profile(load_profile(run_id)))


def format_context(context: dict[str, Any]) -> str:
    state = "approved" if context["approved"] else "UNAPPROVED (review, then `context --approve`)"
    body = json.dumps(context["draft"], indent=2, ensure_ascii=False) if context["draft"] else context["text"]
    return f"CONTEXT ({context['source']}, {state})\n{body}"


def _context(run_id: uuid.UUID, context_file: Path | None, redraft: bool = False) -> None:
    human = context_file.read_text(encoding="utf-8") if context_file else None
    print(format_context(ensure_context(run_id, human, redraft=redraft)))


def _card(run_id: uuid.UUID, budget: int | None = None, show: bool = True, exact: bool = False) -> None:
    card = render_card(run_id, budget, exact=exact)
    if show:
        print(card.text)
    print(format_rows(run_metrics(run_id)))


def _tool(name: str, raw_args: str, run_id: uuid.UUID) -> None:
    if name == "list":
        for tool_name, (_, _, desc) in TOOLS.items():
            print(f"{tool_name:<20} {desc}")
        return
    print(json.dumps(run_tool(name, json.loads(raw_args), run_id), indent=2, ensure_ascii=False, default=str))


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "init-db":
        migrate()
    elif args.command == "ingest":
        run_id = ingest_file(args.path, force=args.force)
        print(format_funnel(get_run(run_id)))
    elif args.command == "runs":
        print(format_runs(recent_runs(args.limit)))
    elif args.command == "profile":
        _profile(args.run_id or latest_succeeded_run())
    elif args.command == "context":
        run_id = args.run_id or latest_succeeded_run()
        if args.compare:
            print("\n".join(compare_contexts(run_id)))
        elif args.approve and not args.context_file:
            print("approved" if approve_context(run_id) else "no inferred context stored for this run yet")
        else:
            _context(run_id, args.context_file, redraft=args.redraft)
    elif args.command == "card":
        _card(args.run_id or latest_succeeded_run(), args.budget, show=args.show, exact=args.exact)
    elif args.command == "metrics":
        print(format_rows(run_metrics(args.run_id or latest_succeeded_run())))
    elif args.command == "tool":
        _tool(args.name, args.args, args.run_id or latest_succeeded_run())
    elif args.command == "validate-rules":
        report = validate_rules(from_yaml(args.path.read_text(encoding="utf-8")), args.run_id or latest_succeeded_run())
        print(report.feedback())
        return 0 if report.ok else 1
    elif args.command == "default-rules":
        print(to_yaml(default_rules(args.run_id or latest_succeeded_run())))
    elif args.command == "run-all":
        run_id = ingest_file(args.path, force=args.force)
        print(format_funnel(get_run(run_id)))
        _profile(run_id)
        try:
            _context(run_id, args.context_file)
        except LLMError as exc:  # no key / no recording: the card still renders, marked "no context"
            get_logger("run-all", run_id=str(run_id)).warning("context skipped", error=str(exc)[:200])
        _card(run_id)
    else:
        get_logger(args.command).warning("not implemented yet", command=args.command)
    return 0


if __name__ == "__main__":
    sys.exit(main())
