"""Rule-by-rule review of a proposal: approve or reject each column rule, row check and dedup.

Decisions live next to the proposal in rules/<run>/decisions.json (an audit record that stays
after approval). Approving records the rule's fingerprint, so a later edit makes the decision
stale. Rejecting changes proposed.yaml at once, through the validator like any edit:
  column rule -> the column's safe default rule (trim + hidden characters, type only when
                 >= 99% sure), keeping the target name so other rules still refer to it;
  row check / dedup -> removed;
and every issue only that rule fixed becomes `ignored` with the reviewer's reason, so coverage
still holds and the reason is on record. Undo restores the rule and the issue resolutions.
When every rule is decided, `finish` runs the graph's approval, which re-validates the file.
"""

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.profiling import load_profile
from pipeline.silver.defaults import default_column_rule
from pipeline.silver.rules_schema import SilverRules, from_yaml, proposed_path, to_yaml
from pipeline.web import rules_edit

MIN_REASON = 10
REJECTED_PREFIX = "Reviewer rejected the rule:"  # an ignored issue needs a reason of >= 10 characters (rules_schema)


class ReviewError(ValueError):
    """The decision can't be applied (unknown rule, not editable, missing reason, ...)."""


def _path(run_id: uuid.UUID | str) -> Path:
    return proposed_path(str(run_id)).parent / "decisions.json"


def load(run_id: uuid.UUID | str) -> dict[str, dict[str, Any]]:
    """Per-rule decisions of a run (rules/<run>/decisions.json)."""
    path = _path(run_id)
    return json.loads(path.read_text()) if path.exists() else {}


def _write(run_id: uuid.UUID | str, decisions: dict[str, Any]) -> None:
    _path(run_id).write_text(json.dumps(decisions, indent=2, sort_keys=True))


def clear(run_id: uuid.UUID | str) -> None:
    """Forget all per-rule decisions of a run."""
    _path(run_id).unlink(missing_ok=True)


def fingerprint(rule: dict[str, Any] | None) -> str | None:
    """Hash of a rule's content: a decision goes stale when the rule changes after it."""
    if rule is None:
        return None
    content = {k: v for k, v in rule.items() if k != "generated_by"}  # approval re-labels authors; not a change
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()[:16]


def row_check_key(check: dict[str, Any]) -> str:
    """Stable review key of a row check (kind + params)."""
    return f"row_check:{check['kind']}:{json.dumps(check['params'], sort_keys=True)}"


def items(rules: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    """(key, kind, rule) for every reviewable rule, in page order."""
    out = [(f"column:{c['name']}", "column", c) for c in rules.get("columns", [])]
    out += [(row_check_key(ch), "row_check", ch) for ch in rules.get("row_checks", [])]
    if rules.get("dedup"):
        out.append(("dedup", "dedup", rules["dedup"]))
    return out


def _find(rules: dict[str, Any], key: str) -> dict[str, Any] | None:
    return next((r for k, _, r in items(rules) if k == key), None)


def status(run_id: uuid.UUID | str, rules: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """key -> {decision: pending|approved|rejected|stale, reason, at, original}."""
    decisions = load(run_id)
    out = {}
    for key, _, rule in items(rules):
        d = decisions.get(key)
        if not d:
            out[key] = {"decision": "pending"}
        elif d["fingerprint"] != fingerprint(rule):
            out[key] = {**d, "decision": "stale", "was": d["decision"]}
        else:
            out[key] = d
    return out


def summary(states: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Counts per decision state, and whether the review can be finished."""
    counts = {s: sum(v["decision"] == s for v in states.values()) for s in ("pending", "approved", "rejected", "stale")}
    return {**counts, "total": len(states), "can_finish": bool(states) and counts["pending"] == counts["stale"] == 0}


# --- decisions -----------------------------------------------------------------------------


def _current(run_id: uuid.UUID) -> dict[str, Any]:
    if not rules_edit.editable(run_id):
        raise ReviewError("the rules can only be reviewed while they wait for approval")
    return SilverRules.model_validate(from_yaml(proposed_path(str(run_id)).read_text())).model_dump(mode="json")


def _cited(rules: dict[str, Any]) -> set[str]:
    return {a for _, _, r in items(rules) for a in r.get("addresses", [])}


def _save(run_id: uuid.UUID, rules: dict[str, Any], decisions: dict[str, Any]) -> dict[str, Any]:
    """Validate and write proposed.yaml + validation.json, then the decisions; nothing on failure."""
    report = rules_edit.save(run_id, to_yaml(SilverRules.model_validate(rules)))
    if report["ok"]:
        _write(run_id, decisions)
    return report


def approve(run_id: uuid.UUID, key: str) -> dict[str, Any]:
    """Approve one rule as it is now."""
    rules = _current(run_id)
    rule = _find(rules, key)
    if rule is None:
        raise ReviewError(f"no rule {key!r} in the proposal")
    decisions = load(run_id)
    decisions[key] = {"decision": "approved", "fingerprint": fingerprint(rule), "at": datetime.now(UTC).isoformat()}
    _write(run_id, decisions)
    return {"ok": True}


def reject(run_id: uuid.UUID, key: str, reason: str) -> dict[str, Any]:
    """Reject one rule with a reason (rejected column rules fall back to their default rule)."""
    reason = (reason or "").strip()
    if len(reason) < MIN_REASON:
        raise ReviewError(f"please give a reason of at least {MIN_REASON} characters")
    rules = _current(run_id)
    rule = _find(rules, key)
    if rule is None:
        raise ReviewError(f"no rule {key!r} in the proposal")
    if key.startswith("column:"):
        profiles = {p["column"]: p for p in load_profile(uuid.UUID(str(run_id)))["columns"]}
        if rule["source"] not in profiles:
            raise ReviewError(f"no health check for column {rule['source']!r}")
        fallback = default_column_rule(profiles[rule["source"]]).model_dump(mode="json")
        fallback |= {"name": rule["name"], "generated_by": "human",
                     "why": f"Reviewer rejected the AI's rule ({reason[:200]}); safe default applied."}  # fmt: skip
        rules["columns"] = [fallback if c is rule else c for c in rules["columns"]]
        replacement: dict[str, Any] | None = fallback
    elif key == "dedup":
        rules["dedup"], replacement = None, None
    else:
        position = rules["row_checks"].index(rule)
        rules["row_checks"] = [c for c in rules["row_checks"] if c is not rule]
        replacement = None
    still_cited = _cited(rules)
    dropped = [i for i in rule.get("addresses", []) if i not in still_cited]
    before = {r["issue_id"]: r for r in rules.get("addressed_issues", [])}
    ignored = {i: {"issue_id": i, "resolution": "ignored", "reason": f"{REJECTED_PREFIX} {reason}"} for i in dropped}
    # replaced in place, so an undo gives back the identical file
    rules["addressed_issues"] = [ignored.get(r["issue_id"], r) for r in rules.get("addressed_issues", [])]
    rules["addressed_issues"] += [ignored[i] for i in dropped if i not in before]
    decisions = load(run_id)
    decisions[key] = {"decision": "rejected", "reason": reason, "at": datetime.now(UTC).isoformat(),
                      "fingerprint": fingerprint(replacement), "original": rule,
                      "position": position if key.startswith("row_check:") else None,
                      "original_resolutions": [before[i] for i in dropped if i in before]}  # fmt: skip
    return _save(run_id, rules, decisions)


def undo(run_id: uuid.UUID, key: str) -> dict[str, Any]:
    """Withdraw a decision on one rule (a rejection's change is reverted)."""
    decisions = load(run_id)
    record = decisions.pop(key, None)
    if record is None:
        raise ReviewError(f"no decision for {key!r}")
    if record["decision"] == "approved":
        _current(run_id)  # still editable?
        _write(run_id, decisions)
        return {"ok": True}
    rules = _current(run_id)
    original = record["original"]
    if key.startswith("column:"):
        rules["columns"] = [original if c["name"] == original["name"] else c for c in rules["columns"]]
    elif key == "dedup":
        rules["dedup"] = original
    else:
        checks = list(rules.get("row_checks", []))
        checks.insert(record.get("position") or len(checks), original)
        rules["row_checks"] = checks
    restored = {r["issue_id"]: r for r in record.get("original_resolutions", [])}
    ids = set(original.get("addresses", []))

    def mine(r: dict[str, Any]) -> bool:
        return r["issue_id"] in ids and r["resolution"] == "ignored" and r.get("reason", "").startswith(REJECTED_PREFIX)

    rules["addressed_issues"] = [restored.get(r["issue_id"], r) if mine(r) else r for r in rules.get("addressed_issues", [])
                                 if not mine(r) or r["issue_id"] in restored]  # fmt: skip
    return _save(run_id, rules, decisions)


def ready(run_id: uuid.UUID) -> dict[str, Any]:
    """Raise unless every rule has a current decision."""
    rules = _current(run_id)
    s = summary(status(run_id, rules))
    if not s["can_finish"]:
        raise ReviewError(f"{s['pending'] + s['stale']} rule(s) still need a decision")
    return s
