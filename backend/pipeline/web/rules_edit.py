"""Edit a run's proposed rules (rules/<run>/proposed.yaml) from the UI.

Same contract as editing the file by hand: an edit is only written when it passes the full
validator (schema, meaning, dry run, coverage), and the approve step re-reads and re-validates
the file anyway, marking changed columns as made by a person. Edits are allowed only while the
proposal waits for approval.
"""

import json
import uuid
from typing import Any

import yaml

from pipeline.profile_values import normalization_key
from pipeline.silver.rules_schema import SilverRules, from_yaml, proposed_path, to_yaml
from pipeline.silver.validator import validate_rules


class NotEditable(RuntimeError):
    """The proposal is not waiting for approval (approved, rejected, or no proposal)."""


def _strip(rule: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in rule.items() if k != "generated_by"}


def agent_rules(run_id: uuid.UUID) -> dict[str, Any] | None:
    """The rules exactly as the AI proposed them (kept in the agent checkpoint)."""
    from pipeline.dq_agent import runner

    thread_id = runner.latest_thread(str(run_id))
    if not thread_id:
        return None
    with runner.compiled() as graph:
        return (graph.get_state(runner.config(thread_id)).values or {}).get("agent_rules")


def edited_columns(run_id: uuid.UUID, current: dict[str, Any]) -> list[str]:
    """Target columns whose rule differs from the AI's (added columns count as edited)."""
    original = agent_rules(run_id)
    if not original:
        return []
    theirs = {c["name"]: _strip(c) for c in original.get("columns", [])}
    return [c["name"] for c in current.get("columns", []) if theirs.get(c["name"]) != _strip(c)]


def editable(run_id: uuid.UUID) -> bool:
    from pipeline.web.views import agent_snapshot

    snap = agent_snapshot(run_id)
    return bool(snap and snap["status"] == "awaiting_approval" and proposed_path(str(run_id)).exists())


def read(run_id: uuid.UUID) -> dict[str, Any] | None:
    path = proposed_path(str(run_id))
    if not path.exists():
        return None
    original = agent_rules(run_id)
    return {"text": path.read_text(), "editable": editable(run_id),
            "ai_text": to_yaml(SilverRules.model_validate(original)) if original else None}  # fmt: skip


def check(run_id: uuid.UUID, text: str) -> dict[str, Any]:
    """Parse + validate without writing. Returns {ok, layer, errors, warnings, stats}."""
    try:
        raw = from_yaml(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f"line {mark.line + 1}, column {mark.column + 1}: " if mark else ""
        return {"ok": False, "layer": "yaml", "errors": [f"{where}{getattr(exc, 'problem', None) or exc}"],
                "warnings": [], "stats": {}}  # fmt: skip
    except TypeError as exc:
        return {"ok": False, "layer": "yaml", "errors": [str(exc)], "warnings": [], "stats": {}}
    report = validate_rules(raw, run_id)
    return {"ok": report.ok, "layer": report.layer, "errors": report.errors, "warnings": report.warnings,
            "stats": report.stats}  # fmt: skip


def save(run_id: uuid.UUID, text: str) -> dict[str, Any]:
    """Validate, then write proposed.yaml (normalised) and validation.json. Nothing is written if invalid."""
    if not editable(run_id):
        raise NotEditable("the rules can only be edited while they wait for approval")
    report = check(run_id, text)
    if report["ok"]:
        rules = SilverRules.model_validate(from_yaml(text))
        path = proposed_path(str(run_id))
        path.write_text(to_yaml(rules))
        (path.parent / "validation.json").write_text(json.dumps(report, indent=2, default=str))
    return report


def reset(run_id: uuid.UUID) -> dict[str, Any]:
    """Put the AI's original proposal back."""
    if not editable(run_id):
        raise NotEditable("the rules can only be edited while they wait for approval")
    original = agent_rules(run_id)
    if not original:
        raise NotEditable("the AI's original proposal is not available")
    return save(run_id, to_yaml(SilverRules.model_validate(original)))


# --- standard values: choices from the review page -------------------------------------------

VALUE_ACTIONS = ("merge", "new", "empty", "keep", "remove")


def set_values(run_id: uuid.UUID, column: str, choices: list[dict[str, Any]]) -> dict[str, Any]:
    """Apply review-page choices to one column's standard values, then save through the validator.

    merge  value -> an existing standard value (target)    new    value becomes its own standard value
    empty  value -> treated as empty (a null token)         keep   nothing
    remove a spelling (value) from a standard value (target)
    """
    if not editable(run_id):
        raise NotEditable("the rules can only be edited while they wait for approval")
    path = proposed_path(str(run_id))
    rules = SilverRules.model_validate(from_yaml(path.read_text())).model_dump(mode="json")
    rule = next((c for c in rules["columns"] if c["name"] == column), None)
    if rule is None:
        raise ValueError(f"no rule for column {column!r}")
    mapping: dict[str, list[str]] = {k: list(v) for k, v in (rule.get("mapping") or {}).items()}
    for choice in choices:
        action, value, target = choice.get("action"), str(choice.get("value") or "").strip(), choice.get("target")
        if action not in VALUE_ACTIONS:
            raise ValueError(f"unknown action {action!r}")
        if action == "keep":
            continue
        if not value:
            raise ValueError("a value is required")
        if action == "merge" and target not in mapping:
            raise ValueError(f"{target!r} is not a standard value of {column}")
        spellings = {value.lower(), *(str(x).strip().lower() for x in choice.get("spellings") or [])}
        for variants in mapping.values():  # a value belongs to one place only
            if action != "remove" and value in variants:
                variants.remove(value)
        if action != "empty":  # undo an earlier "treat as empty"
            rule["null_tokens"] = [t for t in rule.get("null_tokens", []) if t.lower() not in spellings]
        if action in ("merge", "empty", "remove") and value in mapping and target != value:
            if mapping[value]:
                raise ValueError(f"{value!r} is a standard value with its own spellings; move those first")
            del mapping[value]  # undo an earlier "new standard value"
        if action != "new":
            # a standard value left with no spellings whose own name matches this value existed only for it
            # (an earlier "new standard value"); it would keep matching the value by name, so it goes too
            key = normalization_key(value)
            for name in [t for t, vs in mapping.items() if not vs and t != target and normalization_key(t) == key]:
                del mapping[name]
        if action == "merge":
            mapping[target].append(value)
        elif action == "new":
            mapping.setdefault(value, [])
        elif action == "empty":  # null tokens match exact spellings (any case), so every spelling is listed
            tokens = list(rule.get("null_tokens", []))
            for spelling in [value, *(str(x).strip() for x in choice.get("spellings") or [])]:
                if spelling and spelling.lower() not in {t.lower() for t in tokens}:
                    tokens.append(spelling)
            rule["null_tokens"] = tokens
        elif action == "remove" and target in mapping and value in mapping[target]:
            mapping[target].remove(value)
    rule["mapping"] = mapping or None
    return save(run_id, to_yaml(SilverRules.model_validate(rules)))


def reopen(run_id: uuid.UUID) -> dict[str, Any]:
    """Take back an approval so the rules can be changed and approved again.

    The agent thread is put back at its approval pause (as if the leftover mapping had just run,
    so no AI call is made) and the approved rules become the proposal again. The approved file is
    left in place (silver and gold stay valid) until the revised rules are approved.
    """
    from pipeline.db import transaction
    from pipeline.dq_agent import runner

    thread_id = runner.latest_thread(str(run_id))
    if not thread_id:
        raise NotEditable("there is no review to reopen")
    with runner.compiled() as graph:
        snap = runner.snapshot(graph, thread_id)
        if snap["status"] != "approved" or snap["next"]:
            raise NotEditable("only a finished, approved review can be reopened")
        approved_rules = (graph.get_state(runner.config(thread_id)).values or {}).get("proposed_rules")
        graph.update_state(runner.config(thread_id), {"status": "awaiting_approval", "approval_errors": None},
                           as_node="map_leftovers")  # fmt: skip
        graph.invoke(None, runner.config(thread_id))  # runs `approve` up to its interrupt
        if runner.snapshot(graph, thread_id)["status"] != "awaiting_approval":
            raise RuntimeError("the review could not be put back at its approval step")
    path = proposed_path(str(run_id))
    # The approved file stays: silver and gold remain built from it until the revised rules are approved.
    if approved_rules:
        path.write_text(to_yaml(SilverRules.model_validate(approved_rules)))
    with transaction() as conn:
        conn.execute("UPDATE meta.agent_runs SET status = 'awaiting_approval', updated_at = now() WHERE thread_id = %s",
                     (thread_id,))  # fmt: skip
    return {"ok": True, "thread_id": thread_id}


def auto_map(run_id: uuid.UUID) -> dict[str, Any]:
    """Run the leftover-mapping step on the current proposal (for proposals made before it existed,
    or after edits). The thread is stepped back through `map_leftovers`, so the AI's decisions count
    as the AI's proposal (not as human edits), and it pauses at approval again."""
    from pipeline.config import settings
    from pipeline.dq_agent import runner
    from pipeline.silver import mapping_assist

    if not editable(run_id):
        raise NotEditable("the rules can only be changed while they wait for approval")
    path = proposed_path(str(run_id))
    rules = SilverRules.model_validate(from_yaml(path.read_text()))
    done = mapping_assist.complete(str(run_id), rules)
    report = mapping_assist.write(path.parent, done, settings.cheap_model)
    if report is not None:
        dumped = done["rules"].model_dump(mode="json")
        thread_id = runner.latest_thread(str(run_id))
        with runner.compiled() as graph:
            graph.update_state(runner.config(thread_id), {"proposed_rules": dumped, "agent_rules": dumped,
                                                          "validation": report}, as_node="map_leftovers")  # fmt: skip
            graph.invoke(None, runner.config(thread_id))  # back to the approval pause
    return {"ok": True, "decided": sum(map(len, done["suggestions"].values())), "dropped": len(done["dropped"]),
            "skipped_private": done["skipped_private"], "cost_usd": done["cost_usd"]}  # fmt: skip


def mapping_decisions(run_id: uuid.UUID, rules: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """The AI's leftover decisions per column, with what the rules do with each value now."""
    import json

    path = proposed_path(str(run_id)).parent / "mapping_suggestions.json"
    if not path.exists():
        return {}
    suggestions = json.loads(path.read_text()).get("suggestions", {})
    out: dict[str, list[dict[str, Any]]] = {}
    for c in rules.get("columns", []):
        mapping = c.get("mapping") or {}
        tokens = {t.lower() for t in c.get("null_tokens", [])}
        for d in suggestions.get(c["name"], []):
            value = d["value"]
            if value.lower() in tokens:
                now = {"action": "empty", "target": None}
            elif value in mapping:
                now = {"action": "new", "target": value}
            else:
                hit = next((t for t, vs in mapping.items() if value in vs), None)
                now = {"action": "new" if hit and hit == d.get("target") and d["action"] == "new" else "merge",
                       "target": hit} if hit else {"action": "keep", "target": None}  # fmt: skip
            changed = (now["action"], now["target"]) != (d["action"], d.get("target"))
            out.setdefault(c["name"], []).append({**d, "now": now, "changed": changed})
    return out
