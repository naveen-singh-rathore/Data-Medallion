"""System prompt (versioned file) and the task message for the DQ agent."""

from typing import Any

from pipeline.config import settings
from pipeline.silver.rules_schema import rules_json_schema

PROMPT_VERSION = "dq_agent_v1"
SUBMIT_TOOL = "submit_rules"


def system_prompt(version: str = PROMPT_VERSION) -> str:
    """The DQ agent's system prompt (prompts/<version>.md)."""
    return (settings.prompts_dir / f"{version}.md").read_text().strip()


def submit_rules_tool() -> dict[str, Any]:
    """The submit_rules tool: its input schema is the SilverRules JSON schema."""
    return dict(
        name=SUBMIT_TOOL,
        description="Submit the complete SilverRules for the flagged columns (call once; resubmit only after "
        "validation feedback). Default rules for the other columns are merged in by the pipeline.",
        input_schema=rules_json_schema(),
    )


def task_message(context: dict[str, Any] | None, card: str, flagged: list[str], defaults: list[str],
                 required_ids: list[str], tool_budget: int) -> str:  # fmt: skip
    """The first user message: context, dataset card, flagged columns and the issues to cover."""
    context_text = f"{context['text'].strip()} (source: {context['source']})" if context else "(none provided)"
    return (
        f"BUSINESS CONTEXT\n{context_text}\n\n"
        f"DATASET CARD\n{card.strip()}\n\n"
        "TASK\n"
        f"Flagged columns needing rules: {', '.join(flagged) or '-'}.\n"
        f"Columns already covered by default rules (do not redo unless an issue requires it): {', '.join(defaults) or '-'}.\n"
        f"HIGH/MEDIUM issue ids you must address: {', '.join(required_ids) or '-'}.\n"
        f"Budget: ~{tool_budget} tool calls. Finish with one submit_rules call."
    )
