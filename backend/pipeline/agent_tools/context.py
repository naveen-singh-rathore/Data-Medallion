"""Per-call facts a tool needs: profiles, issues, header, context tags (all from meta)."""

import uuid
from dataclasses import dataclass
from typing import Any

from pipeline.context import get_context
from pipeline.privacy import column_policies, context_tags
from pipeline.profiling import load_profile


class ToolError(ValueError):
    """A problem with the tool arguments; the message is returned to the agent."""


@dataclass(frozen=True)
class RunView:
    run_id: str
    profiles: dict[str, dict[str, Any]]  # column -> profile
    issues: list[dict[str, Any]]
    tags: dict[str, str]
    policies: dict[str, str]

    def profile(self, column: str) -> dict[str, Any]:
        if column not in self.profiles:
            raise ToolError(f"unknown column {column!r}; columns: {list(self.profiles)}")
        return self.profiles[column]


def run_view(run_id: str) -> RunView:
    profile = load_profile(uuid.UUID(run_id))
    context = get_context(uuid.UUID(run_id))
    return RunView(run_id, {p["column"]: p for p in profile["columns"]}, profile["issues"], context_tags(context),
                   column_policies(profile["columns"], context))  # fmt: skip
