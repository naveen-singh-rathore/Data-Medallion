"""Expected, user-facing pipeline failures.

Everything that subclasses PipelineError has an actionable message (a missing prerequisite, a failed
reconciliation, an LLM answer that isn't recorded). Entry points show the message on one line; any
other exception is a bug and keeps its traceback. Each subclass also keeps its historical builtin
base (RuntimeError / ValueError), so existing `except` clauses behave as before.
"""


class PipelineError(Exception):
    """Base class for expected failures with an actionable message."""


class MissingRunError(PipelineError, ValueError):
    """The requested ingestion run (or its profile) does not exist yet."""
