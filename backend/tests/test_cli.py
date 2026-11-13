"""CLI dispatch and its one-line errors for expected failures."""

import argparse

import psycopg
import pytest

from pipeline import cli
from pipeline.errors import MissingRunError
from pipeline.llm import LLMCacheMiss, LLMError


def test_every_subcommand_has_a_handler() -> None:
    sub = next(a for a in cli._build_parser()._actions if isinstance(a, argparse._SubParsersAction))
    assert set(sub.choices) == set(cli.COMMANDS)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (LLMCacheMiss("dq_agent answer not in the recorded cache; set LLM_MODE=live"), "LLM_MODE=live"),
        (LLMError("ServiceUnavailableError: Error code: 503"), "LLM call failed"),
        (psycopg.OperationalError("connection refused\nIs the server running?"), "database not reachable"),
        (MissingRunError("no succeeded ingestion run found; run `ingest` first"), "run `ingest` first"),
    ],
)
def test_expected_failures_print_one_line_and_exit_2(monkeypatch, capsys, error, expected) -> None:
    def fail(_args):
        raise error

    monkeypatch.setitem(cli.COMMANDS, "dq-status", fail)
    assert cli.main(["dq-status"]) == 2
    err = capsys.readouterr().err
    assert expected in err and len(err.strip().splitlines()) == 1 and "Traceback" not in err


def test_unexpected_errors_are_not_swallowed(monkeypatch) -> None:
    def bug(_args):
        raise KeyError("a bug")

    monkeypatch.setitem(cli.COMMANDS, "dq-status", bug)
    with pytest.raises(KeyError):
        cli.main(["dq-status"])
