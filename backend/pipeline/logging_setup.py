"""Structured key=value logging to stdout.

Usage:
    log = get_logger("bronze", run_id="abc")
    log.info("rows loaded", rows=10000)
    # 2026-11-12T12:00:00Z level=INFO stage=bronze run_id=abc msg="rows loaded" rows=10000
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from pipeline.config import settings

_ROOT = "pipeline"
# Keyword args that logging itself understands; everything else becomes a key=value field.
_RESERVED_KWARGS = {"exc_info", "stack_info", "stacklevel", "extra"}


def _fmt_value(value: Any) -> str:
    text = str(value)
    if text == "" or any(c in text for c in ' "='):
        return json.dumps(text)
    return text


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        fields = {
            "level": record.levelname,
            "stage": getattr(record, "stage", "-"),
            "run_id": getattr(record, "run_id", "-"),
            "msg": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        line = ts + " " + " ".join(f"{k}={_fmt_value(v)}" for k, v in fields.items())
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class _StageLogger(logging.LoggerAdapter):
    """Attaches stage/run_id and turns extra kwargs into key=value fields."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        fields = {k: kwargs.pop(k) for k in list(kwargs) if k not in _RESERVED_KWARGS}
        kwargs["extra"] = {**self.extra, "fields": fields}
        return msg, kwargs


def _configure_root() -> logging.Logger:
    root = logging.getLogger(_ROOT)
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_KeyValueFormatter())
        root.addHandler(handler)
        root.setLevel(settings.log_level)
        root.propagate = False
    return root


def get_logger(stage: str, run_id: str = "-") -> _StageLogger:
    """Return a logger that tags every line with the given stage and run_id."""
    _configure_root()
    return _StageLogger(logging.getLogger(f"{_ROOT}.{stage}"), {"stage": stage, "run_id": run_id})
