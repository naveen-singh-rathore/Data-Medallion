"""Background work for the UI: one job at a time per run, progress kept in memory.

Every result a job produces is written by the pipeline's own functions to Postgres / rules/, so
the UI's views read the truth from there; the job record only says what is running now and the
last error. It is per process: after a restart the views still show everything that finished.
"""

import threading
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from pipeline.card import render_card
from pipeline.context import ensure_context, get_context
from pipeline.llm import LLMError
from pipeline.logging_setup import get_logger
from pipeline.profiling import profile_run


class Busy(RuntimeError):
    """Another job is already running for this run."""


@dataclass
class Job:
    run_id: str
    kind: str
    state: str = "running"  # running | done | error
    step: str = ""
    error: str | None = None
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    def log(self, step: str, message: str, state: str = "done") -> None:
        self.events.append({"at": datetime.now(UTC).isoformat(), "step": step, "state": state, "message": message})


_jobs: dict[str, Job] = {}
_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ui-job")


def current(run_id: uuid.UUID | str) -> dict[str, Any] | None:
    job = _jobs.get(str(run_id))
    return asdict(job) if job else None


def _start(run_id: uuid.UUID | str, kind: str, work: Callable[[Job], None]) -> dict[str, Any]:
    key = str(run_id)
    with _lock:
        if (existing := _jobs.get(key)) and existing.state == "running":
            raise Busy(f"'{existing.kind}' is still running for this run")
        job = Job(key, kind)
        _jobs[key] = job

    def run() -> None:
        log = get_logger("web.job", run_id=key)
        try:
            work(job)
            job.state = "done"
        except Exception as exc:  # noqa: BLE001 - shown in the UI, also logged
            job.state, job.error = "error", f"{type(exc).__name__}: {str(exc)[:500]}"
            job.log(job.step, job.error, "error")
            log.error("job failed", kind=kind, step=job.step, error=job.error, trace=traceback.format_exc()[-1500:])
        finally:
            job.finished_at = datetime.now(UTC).isoformat()

    _pool.submit(run)
    return asdict(job)


def _step(job: Job, step: str, message: str) -> None:
    job.step = step
    job.log(step, message, "running")


# --- the steps after an upload ---------------------------------------------------------


def _draft_context(job: Job, run_id: uuid.UUID) -> None:
    _step(job, "context", "The AI is working out what this data is for")
    try:
        ctx = ensure_context(run_id)
        job.log("context", f"Business context decided ({'by a person' if ctx['source'] == 'human' else 'by AI'})")
    except LLMError as exc:
        # Not fatal: the card renders without context; the review retries the draft or runs without it.
        job.log("context", f"AI could not write the business context ({str(exc)[:160]})", "error")


def _card(job: Job, run_id: uuid.UUID) -> None:
    _step(job, "card", "Preparing the briefing the AI reviewer reads")
    card = render_card(run_id)
    m = card.metrics
    job.log("card", f"Briefing ready: {m.get('issues_shown', '?')}/{m.get('issues_total', '?')} issues")


def start_intake(run_id: uuid.UUID) -> dict[str, Any]:
    """profile -> context draft -> card (bronze already ran in the upload request)."""

    def work(job: Job) -> None:
        job.log("bronze", "Raw data stored in Bronze")
        _step(job, "profile", "Checking every column for gaps, formats and placeholders")
        profile_run(run_id)
        job.log("profile", "Health check finished")
        _draft_context(job, run_id)
        _card(job, run_id)

    return _start(run_id, "intake", work)


def start_reprofile(run_id: uuid.UUID) -> dict[str, Any]:
    return start_intake(run_id)


def save_human_context(run_id: uuid.UUID, text: str) -> dict[str, Any]:
    def work(job: Job) -> None:
        _step(job, "context", "Saving your business description")
        ensure_context(run_id, text)
        job.log("context", "Business context saved; it replaces the AI's version")
        _card(job, run_id)

    return _start(run_id, "context", work)


def warm_preview(run_id: uuid.UUID) -> None:
    """Compute the silver preview in the background so the Silver step opens instantly."""
    from pipeline.web import views

    if not views.is_cached(run_id):
        _pool.submit(lambda: views.silver_preview(run_id))


def start_agent(run_id: uuid.UUID, fresh: bool) -> dict[str, Any]:
    from pipeline.dq_agent import runner

    def work(job: Job) -> None:
        if get_context(run_id) is None:
            _draft_context(job, run_id)
        _step(job, "agent", "The AI reviewer is investigating the data and writing cleaning rules")
        result = runner.run(str(run_id), fresh=fresh)
        if result["status"] == "failed":
            raise RuntimeError("the AI could not produce rules that pass validation (see the review step)")
        job.log("agent", f"AI review stopped: {result['status']}")
        warm_preview(run_id)

    return _start(run_id, "agent", work)


def auto_map(run_id: uuid.UUID) -> dict[str, Any]:
    from pipeline.web import rules_edit

    def work(job: Job) -> None:
        _step(job, "automap", "The AI is deciding the values left outside the standard lists")
        result = rules_edit.auto_map(run_id)
        job.log("automap", f"AI decided {result['decided']} value(s)"
                + (f"; private columns not sent: {', '.join(result['skipped_private'])}" if result["skipped_private"] else ""))
        warm_preview(run_id)

    return _start(run_id, "automap", work)


def decide_rules(run_id: uuid.UUID, approve: bool, reason: str | None) -> dict[str, Any]:
    from pipeline.dq_agent import runner

    def work(job: Job) -> None:
        _step(job, "resume", "Re-validating the rules and saving your decision" if approve else "Recording the rejection")
        payload = {"action": "approve"} if approve else {"action": "reject", "reason": reason or ""}
        result = runner.resume(str(run_id), payload)
        errors = (result.get("paused") or {}).get("errors") or []
        if approve and errors:
            raise RuntimeError("the edited rules did not pass validation: " + "; ".join(errors)[:400])
        job.log("resume", f"Decision saved: {result['status']}")
        warm_preview(run_id)

    return _start(run_id, "resume", work)
