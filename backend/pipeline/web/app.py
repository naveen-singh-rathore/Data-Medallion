"""HTTP layer for the pipeline UI. Run from backend/:

    ../.venv/bin/python -m uvicorn pipeline.web.app:app --port 8000      (or `make ui`)

Thin by design: routes call the pipeline's own functions (ingest, profile, context, card, DQ
agent) and the read-only views in pipeline.web.views. No pipeline logic lives here.
"""

import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from pipeline.bronze import find_run, hash_file, ingest_file
from pipeline.config import REPO_ROOT, settings
from pipeline.web import jobs, review, rules_edit, views

UPLOAD_DIR = REPO_ROOT / "data" / "uploads"
STATIC_DIR = Path(__file__).parent / "static"
MAX_UPLOAD_BYTES = 200 * 1024 * 1024
REQUIRED_FILE = File(...)

app = FastAPI(title="Data Medallion", docs_url="/api/docs", openapi_url="/api/openapi.json")


def _found(value: Any, what: str = "not found") -> Any:
    if value is None:
        raise HTTPException(404, what)
    return value


def _busy(call: Any) -> Any:
    try:
        return call()
    except jobs.Busy as exc:
        raise HTTPException(409, str(exc)) from exc


def _safe_name(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).name).strip("._") or "upload.csv"
    return stem[:120]


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "llm_mode": settings.llm_mode, "main_model": settings.main_model,
            "cheap_model": settings.cheap_model}  # fmt: skip


@app.get("/api/runs")
def runs() -> list[dict[str, Any]]:
    out = []
    for r in views.list_runs():
        job = jobs.current(r["run_id"])
        out.append(views.jsonable(r | {"file": views.file_label(r["source_file"]),
                                       "job": job and {"state": job["state"], "step": job["step"]}}))  # fmt: skip
    return out


@app.post("/api/upload")
async def upload(file: UploadFile = REQUIRED_FILE, force: bool = Query(False)) -> dict[str, Any]:
    """Store the file, land it in bronze (in the request: fast and reconciled), then profile,
    draft context and render the card in the background."""
    if not (file.filename or "").lower().endswith((".csv", ".txt", ".tsv")):
        raise HTTPException(415, "please upload a CSV file")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # unique per upload: a same-second re-upload must never overwrite a file a run points to
    path = UPLOAD_DIR / f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}__{_safe_name(file.filename or '')}"
    size = 0
    with path.open("wb") as out:
        while chunk := await file.read(1024 * 1024):
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                out.close()
                path.unlink(missing_ok=True)
                raise HTTPException(413, f"file is larger than {MAX_UPLOAD_BYTES // 1024 // 1024} MB")
            out.write(chunk)
    sha = await run_in_threadpool(hash_file, path)
    before = find_run(sha)
    if before and before[1] == "succeeded" and not force:
        path.unlink(missing_ok=True)  # same bytes already loaded: idempotent, show that run
        return {"run_id": str(before[0]), "duplicate": True}
    try:
        run_id = await run_in_threadpool(ingest_file, path, force)
    except Exception as exc:  # the failed run row is kept and shown
        failed = find_run(sha)
        if failed:
            return {"run_id": str(failed[0]), "duplicate": False, "error": str(exc)[:500]}
        raise HTTPException(422, f"could not load the file: {str(exc)[:300]}") from exc
    _busy(lambda: jobs.start_intake(run_id))
    return {"run_id": str(run_id), "duplicate": False}


@app.get("/api/runs/{run_id}")
def run_overview(run_id: uuid.UUID) -> dict[str, Any]:
    view = _found(views.overview(run_id, jobs.current(run_id)), "run not found")
    if view["rules_state"]:
        jobs.warm_preview(run_id)
    return view


@app.get("/api/runs/{run_id}/bronze")
def bronze(run_id: uuid.UUID) -> dict[str, Any]:
    _found(views.run_row(run_id), "run not found")
    return views.bronze_view(run_id)


@app.get("/api/runs/{run_id}/bronze/rows")
def bronze_rows(run_id: uuid.UUID, status: str | None = None, offset: int = 0, limit: int = 50,
                q: str | None = None) -> dict[str, Any]:  # fmt: skip
    return views.bronze_rows(run_id, status, offset, limit, q)


@app.get("/api/runs/{run_id}/profile")
def profile(run_id: uuid.UUID) -> dict[str, Any]:
    return _found(views.profile_view(run_id), "not profiled yet")


@app.get("/api/runs/{run_id}/profile/{column}")
def profile_column(run_id: uuid.UUID, column: str) -> dict[str, Any]:
    return _found(views.column_profile(run_id, column), "column not found")


@app.get("/api/runs/{run_id}/context")
def context(run_id: uuid.UUID) -> dict[str, Any]:
    return views.context_view(run_id)


class ContextIn(BaseModel):
    text: str = Field(min_length=20, max_length=20_000)


@app.post("/api/runs/{run_id}/context")
def context_save(run_id: uuid.UUID, body: ContextIn) -> dict[str, Any]:
    _found(views.run_row(run_id), "run not found")
    return _busy(lambda: jobs.save_human_context(run_id, body.text))


@app.post("/api/runs/{run_id}/reprofile")
def reprofile(run_id: uuid.UUID) -> dict[str, Any]:
    return _busy(lambda: jobs.start_reprofile(run_id))


@app.get("/api/runs/{run_id}/card")
def card(run_id: uuid.UUID) -> dict[str, Any]:
    return _found(views.card_view(run_id), "no briefing yet")


@app.get("/api/runs/{run_id}/agent")
def agent(run_id: uuid.UUID) -> dict[str, Any]:
    return views.agent_view(run_id)


class AgentStart(BaseModel):
    fresh: bool = False


@app.post("/api/runs/{run_id}/agent/start")
def agent_start(run_id: uuid.UUID, body: AgentStart) -> dict[str, Any]:
    _found(views.run_row(run_id), "run not found")
    return _busy(lambda: jobs.start_agent(run_id, body.fresh))


@app.get("/api/runs/{run_id}/rules")
def rules(run_id: uuid.UUID) -> dict[str, Any]:
    return _found(views.rules_view(run_id), "no rules proposed yet")


class Decision(BaseModel):
    approve: bool
    reason: str | None = Field(default=None, max_length=2000)


@app.post("/api/runs/{run_id}/rules/decision")
def rules_decision(run_id: uuid.UUID, body: Decision) -> dict[str, Any]:
    if not body.approve and not (body.reason and body.reason.strip()):
        raise HTTPException(422, "please give a reason for rejecting")
    return _busy(lambda: jobs.decide_rules(run_id, body.approve, body.reason))


class RulesText(BaseModel):
    text: str = Field(min_length=1, max_length=500_000)


def _idle(run_id: uuid.UUID) -> None:
    job = jobs.current(run_id)
    if job and job["state"] == "running":
        raise HTTPException(409, f"'{job['kind']}' is still running for this run")


@app.get("/api/runs/{run_id}/rules/yaml")
def rules_yaml(run_id: uuid.UUID) -> dict[str, Any]:
    return _found(rules_edit.read(run_id), "no proposed rules yet")


@app.post("/api/runs/{run_id}/rules/check")
def rules_check(run_id: uuid.UUID, body: RulesText) -> dict[str, Any]:
    """Validate an edited YAML without saving it (schema, meaning, dry run, coverage)."""
    return rules_edit.check(run_id, body.text)


@app.put("/api/runs/{run_id}/rules/yaml")
def rules_save(run_id: uuid.UUID, body: RulesText) -> dict[str, Any]:
    """Save an edit; only written when valid. Approve afterwards to use it."""
    _idle(run_id)
    try:
        report = rules_edit.save(run_id, body.text)
    except rules_edit.NotEditable as exc:
        raise HTTPException(409, str(exc)) from exc
    if report["ok"]:
        jobs.warm_preview(run_id)
    return report


@app.post("/api/runs/{run_id}/rules/reset")
def rules_reset(run_id: uuid.UUID) -> dict[str, Any]:
    _idle(run_id)
    try:
        report = rules_edit.reset(run_id)
    except rules_edit.NotEditable as exc:
        raise HTTPException(409, str(exc)) from exc
    if report["ok"]:
        review.clear(run_id)  # decisions were about the edited rules
    jobs.warm_preview(run_id)
    return report


class RuleDecision(BaseModel):
    key: str = Field(min_length=1, max_length=500)
    decision: str = Field(pattern="^(approve|reject|undo)$")
    reason: str | None = Field(default=None, max_length=2000)


@app.post("/api/runs/{run_id}/rules/review")
def rules_review(run_id: uuid.UUID, body: RuleDecision) -> dict[str, Any]:
    """Approve, reject (reason required) or undo the decision on ONE rule."""
    _idle(run_id)
    try:
        if body.decision == "approve":
            report = review.approve(run_id, body.key)
        elif body.decision == "reject":
            report = review.reject(run_id, body.key, body.reason or "")
        else:
            report = review.undo(run_id, body.key)
    except (review.ReviewError, rules_edit.NotEditable) as exc:
        raise HTTPException(409 if isinstance(exc, rules_edit.NotEditable) else 422, str(exc)) from exc
    if not report["ok"]:
        raise HTTPException(422, "the change would make the rules invalid: " + "; ".join(report["errors"])[:600])
    jobs.warm_preview(run_id)
    return report


class ValueChoice(BaseModel):
    value: str = Field(max_length=500)
    action: str = Field(pattern="^(merge|new|empty|keep|remove)$")
    target: str | None = Field(default=None, max_length=500)
    spellings: list[str] = Field(default_factory=list, max_length=100)  # other spellings of the same value


class ValueChoices(BaseModel):
    column: str = Field(min_length=1, max_length=200)
    choices: list[ValueChoice] = Field(min_length=1, max_length=500)


@app.post("/api/runs/{run_id}/rules/values")
def rules_values(run_id: uuid.UUID, body: ValueChoices) -> dict[str, Any]:
    """Merge leftover values into standard values, add new ones, treat as empty, or remove a spelling."""
    _idle(run_id)
    try:
        report = rules_edit.set_values(run_id, body.column, [c.model_dump() for c in body.choices])
    except rules_edit.NotEditable as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if not report["ok"]:
        raise HTTPException(422, "the change would make the rules invalid: " + "; ".join(report["errors"])[:600])
    jobs.warm_preview(run_id)
    return report


@app.post("/api/runs/{run_id}/rules/automap")
def rules_automap(run_id: uuid.UUID) -> dict[str, Any]:
    """Let the AI decide the values outside the standard lists (runs in the background)."""
    if not rules_edit.editable(run_id):
        raise HTTPException(409, "the rules can only be changed while they wait for approval")
    return _busy(lambda: jobs.auto_map(run_id))


@app.post("/api/runs/{run_id}/rules/reopen")
def rules_reopen(run_id: uuid.UUID) -> dict[str, Any]:
    """Withdraw a finished approval so the rules can be changed and approved again."""
    _idle(run_id)
    try:
        return rules_edit.reopen(run_id)
    except rules_edit.NotEditable as exc:
        raise HTTPException(409, str(exc)) from exc


@app.post("/api/runs/{run_id}/rules/finish")
def rules_finish(run_id: uuid.UUID) -> dict[str, Any]:
    """Every rule decided: run the final approval (re-validates and saves the approved rules)."""
    _idle(run_id)
    try:
        review.ready(run_id)
    except (review.ReviewError, rules_edit.NotEditable) as exc:
        raise HTTPException(409, str(exc)) from exc
    return _busy(lambda: jobs.decide_rules(run_id, True, None))


@app.post("/api/runs/{run_id}/silver/build")
def silver_build(run_id: uuid.UUID) -> dict[str, Any]:
    """Apply the approved rules and store the cleaned table (runs in the background)."""
    from pipeline.silver.build import BuildError, approved_rules

    try:
        approved_rules(str(run_id), views.APPROVED_PATH)
    except BuildError as exc:
        raise HTTPException(409, str(exc)) from exc
    return _busy(lambda: jobs.start_build(run_id))


@app.get("/api/runs/{run_id}/silver")
def silver(run_id: uuid.UUID) -> dict[str, Any]:
    return _found(views.silver_summary(run_id), "no rules to preview yet")


@app.get("/api/runs/{run_id}/silver/rows")
def silver_rows(run_id: uuid.UUID, show: str = "changed", column: str | None = None, offset: int = 0,
                limit: int = 50) -> dict[str, Any]:  # fmt: skip
    return _found(views.silver_rows(run_id, show, column, offset, limit), "no rules to preview yet")


@app.get("/api/runs/{run_id}/silver/columns/{column}")
def silver_column(run_id: uuid.UUID, column: str) -> dict[str, Any]:
    return _found(views.silver_column(run_id, column), "column not found")


@app.get("/api/runs/{run_id}/trace/{bronze_id}")
def trace(run_id: uuid.UUID, bronze_id: int) -> dict[str, Any]:
    return _found(views.trace(run_id, bronze_id), "record not found")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/{_path:path}", include_in_schema=False)
def spa(_path: str) -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
