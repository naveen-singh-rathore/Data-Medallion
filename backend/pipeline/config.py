"""Settings loaded once from environment variables (and the repo-root .env)."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(REPO_ROOT / ".env")


LLM_MODES = ("live", "replay", "record")


@dataclass(frozen=True)
class Settings:
    """Runtime settings, read once from the environment (and .env)."""

    database_url: str
    log_level: str
    batch_size: int
    sql_dir: Path
    prompts_dir: Path
    llm_provider: str
    llm_api_key: str | None  # None: the SDK falls back to ANTHROPIC_API_KEY / a CLI profile
    cheap_model: str
    main_model: str
    llm_mode: str  # live | replay | record (see pipeline/llm.py)
    card_budget_tokens: int
    card_chars_per_token: float  # token estimate when no exact count exists (measured ~1.94 on raw_tickets)
    classify_concurrency: int  # parallel classification batches (bounded by the provider's rate limit)


def _load() -> Settings:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set (copy .env.example to .env)")
    llm_mode = os.getenv("LLM_MODE", "replay").lower()
    if llm_mode not in LLM_MODES:
        raise RuntimeError(f"LLM_MODE must be one of {LLM_MODES}, got {llm_mode!r}")
    return Settings(
        database_url=database_url,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        batch_size=int(os.getenv("BATCH_SIZE", "1000")),
        sql_dir=REPO_ROOT / "db" / "sql",
        prompts_dir=REPO_ROOT / "prompts",
        llm_provider=os.getenv("LLM_PROVIDER", "anthropic"),
        llm_api_key=os.getenv("LLM_API_KEY") or None,
        cheap_model=os.getenv("CHEAP_MODEL", "claude-haiku-5-5"),
        main_model=os.getenv("MAIN_MODEL", "claude-opus-5-5"),
        llm_mode=llm_mode,
        card_budget_tokens=int(os.getenv("CARD_BUDGET_TOKENS", "4000")),
        card_chars_per_token=float(os.getenv("CARD_CHARS_PER_TOKEN", "2")),
        classify_concurrency=max(1, int(os.getenv("CLASSIFY_CONCURRENCY", "4"))),
    )


settings = _load()
