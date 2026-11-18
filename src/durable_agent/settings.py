"""Settings, read from environment variables so a test can start a worker
process with exactly the behaviour it wants."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: bool) -> bool:
    return os.environ.get(name, "1" if default else "0").strip().lower() in {"1", "true", "yes"}


@dataclass(frozen=True)
class Settings:
    db_url: str
    ops_url: str
    approval_timeout_sec: float
    verify_interval_sec: float
    verify_attempts: int
    use_idempotency_keys: bool
    queue_concurrency: int
    crash_at: str  # test hook: exit the process at this named point
    planner_bug: str  # test hook: make the planner wrong on purpose

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_url=os.environ.get("DBOS_SYSTEM_DATABASE_URL", "postgresql://postgres@127.0.0.1:5433/durable_agent"),
            ops_url=os.environ.get("OPS_URL", "http://127.0.0.1:8095").rstrip("/"),
            approval_timeout_sec=float(os.environ.get("APPROVAL_TIMEOUT_SEC", "3600")),
            verify_interval_sec=float(os.environ.get("VERIFY_INTERVAL_SEC", "5")),
            verify_attempts=int(os.environ.get("VERIFY_ATTEMPTS", "6")),
            use_idempotency_keys=_flag("USE_IDEMPOTENCY_KEYS", True),
            queue_concurrency=int(os.environ.get("QUEUE_CONCURRENCY", "2")),
            crash_at=os.environ.get("CRASH_AT", ""),
            planner_bug=os.environ.get("PLANNER_BUG", ""),
        )


SETTINGS = Settings.from_env()


def crash_point(name: str) -> None:
    """Exit the process right here if CRASH_AT names this point.

    os._exit skips finally blocks, atexit handlers and DBOS's own shutdown, so
    from the database's point of view it's the same as kill -9 or a node
    dying: the workflow is left half done, exactly where it stopped.
    """
    if SETTINGS.crash_at == name:
        print(f"CRASH at {name}", flush=True)
        os._exit(137)
