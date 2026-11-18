"""The 'model' that picks a fix.

This is a scripted stand-in, not an LLM: it reads the same signals a model
would and returns the same shape of answer, deterministically, so the tests
measure the workflow and not a model's mood. A real model call would replace
plan() and run inside the same DBOS step, which is the point: its answer is
recorded once, and a recovered run reuses it instead of asking again (and
maybe getting a different answer).
"""

from __future__ import annotations

from typing import Any


def plan(signals: dict[str, Any], bug: str = "") -> dict[str, Any]:
    status, logs = signals["status"], "\n".join(signals["logs"])
    if bug == "always_restart" and not status["healthy"]:
        # The deliberate bug used to demonstrate forking a run after a fix.
        return {"action": "restart", "risk": "low", "reason": "restart fixes most things (wrong)"}
    if status["healthy"]:
        return {"action": "none", "risk": "low", "reason": "error rate is normal"}
    if "OOMKilled" in logs:
        return {"action": "restart", "risk": "low",
                "reason": "containers are being killed for running out of memory; a restart clears it"}
    if "since deploy" in logs and status.get("previous_version"):
        return {"action": "rollback", "to_version": status["previous_version"], "risk": "high",
                "reason": f"errors started with deploy {status['version']}; roll back to {status['previous_version']}"}
    return {"action": "escalate", "risk": "high", "reason": "no known fix for these signals"}
