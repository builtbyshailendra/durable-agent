"""The remediation workflow.

A DBOS workflow is ordinary Python. What makes it durable: DBOS records the
start of the workflow and the result of every step in PostgreSQL. If the
process dies, another process (or this one, restarted) runs the workflow
function again from the top, and each step that already finished returns its
recorded result instead of running. So the run continues from where it
stopped, with the same answers it had before.

Two rules follow, and the tests check both:
- Everything that touches the outside world, or isn't deterministic (an HTTP
  call, a model call, the clock), goes in a step.
- A step that was running when the process died runs again. Steps are
  at-least-once, so any side effect needs an idempotency key to happen once.
"""

from __future__ import annotations

from typing import Any

import httpx
from dbos import DBOS

from durable_agent import planner
from durable_agent.settings import SETTINGS, crash_point

QUEUE = "remediations"


def register_queue() -> None:
    """Every run goes through this queue: at most N remediations at once,
    across all workers, so an alert storm can't have the agent restarting half
    the fleet in parallel. DBOS 3 keeps queue settings in the database, so
    this runs after launch."""
    DBOS.register_queue(QUEUE, global_concurrency=SETTINGS.queue_concurrency,
                        polling_interval_sec=0.2, on_conflict="always_update")

_http = httpx.Client(base_url=SETTINGS.ops_url, timeout=10)


def _ok(resp: httpx.Response) -> Any:
    resp.raise_for_status()
    return resp.json()


@DBOS.step()
def triage(service: str, run_id: str) -> dict[str, Any]:
    status = _ok(_http.get(f"/services/{service}", headers={"X-Run-Id": run_id}))
    logs = _ok(_http.get(f"/services/{service}/logs"))["lines"]
    return {"status": status, "logs": logs}


@DBOS.step()
def make_plan(signals: dict[str, Any]) -> dict[str, Any]:
    return planner.plan(signals, bug=SETTINGS.planner_bug)


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=0.2)
def execute(service: str, plan: dict[str, Any], key: str) -> dict[str, Any]:
    body: dict[str, Any] = {"idempotency_key": key if SETTINGS.use_idempotency_keys else None}
    if plan["action"] == "rollback":
        body["to_version"] = plan["to_version"]
    reply = _ok(_http.post(f"/services/{service}/{plan['action']}", json=body))
    # The effect has happened, but DBOS hasn't recorded this step as done.
    # A crash here is the case durable execution can't hide.
    crash_point("inside_execute")
    return reply


@DBOS.step()
def check_health(service: str) -> bool:
    return bool(_ok(_http.get(f"/services/{service}"))["healthy"])


@DBOS.step()
def page(run_id: str, summary: str) -> dict[str, Any]:
    return _ok(_http.post("/pages", json={"summary": summary, "run_id": run_id,
                                          "idempotency_key": f"{run_id}:page"}))


@DBOS.step()
def close_run(run_id: str) -> None:
    _ok(_http.post(f"/runs/{run_id}/done"))


def _stage(name: str) -> None:
    DBOS.set_event("stage", name)  # readable from outside with get_event


@DBOS.workflow(name="remediate")
def remediate(incident: dict[str, Any]) -> dict[str, Any]:
    run_id = DBOS.workflow_id
    assert run_id is not None
    service = incident["service"]
    report: dict[str, Any] = {"incident": run_id, "service": service}

    def finish(outcome: str, **extra: Any) -> dict[str, Any]:
        close_run(run_id)
        _stage("done")
        return {**report, "outcome": outcome, **extra}

    _stage("triage")
    signals = triage(service, run_id)
    crash_point("after_triage")

    _stage("planning")
    plan = make_plan(signals)
    report["plan"] = plan
    DBOS.set_event("plan", plan)
    if plan["action"] == "none":
        return finish("nothing_to_do")
    if plan["action"] == "escalate":
        page(run_id, f"{service}: {plan['reason']}")
        return finish("escalated")

    if plan["risk"] == "high":
        # Waiting here costs nothing durable: the wait is recorded, and if
        # the worker restarts the run comes back to this same wait.
        _stage("awaiting_approval")
        decision = DBOS.recv("approval", timeout_seconds=SETTINGS.approval_timeout_sec)
        if decision is None:
            page(run_id, f"{service}: {plan['action']} needs approval and nobody answered")
            return finish("escalated", reason="approval timed out")
        if not decision.get("approved"):
            return finish("rejected", by=decision.get("by"))
        report["approved_by"] = decision.get("by")

    _stage("executing")
    report["applied"] = execute(service, plan, f"{run_id}:execute")
    crash_point("after_execute")

    _stage("verifying")
    for _ in range(SETTINGS.verify_attempts):
        if check_health(service):
            return finish("resolved")
        DBOS.sleep(SETTINGS.verify_interval_sec)  # durable: a restart doesn't restart the wait
    page(run_id, f"{service}: applied {plan['action']} but it is still unhealthy")
    return finish("not_fixed")
