"""The operator API: start incidents, see where runs are, approve, fork.

It never runs a workflow itself. It talks to the DBOS system database through
DBOSClient, so it can be restarted or scaled without touching the workers,
and a worker crash doesn't take the API down with it.

    uvicorn durable_agent.api:app --port 8096
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from dbos import DBOSClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from durable_agent.settings import SETTINGS

QUEUE, WORKFLOW = "remediations", "remediate"


class IncidentIn(BaseModel):
    id: str
    service: str


class DecisionIn(BaseModel):
    by: str


class ForkIn(BaseModel):
    from_step: str  # a step name, e.g. "make_plan"


def _summary(st: Any) -> dict[str, Any]:
    service = None
    if st.input and st.input.get("args"):
        service = st.input["args"][0].get("service")
    out = st.output if isinstance(st.output, dict) else {}
    return {"id": st.workflow_id, "status": st.status, "service": service, "outcome": out.get("outcome"),
            "forked_from": st.forked_from, "created_at": st.created_at}


def create_app(db_url: str | None = None) -> FastAPI:
    client = DBOSClient(system_database_url=db_url or SETTINGS.db_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        client.destroy()  # close the pool when the API shuts down

    app = FastAPI(title="durable-agent operator API", lifespan=lifespan)
    app.state.client = client

    def stage(run_id: str) -> Any:
        return client.get_event(run_id, "stage", timeout_seconds=0.1)

    def status(run_id: str) -> Any:
        found = client.list_workflows(workflow_ids=[run_id])
        if not found:
            raise HTTPException(404, f"no run {run_id}")
        return found[0]

    @app.post("/incidents", status_code=202)
    def start(body: IncidentIn) -> dict[str, Any]:
        # The incident ID is the workflow ID: a repeated alert attaches to the
        # run that's already going instead of starting a second one.
        handle = client.enqueue({"queue_name": QUEUE, "workflow_name": WORKFLOW, "workflow_id": body.id},
                                {"service": body.service})
        return {"id": handle.get_workflow_id()}

    @app.get("/incidents")
    def list_runs() -> list[dict[str, Any]]:
        return [_summary(st) for st in client.list_workflows(name=WORKFLOW, sort_desc=True, limit=50)]

    @app.get("/incidents/{run_id}")
    def show(run_id: str) -> dict[str, Any]:
        st = status(run_id)
        return {**_summary(st), "stage": stage(run_id),
                "plan": client.get_event(run_id, "plan", timeout_seconds=0.1),
                "result": st.output}

    def decide(run_id: str, approved: bool, by: str) -> dict[str, Any]:
        status(run_id)
        if stage(run_id) != "awaiting_approval":
            raise HTTPException(409, f"run {run_id} is not waiting for approval (stage: {stage(run_id)})")
        client.send(run_id, {"approved": approved, "by": by}, topic="approval")
        return {"id": run_id, "sent": "approve" if approved else "reject", "by": by}

    @app.post("/incidents/{run_id}/approve")
    def approve(run_id: str, body: DecisionIn) -> dict[str, Any]:
        return decide(run_id, True, body.by)

    @app.post("/incidents/{run_id}/reject")
    def reject(run_id: str, body: DecisionIn) -> dict[str, Any]:
        return decide(run_id, False, body.by)

    @app.get("/incidents/{run_id}/steps")
    def steps(run_id: str) -> list[dict[str, Any]]:
        status(run_id)
        return [{"step": s["function_id"], "name": s["function_name"],
                 "error": None if s["error"] is None else str(s["error"]),
                 "ms": None if not (s["started_at_epoch_ms"] and s["completed_at_epoch_ms"])
                 else s["completed_at_epoch_ms"] - s["started_at_epoch_ms"]}
                for s in client.list_workflow_steps(run_id, load_output=False)]

    @app.post("/incidents/{run_id}/fork", status_code=202)
    def fork(run_id: str, body: ForkIn) -> dict[str, Any]:
        """Re-run a finished run from one of its steps. Steps before that
        point keep their recorded results (triage isn't redone); the named
        step and everything after it run again under a new ID."""
        status(run_id)
        for s in client.list_workflow_steps(run_id, load_output=False):
            if s["function_name"].split(".")[-1] == body.from_step:
                handle = client.fork_workflow(run_id, s["function_id"], queue_name=QUEUE)
                return {"id": handle.get_workflow_id(), "forked_from": run_id, "from_step": s["function_id"]}
        raise HTTPException(404, f"run {run_id} has no step named {body.from_step}")

    return app


def __getattr__(name: str) -> Any:  # build the default app only when uvicorn asks for it
    if name == "app":
        return create_app()
    raise AttributeError(name)
