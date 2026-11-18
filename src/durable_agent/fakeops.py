"""A fake operations API: the outside world the agent acts on.

It stands in for a cluster API and a pager. It keeps its own in-memory state,
separate from the agent's database, so a side effect really is outside the
workflow: if the agent's process dies after calling it, the effect stays.

It also counts everything, so tests can prove what happened: how many times
the logs were read, how many restarts were applied, and how many agent runs
were active at once.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

RECOVERY_DELAY_SEC = 0.5  # a fix takes effect this long after it's applied


@dataclass
class Service:
    name: str
    version: str
    previous_version: str
    error_rate: float = 0.01
    fault: str | None = None  # "bad_deploy" or "memory_leak"
    recovers_at: float | None = None


@dataclass
class Ops:
    services: dict[str, Service] = field(default_factory=dict)
    actions: list[dict[str, Any]] = field(default_factory=list)
    pages: list[dict[str, Any]] = field(default_factory=list)
    reads: Counter = field(default_factory=Counter)
    replies: dict[str, dict[str, Any]] = field(default_factory=dict)  # idempotency key -> first reply
    active_runs: set[str] = field(default_factory=set)
    max_active_runs: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def service(self, name: str) -> Service:
        svc = self.services.get(name)
        if svc is None:
            raise HTTPException(404, f"unknown service {name}")
        if svc.recovers_at is not None and time.monotonic() >= svc.recovers_at:
            svc.error_rate, svc.fault, svc.recovers_at = 0.01, None, None
        return svc


def seeded() -> Ops:
    ops = Ops()
    for name in ("payments-api", "checkout-web", "ledger-worker", "search-api"):
        ops.services[name] = Service(name=name, version="v42", previous_version="v41")
    return ops


class FaultIn(BaseModel):
    kind: str  # bad_deploy | memory_leak


class ActionIn(BaseModel):
    idempotency_key: str | None = None
    to_version: str | None = None


class PageIn(BaseModel):
    summary: str
    run_id: str
    idempotency_key: str | None = None


def create_app(ops: Ops | None = None) -> FastAPI:
    ops = ops or seeded()
    app = FastAPI(title="fake ops API")
    app.state.ops = ops

    def track(run_id: str | None) -> None:
        if run_id:
            ops.active_runs.add(run_id)
            ops.max_active_runs = max(ops.max_active_runs, len(ops.active_runs))

    @app.post("/services/{name}/fault")
    def inject(name: str, body: FaultIn) -> dict:
        with ops.lock:
            svc = ops.service(name)
            svc.fault = body.kind
            svc.error_rate = {"bad_deploy": 0.32, "memory_leak": 0.18}.get(body.kind, 0.25)
            return {"ok": True}

    @app.get("/services/{name}")
    def status(name: str, x_run_id: str | None = Header(default=None)) -> dict:
        with ops.lock:
            ops.reads["status"] += 1
            track(x_run_id)
            svc = ops.service(name)
            return {"name": svc.name, "version": svc.version, "previous_version": svc.previous_version,
                    "error_rate": svc.error_rate, "healthy": svc.error_rate < 0.05}

    @app.get("/services/{name}/logs")
    def logs(name: str) -> dict:
        with ops.lock:
            ops.reads["logs"] += 1
            svc = ops.service(name)
            lines = {
                "bad_deploy": [f"ERROR NullPointerException in CheckoutHandler.submit (since deploy {svc.version})",
                               "ERROR 500 POST /checkout"],
                "memory_leak": [f"WARN container {svc.name} OOMKilled, exit code 137", "WARN restarting container"],
            }.get(svc.fault or "", ["INFO all requests healthy"])
            return {"lines": lines}

    def apply(kind: str, name: str, body: ActionIn, fix: str | None) -> dict:
        # An idempotency key makes a repeat of the same request a no-op that
        # returns the first answer. That's what turns "at least once" into
        # "exactly once" for the effect.
        with ops.lock:
            if body.idempotency_key and body.idempotency_key in ops.replies:
                return {**ops.replies[body.idempotency_key], "replayed": True}
            svc = ops.service(name)
            if kind == "rollback":
                svc.version, svc.previous_version = body.to_version or svc.previous_version, svc.version
            if svc.fault is not None and svc.fault == fix:
                svc.recovers_at = time.monotonic() + RECOVERY_DELAY_SEC
            action = {"n": len(ops.actions) + 1, "kind": kind, "service": name,
                      "to_version": body.to_version, "idempotency_key": body.idempotency_key}
            ops.actions.append(action)
            reply = {"applied": action, "replayed": False}
            if body.idempotency_key:
                ops.replies[body.idempotency_key] = reply
            return reply

    @app.post("/services/{name}/restart")
    def restart(name: str, body: ActionIn) -> dict:
        return apply("restart", name, body, fix="memory_leak")

    @app.post("/services/{name}/rollback")
    def rollback(name: str, body: ActionIn) -> dict:
        return apply("rollback", name, body, fix="bad_deploy")

    @app.post("/pages")
    def page(body: PageIn) -> dict:
        with ops.lock:
            if body.idempotency_key and body.idempotency_key in ops.replies:
                return {**ops.replies[body.idempotency_key], "replayed": True}
            entry = {"n": len(ops.pages) + 1, "summary": body.summary, "run_id": body.run_id}
            ops.pages.append(entry)
            reply = {"paged": entry, "replayed": False}
            if body.idempotency_key:
                ops.replies[body.idempotency_key] = reply
            return reply

    @app.post("/runs/{run_id}/done")
    def run_done(run_id: str) -> dict:
        with ops.lock:
            ops.active_runs.discard(run_id)
            return {"ok": True}

    @app.get("/actions")
    def list_actions() -> list[dict]:
        with ops.lock:
            return list(ops.actions)

    @app.get("/pages")
    def list_pages() -> list[dict]:
        with ops.lock:
            return list(ops.pages)

    return app


app = create_app()  # for: uvicorn durable_agent.fakeops:app --port 8095
