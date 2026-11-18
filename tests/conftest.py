"""Test harness.

Each test gets: a fresh PostgreSQL database for DBOS, the fake ops API on a
free port (in this process, so tests can read its counters directly), and a
factory for worker *processes*, real ones, so a test can kill one mid-run
and start another, exactly like a crash and a restart in production.

PostgreSQL: the local test cluster on 127.0.0.1:5433 (override TEST_PG).
"""

from __future__ import annotations

import os
import queue
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pytest
import uvicorn
from dbos import DBOSClient

from durable_agent.fakeops import Ops, create_app, seeded

PG = os.environ.get("TEST_PG", "postgresql://postgres@127.0.0.1:5433")
SRC = Path(__file__).resolve().parents[1] / "src"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Worker:
    """One worker process. Its output is collected for failure messages."""

    def __init__(self, env: dict[str, str]):
        self.lines: list[str] = []
        self._q: queue.Queue[str | None] = queue.Queue()
        self.proc = subprocess.Popen([sys.executable, "-m", "durable_agent.worker"], env=env,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self._q.put(line.rstrip())
        self._q.put(None)

    def wait_for(self, text: str, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            if line is None:
                break
            self.lines.append(line)
            if text in line:
                return
        raise AssertionError(f"worker never printed {text!r}; output:\n" + "\n".join(self.lines))

    def wait_exit(self, timeout: float = 60) -> int:
        return self.proc.wait(timeout=timeout)

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()  # TerminateProcess on Windows, SIGKILL elsewhere
            self.proc.wait(timeout=10)


@dataclass
class Harness:
    db_url: str
    ops_url: str
    ops: Ops
    base_env: dict[str, str]
    workers: list[Worker] = field(default_factory=list)
    _client: DBOSClient | None = None

    def worker(self, wait_ready: bool = True, **env: str) -> Worker:
        w = Worker({**self.base_env, **env})
        self.workers.append(w)
        if wait_ready:
            w.wait_for("worker ready")
        return w

    @property
    def client(self) -> DBOSClient:
        if self._client is None:
            self._client = DBOSClient(system_database_url=self.db_url)
        return self._client

    def fault(self, service: str, kind: str) -> None:
        import httpx
        httpx.post(f"{self.ops_url}/services/{service}/fault", json={"kind": kind}).raise_for_status()

    def start(self, incident_id: str, service: str) -> str:
        handle = self.client.enqueue({"queue_name": "remediations", "workflow_name": "remediate",
                                      "workflow_id": incident_id}, {"service": service})
        return handle.get_workflow_id()

    def wait_stage(self, wf: str, stage: str, timeout: float = 30) -> None:
        deadline, seen = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            seen = self.client.get_event(wf, "stage", timeout_seconds=0.5)
            if seen == stage:
                return
        raise AssertionError(f"{wf} never reached stage {stage!r} (last seen {seen!r})")

    def result(self, wf: str, timeout: float = 60) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        handle = self.client.retrieve_workflow(wf)
        while time.monotonic() < deadline:
            status = handle.get_status().status
            if status == "SUCCESS":
                return handle.get_result()
            if status not in ("PENDING", "ENQUEUED"):
                raise AssertionError(f"{wf} ended with status {status}")
            time.sleep(0.2)
        raise AssertionError(f"{wf} did not finish within {timeout}s")

    def approve(self, wf: str, approved: bool = True, by: str = "asha") -> None:
        self.client.send(wf, {"approved": approved, "by": by}, topic="approval")

    def close(self) -> None:
        for w in self.workers:
            w.kill()
        if self._client is not None:
            self._client.destroy()


@pytest.fixture
def harness():
    name = f"durable_agent_t_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{PG}/postgres", autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')

    ops = seeded()
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(create_app(ops), host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.05)

    db_url = f"{PG}/{name}"
    env = {**os.environ, "PYTHONPATH": str(SRC), "DBOS_SYSTEM_DATABASE_URL": db_url,
           "OPS_URL": f"http://127.0.0.1:{port}", "VERIFY_INTERVAL_SEC": "0.2", "VERIFY_ATTEMPTS": "10",
           "APPROVAL_TIMEOUT_SEC": "30", "PYTHONUNBUFFERED": "1"}
    for k in ("CRASH_AT", "PLANNER_BUG", "USE_IDEMPOTENCY_KEYS", "QUEUE_CONCURRENCY"):
        env.pop(k, None)
    h = Harness(db_url=db_url, ops_url=f"http://127.0.0.1:{port}", ops=ops, base_env=env)
    try:
        yield h
    finally:
        h.close()
        server.should_exit = True
        thread.join(timeout=5)
        with psycopg.connect(f"{PG}/postgres", autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
