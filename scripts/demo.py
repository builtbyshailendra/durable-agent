"""A narrated run with real processes on real ports.

    python scripts/demo.py            (PostgreSQL on 127.0.0.1:5433, or set TEST_PG)

Starts the fake ops API, a worker and the operator API; breaks a service;
opens an incident; kills the worker while the run waits for approval; starts
a new worker; approves; and prints how the run finished and its steps.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[1]
PG = os.environ.get("TEST_PG", "postgresql://postgres@127.0.0.1:5433")
DB = "durable_agent_demo"
OPS, API = "http://127.0.0.1:8095", "http://127.0.0.1:8096"
ENV = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "DBOS_SYSTEM_DATABASE_URL": f"{PG}/{DB}",
       "OPS_URL": OPS, "VERIFY_INTERVAL_SEC": "0.5", "PYTHONUNBUFFERED": "1"}
procs: list[subprocess.Popen] = []


def spawn(*args: str) -> subprocess.Popen:
    p = subprocess.Popen([sys.executable, *args], env=ENV, cwd=ROOT,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(p)
    return p


def wait_http(url: str) -> None:
    for _ in range(150):
        try:
            httpx.get(url, timeout=1)
            return
        except httpx.HTTPError:
            time.sleep(0.2)
    raise SystemExit(f"{url} never came up")


def wait_stage(run: str, want: str) -> dict:
    for _ in range(300):
        r = httpx.get(f"{API}/incidents/{run}").json()
        if r.get("stage") == want:
            return r
        time.sleep(0.2)
    raise SystemExit(f"{run} never reached {want}")


def say(text: str) -> None:
    print(f"\n> {text}", flush=True)


def main() -> None:
    with psycopg.connect(f"{PG}/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{DB}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{DB}"')
    try:
        spawn("-m", "uvicorn", "durable_agent.fakeops:app", "--port", "8095", "--log-level", "warning")
        wait_http(f"{OPS}/actions")
        worker = spawn("-m", "durable_agent.worker")
        time.sleep(4)  # let the first worker create DBOS's tables
        spawn("-m", "uvicorn", "durable_agent.api:app", "--port", "8096", "--log-level", "warning")
        wait_http(f"{API}/incidents")

        say("checkout-web gets a bad deploy")
        httpx.post(f"{OPS}/services/checkout-web/fault", json={"kind": "bad_deploy"}).raise_for_status()
        print(httpx.get(f"{OPS}/services/checkout-web").json())

        say("an alert opens incident inc-900")
        print(httpx.post(f"{API}/incidents", json={"id": "inc-900", "service": "checkout-web"}).json())
        run = wait_stage("inc-900", "awaiting_approval")
        print("plan:", run["plan"])

        say("the worker is killed while the run waits for a human")
        worker.kill()
        worker.wait()
        print("worker exit code:", worker.returncode, "(1 on Windows: TerminateProcess; 137 on Linux)")

        say("a new worker starts")
        spawn("-m", "durable_agent.worker")
        time.sleep(3)
        print("stage:", httpx.get(f"{API}/incidents/inc-900").json()["stage"])

        say("asha approves the rollback")
        print(httpx.post(f"{API}/incidents/inc-900/approve", json={"by": "asha"}).json())
        wait_stage("inc-900", "done")
        print("outcome:", httpx.get(f"{API}/incidents/inc-900").json()["result"]["outcome"])

        say("what the outside world saw")
        print("actions:", httpx.get(f"{OPS}/actions").json())
        print("service:", httpx.get(f"{OPS}/services/checkout-web").json())

        say("the run's recorded steps")
        for s in httpx.get(f"{API}/incidents/inc-900/steps").json():
            print(f"  {s['step']:>2}  {s['name']:<28} {s['ms'] if s['ms'] is not None else '':>5} ms")
    finally:
        for p in procs:
            p.kill()


if __name__ == "__main__":
    main()
