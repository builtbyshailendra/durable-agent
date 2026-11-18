"""Operating the agent: the API, the queue limit, and forking a bad run."""

from fastapi.testclient import TestClient

from durable_agent.api import create_app


def test_operator_api_end_to_end(harness):
    harness.worker()
    harness.fault("checkout-web", "bad_deploy")
    with TestClient(create_app(harness.db_url)) as api:
        _drive_api(harness, api)


def _drive_api(harness, api):
    assert api.post("/incidents", json={"id": "inc-301", "service": "checkout-web"}).json() == {"id": "inc-301"}
    harness.wait_stage("inc-301", "awaiting_approval")

    run = api.get("/incidents/inc-301").json()
    assert run["stage"] == "awaiting_approval" and run["plan"]["action"] == "rollback"
    assert api.post("/incidents/inc-301/approve", json={"by": "asha"}).status_code == 200

    out = harness.result("inc-301")
    assert out["outcome"] == "resolved"
    # Approving a run that isn't waiting is refused, not silently queued.
    assert api.post("/incidents/inc-301/approve", json={"by": "asha"}).status_code == 409
    assert api.get("/incidents/nope").status_code == 404

    names = [s["name"].split(".")[-1] for s in api.get("/incidents/inc-301/steps").json()]
    for step in ("triage", "make_plan", "execute", "check_health", "close_run"):
        assert step in names
    assert names.index("triage") < names.index("make_plan") < names.index("execute")
    assert [r["id"] for r in api.get("/incidents").json()] == ["inc-301"]


def test_queue_limits_how_many_runs_go_at_once(harness):
    harness.worker(QUEUE_CONCURRENCY="2", VERIFY_INTERVAL_SEC="0.3")
    services = ["payments-api", "checkout-web", "ledger-worker", "search-api"]
    for s in services:
        harness.fault(s, "memory_leak")
    runs = [harness.start(f"inc-31{i}", s) for i, s in enumerate(services)]
    assert all(harness.result(r)["outcome"] == "resolved" for r in runs)
    assert harness.ops.max_active_runs == 2, "two at a time: never more, and they did overlap"
    assert len(harness.ops.actions) == 4


def test_fork_a_bad_run_after_fixing_the_planner(harness):
    """A planner bug makes the agent restart a service that needed a rollback.
    After the fix, the run is forked from its planning step: triage's recorded
    result is reused, and only planning onwards runs again."""
    harness.fault("checkout-web", "bad_deploy")
    buggy = harness.worker(PLANNER_BUG="always_restart", VERIFY_ATTEMPTS="3")
    out = harness.result(harness.start("inc-320", "checkout-web"))
    assert out["outcome"] == "not_fixed" and out["plan"]["action"] == "restart"
    assert len(harness.ops.pages) == 1
    buggy.kill()

    harness.worker()  # the fixed planner
    with TestClient(create_app(harness.db_url)) as api:
        _fork_and_finish(harness, api)


def _fork_and_finish(harness, api):
    forked = api.post("/incidents/inc-320/fork", json={"from_step": "make_plan"}).json()
    harness.wait_stage(forked["id"], "awaiting_approval")
    harness.approve(forked["id"], by="asha")
    again = harness.result(forked["id"])

    assert again["outcome"] == "resolved" and again["plan"]["action"] == "rollback"
    assert harness.ops.reads["logs"] == 1, "triage came from the original run's record"
    assert [a["kind"] for a in harness.ops.actions] == ["restart", "rollback"]
    listed = {r["id"]: r for r in api.get("/incidents").json()}
    assert listed[forked["id"]]["forked_from"] == "inc-320"
