"""The workflow's normal paths, with one worker that never crashes."""


def test_low_risk_fix_runs_without_approval(harness):
    harness.worker()
    harness.fault("payments-api", "memory_leak")
    wf = harness.start("inc-101", "payments-api")
    out = harness.result(wf)
    assert out["outcome"] == "resolved"
    assert out["plan"]["action"] == "restart"
    assert [a["kind"] for a in harness.ops.actions] == ["restart"]
    assert harness.ops.actions[0]["idempotency_key"] == "inc-101:execute"
    assert harness.ops.pages == []


def test_high_risk_fix_waits_for_a_human(harness):
    harness.worker()
    harness.fault("checkout-web", "bad_deploy")
    wf = harness.start("inc-102", "checkout-web")
    harness.wait_stage(wf, "awaiting_approval")
    assert harness.ops.actions == []  # nothing happens until someone says yes
    harness.approve(wf, by="asha")
    out = harness.result(wf)
    assert out["outcome"] == "resolved"
    assert out["approved_by"] == "asha"
    assert harness.ops.actions[0]["kind"] == "rollback"
    assert harness.ops.actions[0]["to_version"] == "v41"


def test_rejected_fix_is_never_applied(harness):
    harness.worker()
    harness.fault("checkout-web", "bad_deploy")
    wf = harness.start("inc-103", "checkout-web")
    harness.wait_stage(wf, "awaiting_approval")
    harness.approve(wf, approved=False, by="ravi")
    out = harness.result(wf)
    assert out == {**out, "outcome": "rejected", "by": "ravi"}
    assert harness.ops.actions == []


def test_nobody_answers_so_it_pages(harness):
    harness.worker(APPROVAL_TIMEOUT_SEC="1")
    harness.fault("checkout-web", "bad_deploy")
    wf = harness.start("inc-104", "checkout-web")
    out = harness.result(wf)
    assert out["outcome"] == "escalated"
    assert harness.ops.actions == []
    assert len(harness.ops.pages) == 1 and "nobody answered" in harness.ops.pages[0]["summary"]


def test_same_incident_twice_is_one_run(harness):
    harness.worker()
    harness.fault("payments-api", "memory_leak")
    first = harness.start("inc-105", "payments-api")
    second = harness.start("inc-105", "payments-api")  # the alert fires again
    assert first == second
    harness.result(first)
    assert harness.ops.reads["logs"] == 1
    assert len(harness.ops.actions) == 1


def test_healthy_service_needs_nothing(harness):
    harness.worker()
    out = harness.result(harness.start("inc-106", "search-api"))
    assert out["outcome"] == "nothing_to_do"
    assert harness.ops.actions == [] and harness.ops.pages == []
