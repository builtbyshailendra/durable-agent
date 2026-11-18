"""Killing the worker mid-run and starting a new one.

CRASH_AT makes the worker exit with os._exit(137) at a named point: no
cleanup, no finally blocks, the same as kill -9 or a node dying. A second
worker is then started on the same database and must finish the run.
"""


def test_crash_between_steps_resumes_without_repeating_work(harness):
    harness.fault("payments-api", "memory_leak")
    crashing = harness.worker(CRASH_AT="after_triage")
    wf = harness.start("inc-201", "payments-api")
    assert crashing.wait_exit() == 137
    assert harness.ops.reads["logs"] == 1  # triage ran once before the crash

    harness.worker()  # a fresh process: DBOS recovers the unfinished run on launch
    out = harness.result(wf)
    assert out["outcome"] == "resolved"
    assert harness.ops.reads["logs"] == 1, "triage was recorded, so it must not run again"
    assert len(harness.ops.actions) == 1


def test_crash_inside_a_step_repeats_the_effect_without_a_key(harness):
    """The honest limit of durable execution: a step that was running when the
    process died runs again. Here the restart reached the ops API, then the
    worker died before DBOS recorded the step, so the recovered run sends it
    again, and without an idempotency key it is applied twice."""
    harness.fault("payments-api", "memory_leak")
    crashing = harness.worker(CRASH_AT="inside_execute", USE_IDEMPOTENCY_KEYS="0")
    wf = harness.start("inc-202", "payments-api")
    assert crashing.wait_exit() == 137
    assert len(harness.ops.actions) == 1

    harness.worker(USE_IDEMPOTENCY_KEYS="0")
    assert harness.result(wf)["outcome"] == "resolved"
    assert [a["kind"] for a in harness.ops.actions] == ["restart", "restart"]


def test_idempotency_key_makes_the_effect_happen_once(harness):
    harness.fault("payments-api", "memory_leak")
    crashing = harness.worker(CRASH_AT="inside_execute")
    wf = harness.start("inc-203", "payments-api")
    assert crashing.wait_exit() == 137

    harness.worker()
    out = harness.result(wf)
    assert out["outcome"] == "resolved"
    assert len(harness.ops.actions) == 1, "the retry carried the same key, so the API ignored it"
    assert out["applied"]["replayed"] is True


def test_crash_after_the_step_is_recorded_does_not_repeat_it(harness):
    harness.fault("payments-api", "memory_leak")
    crashing = harness.worker(CRASH_AT="after_execute", USE_IDEMPOTENCY_KEYS="0")
    wf = harness.start("inc-204", "payments-api")
    assert crashing.wait_exit() == 137

    harness.worker(USE_IDEMPOTENCY_KEYS="0")
    assert harness.result(wf)["outcome"] == "resolved"
    assert len(harness.ops.actions) == 1, "a finished step is never re-run, key or no key"


def test_waiting_for_approval_survives_a_restart(harness):
    harness.fault("checkout-web", "bad_deploy")
    first = harness.worker()
    wf = harness.start("inc-205", "checkout-web")
    harness.wait_stage(wf, "awaiting_approval")
    first.kill()  # the worker is gone while the run waits for a human

    harness.worker()
    harness.wait_stage(wf, "awaiting_approval")
    harness.approve(wf, by="meera")
    out = harness.result(wf)
    assert out["outcome"] == "resolved" and out["approved_by"] == "meera"
    assert harness.ops.reads["logs"] == 1
    assert [a["kind"] for a in harness.ops.actions] == ["rollback"]


def test_approval_sent_while_no_worker_is_running_is_not_lost(harness):
    harness.fault("checkout-web", "bad_deploy")
    first = harness.worker()
    wf = harness.start("inc-206", "checkout-web")
    harness.wait_stage(wf, "awaiting_approval")
    first.kill()
    harness.approve(wf, by="meera")  # nobody is running: the message waits in PostgreSQL

    harness.worker()
    assert harness.result(wf)["outcome"] == "resolved"
