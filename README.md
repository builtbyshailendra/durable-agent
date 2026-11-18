# durable-agent

An incident-remediation agent run that survives crashes. Each run is a [DBOS](https://github.com/dbos-inc/dbos-transact-py) durable workflow on PostgreSQL: kill the worker at any point, start another, and the run carries on from its last finished step, without repeating work it already did.

Status: built step by step, locally. Not published. The "model" is a scripted stand-in, and the infrastructure it fixes is a fake ops API.

## Build log

### Step 1: the workflow and its normal paths

- `runbook.py`: one DBOS workflow, `remediate`. Steps: triage (read status and logs) → plan → (approval if risky) → execute → verify with durable sleeps between checks → report. Its progress is published as a DBOS event (`stage`) so anything outside can see where a run is.
- `fakeops.py`: a FastAPI app standing in for the outside world (a service API and a pager). It counts every read and action and supports idempotency keys, so tests can prove exactly what happened.
- `planner.py`: the "model". A scripted stand-in that reads the same signals an LLM would and answers in the same shape, so tests measure the workflow, not a model. Low-risk fixes (restart) run straight away; high-risk ones (rollback) wait for a human.
- `worker.py`: the process that runs workflows from the `remediations` queue.

Tests run against **real worker processes** and a fresh PostgreSQL database each (the local test cluster on port 5433): a low-risk fix with no approval, a rollback that waits for a human, a rejected fix, nobody answering (pages after the timeout), the same incident ID enqueued twice (one run, logs read once), and a healthy service.

What broke: DBOS 3 no longer lets you construct `Queue(...)` directly. Queues are stored in the system database now, registered with `DBOS.register_queue` after `DBOS.launch()`.
