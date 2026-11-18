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

### Step 2: crashes

`CRASH_AT` makes the worker exit with `os._exit(137)` at a named point: no cleanup, no `finally`, the same as `kill -9` or a node dying. A new worker process then starts on the same database.

| Crash | What the test proves |
|---|---|
| between triage and planning | the new worker finishes the run, and triage is **not** repeated (the fake API saw the logs read exactly once) |
| inside the execute step, after the API call, **no idempotency key** | the restart is applied **twice**. A step that was running when the process died runs again: steps are at-least-once |
| the same, **with** an idempotency key (`<run id>:execute`) | applied once; the retry gets the first reply back, marked `replayed` |
| right after the execute step finished | applied once even without a key: a finished step never runs again |
| while waiting for approval (worker killed) | a new worker picks up the wait; the approval is accepted and the run finishes |
| approval sent while no worker is running at all | the message waits in PostgreSQL and is delivered when a worker starts |

The third and fourth rows are the lesson: durable execution makes the *workflow* exactly-once, but a step that dies halfway is retried, so any step with an outside effect needs an idempotency key that the other side honours.

What broke: nothing; all six passed on the first run.
