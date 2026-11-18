# Learning notes: durable-agent

## 1. The problem

An agent that fixes incidents runs for minutes or hours: it reads signals, asks a model, waits for a human, applies a fix, waits to see if it worked. Processes die in that time: deploys, crashes, node failures, laptops closing. Without help you get one of two bad outcomes:
- the run is lost, and nobody knows whether the fix was applied, or
- it's retried from the start, and the fix (or the page, or the payment) happens twice.

## 2. Durable execution in one paragraph

Write the run as an ordinary function. The framework records, in a database, that the run started and what each step returned. If the process dies, another process calls the same function again with the same input; every step that already finished returns its recorded result instantly, so execution fast-forwards to where it stopped and carries on. Waits (sleeps, waiting for a message) are recorded too, so a run can wait for hours without holding anything but a database row.

## 3. DBOS pieces used here

| Piece | Where | What it does |
|---|---|---|
| `@DBOS.workflow()` | `runbook.remediate` | the durable function; re-run from the top on recovery |
| `@DBOS.step()` | `triage`, `make_plan`, `execute`, ... | its result is recorded; a finished step never runs again |
| `retries_allowed=True` | `execute` | automatic retries with backoff for transient errors |
| `DBOS.recv(topic, timeout)` | waiting for approval | a durable wait for a message; `DBOSClient.send` delivers it, even from another process, even while no worker runs |
| `DBOS.set_event` / `get_event` | `stage`, `plan` | publish progress that anything outside can read |
| `DBOS.sleep` | verify loop | a durable sleep: after a restart it only waits for the time that's left |
| `DBOS.register_queue` | `worker.py` | queue with `global_concurrency=2` across all workers |
| workflow ID = incident ID | `POST /incidents` | the same alert twice attaches to the same run |
| `DBOSClient` | `api.py`, tests | start, inspect, message and fork runs without being a worker |
| `fork_workflow(id, step)` | `POST /incidents/{id}/fork` | a new run that copies recorded steps before `step` and re-runs from there |
| recovery on launch | `worker.py` | `DBOS.launch()` picks up this executor's unfinished runs |

All the state lives in PostgreSQL tables (schema `dbos`): workflow status, step outputs, messages, events, queues. Open the demo database with `psql` and look; it's a good way to understand it.

## 4. The rules you must follow (and why)

1. **The workflow function must be deterministic.** On recovery it runs again from the top and must make the same calls in the same order, so recorded results line up with the steps. `datetime.now()`, `random`, HTTP calls and model calls go inside steps.
2. **Steps are at-least-once.** If the process dies *during* a step, the step runs again. The test `test_crash_inside_a_step_repeats_the_effect_without_a_key` shows a restart applied twice. Fix: an idempotency key the other system honours (`<run id>:execute`).
3. **Changing code under live runs needs care.** DBOS records an application version per run. Recover on the same version, or fork onto a new one deliberately.

## 5. Explain it without notes

1. Walk through what happens, step by step, when the worker is killed right after triage and a new worker starts.
2. Why did the fix get applied twice in one test and once in the next? What exactly is different?
3. Why is the model call a step? What would go wrong if it weren't?
4. How does a run wait an hour for approval without a process sitting idle for an hour? What happens if the approval arrives while no worker is running?
5. What does "exactly-once" mean in this project, honestly?
6. The same alert fires twice. Why is there only one run?
7. What does `global_concurrency=2` protect against? How did the test prove it was really two at a time, and not one?
8. Explain forking: what's copied, what re-runs, and why the logs were read only once across both runs.
9. When would you choose Temporal instead? When would a plain queue and cron be enough?
10. Name two things this project doesn't handle that a production version would need.

## 6. Your turn

1. **Kill during verify:** add a crash point inside the verify loop and a test that the restarted run doesn't start the sleep over. (Hint: compare timestamps of `DBOS.sleep` steps.)
2. **Real model:** replace `planner.plan` with a Claude call (structured output with the same fields). Keep it inside the `make_plan` step, and write down why that placement matters.
3. **Escalation ladder:** if nobody approves in 15 minutes, page the secondary; after 30, the manager. Use `recv` timeouts, not a scheduler.
4. **Rate limit model calls:** at most 5 plans a minute across all runs, with a queue `limiter`.
