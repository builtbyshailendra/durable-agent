"""The worker process: runs remediation workflows from the queue.

    python -m durable_agent.worker

On start, DBOS also recovers any workflow this executor left unfinished
(a crash, a kill, a reboot) and carries on from its last recorded step.
"""

from __future__ import annotations

import signal
import threading

from dbos import DBOS, DBOSConfig

from durable_agent.settings import SETTINGS


def main() -> None:
    config: DBOSConfig = {
        "name": "durable-agent",
        "system_database_url": SETTINGS.db_url,
        "log_level": "WARNING",
    }
    DBOS(config=config)
    from durable_agent import runbook  # registers the workflow

    DBOS.launch()
    runbook.register_queue()
    print("worker ready", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    while not stop.wait(0.5):  # a timed wait lets Ctrl+C through on Windows
        pass
    DBOS.destroy()


if __name__ == "__main__":
    main()
