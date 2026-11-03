"""A process that records a run in a status outbox while the database is unreachable, then dies (E15.F1.S5).

``python e15_outbox_child.py ATTEMPT DB_URL MODE EVENTS`` where MODE is one of

* ``kill-after-ack``    emit EVENTS events, then SIGKILL itself immediately after the last one's durable acknowledgement;
* ``kill-before-write`` emit EVENTS events, then SIGKILL itself inside the next append, before its line is written;
* ``unwritable``        emit EVENTS events, make the outbox directory read-only, emit 3 more, SIGKILL itself at once;
* ``clean``             emit EVENTS events and finish normally (the database stays unreachable).

Every acknowledged event is reported on stdout as ``acked <seq>`` (flushed) so the parent knows which ones the child
saw acknowledged before it died.
"""
import os
import signal
import sys
import time
from pathlib import Path

from neo4j_graph_loader.status import StatusTracker, locator, outbox, sql_store

attempt, url, mode, count = Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
os.environ["GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S"] = "0.3"

execution = StatusTracker(mode="stream", run_id="child", runtime="process")
locator.write_locator(attempt, locator.build_locator(
    store="sql", execution_id=execution.execution_id, run_id="child", created_at=execution.snapshot()["started_at"], db_url=url))
store = sql_store.SqlStatusStore(url)                    # the database is down: nothing is ever written to it here
journal = outbox.OutboxJournal(attempt, store, execution.execution_id, snapshot_interval=0.05, retry_seconds=0.05,
                               max_retry_seconds=0.1, on_degraded=execution.mark_history_incomplete,
                               lock=outbox.AttemptLock(attempt))
journal._lockfile.acquire()
assert execution.attach_journal(journal)
execution.subscribe(on_snapshot=journal.offer_snapshot)


def emit(label: str) -> None:
    execution.error("child", label)
    print(f"acked {execution.events()[-1]['seq']}", flush=True)


for index in range(count):
    emit(f"event {index}")

deadline = time.monotonic() + 5
while not (attempt / outbox.OUTBOX_DIR / outbox.SNAPSHOT_NAME).exists() and time.monotonic() < deadline:
    time.sleep(0.02)                                      # the first snapshot is in the outbox (it is only the latest that counts)

if mode == "kill-after-ack":
    os.kill(os.getpid(), signal.SIGKILL)
elif mode == "kill-before-write":
    real = outbox.Outbox.append

    def die_first(self, make_event):
        os.kill(os.getpid(), signal.SIGKILL)

    outbox.Outbox.append = die_first
    emit("never acknowledged")
elif mode == "unwritable":
    journal._outbox.close_segment()                      # the next append must create a file: a read-only directory refuses
    (attempt / outbox.OUTBOX_DIR).chmod(0o500)
    for index in range(3):
        emit(f"after the outbox became unwritable {index}")
    os.kill(os.getpid(), signal.SIGKILL)
elif mode == "clean":
    execution.finish(0)
    print("closed", journal.close(0.3), flush=True)
