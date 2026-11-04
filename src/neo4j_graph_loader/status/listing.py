"""``graph-loader status --list`` (E15.F1.S6): the runs of a store, with the reader's judgement of each."""
from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

from neo4j_graph_loader.status import locator as locator_module, reader, view

logger = logging.getLogger(__name__)


def _row(status: dict, assessment: dict, attempt: Path | None) -> dict:
    return {"execution_id": status["execution_id"], "run_id": status["run_id"], "state": status["state"], "label": assessment["label"],
            "mode": status["mode"], "runtime": status["runtime"], "started_at": status["started_at"],
            "heartbeat_age_seconds": assessment["heartbeat_age_seconds"], "exit_code": assessment["exit_code"],
            "attempt": None if attempt is None else str(attempt)}


def list_runs(*, store: str = "json", base: Path | str | None = None, active: bool = False, stale_after: float | None = None,
              limit: int = 200, now: float | None = None) -> list[dict]:
    """Rows, newest first. ``json``: every attempt directory under ``base`` (default: the OS temporary directory) whose
    locator names the JSON store; ``sql``: the executions of the database named by ``GRAPH_LOADER_STATUS_DB_URL``."""
    now = time.time() if now is None else now
    rows: list[dict] = []
    if store == "sql":
        from neo4j_graph_loader.status import sql_store
        database = sql_store.SqlStatusStore()
        try:
            database.check_schema()
            for item in database.list_executions(active=active, limit=limit):
                status = {"state": item.state, "heartbeat_at": item.heartbeat_at, "host": item.host, "pid": item.pid,
                          "execution_id": item.execution_id, "run_id": item.run_id, "mode": item.mode, "runtime": item.runtime,
                          "started_at": item.started_at}
                rows.append(_row(status, view.assess(status, now=now, stale_after=stale_after), None))
        finally:
            database.close()
        return rows
    root = Path(base) if base is not None else Path(tempfile.gettempdir())
    for attempt in sorted(root.glob(f"{reader.ATTEMPT_PREFIX}*")):
        try:
            locator = locator_module.read_locator(attempt)
            if locator["store"] != "json":
                continue
            status = reader.read_status(attempt, validate=False)
        except (locator_module.LocatorError, reader.StatusNotFound, reader.StatusDataError, OSError) as exc:
            logger.debug("stage=status reason=skipping %s: %s", attempt, exc)
            continue
        if active and status["state"] not in view.ACTIVE_STATES:
            continue
        rows.append(_row(status, view.assess(status, now=now, stale_after=stale_after, attempt=attempt), attempt))
    rows.sort(key=lambda row: row["started_at"], reverse=True)
    return rows[:limit]

