"""Lock-wait and deadlock-recovery assertion helpers shared by every stress/correctness test.

Every "zero lock waits" claim in this package is proven from Neo4j's own
query log, turned on and read back by this module, never from a sampling
poll (which can miss a wait entirely between polls). Every "zero unhandled
deadlocks" claim is proven by letting a real exception propagate unmodified
through :func:`assert_no_unhandled_deadlock`, plus counting the writer's own
retry log lines.

Config-key correction (see ``plan/v6/slice-4/impl_plan_story_2.md`` for the
empirical evidence): Neo4j 5.21+'s query log (5.26 is the shared pin) already defaults to
``db.logs.query.enabled=VERBOSE`` and ``db.logs.query.threshold=0s``; the one
setting that is *not* already this way is
``db.logs.query.parameter_logging_enabled`` (default ``true``), which this
module turns off explicitly so a 5000-row chunk's parameters are never
logged. The query log's own line format has no ``"ms"`` token directly after
``waiting:`` -- only the outer elapsed-time field does.
"""
from __future__ import annotations

import contextlib
import faulthandler
import re
import threading
import urllib.error
import urllib.request
from typing import Callable, Iterator

_WAIT_RE = re.compile(r"waiting:\s*(\d+)\)")
_METRIC_LINE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{[^}]*\})?\s+([0-9eE+\-.]+)\s*$")


def with_verbose_query_log(container):
    """Log every query, with its exact lock/planning time, and never its parameters.

    Must be called before ``.start()``. ``db.logs.query.enabled``/
    ``threshold`` are already Neo4j 5.21's own defaults; only
    ``parameter_logging_enabled`` needs to be turned off explicitly (else a
    5000-row chunk's whole parameter list is logged, growing logs by tens of
    GB at 1M-row scale).
    """
    return (
        container
        .with_env("NEO4J_db_logs_query_enabled", "VERBOSE")
        .with_env("NEO4J_db_logs_query_threshold", "0ms")
        .with_env("NEO4J_db_logs_query_parameter__logging__enabled", "false")
    )


def with_apoc(container):
    """Enable APOC procedures/functions (``apoc.lock.nodes``, ``apoc.util.sleep``).

    Must be called before ``.start()``. Not installed by default on a plain
    ``neo4j:5.26-enterprise`` container/image. Only needed by tests that use
    ``ApocLockedEdgeWriter(explicit_locks=True)`` (the classic/shared-overlap
    default) or that manufacture contention with ``apoc.util.sleep``;
    planned mode and bulk-lease mode never call an APOC procedure (traced
    against ``src.loader.edge_execution.build_apoc_locked_upsert_query``).
    """
    return (
        container
        .with_env("NEO4J_PLUGINS", '["apoc"]')
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
        .with_env("NEO4J_dbms_security_procedures_allowlist", "apoc.*")
    )


class QueryLogUnavailable(RuntimeError):
    """The query log could not be read from the container (missing, permission, etc.)."""


def with_transaction_timeout(container, seconds: int = 300):
    """Cap every Neo4j transaction's server-side lifetime, a safety net against a truly stuck writer.

    Must be called before ``.start()``. Confirmed against a running
    `neo4j:5.21-enterprise` container (`SHOW SETTINGS`):
    `NEO4J_db_transaction_timeout=300s` -> `db.transaction.timeout = 5m`.
    A stress test's harness-level `timeout_s` (see `planned_harness.
    run_planned_mode`) can abort the *dispatcher*, but a worker thread
    already blocked inside a Neo4j transaction keeps running until the
    driver call returns; this setting bounds that from the server side so
    a genuinely stuck write eventually errors out instead of hanging the
    whole test process past `hard_timeout`'s own ceiling.
    """
    return container.with_env("NEO4J_db_transaction_timeout", f"{int(seconds)}s")


def _query_log_bytes(container) -> bytes:
    # ``query.log*`` (via a shell, since exec_run has no globbing of its own)
    # also picks up rotated files (Neo4j's default: 20 MiB x 7) -- reading
    # only `query.log` would silently drop waits recorded before a rotation.
    # Oldest first (rotated files by mtime, then the live file), so a byte-offset mark taken before a rotation
    # still lines up with the concatenation order.
    result = container.get_wrapped_container().exec_run(
        ["sh", "-c", "for f in $(ls -tr /logs/query.log.* 2>/dev/null) /logs/query.log; do cat \"$f\" || exit 1; done"]
    )
    if result.exit_code != 0:
        raise QueryLogUnavailable(
            f"could not read the Neo4j query log (exit code {result.exit_code}): "
            f"{result.output.decode('utf-8', errors='replace')[:500]!r}"
        )
    return result.output


def query_log_mark(container) -> int:
    """Return the query log's current size in bytes, to use as a ``since=`` window start.

    Every caller that shares one container across more than one measured
    phase (e.g. Story 4's rule that the measurement window starts only once
    node seeding is done) should mark before seeding and pass that mark as
    ``since=`` to :func:`total_lock_wait_ms`/:func:`assert_zero_lock_wait`,
    rather than relying on the earlier phase happening to be wait-free.
    """
    return len(_query_log_bytes(container))


def _window_text(container, since: int) -> str:
    data = _query_log_bytes(container)
    if since < 0 or since > len(data):
        raise ValueError(f"since={since} is out of range for a {len(data)}-byte query log")
    return data[since:].decode("utf-8", errors="replace")


def total_lock_wait_ms(container, *, since: int = 0) -> int:
    """Sum every ``waiting: N`` in the container's query log at or after byte offset ``since``."""
    return sum(int(match.group(1)) for match in _WAIT_RE.finditer(_window_text(container, since)))


def any_lock_wait(container, *, since: int = 0) -> int | None:
    """Return the first nonzero ``waiting: N`` value at or after ``since``, or ``None``."""
    for match in _WAIT_RE.finditer(_window_text(container, since)):
        value = int(match.group(1))
        if value:
            return value
    return None


def assert_zero_lock_wait(container, *, since: int = 0, min_queries: int = 1) -> None:
    """Assert zero lock-wait time, but only after confirming the log actually has queries in it.

    Without ``min_queries``, an empty/missing/rotated-away query log or a
    container that forgot :func:`with_verbose_query_log` would all report
    "0 ms of waiting" -- true, but proving nothing. ``min_queries`` (default
    1) requires at least that many logged queries with a ``waiting:`` field
    in the window before trusting a zero.
    """
    window = _window_text(container, since)
    matches = list(_WAIT_RE.finditer(window))
    if len(matches) < min_queries:
        raise AssertionError(
            f"expected at least {min_queries} logged quer{'y' if min_queries == 1 else 'ies'} with a "
            f"waiting: field since byte offset {since}, found {len(matches)} -- the query log may be "
            f"missing, rotated away, or query logging may not be enabled (with_verbose_query_log not applied)"
        )
    total = sum(int(match.group(1)) for match in matches)
    assert total == 0, f"expected 0 ms of Neo4j lock waiting, measured {total} ms"


def poll_show_transactions(driver, stop: threading.Event, out: list, interval_s: float = 0.05,
                           samples: list | None = None) -> None:
    """Background-thread debugging aid: record any transaction Neo4j reports as blocked.

    Not the pass/fail signal for "zero lock waits" (a sampling poll can miss
    a wait entirely between polls -- the query log is authoritative and
    complete). Exists only so a failing stress test's CI log shows what was
    running when a wait or deadlock happened. Pass ``samples`` (a list) to learn how many polls succeeded: one
    ``True`` is appended per successful poll, so a caller can assert the poller really ran.
    """
    while not stop.is_set():
        try:
            with driver.session() as session:
                for record in session.run(
                    "SHOW TRANSACTIONS YIELD status, activeLockCount, currentQuery "
                    "WHERE status CONTAINS 'Blocked' RETURN status, activeLockCount, currentQuery"
                ):
                    out.append(dict(record))
            if samples is not None:
                samples.append(True)
        except Exception:  # pragma: no cover - best effort debugging aid only
            pass
        stop.wait(interval_s)


def count_retry_log_lines(caplog, edge_type: str | None = None) -> int:
    """Count ``stage=edge-write-retry`` log records, optionally filtered by edge type.

    The retry log call is
    ``logger.warning("stage=edge-write-retry edge=%s records=%s attempt=%s/%s
    delay_s=%.3f code=%s", self._edge_type, ...)`` (``edge_execution.py``),
    so the edge type is ``record.args[0]``.
    """
    count = 0
    for record in caplog.records:
        if record.name != "src.loader.edge_execution" or not record.getMessage().startswith("stage=edge-write-retry"):
            continue
        if edge_type is not None and (not record.args or record.args[0] != edge_type):
            continue
        count += 1
    return count


def assert_no_unhandled_deadlock(write_call: Callable[[], int]) -> int:
    """Call ``write_call``; let any exception propagate; return the attempt count used.

    An unhandled deadlock (or any other exception escaping ``write_call``)
    *must* fail the caller's test -- this function never wraps the call in a
    ``try``/``except``, so nothing here can swallow it.
    """
    attempts = write_call()
    assert isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 0, (
        f"write_call() must return a nonnegative attempt count, got {attempts!r}"
    )
    return attempts


@contextlib.contextmanager
def hard_timeout(seconds: float) -> Iterator[None]:
    """Hard-fail (dump every thread's traceback and exit) if the block runs longer than ``seconds``.

    A stdlib-only (``faulthandler``, no new dependency) ceiling for a
    genuine hang, not a performance assertion: `requirements.txt`/
    `requirements-dev.txt` were checked first and neither lists
    `pytest-timeout` -- adding it just for this would be a
    `requirements*.txt` edit this slice must not make (flagged instead in
    `plan/v6/completion_slice-4.md` as a candidate dependency for a future
    slice). A slow-but-completing run is unaffected; only a real hang past
    the ceiling triggers `faulthandler.dump_traceback_later(..., exit=True)`,
    which calls ``os._exit(1)`` after printing every thread's traceback --
    this fails the test process loudly instead of the whole CI job timing
    out uninformatively.
    """
    faulthandler.dump_traceback_later(seconds, exit=True)
    try:
        yield
    finally:
        faulthandler.cancel_dump_traceback_later()


def try_scrape_metric(url: str, name: str, *, labels: dict[str, str] | None = None) -> float | None:
    """Best-effort Prometheus-text scrape; ``None`` if unreachable or the series is absent.

    Never imports ``prometheus_client`` -- this slice must not gain a hard
    dependency on Slice 2's own dependency. Every caller treats ``None`` as
    "metrics not available yet" and falls back to the query-log/retry-log
    signals in this module; it never fails a test only because the metrics
    endpoint wasn't reachable.

    Without ``labels``, every label combination of ``name`` is summed
    (e.g. ``kind="node"`` and ``kind="edge"`` series would be added
    together) -- pass ``labels`` to require an exact match on that subset of
    labels instead (other labels on the line are ignored).
    """
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, ValueError):
        return None
    total = None
    for line in body.splitlines():
        match = _METRIC_LINE.match(line)
        if not match or match.group(1) != name:
            continue
        if labels is not None:
            label_text = match.group(2) or ""
            found = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', label_text))
            if any(found.get(key) != value for key, value in labels.items()):
                continue
        total = (total or 0.0) + float(match.group(3))
    return total
