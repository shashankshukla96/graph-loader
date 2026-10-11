"""Stream-mode Neo4j outage ride-out for the live edge and node loaders.

Before this module, a live stream loader whose Neo4j write failed exited
non-zero, and stream supervision then stopped the whole fleet (rc 1). That was
safe, but it was not resilient. Now a stream loader handles a
connectivity-class failure (see :mod:`src.loader.write_errors`) like this:

* The failed transaction has already rolled back. The lease coordinator gave
  up every lease of the failed flush: it released confirmed ones, and it closed
  the connection for ambiguous ones, so the lease server quarantines those
  until ``T + margin`` (the P5-Q1 fencing rule). The loader holds no lease
  while it waits.
* It commits nothing. The batch stays in memory, and its offsets stay
  unresolved.
* It pauses its assigned partitions and keeps polling Kafka every
  ``WAIT_SLICE_SECONDS`` (callbacks only, no new data). It never exceeds
  ``max.poll.interval.ms``, so the group does not rebalance.
* It waits a capped exponential delay (``OUTAGE_BACKOFF_SECONDS``: 0.5 s,
  doubling to 30 s), then retries the same batch, in the same order.

A retry is equivalent to the redelivery that a crash and restart would cause.
The rows are the same and their order is the same, and nothing past them was
committed. The difference is that the process and its Kafka assignment
survive. ``MERGE`` is idempotent, so rows of cells that committed before the
failure are rewritten without harm.

What still fails as before:

* row, unknown, capacity and server-class errors (the loader's existing
  DLQ routing or fail-closed exit);
* fatal-class errors (authentication, a missing database) that persist
  continuously for ``fatal_after_seconds`` (600 s by default, the retry
  consumer's D2 rule and the same constants). The loader then exits 1, and
  supervision stops the fleet.

A stop (SIGTERM) during an outage raises :class:`WriteOutageDrainAbandoned`, a
:class:`~src.loader.edge_execution.LeaseDrainAbandoned`. The loaders already
turn that into exit 0, with the batch left uncommitted for redelivery.

Observability: a WARNING when an outage begins and after each failed retry;
a ``reason=neo4j_outage`` ERROR once the outage has lasted
``error_after_seconds``, then again every ``error_interval_seconds``; an INFO
on recovery; and the ``graph_loader_write_outage_seconds{kind,name}`` gauge.

Only the stream entrypoints construct it, and only for lease-fenced writers:
``edge_loader.main`` with ``--mode stream``, no ``--slot-gating`` and a
bucket-lease address, and ``node_loader.main`` with ``--mode stream`` and a
``LeasedNodeWriter``. An unleased writer has no transaction timeout and no
fencing, so a retry after an ambiguous failure could wait on an orphaned
transaction's locks; it keeps failing fast. Bulk loaders, planned mode, the
planned writers and the bulk monitors never construct it (``edge_loader``
imports this module at module level, but a bulk process never instantiates it),
so their fail-fast paths are unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
from threading import Event, Lock
import time
from typing import Callable

from src.loader import write_errors
from src.loader.edge_execution import LeaseDrainAbandoned
from src.utils import metrics as metrics_module

logger = logging.getLogger(__name__)

OUTAGE_BACKOFF_SECONDS = (0.5, 30.0)
"""Retry delay after the n-th consecutive failure: ``0.5 s * 2**(n-1)``, capped at 30 s."""
OUTAGE_ERROR_AFTER_SECONDS = 30.0
"""The ``reason=neo4j_outage`` ERROR is first logged once an outage has lasted this long."""
OUTAGE_ERROR_INTERVAL_SECONDS = 300.0
"""Then it is repeated at most this often while the outage lasts."""
WAIT_SLICE_SECONDS = 0.2
"""How often a waiting loader polls Kafka and checks for a stop."""
NOT_RIDDEN_TRANSIENT_CODE_PREFIXES = (
    "Neo.TransientError.General.OutOfMemoryError",
    "Neo.TransientError.General.StackOverFlowError",
)
"""Transient by Neo4j's classification, but one huge batch can cause them by
itself: like server and capacity errors, the live loaders fail on them as they
always did instead of retrying the same batch forever. Only this ride-out
excludes them; the shared taxonomy (and so the retry consumer) is unchanged."""

RETRY = "retry"
FAIL = "fail"
FATAL = "fatal"

WAIT_ELAPSED = "elapsed"
WAIT_STOPPED = "stopped"
WAIT_POLL_FAILED = "poll_failed"
WAIT_DROPPED = "dropped"


class WriteOutageDrainAbandoned(LeaseDrainAbandoned):
    """A stop arrived while a stream loader was riding out a Neo4j outage.

    Nothing of the held batch was committed, so it is redelivered after a
    restart. The loaders exit 0, as for any other drain abandon."""


@dataclass(frozen=True)
class OutageVerdict:
    """What a loader does with one failed write.

    ``action`` is ``RETRY`` (wait ``delay_seconds``, then rewrite the same
    batch), ``FAIL`` (today's behaviour) or ``FATAL`` (a persistent
    authentication or missing-database error: exit 1)."""

    action: str
    error_class: str
    delay_seconds: float = 0.0


def _describe(exc: BaseException | None) -> str:
    if exc is None:
        return "-"
    text = " ".join(str(exc).split())
    if len(text) > 300:
        text = text[:297] + "..."
    return f"{type(exc).__name__}: {text}"


class StreamWriteOutage:
    """Outage state of one stream loader process: one entity, one gauge series.

    It is not thread-safe, and it does not need to be: only the loader's poll
    thread calls it."""

    def __init__(
        self, *, kind: str, name: str,
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
        event_logger: logging.Logger = logger,
        context: str = "",
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        backoff_seconds: tuple[float, float] = OUTAGE_BACKOFF_SECONDS,
        error_after_seconds: float = OUTAGE_ERROR_AFTER_SECONDS,
        error_interval_seconds: float = OUTAGE_ERROR_INTERVAL_SECONDS,
        fatal_after_seconds: float = write_errors.FATAL_OUTAGE_SECONDS,
        fatal_gap_seconds: float = write_errors.FATAL_GAP_SECONDS,
        wait_slice_seconds: float = WAIT_SLICE_SECONDS,
    ) -> None:
        if kind not in ("node", "edge"):
            raise ValueError("the stream write-outage ride-out is for node and edge loaders only")
        base, cap = backoff_seconds
        if not 0 < base <= cap:
            raise ValueError("outage backoff must satisfy 0 < base <= cap")
        if min(error_after_seconds, error_interval_seconds, fatal_after_seconds, fatal_gap_seconds) < 0:
            raise ValueError("outage thresholds must be nonnegative")
        if wait_slice_seconds <= 0:
            raise ValueError("outage wait slice must be positive")
        self._kind, self._name = kind, name
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self._logger = event_logger
        self._context = f" {context.strip()}" if context.strip() else ""
        self._clock, self._sleep = clock, sleep
        self._backoff = (float(base), float(cap))
        self._error_after = error_after_seconds
        self._error_interval = error_interval_seconds
        self._fatal_after = fatal_after_seconds
        self._fatal_gap = fatal_gap_seconds
        self._slice = wait_slice_seconds
        self._since: float | None = None
        self._failures = 0
        self._last_error: BaseException | None = None
        self._error_logged_at: float | None = None
        # (first, last) clock readings of the current continuous fatal-class run.
        self._fatal: tuple[float, float] | None = None
        self._dlq_memory: "HeldBatchDlqMemory | None" = None
        # The gauge is computed at scrape (and snapshot) time, so it keeps rising
        # while a loader is blocked inside a long attempt (a lease wait, a driver
        # connect timeout). It reads a real 0 from start-up, in stream processes only.
        kind, name = self._kind, self._name
        metrics_module.safe_emit(
            lambda: self._metrics.write_outage.labels(kind=kind, name=name).set_function(self._gauge_value)
        )

    def _gauge_value(self) -> float:
        try:
            return self.outage_seconds
        except Exception:  # noqa: BLE001 - a scrape must never fail because of this gauge
            return 0.0

    # -- state ---------------------------------------------------------------

    @property
    def active(self) -> bool:
        """Whether the last write attempt failed with a connectivity-class error."""
        return self._since is not None

    @property
    def outage_seconds(self) -> float:
        return 0.0 if self._since is None else max(0.0, self._clock() - self._since)

    @property
    def failures(self) -> int:
        return self._failures

    @staticmethod
    def rides_out(exc: BaseException) -> bool:
        """Whether a failure of this class is ridden out (until the fatal rule applies).

        The same decision as :meth:`on_failure`'s RETRY/FATAL versus FAIL, so the
        loaders log a failure that is not ridden out as an ERROR and, inside a
        rebalance callback, fail closed on it instead of handing the batch over."""
        if write_errors.classify_write_error(exc) != "connectivity":
            return False
        return not (getattr(exc, "code", None) or "").startswith(NOT_RIDDEN_TRANSIENT_CODE_PREFIXES)

    def on_failure(self, exc: BaseException) -> OutageVerdict:
        """Record one failed write attempt of the held batch and decide what to do."""
        error_class = write_errors.classify_write_error(exc)
        if error_class != "connectivity":
            return OutageVerdict(FAIL, error_class)
        if not self.rides_out(exc):
            return OutageVerdict(FAIL, "server")  # OutOfMemoryError / StackOverFlowError
        now = self._clock()
        # The retry consumer's F2 rule: only Neo4j's own answer resets a fatal
        # run (a non-fatal Neo4j code, or a successful write). An error without
        # a Neo4j code, such as a dropped connection, neither resets nor extends it.
        if write_errors.is_fatal_error(exc):
            first, last = self._fatal if self._fatal is not None else (now, now)
            if now - last > self._fatal_gap:
                first = now  # a gap: not one continuous run
            self._fatal = (first, now)
        elif write_errors.is_neo4j_coded(exc):
            self._fatal = None
        if self._since is None:
            self._since, self._failures, self._error_logged_at = now, 0, None
            self._logger.warning(
                "stage=write-outage kind=%s name=%s%s reason=neo4j_unavailable error_class=%s error=%s -- "
                "holding the batch uncommitted (rolled back, no lease held) and retrying it in order",
                self._kind, self._name, self._context, error_class, _describe(exc),
            )
        self._failures += 1
        self._last_error = exc
        self.refresh()
        if self._fatal is not None and now - self._fatal[0] >= self._fatal_after:
            return OutageVerdict(FATAL, error_class)
        base, cap = self._backoff
        delay = min(base * 2 ** min(self._failures - 1, 16), cap)
        return OutageVerdict(RETRY, error_class, delay)

    def log_retry(self, verdict: OutageVerdict, exc: BaseException) -> None:
        self._logger.warning(
            "stage=write-outage-retry kind=%s name=%s%s attempt=%s outage_s=%.1f delay_s=%.1f error=%s",
            self._kind, self._name, self._context, self._failures, self.outage_seconds,
            verdict.delay_seconds, _describe(exc),
        )

    def log_fatal(self, exc: BaseException) -> None:
        first = self._fatal[0] if self._fatal is not None else self._clock()
        self._logger.error(
            "stage=write-outage kind=%s name=%s%s reason=neo4j_fatal fatal_s=%.0f attempts=%s error=%s -- "
            "authentication or database errors have persisted; exiting non-zero so stream supervision "
            "stops the fleet (D2). Nothing of the held batch was committed",
            self._kind, self._name, self._context, self._clock() - first, self._failures, _describe(exc),
        )

    def remember_dlq_acks(self) -> "HeldBatchDlqMemory":
        """The held batch's DLQ memory (see :class:`HeldBatchDlqMemory`); the stream
        entrypoint wraps the missing-endpoint sink and the parked-pair index with it."""
        if self._dlq_memory is None:
            self._dlq_memory = HeldBatchDlqMemory()
        return self._dlq_memory

    def batch_done(self) -> None:
        """The held batch was committed or handed over: forget its DLQ'd rows."""
        if self._dlq_memory is not None:
            self._dlq_memory.forget()

    def recovered(self) -> None:
        """A write of the held batch (or any later batch) succeeded."""
        self._fatal = None
        self.batch_done()
        if self._since is None:
            return
        self._logger.info(
            "stage=write-outage-recovered kind=%s name=%s%s outage_s=%.1f attempts=%s",
            self._kind, self._name, self._context, self.outage_seconds, self._failures,
        )
        self._reset()

    def abandon(self, reason: str) -> None:
        """The held batch was handed to its next owner (a revoke during the outage).

        Only the backoff restarts. The outage itself is not over: its clock, the
        gauge and the ERROR cadence carry on until a write succeeds, so repeated
        rebalances during a long outage cannot keep the alert from firing."""
        self.batch_done()
        if self._since is None:
            return
        self._logger.warning(
            "stage=write-outage-abandoned kind=%s name=%s%s outage_s=%.1f attempts=%s reason=%s -- "
            "the outage clock keeps running until a write succeeds",
            self._kind, self._name, self._context, self.outage_seconds, self._failures, reason,
        )
        self._failures = 0

    def _reset(self) -> None:
        self._since, self._failures, self._last_error, self._error_logged_at = None, 0, None, None

    def refresh(self) -> None:
        """Update the gauge, and log the sustained-outage ERROR when it is due."""
        if self._since is None:
            return
        now = self._clock()
        seconds = max(0.0, now - self._since)
        if seconds >= self._error_after and (
            self._error_logged_at is None or now - self._error_logged_at >= self._error_interval
        ):
            self._error_logged_at = now
            self._logger.error(
                "stage=write-outage kind=%s name=%s%s reason=neo4j_outage outage_s=%.0f attempts=%s error=%s -- "
                "Neo4j writes keep failing; the batch is held uncommitted (no lease held, partitions paused) "
                "and retried in order with capped backoff",
                self._kind, self._name, self._context, seconds, self._failures, _describe(self._last_error),
            )

    # -- waiting -------------------------------------------------------------

    def wait(
        self, seconds: float, *, poll: Callable[[], bool], stop: Event, pending: Callable[[], bool],
    ) -> str:
        """Wait ``seconds`` while keeping the Kafka group membership alive.

        ``poll`` services Kafka callbacks (the partitions are paused by the
        caller) and returns False on a rebalance failure. ``pending`` reports
        whether the held batch still exists; a revoke during the wait may hand
        it to its next owner. Returns ``WAIT_STOPPED``, ``WAIT_POLL_FAILED``,
        ``WAIT_DROPPED`` or ``WAIT_ELAPSED``."""
        deadline = self._clock() + max(0.0, seconds)
        while True:
            if stop.is_set():
                return WAIT_STOPPED
            if not poll():
                return WAIT_POLL_FAILED
            if not pending():
                return WAIT_DROPPED
            self.refresh()
            remaining = deadline - self._clock()
            if remaining <= 0:
                return WAIT_ELAPSED
            self._sleep(min(self._slice, remaining))


class HeldBatchDlqMemory:
    """The rows of the held batch that an earlier attempt already dead-lettered.

    A retried attempt routes the same unresolved rows again: their pair is still
    in the parked-pair index, so the writer treats them as blocked, which is
    right. Routing a row means two things: ``IndexedMissingEndpointSink`` first
    adds the row to the index, then publishes its DLQ envelope. For a row that
    an earlier attempt of the same held batch already added *and* published
    (acknowledged), both must be skipped:

    * Skipping only the publish orphans an index entry. Suppose the retry
      consumer has resolved the first copy and removed its entry between the
      attempts. The re-add creates an entry that no DLQ copy will ever remove,
      so the pair blocks forever.
    * Repeating both resurrects a resolved row. The re-added entry makes the new
      copy look outstanding, and the retry consumer can then apply it after a
      newer row of the same pair: an older value overwrites a newer one. A
      Docker proof hit this (48 WORKS_AT pairs).

    Skipping both is exactly right. If the row is still indexed, its first copy
    is outstanding and will be retried. If not, the retry consumer has already
    written or parked it. The writer's own decisions (blocked, parked pairs) are
    unchanged. A row is remembered only after its publish was acknowledged, so a
    row whose cell never committed, or whose publish failed, is routed in full
    on the next attempt. Keys are the rows' Kafka provenance
    ``(topic, partition, offset)``. The memory is cleared once the batch is
    committed or handed over (``StreamWriteOutage.batch_done``), so it holds at
    most one batch. The cell workers call it from several threads."""

    def __init__(self) -> None:
        self._acked: set[tuple] = set()
        self._lock = Lock()
        self.skipped_adds = 0
        self.skipped_publishes = 0

    def wrap_sink(self, sink) -> "_DlqAckRecordingSink":
        return _DlqAckRecordingSink(sink, self)

    def wrap_index(self, index) -> "_DlqAckAwareIndex":
        return _DlqAckAwareIndex(index, self)

    def acked(self, key: tuple) -> bool:
        with self._lock:
            return key in self._acked

    def mark(self, key: tuple) -> None:
        with self._lock:
            self._acked.add(key)

    def forget(self) -> None:
        with self._lock:
            self._acked.clear()


class _DlqAckRecordingSink:
    """The missing-endpoint sink, minus rows already acknowledged for the held batch."""

    def __init__(self, sink, memory: HeldBatchDlqMemory) -> None:
        self._sink, self._memory = sink, memory

    def route_unresolved_endpoint(self, **kwargs) -> None:
        key = (kwargs["topic"], kwargs["partition"], kwargs["offset"])
        if self._memory.acked(key):
            with self._memory._lock:
                self._memory.skipped_publishes += 1
            return
        self._sink.route_unresolved_endpoint(**kwargs)
        self._memory.mark(key)


class _DlqAckAwareIndex:
    """The parked-pair index as the live writer uses it, minus re-adds of rows already
    acknowledged for the held batch. Every other call (``is_blocked`` and so on) is the
    index's own; ``scoped`` (v7-S7 multi-pair types) returns a wrapped scoped view."""

    def __init__(self, index, memory: HeldBatchDlqMemory) -> None:
        self._index, self._memory = index, memory

    def add(self, source_key, target_key, *, original_topic, original_partition, original_offset,
            first_failure_at_ms) -> None:
        if self._memory.acked((original_topic, original_partition, original_offset)):
            with self._memory._lock:
                self._memory.skipped_adds += 1
            return
        self._index.add(
            source_key, target_key, original_topic=original_topic, original_partition=original_partition,
            original_offset=original_offset, first_failure_at_ms=first_failure_at_ms,
        )

    def scoped(self, endpoint_pair):
        return _DlqAckAwareIndex(self._index.scoped(endpoint_pair), self._memory)

    def __getattr__(self, name):
        return getattr(self._index, name)
