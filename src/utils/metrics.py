"""Shared Prometheus metrics facade for every graph-loader process kind.

This module is the single source of truth for metric names, types, and
label shapes across the whole library (`plan/v6/slices.md`'s Phase Summary:
"P6-S2's catalogue is the single source of truth for metric names"). Every
process kind (node loader, edge loader, planned writer, planned dispatcher,
lease server, clock runner, CLI/orchestrator) imports this module rather
than constructing its own `prometheus_client` objects.

Label policy (verbatim; this table is the load-bearing contract — anyone
adding a new `.labels(...)` call site anywhere in `src/` must stay inside
it):

| Label         | Allowed values                                                     | Why bounded |
|---------------|---------------------------------------------------------------------|-------------|
| kind          | closed enum: node, edge, planned-writer, planned-dispatcher,        | one value per process *type*, fixed at 8 forever (P5-S4 amends this from 7 -- see PROCESS_KINDS) |
|               | lease-server, clock, cli, retry-consumer                             | |
| name          | a node label / edge type declared in the schema, or "-" for process  | bounded by schema size, never by record volume |
|               | kinds with no single entity (lease-server, planned-dispatcher,       | |
|               | clock, cli)                                                          | |
| label_or_type | same domain as `name` (records_consumed_total only)                  | forward-looking, kept as its own label per the epic's literal series shape |
| stage         | node-write \\| edge-write                                            | matches write_timing_capture.py's stage= field |
| reason        | closed enum, see RETRY_REASONS                                       | small fixed enum, never an exception's free-text message |
| failure_class | closed enum, see dead_letter.FAILURE_CLASSES (malformed,              | fixed by Phase 5's own DLQ envelope's failure_class field |
|               | missing_endpoint, ambiguous_endpoint)                                 | |
| topic         | a schema-declared Kafka topic                                        | bounded by schema size |

Never used as a label anywhere in this catalogue: a Kafka message key, a
node/relationship key property value, a raw error message, a run_id, an
offset, a partition number, or a container/replica id. A `run_id` label
would give Prometheus unbounded cardinality over time (a fresh value every
run, retained forever by a long-lived server) for no aggregate value pull
scraping doesn't already give per-container; live run correlation is a
Slice 3 Docker Service Discovery *target* label, not a metric label.

Registry model — registry-per-process, not global-default-per-import: a
naive module-level `Counter(..., registry=REGISTRY)` statement is safe in
production (one Python process per container, one import), but many test
files construct multiple `NodeLoader`/`EdgeLoader` instances in one pytest
process; tying metric *construction* to instance construction would
collide the second instance's `Counter(...)` call with the first's already-
registered name. This module resolves that with a factory
(`GraphLoaderMetrics(registry=...)`), not bare module globals: production
uses one lazily-constructed, never-rebuilt instance (`process_metrics()`),
tests construct their own isolated instance per test case.

Env vars (read once, at process start, never on the hot path):

- `GRAPH_LOADER_METRICS_ENABLED` — default enabled; `"0"` or `"false"`
  (case-insensitive) disables the HTTP server (and, per Story 9, the
  exit-time and periodic snapshot writes) entirely. Mirrors
  the existing `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS` boolean-toggle convention
  (`os.environ.get(...) != "0"`).
- `GRAPH_LOADER_METRICS_PORT` — default `9464`. **Orchestrator correction**
  (binding, overrides `plan/v6/slice-2/stories.md`'s literal text, which
  proposed `9090`): 9090 is Prometheus server's own default port, and Slice
  3's optional observability compose profile may run a Prometheus server
  container on the same `graph-loader-net` — giving every *loader*
  container's internal metrics port the identical number to the scraper's
  own default invites confusion even though the two are technically
  different containers. `9464` is used instead: it collides with neither
  the lease server (`7700`), the planned dispatcher (`7701`), nor
  Prometheus's own `9090`. Every occurrence of `9090` in the Slice 2
  stories document is superseded by `9464` (see
  `plan/v6/completion_slice-2.md`).
- `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` — per-replica path (Story 9), set by
  `docker_service.py`, not read by an operator directly.
- `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` — seconds between periodic
  snapshot rewrites; default `15`, `0` disables the periodic timer (the
  exit-time write still happens), minimum `1` (smaller positive values are
  raised to 1), maximum `86400` (larger values are clamped); non-finite or
  unparsable values fall back to the default.
"""
from __future__ import annotations

import logging
import math
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)
from prometheus_client.exposition import start_http_server

logger = logging.getLogger(__name__)

# Orchestrator correction: NOT 9090 (Prometheus server's own default port).
DEFAULT_METRICS_PORT = 9464


def _metrics_disabled() -> bool:
    """``GRAPH_LOADER_METRICS_ENABLED`` of ``0`` or ``false`` (case-insensitive)."""
    return os.environ.get("GRAPH_LOADER_METRICS_ENABLED", "1").strip().lower() in ("0", "false")

WRITE_DURATION_BUCKETS: tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
)
"""Shared by every ``_seconds`` histogram observing a write/lease-wait
duration. `docs/USAGE.md` §9 measures real per-batch write times from tens
of ms (small node batches) to low seconds (large planned chunks under
contention): a 1 ms floor catches an unusually fast small batch without
every sample landing in the ``+Inf``-neighboring overflow bucket; a 30 s
ceiling catches a batch stuck behind a lock/lease wait before the loader's
own retry/backoff would have already intervened.
"""

PROCESS_KINDS = (
    "node",
    "edge",
    "planned-writer",
    "planned-dispatcher",
    "lease-server",
    "clock",
    "cli",
    "retry-consumer",
)
"""Closed enum for the `kind` label. Fixed at 7 forever, per Story 1 --
amended to 8 by P5-S4 (Decisions §7), adding "retry-consumer" for exactly
this new process type's own process_up/process_start_time/process_info
triad and its own DLQ-topic consumer_lag series. This is the same category
of change P5-S1 made to BucketLeaseStreamConfig's settings surface for a
genuinely new, bounded, source-controlled process type -- not a per-record
or per-key value, which is what the "forever" language was actually
defending against. records_consumed_total/write_duration/
deadlocks_recovered/retry_total/dlq_routed_total for the retry consumer's
own Neo4j writes stay under kind="edge" (they describe *edge writes*,
caller-agnostic, exactly the dashboard behavior an operator wants: total
write cost for an edge type, live and retried, in one series)."""

RETRY_REASONS = ("neo4j-transient", "missing-endpoint")
"""Closed enum for `retry_total`'s `reason` label. `"missing-endpoint"` is
registered now (reads 0 until Phase 5's retry consumer lands) per P6-Q1/
P5-Q5's "registered now, held at zero" convention. Note this label's
spelling (hyphenated) is independent of `dead_letter.FAILURE_CLASSES`'s
spelling (underscored `missing_endpoint`) — they are two different label
domains (`reason` on `retry_total` vs. `failure_class` on
`dlq_routed_total`) owned by two different enums; not a typo.
"""

AT_LEAST_ONCE_NOTE = (
    "at-least-once: a crash between a durable write/DLQ-publish and this "
    "counter's increment can double-count on restart; the graph itself "
    "stays exact via idempotent MERGE (P5-Q5). Do not treat a raw counter "
    "delta as an exact retry/DLQ count."
)
"""Repeated verbatim as each Counter's documentation in Story 2, per
`plan/v5-v6-orchestration.md` P5-Q5."""

FAILURE_CLASSES = ("malformed", "missing_endpoint", "ambiguous_endpoint")
"""Closed enum for `dlq_routed_total`'s `failure_class` label. Must match
`src.loader.dead_letter.FAILURE_CLASSES` verbatim (checked by a test in
`tests/test_metrics.py`) — duplicated here, rather than imported, so that
`src/utils/metrics.py` never imports `src/loader/dead_letter.py` at module
scope (keeps this module's own "no prometheus_client calls at import time,
no other project-internal imports" isolation property, and avoids any risk
of a future circular import once `dead_letter.py` grows a reason to import
this module too)."""

PROCESS_VERSION = os.environ.get("GRAPH_LOADER_VERSION", "dev")
"""Reported on `graph_loader_process_info`. Not a schema/record value, so
safe as a label: closed to whatever a deploy pins via env, never derived
from a record."""

# Metric family base names, exactly as `CollectorRegistry.collect()` reports
# them (prometheus_client strips the `_total` counter suffix and the
# `_info` Info suffix from `family.name` — see tests/test_metrics.py for
# the empirical proof this constant is built to match).
EXPECTED_METRIC_FAMILIES = frozenset(
    {
        "graph_loader_records_consumed",
        "graph_loader_neo4j_write_duration_seconds",
        "graph_loader_deadlocks_recovered",
        "graph_loader_retry",
        "graph_loader_dlq_routed",
        "graph_loader_parked",
        "graph_loader_dlq_backlog",
        "graph_loader_retry_stalled_seconds",
        "graph_loader_parked_pair_index_wait_seconds",
        "graph_loader_write_outage_seconds",
        "graph_loader_consumer_lag_records",
        "graph_loader_lease_wait_seconds",
        "graph_loader_lease_grants",
        "graph_loader_lease_denials",
        "graph_loader_lease_wait_timeouts",
        "graph_loader_lease_held_resources",
        "graph_loader_lease_waiters",
        "graph_loader_plan_chunks_planned",
        "graph_loader_plan_chunks_committed",
        "graph_loader_plan_active_writers",
        "graph_loader_plan_resource_wait_seconds",
        "graph_loader_planner_rows_read",
        "graph_loader_planner_chunks_published",
        "graph_loader_process_start_time_seconds",
        "graph_loader_process_up",
        "graph_loader_process",
    }
)
"""The slice's full catalogue: 22 series as of P6-S2 (`stories.md` Story
2's own count of "19" undercounts its own 22-row table — corrected here;
see `plan/v6/completion_slice-2.md`), plus one more (`graph_loader_dlq_backlog`)
added by P5-S4 Story 7 per the P5-Q4 follow-up ("P5-S4 owns a true DLQ
backlog gauge"), for 23 total, plus `graph_loader_retry_stalled_seconds`
(P5-S4 round 4, owner decision D1: the retry consumer's sustained-stall
signal), for 24, plus `graph_loader_parked_pair_index_wait_seconds` (v7-S4: a live
edge loader's post-assignment parked-pair index barrier), for 25, plus
`graph_loader_write_outage_seconds` (a stream loader riding out a Neo4j outage,
src/loader/write_outage.py), for 26. `tests/test_metrics.py`'s completeness test asserts the
registry contains exactly this set, no more, no fewer."""


def build_registry() -> CollectorRegistry:
    """Return one fresh, independent ``CollectorRegistry``.

    Production calls this exactly once per process, lazily, via
    :func:`process_metrics` (never rebuilt for the life of the process).
    Tests call it once per test case to get full isolation without
    monkeypatching ``prometheus_client``'s process-global default registry.
    """
    return CollectorRegistry()


class GraphLoaderMetrics:
    """Every metric object in the catalogue, bound to one registry.

    One instance per process in production (see :func:`process_metrics`);
    one instance per test case in tests that need to assert exact values —
    never shared across test cases, so counter values from one test never
    leak into another's assertions, and never shared across production
    replicas (each replica is already its own OS process with its own
    registry, so no explicit isolation code is needed there either).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else build_registry()
        r = self.registry

        # -- Per-entity write-path series (kind, name) -----------------
        # AT_LEAST_ONCE_NOTE applies to every Counter below.
        self.records_consumed = Counter(
            "graph_loader_records_consumed_total",
            "Records durably written to Neo4j per flushed batch. " + AT_LEAST_ONCE_NOTE,
            ["kind", "name", "label_or_type"],
            registry=r,
        )
        self.write_duration = Histogram(
            "graph_loader_neo4j_write_duration_seconds",
            "Wall time to durably write one batch/chunk, including any "
            "bounded retries and their backoff sleeps before eventual "
            "success (same timer as the stage=node-write/edge-write "
            "write_ms log field; P6-S2 review finding S4-2 - do not read "
            "this as a single transaction's time under retry storms).",
            ["kind", "name", "stage"],
            buckets=WRITE_DURATION_BUCKETS,
            registry=r,
        )
        self.deadlocks_recovered = Counter(
            "graph_loader_deadlocks_recovered_total",
            "A TransientError that was retried successfully (classic/lease "
            "ApocLockedEdgeWriter only; planned mode stays at 0 by "
            "construction). " + AT_LEAST_ONCE_NOTE,
            ["kind", "name"],
            registry=r,
        )
        self.retry = Counter(
            "graph_loader_retry_total",
            "Any bounded retry attempt (node-write retryable-Neo4j-error "
            "today; Phase 5's retry consumer, reason=\"missing-endpoint\", "
            "later). " + AT_LEAST_ONCE_NOTE,
            ["kind", "name", "reason"],
            registry=r,
        )
        self.dlq_routed = Counter(
            "graph_loader_dlq_routed_total",
            "One acknowledged DLQ publish (dead_letter.on_dlq_routed). "
            + AT_LEAST_ONCE_NOTE,
            ["kind", "name", "failure_class"],
            registry=r,
        )
        self.parked = Counter(
            "graph_loader_parked_total",
            "Registered now, held at 0 until Phase 5 Slice 4's retry "
            "consumer/parking-lot lands. " + AT_LEAST_ONCE_NOTE,
            ["kind", "name"],
            registry=r,
        )
        self.dlq_backlog = Gauge(
            "graph_loader_dlq_backlog",
            "Records waiting, split by queue: queue=dlq is the DLQ topic "
            "high watermark minus the retry consumer's own committed "
            "offset (retry work that should drain); queue=parked is the "
            "parking-lot topic's current size (high minus low watermark; "
            "terminal, cumulative -- replay does not trim it -- needs a "
            "human). Entity-scoped (kind=node|edge, "
            "name=<label|type>), like dlq_routed_total/parked_total -- NOT "
            "process-scoped, even though the retry consumer computes it "
            "(P5-S4 Decisions §7). Computed periodically "
            "(retry_consumer.backlog_check_interval_ms), never on the hot "
            "path; a brief staleness window between computations is "
            "expected and documented, not a bug. P5-Q4 follow-up: this is "
            "the true backlog gauge P6-S3's growth-rate proxy alert "
            "switches to.",
            ["kind", "name", "queue"],
            registry=r,
        )
        self.retry_stalled = Gauge(
            "graph_loader_retry_stalled_seconds",
            "Seconds the retry consumer's writes of this edge type have been "
            "failing with nothing written (a read-only or unavailable "
            "database, an unrecognised server-side error); 0 when not "
            "stalled. Owner decision D1: without evidence that a failure is "
            "the row's own, rows are never counted or parked for it -- they "
            "wait, and this gauge (plus a reason=retry_stalled ERROR) is the "
            "signal. Entity-scoped like graph_loader_dlq_backlog "
            "(kind=edge, name=<type>); emitted only by the retry-consumer "
            "process, refreshed every tick.",
            ["kind", "name"],
            registry=r,
        )
        self.parked_pair_index_wait = Gauge(
            "graph_loader_parked_pair_index_wait_seconds",
            "Seconds a live edge loader has held its newly assigned partitions "
            "because its copy of the parked-pair index has not yet caught up "
            "with the index as of the assignment (v7-S4); 0 when caught up. No "
            "record of a new partition is read, written or DLQ'd before then, so "
            "a lagging index can never let a later row of a parked pair through "
            "or sequence it before an earlier one. Entity-scoped (kind=edge, "
            "name=<type>); emitted only by edge loaders with a parked-pair index.",
            ["kind", "name"],
            registry=r,
        )
        self.write_outage = Gauge(
            "graph_loader_write_outage_seconds",
            "Seconds a live stream loader's Neo4j writes have been failing with "
            "connectivity-class errors (Neo4j unavailable, an expired session, a "
            "connection pool timeout, a read-only database, a transient error, the "
            "lease server unavailable); 0 when writing normally. While it is above "
            "0 the loader holds its current batch uncommitted, holds no lease, "
            "pauses its partitions and retries the same batch in order with capped "
            "backoff (src/loader/write_outage.py); a reason=neo4j_outage ERROR is "
            "logged once it passes 30 s. Entity-scoped (kind=node|edge, "
            "name=<label|type>); emitted only by lease-fenced stream-mode node and "
            "edge loaders; computed at scrape time.",
            ["kind", "name"],
            registry=r,
        )
        # v7-S6 cardinality note: ``name`` is the relationship TYPE and ``topic`` is one of the schema's topics, so a
        # type with K topics owns exactly K series and the total never exceeds the number of schema topics. Never add
        # a per-record, per-pair, per-partition or per-sub-stage label here (precedence sub-stages reuse these series).
        self.consumer_lag = Gauge(
            "graph_loader_consumer_lag_records",
            "High-watermark minus committed offset, summed across this "
            "process's assigned partitions.",
            ["kind", "name", "topic"],
            registry=r,
        )
        self.lease_wait = Histogram(
            "graph_loader_lease_wait_seconds",
            "One outer flush's client-side accumulated bucket-lease wait "
            "time, summed across that flush's concurrent workers (P6-S2 "
            "review finding N-d: with N workers each waiting ~T seconds, "
            "this observes ~N*T, which can exceed the flush's own wall "
            "time - do not read it as a latency figure). Each worker's "
            "measurement times its whole acquire_any() call, including "
            "the lease server's own RPC round trip (review finding T2's "
            "note) - a contention-free production run still reads a "
            "small nonzero value from that round trip alone, not from "
            "real contention.",
            ["kind", "name"],
            buckets=WRITE_DURATION_BUCKETS,
            registry=r,
        )

        # -- Lease-server-scoped series (kind only) --------------------
        self.lease_grants = Counter(
            "graph_loader_lease_grants_total",
            "Server-side LeaseStats.grants. " + AT_LEAST_ONCE_NOTE,
            ["kind"],
            registry=r,
        )
        self.lease_denials = Counter(
            "graph_loader_lease_denials_total",
            "Server-side LeaseStats.denials. " + AT_LEAST_ONCE_NOTE,
            ["kind"],
            registry=r,
        )
        self.lease_wait_timeouts = Counter(
            "graph_loader_lease_wait_timeouts_total",
            "Server-side LeaseStats.wait_timeouts. " + AT_LEAST_ONCE_NOTE,
            ["kind"],
            registry=r,
        )
        self.lease_held_resources = Gauge(
            "graph_loader_lease_held_resources",
            "Currently-held resource count.",
            ["kind"],
            registry=r,
        )
        self.lease_waiters = Gauge(
            "graph_loader_lease_waiters",
            "Currently-parked waiter count.",
            ["kind"],
            registry=r,
        )

        # -- Planned-dispatcher-scoped series (kind only) ---------------
        self.plan_chunks_planned = Gauge(
            "graph_loader_plan_chunks_planned",
            "DispatchState.total, set once at dispatcher start. Named "
            "'_planned', not '_total', despite stories.md's original text: "
            "Prometheus/OpenMetrics convention reserves the '_total' "
            "suffix for counters, and promtool would flag a Gauge using "
            "it; corrected here while nothing yet depends on the name "
            "(see plan/v6/completion_slice-2.md).",
            ["kind"],
            registry=r,
        )
        self.plan_chunks_committed = Gauge(
            "graph_loader_plan_chunks_committed",
            "DispatchState.stats.completed, updated at each commit.",
            ["kind"],
            registry=r,
        )
        self.plan_active_writers = Gauge(
            "graph_loader_plan_active_writers",
            "DispatchStats.active.",
            ["kind"],
            registry=r,
        )
        self.plan_resource_wait = Histogram(
            "graph_loader_plan_resource_wait_seconds",
            "One dispatch wait resolution.",
            ["kind"],
            buckets=WRITE_DURATION_BUCKETS,
            registry=r,
        )

        # -- CLI/in-process planner series (kind only) ------------------
        self.planner_rows_read = Counter(
            "graph_loader_planner_rows_read_total",
            "KafkaEdgePlanner's own rows-read progress counter. "
            + AT_LEAST_ONCE_NOTE,
            ["kind"],
            registry=r,
        )
        self.planner_chunks_published = Counter(
            "graph_loader_planner_chunks_published_total",
            "KafkaEdgePlanner's own chunks-published progress counter. "
            + AT_LEAST_ONCE_NOTE,
            ["kind"],
            registry=r,
        )

        # -- Process-info triad (every process kind) --------------------
        self.process_start_time = Gauge(
            "graph_loader_process_start_time_seconds",
            "Set once, at process start, to time.time().",
            ["kind", "name"],
            registry=r,
        )
        self.process_up = Gauge(
            "graph_loader_process_up",
            "1 once the metrics server has bound; a scrape of a crashed "
            "container reads the previous scrape's staleness, never a "
            "false 1.",
            ["kind", "name"],
            registry=r,
        )
        self.process_info = Info(
            "graph_loader_process",
            "Build/version identification.",
            ["kind", "name"],
            registry=r,
        )


_process_metrics: GraphLoaderMetrics | None = None


_process_metrics_lock = threading.Lock()


def process_metrics() -> GraphLoaderMetrics:
    """The one :class:`GraphLoaderMetrics` instance shared within this process.

    Lazily constructed so importing this module (e.g. transitively, from a
    test that imports ``node_loader`` but never runs ``main()``) constructs
    no graph-loader registry or metric objects — only
    ``prometheus_client``'s own module-level default registry/collectors
    are touched by the top-level ``from prometheus_client import ...``,
    which is harmless and unrelated to this module's own catalogue.

    Double-checked locking: every ``main()`` in this slice calls
    :func:`process_metrics` before spawning any worker thread, which
    already closes the realistic production race: this lock is defense in
    depth for any future entry point (or test) that constructs a loader
    without going through ``main()`` first, at zero cost once
    ``_process_metrics`` is set (the common case skips the lock entirely).
    """
    global _process_metrics
    if _process_metrics is None:
        with _process_metrics_lock:
            if _process_metrics is None:
                _process_metrics = GraphLoaderMetrics()
    return _process_metrics


def _reset_process_metrics_for_tests() -> None:
    """Test-only helper: force the next :func:`process_metrics` call to build
    a fresh instance. Never called from production code."""
    global _process_metrics
    _process_metrics = None


def initialize_zero_series(metrics: GraphLoaderMetrics, *, kind: str, name: str) -> None:
    """Pre-touch every series that must read a real ``0`` rather than being
    absent from a scrape, per Story 2's AC ("a Prometheus query for either
    during this phase returns a real 0 sample, not 'no data'").

    A ``prometheus_client`` ``Counter``/``Histogram``/``Gauge`` only creates
    a labelled child (and therefore only appears in ``/metrics``) once
    ``.labels(...)`` has been called for that label combination at least
    once — merely constructing the parent metric in ``__init__`` is not
    enough, and :func:`start_metrics_server`'s own process-info triad only
    forces the ``(kind, name)``-labelled process series into existence, not
    every *other* series scoped to that ``kind``. Call once, at process
    start (never on the hot path), for every process kind, passing that
    process's own ``name`` (a concrete node label/edge type, or ``"-"`` for
    the four kinds with no single entity).

    Known limitation: the planned writer serves every edge type in a plan
    from one process with a fixed ``name="-"`` triad, so this function
    cannot pre-touch its per-type ``deadlocks_recovered{kind="planned-writer",
    name=<edge type>}`` series at startup (the set of types isn't known
    until the plan is read) — Story 6's own instrumentation still creates
    that series correctly once each type's first chunk write happens; it
    simply is not guaranteed to read `0` before that first write. Documented
    in `plan/v6/completion_slice-2.md`.

    ``parked``/``retry``/``dlq_routed`` are pre-touched only for
    ``kind in ("node", "edge")`` (P6-S2 review finding R2): the lease
    server never retries or DLQs, planned mode is fail-closed with no DLQ
    (P5-Q3), and the planned writer's real series are keyed per edge type,
    not the fixed ``"-"`` this function would otherwise pre-touch —
    creating those series anyway would be dashboard noise for a series
    that structurally can never increment for that process kind.
    """
    if kind in ("node", "edge"):
        metrics.parked.labels(kind=kind, name=name)
        for reason in RETRY_REASONS:
            metrics.retry.labels(kind=kind, name=name, reason=reason)
        metrics.dlq_routed.labels(kind=kind, name=name, failure_class="malformed")
        metrics.deadlocks_recovered.labels(kind=kind, name=name)
    if kind == "lease-server":
        metrics.lease_grants.labels(kind=kind)
        metrics.lease_denials.labels(kind=kind)
        metrics.lease_wait_timeouts.labels(kind=kind)
        metrics.lease_held_resources.labels(kind=kind)
        metrics.lease_waiters.labels(kind=kind)
    if kind == "planned-dispatcher":
        metrics.plan_chunks_planned.labels(kind=kind)
        metrics.plan_chunks_committed.labels(kind=kind)
        metrics.plan_active_writers.labels(kind=kind)
    if kind == "cli":
        metrics.planner_rows_read.labels(kind=kind)
        metrics.planner_chunks_published.labels(kind=kind)


_dlq_routed_target: GraphLoaderMetrics | None = None


def _emit_dlq_routed(kind: str, name: str, topic: str, failure_class: str) -> None:
    """The single listener body installed onto ``dead_letter.on_dlq_routed``.

    Resolves its target metrics instance dynamically at call time (not at
    install time): production installs with ``metrics=None`` so this always
    reads whatever :func:`process_metrics` currently returns (the process
    singleton); tests install with an explicit isolated
    ``GraphLoaderMetrics`` so a DLQ publish during a test increments that
    test's own registry, never the process-global one.
    """
    target = _dlq_routed_target if _dlq_routed_target is not None else process_metrics()
    target.dlq_routed.labels(kind=kind, name=name, failure_class=failure_class).inc()


def install_dlq_routed_listener(metrics: GraphLoaderMetrics | None = None) -> None:
    """Replace ``dead_letter.on_dlq_routed`` with a listener that increments
    ``graph_loader_dlq_routed_total`` on ``metrics`` (or, if ``metrics`` is
    ``None``, on :func:`process_metrics` — resolved fresh on every call, not
    bound at install time).

    Single-writer seam: only the node/edge loaders' ``main()`` call this (in
    production, exactly once each, idempotently re-assigning the same
    function object), per `stories.md`'s own "Cross-Cutting Notes" —
    ``dead_letter.py``'s docstring documents that it looks the attribute up
    by name at call time precisely so this kind of post-construction
    replacement is safe. A second, *different* replacement would silently
    drop this one; do not add a second call site without composing both
    callables explicitly.

    Imports ``src.loader.dead_letter`` locally (not at module scope) so
    ``src/utils/metrics.py`` carries no project-internal import at import
    time, matching Story 1's "no prometheus_client calls at import" style
    isolation guarantee and avoiding any risk of a future circular import.
    """
    global _dlq_routed_target
    _dlq_routed_target = metrics
    from src.loader import dead_letter  # local import: see docstring above.

    dead_letter.on_dlq_routed = _emit_dlq_routed


def _reset_dlq_routed_listener_for_tests() -> None:
    """Test-only helper: clear the installed listener's target so it falls
    back to :func:`process_metrics` again. Never called from production
    code. Does not un-install the listener function itself (harmless no-op
    to re-install the same function twice, per the docstring above)."""
    global _dlq_routed_target
    _dlq_routed_target = None


_parked_target: GraphLoaderMetrics | None = None


def _emit_parked(kind: str, name: str, parked_topic: str) -> None:
    """The single listener body installed onto ``dead_letter.on_parked``
    (P5-S4 Story 7). Structurally identical to :func:`_emit_dlq_routed`:
    resolves its target dynamically at call time, not at install time."""
    target = _parked_target if _parked_target is not None else process_metrics()
    target.parked.labels(kind=kind, name=name).inc()


def install_parked_listener(metrics: GraphLoaderMetrics | None = None) -> None:
    """Replace ``dead_letter.on_parked`` with a listener that increments
    ``graph_loader_parked_total`` on ``metrics`` (or :func:`process_metrics`
    when ``metrics`` is ``None``, resolved fresh on every call).

    Mirrors :func:`install_dlq_routed_listener` exactly: single-writer
    seam, only the retry consumer's ``main()`` calls this (P5-S4 Story 7),
    and ``parked_topic`` (the third positional argument ``dead_letter.on_parked``
    is called with) is accepted but not used as a label -- this catalogue
    never labels a series by a topic name derived from a schema override,
    only by the closed ``kind``/``name`` pair, matching ``dlq_routed_total``'s
    own existing pattern for the identical reason.
    """
    global _parked_target
    _parked_target = metrics
    from src.loader import dead_letter  # local import: matches install_dlq_routed_listener's own isolation reasoning.

    dead_letter.on_parked = _emit_parked


def _reset_parked_listener_for_tests() -> None:
    """Test-only helper: clear the installed listener's target so it falls
    back to :func:`process_metrics` again. Never called from production
    code."""
    global _parked_target
    _parked_target = None


def _start_metrics_server_impl(
    metrics: GraphLoaderMetrics,
    *,
    kind: str,
    name: str,
    addr: str = "0.0.0.0",
    port: int | None = None,
    allow_ephemeral_fallback: bool = False,
) -> Any | None:
    """Bind and start the metrics HTTP server, or return ``None``.

    Never raises. ``GRAPH_LOADER_METRICS_ENABLED=0`` skips entirely
    (returns ``None`` with no port bound, no ``process_up``/
    ``process_start_time`` set — fully disabled means fully silent). A bind
    failure is caught, logged at ``WARNING``, and treated like disabled:
    metrics are unavailable for this run, but the loader keeps writing —
    the write path must never depend on whether this call succeeded. Any
    other unexpected exception is also caught here (Story 10's AC: "any
    exception, not only OSError, never propagates out of any of the six
    main()/serve() functions").

    ``addr`` defaults to ``"0.0.0.0"`` for container processes (the
    Docker-network-isolated shape every other launcher already uses for
    the lease server/dispatcher control ports). The CLI's two host-side call
    sites (Story 8) pass ``addr="127.0.0.1"`` explicitly — an operator's own
    workstation/CI runner is not a shared multi-tenant host the way the
    Docker network is, and ``0.0.0.0`` there would expose the endpoint on
    every host network interface with no Docker-network isolation to fall
    back on.

    ``allow_ephemeral_fallback`` — off by default (container processes: a
    bind conflict on the fixed internal port is a real misconfiguration
    signal worth surfacing as "metrics unavailable this run", not silently
    working around, since Docker already gives each container its own
    network namespace so a same-container collision is not a normal
    deployment shape). The CLI (Story 8) passes ``True``: the host process
    is not guaranteed exclusive use of any given port the way a container
    is, so on a bind failure it retries once on an OS-assigned ephemeral
    port (``port=0``) and logs the actual bound port at ``WARNING`` rather
    than going fully dark for the run's whole lifetime.
    """
    if _metrics_disabled():
        return None
    try:
        if port is not None:
            # An explicit `port` (including the internal ephemeral-fallback
            # call's `port=0`, meaning "OS-assigned") is caller-controlled
            # and never re-validated against the 1-65535 range here.
            resolved_port = port
        else:
            raw_port_env = os.environ.get("GRAPH_LOADER_METRICS_PORT")
            try:
                resolved_port = int(raw_port_env) if raw_port_env else DEFAULT_METRICS_PORT
            except ValueError:
                resolved_port = None
            if resolved_port is None or not 1 <= resolved_port <= 65535:
                # P6-S2 review finding F1: an out-of-range value (e.g. "0"
                # or "70000") parses fine as an int but would either bind a
                # random port while every label/env var still claims the
                # bad value, or be rejected by the OS entirely - fall back
                # to the documented default instead.
                logger.warning(
                    "stage=metrics kind=%s name=%s reason=invalid_port_env "
                    "value=%s falling back to default=%s",
                    kind, name, raw_port_env, DEFAULT_METRICS_PORT,
                )
                resolved_port = DEFAULT_METRICS_PORT
        metrics.process_up.labels(kind=kind, name=name).set(0)
        metrics.process_start_time.labels(kind=kind, name=name).set(time.time())
        metrics.process_info.labels(kind=kind, name=name).info({"version": PROCESS_VERSION})
        server = None
        bound_port = None
        try:
            server, _thread = start_http_server(
                resolved_port, addr=addr, registry=metrics.registry
            )
            bound_port = resolved_port
        except OSError as exc:
            if not allow_ephemeral_fallback:
                logger.warning(
                    "stage=metrics kind=%s name=%s reason=bind_failed port=%s error=%s",
                    kind, name, resolved_port, exc,
                )
                return None
            try:
                server, _thread = start_http_server(0, addr=addr, registry=metrics.registry)
                bound_port = server.server_port
                logger.warning(
                    "stage=metrics kind=%s name=%s reason=port_in_use requested_port=%s "
                    "fallback_port=%s",
                    kind, name, resolved_port, bound_port,
                )
            except OSError as exc2:
                logger.warning(
                    "stage=metrics kind=%s name=%s reason=ephemeral_bind_failed "
                    "requested_port=%s error=%s",
                    kind, name, resolved_port, exc2,
                )
                return None
        metrics.process_up.labels(kind=kind, name=name).set(1)
        logger.info(
            "stage=metrics-ready kind=%s name=%s addr=%s port=%s",
            kind, name, addr, bound_port,
        )
        return server
    except Exception as exc:
        # Defensive outer boundary: start_metrics_server must NEVER raise,
        # per Story 10's AC ("any exception, not only OSError, never
        # propagates out of any of the six main()/serve() functions").
        # Everything above (port parsing, metric pre-touches, both bind
        # attempts) is inside this try so a mistake anywhere in this
        # function degrades to "metrics unavailable this run", never a
        # crashed loader.
        logger.warning(
            "stage=metrics kind=%s name=%s reason=unexpected_error error=%s",
            kind, name, exc,
        )
        return None


def start_metrics_server(
    metrics: GraphLoaderMetrics,
    *,
    kind: str,
    name: str,
    addr: str = "0.0.0.0",
    port: int | None = None,
    allow_ephemeral_fallback: bool = False,
) -> Any | None:
    """Bind and start the metrics HTTP server (see
    :func:`_start_metrics_server_impl`; never raises) and, when a snapshot
    path is configured, start the periodic snapshot timer (P6-MET-07) --
    even if the bind failed, since the durable record does not depend on
    the HTTP listener."""
    server = _start_metrics_server_impl(
        metrics, kind=kind, name=name, addr=addr, port=port,
        allow_ephemeral_fallback=allow_ephemeral_fallback,
    )
    try:
        if not _metrics_disabled():
            # CLI: exactly the final-write file (no timing dir -> no periodic
            # snapshots); never a leftover per-container snapshot path.
            path = cli_snapshot_path() if kind == "cli" else os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            if path:
                start_periodic_snapshots(metrics, path)
    except Exception as exc:  # noqa: BLE001 - never raise
        logger.warning("stage=metrics-snapshot reason=periodic_start_failed error=%s", exc)
    return server


def cli_snapshot_path() -> str | None:
    """The CLI process's snapshot file (periodic and final write alike):
    ``$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom``, or ``None`` when the
    timing dir is unset."""
    timing_dir = os.environ.get("GRAPH_LOADER_TIMING_DIR")
    return str(Path(timing_dir) / "metrics-cli.prom") if timing_dir else None


def stop_metrics_server(server: Any | None) -> None:
    """Best-effort, never raises. Shuts down the WSGI server and closes its
    socket so a repeated bind in the same process (e.g. a test) doesn't
    leak a file descriptor; a missed call is harmless since
    ``start_http_server`` already runs its listener on a daemon thread
    (never blocks process exit on its own)."""
    if server is None:
        return
    try:
        server.shutdown()
        server.server_close()
    except Exception as exc:  # pragma: no cover - defensive, never raise
        logger.warning("stage=metrics reason=shutdown_error error=%s", exc)


def safe_emit(emit: Callable[[], None]) -> None:
    """Run one metrics emission (a ``.labels(...).inc()``/``.observe()``
    call, typically wrapped in a lambda) and swallow any exception,
    logging it at WARNING instead of letting it propagate.

    P6-S2 review finding S4-1: without this, a metrics-library bug (a
    label-set mismatch, a negative ``.inc()`` value, ...) raised from
    *inside* a hot-path emission call would be indistinguishable from a
    real write failure to the surrounding ``try/except`` in
    ``node_loader.py``/``edge_loader.py``/``edge_execution.py`` — an
    already-durably-written batch could be reported as failed (no offset
    commit) purely because *counting* it afterwards raised. Every
    hot-path emission call site in this slice (Stories 4-8) goes through
    this helper for exactly that reason: a metrics failure must never
    become a write failure, matching :func:`start_metrics_server`'s own
    "never raises" contract for the exposition side.
    """
    try:
        emit()
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see docstring
        logger.warning("stage=metrics reason=emit_failed error=%s", exc)


def write_metrics_snapshot(metrics: GraphLoaderMetrics, path: str | os.PathLike | None) -> None:
    """Best-effort snapshot of ``metrics`` to ``path`` in Prometheus text
    exposition format, matching every other ``GRAPH_LOADER_TIMING_DIR``
    writer in this codebase's "never raises, silent no-op when unset"
    contract (Story 9). ``path`` is normally
    ``os.environ["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"]``, read by the
    caller so this function stays a pure, easily-testable helper.

    Also a no-op when ``GRAPH_LOADER_METRICS_ENABLED=0`` — disabling
    metrics disables the snapshot too (Story 3's AC: "no change to write
    behavior" and "fully disables... entirely"), checked here rather than
    relying on every caller to remember, since a caller could otherwise
    pass a leftover ``GRAPH_LOADER_METRICS_SNAPSHOT_PATH`` from a previous,
    metrics-enabled run's environment.

    Writes to a uniquely named ``<file>.<pid>.<tid>.tmp`` sibling and ``os.replace``s it into place, so a
    process killed mid-write (e.g. past the Docker stop timeout, after a
    SIGTERM this function's caller already tried to honor) never leaves a
    truncated, unparsceable ``.prom`` file behind for Story 9's run-report
    reader to choke on.
    """
    stop_periodic_snapshots(metrics)
    _write_snapshot_file(metrics, path)


def _write_snapshot_file(metrics: GraphLoaderMetrics, path: str | os.PathLike | None) -> None:
    if not path:
        return
    if _metrics_disabled():
        return
    target = Path(path)
    # Unique per-writer temp name: the periodic timer and the final exit
    # snapshot must never share (and truncate) one temp file.
    tmp_path = target.with_name(f"{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        tmp_path.write_bytes(generate_latest(metrics.registry))
        os.replace(tmp_path, target)
    except Exception as exc:
        logger.warning("stage=metrics-snapshot reason=%s", exc)
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


# ── P6-MET-07: periodic snapshots ────────────────────────────────────────────

DEFAULT_SNAPSHOT_INTERVAL_S = 15.0
MAX_SNAPSHOT_INTERVAL_S = 86400.0
MIN_SNAPSHOT_INTERVAL_S = 1.0
_snapshotters: dict[int, "_PeriodicSnapshotter"] = {}
_snapshotters_lock = threading.Lock()


def _snapshot_interval_seconds() -> float:
    """``GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S`` (default 15); unparsable
    or non-finite values fall back to the default, non-positive values
    disable (``0``), positive values are clamped to [1, 86400]."""
    raw = os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S")
    if raw is None or not raw.strip():
        return DEFAULT_SNAPSHOT_INTERVAL_S
    try:
        value = float(raw)
    except ValueError:
        logger.warning("stage=metrics-snapshot reason=invalid_interval value=%s", raw)
        return DEFAULT_SNAPSHOT_INTERVAL_S
    if not math.isfinite(value):
        logger.warning("stage=metrics-snapshot reason=invalid_interval value=%s", raw)
        return DEFAULT_SNAPSHOT_INTERVAL_S
    if value <= 0:
        return 0.0
    return min(max(value, MIN_SNAPSHOT_INTERVAL_S), MAX_SNAPSHOT_INTERVAL_S)


class _PeriodicSnapshotter:
    """Daemon thread that atomically rewrites the snapshot file every
    ``interval`` seconds. Never raises; ``stop()`` joins it."""

    def __init__(self, metrics: GraphLoaderMetrics, path: str, interval: float) -> None:
        self._metrics, self._path, self._interval = metrics, path, interval
        self._stop = threading.Event()
        self._write_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="metrics-snapshot", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                with self._write_lock:
                    # Checked under the lock: once stop() has returned no
                    # periodic write can land after the final write.
                    if self._stop.is_set():
                        return
                    _write_snapshot_file(self._metrics, self._path)
            except Exception as exc:  # noqa: BLE001 - never propagate
                logger.warning("stage=metrics-snapshot reason=periodic_error error=%s", exc)

    def stop(self) -> None:
        self._stop.set()
        # Waits out any in-flight periodic write (bounded: a wedged write
        # must not hang shutdown; the loop re-checks the stop flag under
        # the lock, so no later write lands in the normal case).
        if self._write_lock.acquire(timeout=5):
            self._write_lock.release()
            if self._thread.is_alive() and self._thread is not threading.current_thread():
                self._thread.join(timeout=5)
        else:
            # The write is wedged: joining the thread would only wait again (worst case 5 s, not 10 s).
            logger.warning("stage=metrics-snapshot reason=stop_lock_timeout path=%s", self._path)


def start_periodic_snapshots(
    metrics: GraphLoaderMetrics, path: str | os.PathLike | None, *, interval_s: float | None = None,
) -> bool:
    """Start (once per ``metrics``) the periodic snapshot timer. Never raises.
    Returns whether a timer is running. The exit-time
    :func:`write_metrics_snapshot` stops it and writes the final values."""
    try:
        if not path:
            return False
        interval = _snapshot_interval_seconds() if interval_s is None else interval_s
        if interval <= 0:
            return False
        with _snapshotters_lock:
            if id(metrics) in _snapshotters:
                return True
            snapshotter = _PeriodicSnapshotter(metrics, str(path), interval)
            snapshotter.start()
            _snapshotters[id(metrics)] = snapshotter
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("stage=metrics-snapshot reason=periodic_start_failed error=%s", exc)
        return False


def stop_periodic_snapshots(metrics: GraphLoaderMetrics) -> None:
    """Stop the timer for ``metrics`` if any. Never raises."""
    try:
        with _snapshotters_lock:
            snapshotter = _snapshotters.pop(id(metrics), None)
        if snapshotter is not None:
            snapshotter.stop()
    except Exception as exc:  # noqa: BLE001
        logger.warning("stage=metrics-snapshot reason=periodic_stop_failed error=%s", exc)
