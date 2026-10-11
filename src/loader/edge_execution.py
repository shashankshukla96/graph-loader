"""Execution-mode capability checks for concurrent relationship loading."""
from __future__ import annotations

from collections import deque
import heapq
from dataclasses import dataclass, field, replace as dataclass_replace
import json
import logging
import os
import random
from threading import Event, Lock
import time
from typing import TYPE_CHECKING, Callable, Iterable, Protocol, Sequence
from concurrent.futures import ThreadPoolExecutor
from neo4j import Driver
from neo4j.exceptions import ClientError, CypherSyntaxError, TransientError

from src.loader.edge_records import normalize_edge_record
from src.loader.fenced_tx import LeaseDeadlineExceeded, begin_fenced_transaction, check_deadline
from src.loader.lease_client import LeaseCoordinatorError
from src.models.schema import EdgeConfig, edge_dead_letter_topic_name
from src.utils.neo4j_session import open_session
from src.utils import metrics as metrics_module

if TYPE_CHECKING:
    from src.loader.edge_loader import EdgeRecord
    from src.loader.mix_and_batch import RoutedEdgeRecord

logger = logging.getLogger(__name__)


class ExecutionModeError(RuntimeError):
    """Raised when the selected edge execution strategy is unsupported."""


@dataclass(frozen=True)
class ServerCapabilities:
    neo4j_version: str = "unknown"
    cypher_25: bool = False


def probe_server_capabilities(driver: Driver) -> ServerCapabilities:
    """Read-only probe for Cypher 25 support; connectivity errors propagate."""
    server_info = driver.get_server_info()
    version = str(getattr(server_info, "agent", "unknown"))
    try:
        with open_session(driver) as session:
            session.run("CYPHER 25 RETURN 1 AS supported").consume()
        return ServerCapabilities(neo4j_version=version, cypher_25=True)
    except CypherSyntaxError:
        return ServerCapabilities(neo4j_version=version, cypher_25=False)
    except ClientError as exc:
        # Neo4j 5.26 reports an unavailable Cypher 25 dialect as an argument
        # error rather than a syntax error.  This probe issues only the fixed
        # Cypher-25 statement, so recognize that exact server response while
        # preserving authentication, authorization, and unrelated failures.
        message = str(getattr(exc, "message", "")).lower()
        if (
            exc.code == "Neo.ClientError.Statement.ArgumentError"
            and "cypher version" in message
            and "25" in message
        ):
            return ServerCapabilities(neo4j_version=version, cypher_25=False)
        raise


def require_execution_mode(mode: str, capabilities: ServerCapabilities) -> None:
    """Fail closed before polling Kafka when native scheduling is unavailable."""
    if mode == "native_disjoint" and not capabilities.cypher_25:
        raise ExecutionModeError(
            f"Execution mode native_disjoint requires Cypher 25; server={capabilities.neo4j_version}"
        )


def build_apoc_locked_upsert_query(
    edge_config: EdgeConfig, *, explicit_locks: bool = True, sort_rows: bool = True,
) -> str:
    """Return the single-statement validate, globally lock, and merge query.

    Endpoints are resolved once.  Row ``i`` yields ``|sources_i| x |targets_i|``
    matched pairs, so ``checked_rows`` (rows with at least one pair) equals
    ``size($rows)`` and ``matched_pairs`` equals ``size($rows)`` together iff
    every row resolved exactly one source and one target.  ``invalid_rows`` is
    ``missing rows + surplus pairs``, which is zero exactly in that case.  Only
    then does the guarded subquery take the elementId-ordered APOC locks and
    MERGE; otherwise it writes nothing and returns ``merged_rows = 0`` so the
    client raises and rolls back.  Plain ``MATCH`` keeps the endpoint lookups
    non-locking index seeks (``OPTIONAL MATCH``/``COLLECT`` subqueries in an
    updating statement plan as locking unique seeks).

    Without explicit locks, rows are merged in ``elementId`` order to reduce
    deadlocks under Neo4j's own locking -- unless ``sort_rows`` is false
    (planned mode: concurrent transactions are node-disjoint, so the input
    order, which fixes ``SET r += props`` for repeated pairs, is kept).

    Planned mode streams rows straight into ``MERGE`` without collecting them:
    ``count(DISTINCT row_index)`` and ``count(*)`` give the same
    ``checked_rows``/``matched_pairs`` proof, but it is only available after
    the writes, so an invalid batch is rejected by the client, which rolls the
    whole transaction back before anything commits.

    ``edge_config.order_by`` (P5-S4 Story 4): when set, the unconditional
    ``SET r += props`` becomes a conditional one, applied only when the
    incoming value is at least as new as what is already stored --
    ``row.properties[$order_by] IS NULL`` (the incoming record carries no
    ordering value: applied unconditionally, since refusing to update at
    all would be worse) ``OR r[$order_by] IS NULL`` (nothing stored yet to
    compare against: also applied unconditionally) ``OR
    row.properties[$order_by] > r[$order_by]`` (strictly newer). A tie or
    a strictly-older incoming value is a silent no-op for that row (not an
    error, not a DLQ condition) -- counted separately as
    ``skipped_as_not_newer_rows`` rather than folded into ``merged_rows``
    (the relationship is still matched/merged; only the property update is
    skipped). Implemented with ``FOREACH``, not a ``WHERE``-filtered
    ``WITH`` -- a plain ``WITH ... WHERE`` would drop the skipped rows
    from every later aggregate (``merged_rows``/``checked_rows``), silently
    undercounting a batch that must still report every row as accounted
    for. This is the *only* difference order_by makes to this query; for
    ``edge_config.order_by is None`` the query text is byte-for-byte
    identical to before this story.
    """
    edge_config.single_pair  # v7-S7: a multi-pair parent raises MultiPairEdgeError; pass a pair view
    order_by = edge_config.order_by
    if not explicit_locks and not sort_rows:
        if order_by is None:
            return (
                "UNWIND range(0, size($rows) - 1) AS row_index\n"
                "WITH row_index, $rows[row_index] AS row\n"
                f"MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
                f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
                f"MERGE (s)-[r:`{edge_config.type}`]->(t) SET r += row.properties\n"
                "WITH count(DISTINCT row_index) AS checked_rows, count(*) AS matched_pairs\n"
                "RETURN checked_rows, (size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows, "
                "matched_pairs AS merged_rows"
            )
        return (
            "UNWIND range(0, size($rows) - 1) AS row_index\n"
            "WITH row_index, $rows[row_index] AS row\n"
            f"MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
            f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
            f"MERGE (s)-[r:`{edge_config.type}`]->(t)\n"
            "WITH row_index, r, row, (row.properties[$order_by] IS NULL OR r[$order_by] IS NULL "
            "OR row.properties[$order_by] > r[$order_by]) AS should_apply_order_by\n"
            "FOREACH (_x_ IN CASE WHEN should_apply_order_by THEN [1] ELSE [] END | SET r += row.properties)\n"
            "WITH count(DISTINCT row_index) AS checked_rows, count(*) AS matched_pairs, "
            "sum(CASE WHEN should_apply_order_by THEN 0 ELSE 1 END) AS skipped_as_not_newer_rows\n"
            "RETURN checked_rows, (size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows, "
            "matched_pairs AS merged_rows, skipped_as_not_newer_rows"
        )
    merge_and_set = (
        f"MERGE (s)-[r:`{edge_config.type}`]->(t) SET r += props\n"
        "RETURN count(*) AS merged_rows\n"
        if order_by is None else
        f"MERGE (s)-[r:`{edge_config.type}`]->(t)\n"
        "WITH r, props, (props[$order_by] IS NULL OR r[$order_by] IS NULL "
        "OR props[$order_by] > r[$order_by]) AS should_apply_order_by\n"
        "FOREACH (_x_ IN CASE WHEN should_apply_order_by THEN [1] ELSE [] END | SET r += props)\n"
        "RETURN count(*) AS merged_rows, "
        "sum(CASE WHEN should_apply_order_by THEN 0 ELSE 1 END) AS skipped_as_not_newer_rows\n"
    )
    return (
        "UNWIND range(0, size($rows) - 1) AS row_index\n"
        "WITH row_index, $rows[row_index] AS row\n"
        f"MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
        f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
        "WITH count(DISTINCT row_index) AS checked_rows, count(*) AS matched_pairs, "
        "collect({s:s,t:t,props:row.properties}) AS rels, collect(s)+collect(t) AS all_nodes\n"
        "WITH checked_rows, rels, all_nodes, "
        "(size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows\n"
        "CALL (rels, all_nodes, invalid_rows) {\n"
        "WITH rels, all_nodes WHERE invalid_rows = 0\n"
        + (
            "CALL (all_nodes) { UNWIND all_nodes AS n WITH DISTINCT n AS dist_n "
            "ORDER BY elementId(dist_n) RETURN collect(dist_n) AS sorted_nodes }\n"
            "CALL apoc.lock.nodes(sorted_nodes)\n"
            "UNWIND rels AS rel WITH rel.s AS s, rel.t AS t, rel.props AS props "
            if explicit_locks else
            # Neo4j's own MERGE locking; ordered rows reduce deadlocks, which retry.
            "UNWIND rels AS rel WITH rel.s AS s, rel.t AS t, rel.props AS props "
            + ("ORDER BY elementId(s), elementId(t) " if sort_rows else "")
        )
        + merge_and_set
        + "}\n"
        + (
            "RETURN checked_rows, invalid_rows, merged_rows"
            if order_by is None else
            "RETURN checked_rows, invalid_rows, merged_rows, skipped_as_not_newer_rows"
        )
    )


def _preflight_query(edge_config: EdgeConfig) -> str:
    """Return one aggregate endpoint-validity row for an entire transaction."""
    edge_config.single_pair  # v7-S7: views only
    return (
        "UNWIND range(0, size($rows) - 1) AS row_index\n"
        "WITH row_index, $rows[row_index] AS row\n"
        f"OPTIONAL MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
        "WITH row_index, row, count(s) AS source_matches\n"
        f"OPTIONAL MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
        "WITH row_index, source_matches, count(t) AS target_matches\n"
        "RETURN count(*) AS checked_rows, "
        "sum(CASE WHEN source_matches = 1 AND target_matches = 1 THEN 0 ELSE 1 END) AS invalid_rows"
    )


def _endpoint_classification_query(edge_config: EdgeConfig) -> str:
    """Return one row per input row, with its own source/target match counts.

    Read-only (no updating clause anywhere in this statement) -- unlike
    :func:`build_apoc_locked_upsert_query`'s updating statement, an
    ``OPTIONAL MATCH`` here is never promoted to a locking unique seek by
    Neo4j 5's planner. Modeled on :func:`_preflight_query` (already proven
    non-locking in production by ``NativeDisjointEdgeWriter``), but
    per-row instead of aggregated, so P5-S3's caller can distinguish which
    rows failed and why (missing vs. ambiguous, source vs. target).
    """
    edge_config.single_pair  # v7-S7: views only
    return (
        "UNWIND range(0, size($rows) - 1) AS row_index\n"
        "WITH row_index, $rows[row_index] AS row\n"
        f"OPTIONAL MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
        "WITH row_index, row, count(s) AS source_matches\n"
        f"OPTIONAL MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
        "RETURN row_index, source_matches, count(t) AS target_matches"
    )


class MissingEndpointSink(Protocol):
    """Where a row-level-DLQ-enabled writer routes one unresolved relationship row.

    A local ``Protocol`` (not an import of ``src.loader.dead_letter``) --
    preserves the existing one-directional ``dead_letter.py -> schema.py``
    layering; ``edge_loader.py`` already imports both and satisfies this
    duck-typed protocol with a real ``DeadLetterRecordSink``.
    """

    def route_unresolved_endpoint(
        self, *, topic: str, partition: int, offset: int, record: "EdgeRecord",
        edge_type: str, source_key_property: str, target_key_property: str,
        failure_class: str, reason_code: str, error_text: str,
        retry_after_seconds: float, endpoint_labels: "tuple[str, str] | None" = None,
    ) -> None: ...


class ParkedPairs(Protocol):
    """Per-flush, cross-transaction memory of already-parked relationship pairs.

    P5-S3 Story 4 review finding: a lease-flush's cell can be drained FIFO
    across *several sequential transactions* (one per ``batch_size``/merge-
    budget chunk), each a separate :meth:`ApocLockedEdgeWriter.
    write_provenanced_batch` call. Without this, an earlier transaction
    parking row R1 of pair ``(source_key, target_key)`` and a *later*
    transaction (same flush, same cell -- a pair always routes to the same
    cell) committing a resolvable row R2 of the *same* pair would let a
    future retry of R1 overwrite R2's newer properties -- exactly the
    violation Decisions point 4 says a single flush must never introduce.
    Implemented by :class:`_LeaseFlush`, guarded by its own lock (a pair's
    rows only ever come from one cell, and a cell has exactly one holder at
    a time, but two *different* pairs/cells can be processed concurrently
    by different worker threads, so this must still be thread-safe).
    """

    def is_parked(self, pair: tuple) -> bool: ...
    def mark_parked(self, pairs: Iterable[tuple]) -> None: ...


class DurableParkedPairIndex(Protocol):
    """Cross-flush, cross-process record of outstanding DLQ'd rows (P5-S4
    Story 3's ``ParkedPairIndex``), referenced here only through this
    narrow duck-typed protocol -- not an import of
    ``src.loader.parked_pair_index`` -- for the same one-directional
    layering reason :class:`MissingEndpointSink` already documents:
    ``edge_loader.py`` (P5-S4 Story 6) is what imports and constructs the
    real class and wires it in here.

    P5-S4 Story 3's second half (this integration): reuses the *existing*
    :class:`ParkedPairs` protocol/call sites for the live-writer's blocking
    check -- see :class:`_LeaseFlush`'s ``durable_index`` fallback below --
    rather than adding a second, parallel blocking mechanism. Only
    :meth:`is_blocked` is consulted there (the live path never excludes a
    row's own presence, matching :class:`ParkedPairIndex`'s own
    ``is_blocked`` semantics exactly). :meth:`add` is called directly by
    :class:`IndexedMissingEndpointSink` below, once a row is confirmed
    unresolved and immediately before its DLQ publish -- ``mark_parked``'s
    pair-only signature cannot carry the per-row ``(original_topic,
    original_partition, original_offset)`` identity the durable index
    needs to record and later remove the *same* row the retry consumer
    will eventually see, so recording happens here, not through
    ``mark_parked``.
    """

    def is_blocked(self, source_key: object, target_key: object) -> bool: ...

    def add(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int, first_failure_at_ms: int,
    ) -> None: ...


class IndexedMissingEndpointSink:
    """Wrap a :class:`MissingEndpointSink` so every unresolved row is
    durably recorded in a :class:`DurableParkedPairIndex` *before* the
    wrapped sink's own DLQ publish (P5-S4 Story 3's live-writer hook,
    review carry-over item M2(a): "the live writer's index add() must be
    synchronous and acked before the DLQ publish and before the source
    offset commits").

    ``add()`` raising (an unconfirmed publish -- ``ParkedPairIndex``'s own
    fail-closed contract) propagates uncaught here, exactly like a DLQ
    publish failure already does: ``write_provenanced_batch``'s caller
    resolves no offsets, and the whole batch safely replays (idempotent
    ``MERGE``, and ``add()`` itself is idempotent for the same record
    identity -- see ``ParkedPairIndex.add``'s own docstring).
    """

    def __init__(
        self, sink: MissingEndpointSink, index: DurableParkedPairIndex,
        *, clock: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        self._sink = sink
        self._index = index
        self._clock = clock

    def route_unresolved_endpoint(
        self, *, topic: str, partition: int, offset: int, record: "EdgeRecord",
        edge_type: str, source_key_property: str, target_key_property: str,
        failure_class: str, reason_code: str, error_text: str,
        retry_after_seconds: float,
    ) -> None:
        self._index.add(
            record.source_key, record.target_key,
            original_topic=topic, original_partition=partition, original_offset=offset,
            first_failure_at_ms=self._clock(),
        )
        self._sink.route_unresolved_endpoint(
            topic=topic, partition=partition, offset=offset, record=record, edge_type=edge_type,
            source_key_property=source_key_property, target_key_property=target_key_property,
            failure_class=failure_class, reason_code=reason_code, error_text=error_text,
            retry_after_seconds=retry_after_seconds,
        )


class LabelledEndpointSink:
    """Add one label pair's ``endpoint_labels`` to every unresolved row it routes (v7-S7).

    Wraps the type's one DLQ sink for one label pair of a multi-pair type, so the DLQ key is
    ``[S, T, sk, tk]``: Person1->Post1 and Person1->Comment1 are different pairs to every DLQ
    consumer (retry grouping, partitioning, replay). Single-pair types are never wrapped."""

    def __init__(self, sink: MissingEndpointSink, endpoint_labels: "tuple[str, str]") -> None:
        self._sink = sink
        self.endpoint_labels = tuple(endpoint_labels)

    def route_unresolved_endpoint(self, **kwargs) -> None:
        kwargs["endpoint_labels"] = self.endpoint_labels
        self._sink.route_unresolved_endpoint(**kwargs)


class ApocLockedEdgeWriter:
    """Atomically validate, globally lock, and merge one batch of edges.

    One statement per transaction resolves every endpoint once, and only when
    all rows resolve exactly one source and target does it lock and merge.
    Any other outcome raises the missing-endpoint error before commit, so the
    transaction rolls back with nothing written.

    A driver-retryable ``TransientError`` (e.g. DeadlockDetected) rolls the
    whole transaction back; the identical batch is replayed with bounded
    exponential backoff from ``edge_config.retry``.  Endpoint and all other
    failures propagate on the first attempt.

    P5-S1 Story 4 (the P5-Q1 fencing amendment): ``tx_timeout_seconds``, when
    set, is passed to Neo4j as the transaction's own server-enforced timeout
    -- Neo4j itself, not this process, terminates the transaction if it runs
    longer.  This bounds how long a transaction can stay open, but it is
    only *half* of the restart-fencing guarantee by itself: it does not
    stop the lease server from re-granting the same nodes to a second
    writer while the first transaction is still within that bound (review
    finding B4-1).  The other half -- the lease server holding a
    disconnected owner's leases until the moment of the disconnect plus
    ``fence_seconds`` has passed (``BucketLeaseServer``'s
    ``disconnect_hold_seconds``/``quiet_period_seconds``, landed together
    with this story) -- is what actually closes that gap; the two must be
    read together, not this timeout in isolation.  Only the lease/planned
    construction sites pass ``tx_timeout_seconds``; the classic (real Neo4j
    locking, no bucket lease) path leaves it ``None`` (unbounded/server
    default, unchanged), since that path has no lease-server-restart
    exposure to fence against.  ``deadline``, passed per call from a lease
    coordinator, is an absolute ``time.monotonic()`` value (the lease grant
    time plus ``T``) that every retry attempt shares -- a retried write does
    *not* get a fresh ``T``, so the cumulative time a lease may be held
    across ``edge_config.retry``'s backoff-and-retry loop is still bounded
    by the original grant's ``T`` (review finding D1: a naive "T per
    attempt" would let a flaky batch hold a lease for
    ``max_attempts * T + backoff`` -- long past the post-restart quiet
    period ``Q`` a new generation waits out).
    """

    # The floor below which no leased transaction is begun lives in
    # src/loader/fenced_tx.py (MIN_TX_TIMEOUT_SECONDS), shared with node writes.

    def __init__(
        self, driver: Driver, edge_config: EdgeConfig, *,
        missing_endpoint_error_factory: Callable[[str], Exception],
        sleep: Callable[[float], None] = time.sleep,
        explicit_locks: bool | None = None,
        sort_rows: bool = True,
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
        kind: str = "edge",
        tx_timeout_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        missing_endpoint_sink: "MissingEndpointSink | None" = None,
    ) -> None:
        self._driver = driver
        self._error_factory = missing_endpoint_error_factory
        self._edge_type = edge_config.type
        if explicit_locks is None:
            explicit_locks = os.environ.get("GRAPH_LOADER_EDGE_EXPLICIT_LOCKS", "1") != "0"
        self._explicit_locks = explicit_locks
        self._query = build_apoc_locked_upsert_query(
            edge_config, explicit_locks=explicit_locks, sort_rows=sort_rows,
        )
        retry = edge_config.retry
        self._max_attempts = retry.max_attempts
        self._base_delay, self._max_delay = retry.base_delay_seconds, retry.max_delay_seconds
        self._sleep = sleep
        self._clock = clock
        self._tx_timeout_seconds = tx_timeout_seconds
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        # "edge" for the classic/lease loaders (edge_loader.py); Story 6
        # constructs this same class with kind="planned-writer" for the
        # planned writer, which reuses this writer verbatim - the planned
        # run's deadlocks_recovered_total stays at 0 by construction (P6-S2
        # Story 5's own health signal), not because this code path differs.
        self._kind = kind
        # P5-S3 Story 3: row-level missing-endpoint DLQ, opt-in per writer
        # instance (never re-derived from environment/mode inside this
        # class -- the caller, build_edge_execution, decides).
        self._missing_endpoint_sink = missing_endpoint_sink
        # Always built (BUG-2): retry_provenanced_batch classifies endpoints
        # regardless of whether a row-level sink is configured. Whether
        # row-level classification is *active* for write_provenanced_batch
        # is still gated by ``missing_endpoint_sink`` (row_level_dlq_enabled).
        self._classification_query: str = _endpoint_classification_query(edge_config)
        if not self._classification_query:
            raise ValueError(f"edge {edge_config.type}: empty endpoint classification query")
        self._retry_after_seconds = retry.base_delay_seconds
        self._source_key_property = edge_config.source_key_property
        self._target_key_property = edge_config.target_key_property
        # P5-S4 Story 4: an alternative to pair-parking, not an addition to
        # it (Decisions §10) -- self._query already bakes the conditional
        # SET in (see build_apoc_locked_upsert_query); everything else
        # this flag gates lives in write_provenanced_batch/_classify_rows
        # below.
        self._order_by = edge_config.order_by
        # P5-S4 Story 6: retry_provenanced_batch (below) needs the full,
        # graph-validated EdgeConfig to decode a retried DLQ envelope's
        # value_bytes back into an EdgeRecord via normalize_edge_record.
        self._edge_config = edge_config

    def write(self, record: "EdgeRecord", *, deadline: float | None = None,
              lease_liveness_check: Callable[[], bool] | None = None) -> None:
        """Write one record through the same atomic batch path."""
        self.write_batch([record], deadline=deadline, lease_liveness_check=lease_liveness_check)

    def write_batch(self, records: Sequence["EdgeRecord"], *, deadline: float | None = None,
                     lease_liveness_check: Callable[[], bool] | None = None) -> None:
        """Atomically write one batch, replaying it only on transient errors."""
        if not records:
            return
        rows = [{"source_key": r.source_key, "target_key": r.target_key, "properties": r.properties} for r in records]
        self.write_rows(rows, deadline=deadline, lease_liveness_check=lease_liveness_check)

    def write_rows(
        self, rows: list[dict], *, metadata: dict | None = None,
        deadline: float | None = None, lease_liveness_check: Callable[[], bool] | None = None,
    ) -> int:
        """Atomically write prepared ``source_key/target_key/properties`` rows.

        Returns the number of attempts used (1 without transient retries).
        ``deadline`` (a ``time.monotonic()`` value), when given, is shared
        by every retry attempt -- see the class docstring's D1 note.
        """
        if not rows:
            return 0
        retried = False
        for attempt in range(1, self._max_attempts + 1):
            timeout_seconds = self._timeout_for_deadline(deadline, attempt)
            try:
                self._write_rows(
                    rows, metadata, timeout_seconds=timeout_seconds, deadline=deadline,
                    lease_liveness_check=lease_liveness_check,
                )
                if retried:
                    # Count a *recovered* deadlock once, only when at least
                    # one retryable TransientError was caught and this
                    # attempt then succeeded - never once per attempt, and
                    # never on the path that ultimately raises (P6-S2
                    # Story 5). Routed through safe_emit: this sits inside
                    # a `try:` that only catches `TransientError` below, so
                    # an unguarded metrics exception here would otherwise
                    # propagate straight out of a batch that already
                    # durably committed (caught by this story's own test).
                    metrics_module.safe_emit(lambda: self._metrics.deadlocks_recovered.labels(
                        kind=self._kind, name=self._edge_type
                    ).inc())
                return attempt
            except TransientError as exc:
                if not exc.is_retryable() or attempt >= self._max_attempts:
                    raise
                retried = True
                delay = min(self._base_delay * 2 ** (attempt - 1), self._max_delay)
                logger.warning(
                    "stage=edge-write-retry edge=%s records=%s attempt=%s/%s delay_s=%.3f code=%s",
                    self._edge_type, len(rows), attempt, self._max_attempts, delay, getattr(exc, "code", None),
                )
                self._sleep(delay)
        raise AssertionError("unreachable")

    def _timeout_for_deadline(self, deadline: float | None, attempt: int) -> float | None:
        """Return this attempt's Neo4j transaction timeout, or raise if the
        shared lease ``deadline`` (P5-Q1 fencing) has effectively expired.

        Shared by :meth:`write_rows` and :meth:`write_provenanced_batch` (P5-S3
        Story 3) -- both retry loops share one deadline across every attempt
        (the class docstring's D1 note), so both must compute it identically.
        """
        timeout_seconds = self._tx_timeout_seconds
        if deadline is None:
            return timeout_seconds
        # The fenced budget (src/loader/fenced_tx.py): the deadline minus the
        # transaction monitor's slack and the BEGIN allowance. The BEGIN
        # itself recomputes it and verifies when the server began.
        budget = check_deadline(deadline, self._clock(), f"edge={self._edge_type} attempt {attempt}/{self._max_attempts}")
        return budget if timeout_seconds is None else min(budget, timeout_seconds)

    def _begin(self, session, *, timeout_seconds: float | None, deadline: float | None, metadata: dict | None = None):
        """Begin this write's transaction; with a lease deadline, fenced so it ends by the deadline."""
        return begin_fenced_transaction(
            session, deadline=deadline,
            tx_timeout_seconds=timeout_seconds if deadline is None else self._tx_timeout_seconds,
            clock=self._clock, what=f"edge={self._edge_type}", metadata=metadata,
        )

    def _write_rows(
        self, rows: list[dict], metadata: dict | None = None, *,
        timeout_seconds: float | None = None, lease_liveness_check: Callable[[], bool] | None = None,
        strict: bool = True, deadline: float | None = None,
    ) -> bool:
        """Validate endpoints, acquire sorted locks, and merge in one statement.

        Returns ``True`` iff the aggregate check passed and the transaction
        committed. ``strict=True`` (the default, and every call site before
        P5-S3) raises via ``self._error_factory`` on a failed aggregate
        check, exactly as before this story. ``strict=False`` returns
        ``False`` instead of raising, letting the caller
        (:meth:`write_provenanced_batch`) fall through to row-level
        classification -- used only by that method, never by
        :meth:`write_rows`.
        """
        # The P5-Q1 fencing amendment's actual guarantee: Neo4j itself
        # terminates this transaction once its timeout passes, regardless of
        # whether anything on this process ever notices a broken lease
        # connection. With a lease deadline the BEGIN is fenced
        # (src/loader/fenced_tx.py), so the transaction ends by the deadline.
        self._warn_missing_order_by(rows)
        write_rows = self._collapse_same_pair_rows_for_order_by(rows)
        with open_session(self._driver) as session:
            with self._begin(session, timeout_seconds=timeout_seconds, deadline=deadline,
                             metadata=metadata) as transaction:
                result = self._run_write_query(transaction, write_rows)
                counts = list(result)
                result.consume()
                ok = (
                    len(counts) == 1
                    and counts[0]["checked_rows"] == len(rows)
                    and counts[0]["invalid_rows"] == 0
                    and counts[0]["merged_rows"] == len(rows)
                )
                if ok:
                    self._log_skipped_as_not_newer(counts[0])
                if not ok:
                    # Leaving the block without commit rolls the transaction back.
                    if strict:
                        raise self._error_factory(f"Edge '{self._edge_type}' did not resolve exactly one source and target endpoint")
                    return False
                if lease_liveness_check is not None and not lease_liveness_check():
                    # An optimisation, not the guarantee: the fenced BEGIN
                    # (src/loader/fenced_tx.py) makes Neo4j end THIS
                    # transaction by the lease deadline; combined with the lease server holding a
                    # disconnected owner's leases until disconnect_time + T +
                    # margin (BucketLeaseServer's disconnect_hold_seconds,
                    # review finding B4-1), no second writer can be
                    # granted these same nodes while this transaction is
                    # still within that bound. Leaving the block without
                    # commit rolls the transaction back.
                    raise LeaseCoordinatorError(
                        f"edge={self._edge_type} lease connection broke before commit; "
                        "aborting instead of committing"
                    )
                transaction.commit()
                return True

    # ── P5-S4 Story 4: order_by conditional SET ──────────────────────────────

    def _warn_missing_order_by(self, rows: list[dict]) -> None:
        """Log once **per batch** (review finding N20 -- never once per
        row, which would flood the log for a producer that never sends
        this field at all) when any row is missing its ``order_by``
        property entirely.

        A missing incoming value applies unconditionally (see
        :func:`build_apoc_locked_upsert_query`'s docstring) rather than
        being rejected -- but silently applying it forever, with no
        signal, would hide a producer bug (a record that should always
        carry this property but doesn't). Checked in Python, not Cypher:
        this needs no per-row reporting from the query itself, only the
        input this method already has.
        """
        if not self._order_by:
            return
        missing = [row for row in rows if row["properties"].get(self._order_by) is None]
        if not missing:
            return
        sample = ", ".join(f"({row['source_key']},{row['target_key']})" for row in missing[:5])
        logger.warning(
            "stage=edge-order-by-missing edge=%s order_by=%s missing_rows=%d sample_pairs=%s -- "
            "applying unconditionally (no value to compare)",
            self._edge_type, self._order_by, len(missing), sample,
        )

    def _run_write_query(self, transaction, rows: list[dict]):
        """Run ``self._query`` with ``$order_by`` bound whenever this
        writer's edge type declares one (review finding M13): both query
        shapes reference ``$order_by`` in their conditional ``SET``
        whenever ``order_by`` is configured, and Neo4j raises a
        non-transient ``ParameterMissing`` error on every write if it is
        never bound -- stalling that edge type's stream forever, since
        the batch would replay unboundedly on an error that never
        clears. The non-``order_by`` call stays byte-for-byte the same
        (no ``order_by`` kwarg at all) when this writer's edge type has
        none configured.
        """
        if self._order_by is not None:
            return transaction.run(self._query, rows=rows, order_by=self._order_by)
        return transaction.run(self._query, rows=rows)

    def _collapse_same_pair_rows_for_order_by(self, rows: list[dict]) -> list[dict]:
        """Return a copy of ``rows`` where, for each ``(source_key,
        target_key)`` pair with more than one *value-carrying* row in
        this batch, every such row except the one with the greatest
        ``order_by`` value has its ``properties`` replaced with ``{}`` --
        a harmless no-op merge (review finding M14, corrected after a
        second review round -- see below).

        Neo4j's own per-row conditional ``SET`` (see
        :func:`build_apoc_locked_upsert_query`) is only correct for the
        *first* row of a pair Neo4j has ever seen: Cypher's planner
        treats the ``WHERE``-read of ``r[$order_by]`` and the
        ``SET``-write to the same property as a conflict across
        ``UNWIND`` iterations and inserts an eager barrier, so *every*
        row in the flat lease/streaming query shape reads the identical
        pre-batch value rather than a same-batch predecessor's own
        just-applied write. Two same-pair rows in one streaming batch is
        the ordinary case for a hub node under real traffic, not a rare
        edge case -- without this, the later-in-the-list row silently
        wins regardless of which one's ``order_by`` value is actually
        greater.

        Rows missing the ``order_by`` value entirely (``None``) are
        **never emptied and never compared** -- only rows carrying a
        value compete for the single "applies its value" slot per pair:

        - among a pair's value-carrying rows, the one with the greatest
          value keeps its properties; every other value-carrying row of
          that pair is emptied; a tie keeps the earlier occurrence;
        - a row with no value at all is left completely untouched,
          exactly as if it were the only row of its pair.

        An earlier version of this method let a missing-value row "win"
        and empty a value-carrying row that came before it -- e.g. row 0
        ``{order_by: 10}`` then row 1 ``{}`` (no value) would empty row 0,
        so Neo4j would end up applying only row 1's unconditional write
        and never see ``order_by: 10`` at all, silently regressing the
        stored value to whatever it was before this batch. That is
        exactly the overwrite this mechanism exists to prevent. The fix:
        never let a missing-value row participate in, or be affected by,
        the value-carrying competition at all -- it always applies
        (matching sequential per-row semantics, where a missing-value row
        applies unconditionally regardless of order) and it never
        displaces the batch's actual maximum, since Neo4j's own query
        still compares that maximum against the real stored value.

        Every non-winning (emptied) row still resolves its own endpoints
        normally and still counts toward ``merged_rows`` (an
        empty-properties merge is still a merge) -- this slice's "every
        item is durably accounted for" ledger contract never has to
        change; the row is never dropped from the batch, only its
        property update is suppressed. Documented in ``docs/USAGE.md``
        (Story 8): with ``SET +=``, collapsing means a property key that
        exists only on an emptied row is never applied -- consistent with
        "a newer version supersedes," the whole point of this mechanism.
        """
        if not self._order_by or len(rows) < 2:
            return rows
        order_by = self._order_by
        max_index_by_pair: dict[tuple, int] = {}
        max_value_by_pair: dict[tuple, object] = {}
        value_carrying_indices_by_pair: dict[tuple, list[int]] = {}
        for i, row in enumerate(rows):
            value = row["properties"].get(order_by)
            if value is None:
                continue  # never touched, never compared, never displaces anything
            pair = (row["source_key"], row["target_key"])
            value_carrying_indices_by_pair.setdefault(pair, []).append(i)
            if pair not in max_index_by_pair or value > max_value_by_pair[pair]:
                max_index_by_pair[pair] = i
                max_value_by_pair[pair] = value
        loser_indices = {
            i
            for pair, indices in value_carrying_indices_by_pair.items()
            for i in indices
            if i != max_index_by_pair[pair]
        }
        if not loser_indices:
            return rows
        collapsed = list(rows)
        for i in loser_indices:
            collapsed[i] = {**rows[i], "properties": {}}
        return collapsed

    def _log_skipped_as_not_newer(self, counts: dict) -> None:
        """Log (never DLQ, never retry) when an order_by write was silently
        superseded -- a tie or a strictly-older incoming value, per
        :func:`build_apoc_locked_upsert_query`'s ``skipped_as_not_newer_rows``."""
        if not self._order_by:
            return
        skipped = counts.get("skipped_as_not_newer_rows")
        if skipped:
            logger.info(
                "stage=edge-order-by-superseded edge=%s order_by=%s skipped_rows=%s -- "
                "incoming value not newer than what is already stored",
                self._edge_type, self._order_by, skipped,
            )

    # ── P5-S3 Story 3: row-level missing-endpoint DLQ ───────────────────────

    @property
    def row_level_dlq_enabled(self) -> bool:
        """Whether this writer was constructed with a missing-endpoint sink."""
        return self._missing_endpoint_sink is not None

    @staticmethod
    def _as_row(record: "EdgeRecord") -> dict:
        return {"source_key": record.source_key, "target_key": record.target_key, "properties": record.properties}

    def write_provenanced_batch(
        self, items: Sequence, *, deadline: float | None = None,
        lease_liveness_check: Callable[[], bool] | None = None,
        parked_pairs: "ParkedPairs | None" = None,
    ) -> None:
        """Durably write every item to Neo4j, or durably to the DLQ, or raise.

        ``items``: anything exposing ``.record``/``.topic``/``.partition``/
        ``.offset`` (``PendingEdgeRecord`` or ``RoutedEdgeRecord``). On
        return, every item is accounted for -- some via the committed
        Neo4j write, some via an acknowledged DLQ publish. Raising means
        *nothing* in this call is durable (a Neo4j write failure, a DLQ
        publish failure for even one row, or the rare TOCTOU race in
        :meth:`_write_with_row_level_dlq`) -- the caller resolves no
        offsets, and the whole original batch, including any row that
        already durably committed to Neo4j earlier in this same call,
        safely replays on the next attempt because every write here is
        idempotent ``MERGE``.

        ``parked_pairs`` (P5-S3 Story 4 review finding) carries the
        same-pair ordering guard *across* multiple calls to this method
        within one lease flush -- see :class:`ParkedPairs`. When given, any
        row whose ``(source_key, target_key)`` pair was already parked by
        an *earlier* call (in this same flush) is pre-blocked here: Attempt
        1's fast path is skipped entirely for this call (a pre-blocked row
        must never take the all-rows-resolved path, even if every row here
        would otherwise individually resolve), and it goes straight to
        :meth:`_write_with_row_level_dlq`, which also records every newly
        parked/blocked pair back into ``parked_pairs`` after its commit.
        ``None`` (the default) preserves this method's Story-3 behavior
        exactly. Classic (non-lease) bulk (Story 5) *does* also need
        cross-transaction splitting guarded -- ``LaneBatcher.drain_all()``
        can split one lane's queue into several sequential batches when it
        holds more rows than ``batch_size``, and a pair always routes to
        the same lane -- so ``LaneExecutionCoordinator.execute`` passes its
        own per-``execute()``-call ``_ParkedPairSet`` here too, not
        ``None``.

        The ``TransientError`` retry loop below wraps *only* Attempt 1
        (today's all-rows-or-nothing fast path, unmodified query/cost) --
        retrying that identical statement cannot resolve a genuinely
        missing endpoint any faster than falling through to row-level
        classification already will, and a deadlock/transient error is
        vanishingly rare on the read-only classification query or the
        already-narrowed matched-subset write of Attempt 2, so it is not
        separately wrapped; if one occurs there it propagates uncaught,
        exactly like decisions point 5/6 already requires for any other
        Attempt-2 failure (nothing durable resolves; the whole batch
        replays from Kafka).
        """
        if not items:
            return
        assert self._missing_endpoint_sink is not None
        if self._order_by is not None:
            # P5-S4 Decisions §10: order_by is an alternative to
            # pair-parking, not an addition to it -- enforced here rather
            # than left to the caller, so an order_by-configured type
            # never pays this mechanism's bookkeeping cost regardless of
            # what the coordinator passed in.
            parked_pairs = None
        rows = [self._as_row(item.record) for item in items]
        pre_blocked_pairs = self._pre_blocked_pairs(rows, parked_pairs)
        timeout_seconds = self._tx_timeout_seconds
        if not pre_blocked_pairs:
            retried = False
            for attempt in range(1, self._max_attempts + 1):
                timeout_seconds = self._timeout_for_deadline(deadline, attempt)
                try:
                    if self._write_rows(
                        rows, timeout_seconds=timeout_seconds, lease_liveness_check=lease_liveness_check, strict=False,
                        deadline=deadline,
                    ):
                        if retried:
                            metrics_module.safe_emit(lambda: self._metrics.deadlocks_recovered.labels(
                                kind=self._kind, name=self._edge_type
                            ).inc())
                        return  # fast path: every row resolved, identical cost/query to today
                    break  # aggregate check failed (no exception) -- fall through to classification
                except TransientError as exc:
                    if not exc.is_retryable() or attempt >= self._max_attempts:
                        raise
                    retried = True
                    delay = min(self._base_delay * 2 ** (attempt - 1), self._max_delay)
                    logger.warning(
                        "stage=edge-write-retry edge=%s records=%s attempt=%s/%s delay_s=%.3f code=%s",
                        self._edge_type, len(rows), attempt, self._max_attempts, delay, getattr(exc, "code", None),
                    )
                    self._sleep(delay)
        # timeout_seconds/deadline may have shrunk while sleeping between
        # Attempt-1 retries above (or was never computed at all, when
        # pre_blocked_pairs skipped Attempt 1 entirely) -- (re)compute once
        # more for the fallback's own (fresh) transaction.
        timeout_seconds = self._timeout_for_deadline(deadline, self._max_attempts)
        self._write_with_row_level_dlq(
            items, rows, timeout_seconds=timeout_seconds, lease_liveness_check=lease_liveness_check,
            pre_blocked_pairs=pre_blocked_pairs, parked_pairs=parked_pairs, deadline=deadline,
        )

    @staticmethod
    def _pre_blocked_pairs(rows: list[dict], parked_pairs: "ParkedPairs | None") -> set:
        """Which of this batch's pairs were already parked by an earlier
        transaction in the same flush (see ``parked_pairs`` above)."""
        if parked_pairs is None:
            return set()
        pre_blocked: set = set()
        for row in rows:
            pair = (row["source_key"], row["target_key"])
            if pair not in pre_blocked and parked_pairs.is_parked(pair):
                pre_blocked.add(pair)
        return pre_blocked

    def _write_with_row_level_dlq(
        self, items: Sequence, rows: list[dict], *,
        timeout_seconds: float | None = None, lease_liveness_check: Callable[[], bool] | None = None,
        pre_blocked_pairs: set | None = None, parked_pairs: "ParkedPairs | None" = None,
        deadline: float | None = None,
    ) -> None:
        """Classify every row's endpoints, write the matched subset, then DLQ the rest.

        One fresh transaction: a read-only classification query (no
        updating clause, so its ``OPTIONAL MATCH`` stays a non-locking
        index seek) followed by today's exact write query again,
        parameterized with only the matched-and-not-blocked subset, in the
        *same* transaction. DLQ envelopes for unresolved/blocked rows
        publish only after that transaction has committed. ``pre_blocked_pairs``
        (already parked by an earlier transaction in this flush) forces
        those rows to ``"blocked"`` regardless of their own classification;
        every row that ends up not-matched here is recorded into
        ``parked_pairs`` (if given) right after commit, so a *later*
        transaction in the same flush sees it too.
        """
        pre_blocked_pairs = pre_blocked_pairs or set()
        with open_session(self._driver) as session:
            with self._begin(session, timeout_seconds=timeout_seconds, deadline=deadline) as transaction:
                result = transaction.run(self._classification_query, rows=rows)
                by_index = {r["row_index"]: (r["source_matches"], r["target_matches"]) for r in result}
                result.consume()
                statuses = self._classify_rows(
                    rows, by_index, pre_blocked_pairs=pre_blocked_pairs,
                    order_by_configured=self._order_by is not None,
                )
                matched_indices = sorted(i for i, (status, _) in statuses.items() if status == "matched")
                matched_rows = [rows[i] for i in matched_indices]
                if matched_rows:
                    self._warn_missing_order_by(matched_rows)
                    write_rows = self._collapse_same_pair_rows_for_order_by(matched_rows)
                    write_result = self._run_write_query(transaction, write_rows)
                    counts = list(write_result)
                    write_result.consume()
                    ok = (
                        len(counts) == 1
                        and counts[0]["checked_rows"] == len(matched_rows)
                        and counts[0]["invalid_rows"] == 0
                        and counts[0]["merged_rows"] == len(matched_rows)
                    )
                    if ok:
                        self._log_skipped_as_not_newer(counts[0])
                    if not ok:
                        # Rare TOCTOU: an endpoint classified as matched
                        # moments ago no longer resolves. Fail the WHOLE
                        # original batch closed -- no partial commit, no DLQ
                        # routing for anyone, safe retry of everything on
                        # the next attempt (decisions point 5/7).
                        raise self._error_factory(
                            f"Edge '{self._edge_type}' matched-subset write mismatch "
                            "(endpoint changed between classification and write)"
                        )
                if lease_liveness_check is not None and not lease_liveness_check():
                    # Same pre-commit optimisation as _write_rows -- see that
                    # method's docstring; a lease-loss abort here must stay
                    # LeaseCoordinatorError, never a DLQ condition (decisions
                    # point 7). Leaving the block without commit rolls back.
                    raise LeaseCoordinatorError(
                        f"edge={self._edge_type} lease connection broke before commit; "
                        "aborting instead of committing"
                    )
                transaction.commit()
        # Only after the commit above succeeds -- record newly-parked pairs
        # (so a later transaction in this flush sees them) and publish DLQ
        # envelopes for every unresolved/blocked row. A publish failure
        # re-raises uncaught (decisions point 6): the caller resolves NO
        # offsets for this batch, and Neo4j's already-committed matched
        # rows are safely re-written (idempotent MERGE) on the next attempt.
        newly_parked = {
            (rows[index]["source_key"], rows[index]["target_key"])
            for index, (status, _) in statuses.items() if status != "matched"
        }
        if parked_pairs is not None and newly_parked:
            parked_pairs.mark_parked(newly_parked)
        for index, (status, reason_code) in sorted(statuses.items()):
            if status == "matched":
                continue
            item = items[index]
            self._missing_endpoint_sink.route_unresolved_endpoint(
                topic=item.topic, partition=item.partition, offset=item.offset,
                record=item.record, edge_type=self._edge_type,
                source_key_property=self._source_key_property, target_key_property=self._target_key_property,
                failure_class="ambiguous_endpoint" if status == "ambiguous" else "missing_endpoint",
                reason_code=reason_code,
                error_text=f"Edge '{self._edge_type}' row at offset {item.offset}: {reason_code}",
                retry_after_seconds=self._retry_after_seconds,
            )

    @staticmethod
    def _classify_rows(
        rows: list[dict], by_index: dict[int, tuple[int, int]], *, pre_blocked_pairs: set | None = None,
        order_by_configured: bool = False,
    ) -> dict[int, tuple[str, str]]:
        """Classify every row's endpoints, then apply the same-pair ordering guard.

        Returns ``{row_index: (status, reason_code)}`` where ``status`` is
        one of ``"matched"``, ``"missing"``, ``"ambiguous"``, or
        ``"blocked"`` (a resolvable row parked only because an *earlier*
        row of the same ``(source_key, target_key)`` pair -- in this same
        batch, or in an earlier transaction of the same flush via
        ``pre_blocked_pairs`` -- did not resolve -- P5-S3 Decisions point 4:
        writing this row now and later replaying the earlier one would
        overwrite newer properties with older ones).

        ``order_by_configured=True`` (P5-S4 Story 4, Decisions §10) skips
        the same-pair blocking pass entirely -- neither this call's own
        same-batch rows nor ``pre_blocked_pairs`` can ever force a
        ``"matched"`` row to ``"blocked"`` for this edge type: the
        conditional ``SET`` this type's own query already applies (see
        :func:`build_apoc_locked_upsert_query`) makes a stale replay a
        safe no-op by construction, so pair-parking's own bookkeeping
        would be redundant, not additively safer, for a type that has
        already removed the risk it exists to guard against.
        """
        pre_blocked_pairs = pre_blocked_pairs or set()
        statuses: dict[int, tuple[str, str]] = {}
        for i, row in enumerate(rows):
            source_matches, target_matches = by_index[i]
            if source_matches == 1 and target_matches == 1:
                statuses[i] = ("matched", "")
            elif source_matches > 1 or target_matches > 1:
                if source_matches > 1 and target_matches > 1:
                    reason = "ambiguous_both"
                elif source_matches > 1:
                    reason = "ambiguous_source"
                else:
                    reason = "ambiguous_target"
                statuses[i] = ("ambiguous", reason)
            else:
                if source_matches == 0 and target_matches == 0:
                    reason = "missing_both"
                elif source_matches == 0:
                    reason = "missing_source"
                else:
                    reason = "missing_target"
                statuses[i] = ("missing", reason)
        if order_by_configured:
            return statuses
        pairs: dict[tuple, list[int]] = {}
        for i, row in enumerate(rows):
            pairs.setdefault((row["source_key"], row["target_key"]), []).append(i)
        for pair, indices in pairs.items():
            blocked = pair in pre_blocked_pairs
            for i in indices:  # rows[] preserves batch/original offset order
                if blocked and statuses[i][0] == "matched":
                    statuses[i] = ("blocked", "blocked_by_earlier_unresolved_row")
                if statuses[i][0] not in ("matched", "blocked"):
                    blocked = True
        return statuses

    # ── P5-S4 Story 6: the RetryWriter adapter the retry consumer uses ──────

    def retry_provenanced_batch(
        self, envelopes: Sequence, *, deadline: float | None = None,
        lease_liveness_check: Callable[[], bool] | None = None,
        parked_pairs: "ParkedPairs | None" = None,
    ) -> dict[int, tuple[str, str]]:
        """Attempt to resolve every DLQ'd envelope's endpoints and write the
        matched subset, in one transaction -- the real
        ``src.loader.retry_consumer.RetryWriter`` implementation.

        ``envelopes``: anything exposing ``.value_bytes``/
        ``.original_topic``/``.original_partition``/``.original_offset``
        -- a real ``dead_letter.DeadLetterEnvelope`` satisfies this by
        construction. This module never imports that class (preserving
        the same one-directional layering :class:`MissingEndpointSink`'s
        own docstring already documents); the retry consumer passes real
        envelopes in without either module needing to import the other's
        concrete types.

        Returns ``{index: (outcome, reason_code)}`` for every envelope --
        ``outcome`` is the string ``"written"`` or ``"unresolved"``
        (matching ``src.loader.retry_consumer.RETRY_OUTCOME_WRITTEN``/
        ``RETRY_OUTCOME_UNRESOLVED`` *by value*, not by import, for the
        same layering reason). ``reason_code`` is only meaningful for
        ``"unresolved"``. This method never itself publishes to a DLQ or
        parking-lot topic -- unlike :meth:`write_provenanced_batch`, this
        writer's own ``missing_endpoint_sink`` is never consulted here.
        That decision (re-DLQ at an incremented ``attempt_count``, or
        park) belongs entirely to the caller (``RetryConsumer``), which
        already knows each envelope's own ``attempt_count``/
        ``max_retry_attempts`` -- this method only classifies and writes.

        Raises, writing nothing durable, on any Neo4j/lease failure or an
        unreadable envelope value -- the whole batch is safe to retry
        from scratch (idempotent ``MERGE``), exactly like
        :meth:`write_provenanced_batch`.

        ``deadline``/``lease_liveness_check``/``parked_pairs`` mirror
        :meth:`write_provenanced_batch` for a lease-coordinated retry flush
        (the coordinator passes them per granted cell): the transaction
        timeout is capped at the lease deadline (P5-Q1 fencing), a broken
        lease connection aborts before commit, and a pair left unresolved by
        an earlier transaction of the same flush blocks its later rows
        (ignored for an ``order_by`` edge type, like the live writer).
        """
        if not envelopes:
            return {}
        if self._order_by is not None:
            parked_pairs = None
        # Review finding M15: decode/normalize every envelope
        # independently, never as one all-or-nothing list comprehension.
        # A single unreadable value_bytes, or a record that no longer
        # validates against the *current* schema (a property type or a
        # declared property removed since it was DLQ'd), must not raise
        # for the whole batch -- that would fail every healthy sibling
        # record in the same batch too, on every tick, forever (the batch
        # never changes). Such a record is excluded from the write
        # transaction entirely and reported "unresolved" with a dedicated
        # reason_code instead, so the caller's ordinary bounded-attempts/
        # eventual-park machinery (RetryConsumer's own exponential-backoff
        # loop) handles it exactly like any other still-unresolved row.
        valid_positions: list[int] = []
        rows: list[dict] = []
        outcomes: dict[int, tuple[str, str]] = {}
        for envelope_index, envelope in enumerate(envelopes):
            try:
                record = self._record_from_envelope(envelope)
            except Exception as exc:
                logger.error(
                    "stage=retry edge=%s reason=invalid_on_retry topic=%s partition=%s offset=%s "
                    "error=%s -- excluded from this batch's write, reported unresolved",
                    self._edge_type, getattr(envelope, "original_topic", "?"),
                    getattr(envelope, "original_partition", "?"), getattr(envelope, "original_offset", "?"), exc,
                )
                outcomes[envelope_index] = ("unresolved", "invalid_on_retry")
                continue
            valid_positions.append(envelope_index)
            rows.append(self._as_row(record))
        if not rows:
            return outcomes
        timeout_seconds = self._timeout_for_deadline(deadline, 1)
        # The retry consumer already decided, via
        # ParkedPairIndex.is_blocked_by_earlier, which envelopes are
        # candidates at all; only a pair left unresolved by an earlier
        # transaction of this same lease flush (parked_pairs) or by an
        # earlier row of this same batch can still block a row here.
        pre_blocked_pairs = self._pre_blocked_pairs(rows, parked_pairs)
        with open_session(self._driver) as session:
            with self._begin(session, timeout_seconds=timeout_seconds, deadline=deadline) as transaction:
                result = transaction.run(self._classification_query, rows=rows)
                by_index = {r["row_index"]: (r["source_matches"], r["target_matches"]) for r in result}
                result.consume()
                statuses = self._classify_rows(
                    rows, by_index, pre_blocked_pairs=pre_blocked_pairs,
                    order_by_configured=self._order_by is not None,
                )
                matched_indices = sorted(i for i, (status, _) in statuses.items() if status == "matched")
                matched_rows = [rows[i] for i in matched_indices]
                if matched_rows:
                    self._warn_missing_order_by(matched_rows)
                    write_rows = self._collapse_same_pair_rows_for_order_by(matched_rows)
                    write_result = self._run_write_query(transaction, write_rows)
                    counts = list(write_result)
                    write_result.consume()
                    ok = (
                        len(counts) == 1
                        and counts[0]["checked_rows"] == len(matched_rows)
                        and counts[0]["invalid_rows"] == 0
                        and counts[0]["merged_rows"] == len(matched_rows)
                    )
                    if not ok:
                        raise self._error_factory(
                            f"Edge '{self._edge_type}' retry matched-subset write mismatch "
                            "(endpoint changed between classification and write)"
                        )
                    self._log_skipped_as_not_newer(counts[0])
                if lease_liveness_check is not None and not lease_liveness_check():
                    raise LeaseCoordinatorError(
                        f"edge={self._edge_type} lease connection broke before commit; "
                        "aborting instead of committing"
                    )
                transaction.commit()
        if parked_pairs is not None:
            newly_parked = {
                (rows[i]["source_key"], rows[i]["target_key"])
                for i, (status, _) in statuses.items() if status != "matched"
            }
            if newly_parked:
                parked_pairs.mark_parked(newly_parked)
        for local_index, (status, reason_code) in statuses.items():
            original_index = valid_positions[local_index]
            outcomes[original_index] = ("written", "") if status == "matched" else ("unresolved", reason_code)
        return outcomes

    def _record_from_envelope(self, envelope) -> "EdgeRecord":
        """Decode one DLQ envelope's ``value_bytes`` (the wire-input shape
        ``dead_letter.DeadLetterRecordSink.route_unresolved_endpoint`` writes)
        back into an ``EdgeRecord`` via the same ``normalize_edge_record``
        the live loader uses for a fresh Kafka message -- so a retried
        record is validated identically, including property type
        coercion (dates, etc.)."""
        try:
            raw = json.loads(envelope.value_bytes.decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, ValueError) as exc:
            raise self._error_factory(
                f"Edge '{self._edge_type}' retry envelope at {envelope.original_topic}"
                f"[{envelope.original_partition}]@{envelope.original_offset} has unreadable value_bytes: {exc}"
            ) from exc
        labels = _labels_of_key(getattr(envelope, "key_bytes", None))
        if labels is not None and labels != (self._edge_config.nodes.source, self._edge_config.nodes.target):
            # v7-S7: a labelled key of another pair must never be written through this pair's query.
            raise self._error_factory(
                f"Edge '{self._edge_type}' retry envelope at {envelope.original_topic}"
                f"[{envelope.original_partition}]@{envelope.original_offset} is keyed for label pair "
                f"{labels[0]}->{labels[1]}, not {self._edge_config.nodes.source}->{self._edge_config.nodes.target}"
            )
        return normalize_edge_record(raw, self._edge_config)


def _labels_of_key(key_bytes) -> "tuple[str, str] | None":
    """``(S, T)`` of a labelled DLQ key ``[S, T, sk, tk]`` (v7-S7); ``None`` for ``[sk, tk]``."""
    if not key_bytes:
        return None
    try:
        decoded = json.loads(key_bytes)
    except (TypeError, ValueError):
        return None
    if isinstance(decoded, list) and len(decoded) == 4 and all(isinstance(label, str) for label in decoded[:2]):
        return decoded[0], decoded[1]
    return None


class NativeDisjointEdgeWriter:
    """Cypher-25 native concurrent relationship writer, capability-gated."""

    def __init__(self, driver: Driver, edge_config: EdgeConfig, capabilities: ServerCapabilities,
                 *, missing_endpoint_error_factory: Callable[[str], Exception]) -> None:
        require_execution_mode("native_disjoint", capabilities)
        edge_config.single_pair  # v7-S7: views only
        self._driver = driver; self._error_factory = missing_endpoint_error_factory
        self._edge_type = edge_config.type; self._preflight = _preflight_query(edge_config)
        self._concurrency = edge_config.execution.worker_count
        self._batch_size = edge_config.mix_and_batch.batch_size
        self._query = (
            "CYPHER 25\nUNWIND $rows AS row\n"
            f"CALL (row) {{ MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}}) "
            f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}}) "
            f"MERGE (s)-[r:`{edge_config.type}`]->(t) SET r += row.properties RETURN 1 AS wrote }} "
            "IN $concurrency CONCURRENT TRANSACTIONS OF $batch_size ROWS "
            "DISJOINT BY (row.source_key, row.target_key) ON ERROR CONTINUE REPORT STATUS AS status RETURN status"
        )

    def write_batch(self, records: Sequence["EdgeRecord"]) -> None:
        """Preflight endpoints then run native transactions and inspect all statuses."""
        if not records: return
        rows = [{"source_key": r.source_key, "target_key": r.target_key, "properties": r.properties} for r in records]
        with open_session(self._driver) as session:
            preflight = session.run(self._preflight, rows=rows)
            counts = list(preflight); preflight.consume()
            if (
                len(counts) != 1
                or counts[0]["checked_rows"] != len(rows)
                or counts[0]["invalid_rows"] != 0
            ):
                raise self._error_factory(f"Edge '{self._edge_type}' did not resolve exactly one source and target endpoint")
        with open_session(self._driver) as session:
            result = session.run(self._query, rows=rows, concurrency=self._concurrency, batch_size=self._batch_size)
            statuses = list(result); result.consume()
            if not statuses:
                raise ExecutionModeError(f"native_disjoint produced no batch status for edge '{self._edge_type}'")
            for item in statuses:
                status = item["status"]
                committed = status.get("committed") if isinstance(status, dict) else status["committed"]
                error = status.get("errorMessage") if isinstance(status, dict) else status["errorMessage"]
                if committed is not True or (isinstance(error, str) and error.strip()):
                    raise ExecutionModeError(f"native_disjoint batch failed for edge '{self._edge_type}'")

    def write(self, record: "EdgeRecord") -> None:
        """Write one record through the same preflighted native path."""
        self.write_batch([record])


@dataclass(frozen=True)
class LaneExecutionResult:
    """One lane's durable write outcome, with original routing provenance."""
    lane_id: int | None
    routed_records: tuple["RoutedEdgeRecord", ...]
    success: bool
    exception: Exception | None = None


class _ParkedPairSet:
    """Thread-safe :class:`ParkedPairs` for one classic-bulk ``execute()``
    call (one flush) -- P5-S3 Story 5, the same cross-transaction gap
    Story 4 fixed for the lease coordinator applies here too:
    ``LaneBatcher.drain_all()`` can split one lane's queue into several
    sequential batches when it holds more rows than ``batch_size``, and a
    ``(source_key, target_key)`` pair always routes to the same lane
    (deterministic bucket/direction), so it can straddle two of that
    lane's batches -- each written by a separate ``write_provenanced_batch``
    call. One instance is created per ``execute()`` call and shared across
    every lane's thread (different lanes never share a pair by
    construction, but the underlying ``set`` mutation is still guarded for
    safety/clarity).
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._pairs: set = set()

    def is_parked(self, pair: tuple) -> bool:
        with self._lock:
            return pair in self._pairs

    def mark_parked(self, pairs: Iterable[tuple]) -> None:
        with self._lock:
            self._pairs.update(pairs)


class LaneExecutionCoordinator:
    """Bound concurrent APOC lanes while preserving FIFO within each lane."""
    def __init__(self, writer, *, mode: str, worker_count: int,
                 executor_factory=ThreadPoolExecutor) -> None:
        self._writer, self._mode, self._workers, self._executor_factory = writer, mode, worker_count, executor_factory

    def execute(self, lane_batches: Sequence[Sequence["RoutedEdgeRecord"]]) -> tuple[LaneExecutionResult, ...]:
        grouped: dict[int, list[tuple["RoutedEdgeRecord", ...]]] = {}
        for batch in lane_batches:
            if batch:
                grouped.setdefault(batch[0].lane_id, []).append(tuple(batch))
        if self._mode == "native_disjoint":
            # Row-level DLQ is explicitly out of scope for this writer
            # (Decisions point 2): NativeDisjointEdgeWriter already has its
            # own ON ERROR CONTINUE REPORT STATUS per-transaction reporting,
            # a structurally different mechanism.
            records = tuple(item for lane in sorted(grouped) for batch in grouped[lane] for item in batch)
            try:
                self._writer.write_batch([item.record for item in records])
                return (LaneExecutionResult(None, records, True),)
            except Exception as exc:
                return (LaneExecutionResult(None, records, False, exc),)
        # P5-S3 Story 5: one ParkedPairs instance per execute() call (one
        # flush), shared across every lane -- see _ParkedPairSet's docstring.
        row_level_dlq_enabled = getattr(self._writer, "row_level_dlq_enabled", False)
        parked_pairs = _ParkedPairSet() if row_level_dlq_enabled else None
        def run_lane(lane_id: int, batches: list[tuple["RoutedEdgeRecord", ...]]):
            items = tuple(item for batch in batches for item in batch)
            try:
                for batch in batches:
                    if row_level_dlq_enabled:
                        self._writer.write_provenanced_batch(list(batch), parked_pairs=parked_pairs)
                    else:
                        self._writer.write_batch([item.record for item in batch])
                return LaneExecutionResult(lane_id, items, True)
            except Exception as exc:
                return LaneExecutionResult(lane_id, items, False, exc)
        with self._executor_factory(max_workers=self._workers) as executor:
            futures = [(lane, executor.submit(run_lane, lane, items)) for lane, items in sorted(grouped.items())]
            return tuple(future.result() for _lane, future in futures)


def _group_by_pair(pending: Sequence, pair_of: Callable[[object], int]) -> dict[int, list]:
    """``{pair index: items}`` in pair order, each group keeping the input order (stable)."""
    groups: dict[int, list] = {}
    for item in pending:
        groups.setdefault(pair_of(item), []).append(item)
    return dict(sorted(groups.items()))


class _MultiPairWriterGuard:
    """The ``writer`` slot of a multi-pair type's execution (v7-S7).

    Every write of a multi-pair type goes through its pair-split coordinator, which picks the pair
    from the record's topic. A direct write would have to infer the pair from the record, which is
    never allowed (owner decision 3), so it fails loudly."""

    row_level_dlq_enabled = False

    def __init__(self, edge_type: str, writers: dict[int, object]) -> None:
        self._edge_type = edge_type
        self.writers = dict(writers)

    def _refuse(self, *_args, **_kwargs):
        raise ExecutionModeError(
            f"edge={self._edge_type} binds several label pairs; its rows are written through the "
            "pair-split coordinator, never through a type-wide writer"
        )

    write = write_batch = write_rows = write_provenanced_batch = _refuse


class PairSplitLaneExecution:
    """Classic (lane) execution of a multi-pair type: one lane pipeline per label pair (v7-S7).

    ``execute_pending`` splits one flush by the pair of each record's topic (stable) and writes the
    pairs **sequentially**, each through its own ``MixAndBatchPartitioner`` + ``LaneBatcher`` +
    :class:`LaneExecutionCoordinator` built from that pair's view, so no Neo4j transaction ever
    mixes label pairs and flush-scoped ``ParkedPairs`` state stays per pair. Classic is the
    correctness-only mode that may wait on locks; running pairs one after the other keeps its lock
    footprint that of one single-pair type. After a pair fails, the remaining pairs are not written
    (the flush fails, no offset of any pair commits, and the replay rewrites every row); their
    results carry the input items (``PendingEdgeRecord``) rather than routed records.
    ``budget_seconds`` is accepted for interface symmetry and ignored: classic takes no leases."""

    routes_records = True

    def __init__(self, lanes: dict[int, tuple[object, object, "LaneExecutionCoordinator"]],
                 pair_of: Callable[[object], int], *, edge_type: str, pair_names: dict[int, str]) -> None:
        self._lanes = dict(lanes)
        self._pair_of = pair_of
        self._edge_type = edge_type
        self._pair_names = dict(pair_names)
        self._last_flush = ""

    def execute_pending(self, pending: Sequence, *, budget_seconds: float | None = None,
                        ) -> tuple[LaneExecutionResult, ...]:
        results: list[LaneExecutionResult] = []
        details: list[str] = []
        failed: str | None = None
        for pair, items in _group_by_pair(pending, self._pair_of).items():
            name = self._pair_names[pair]
            if failed is not None:
                results.append(LaneExecutionResult(None, tuple(items), False, ExecutionModeError(
                    f"edge={self._edge_type} pair={name} not written: pair {failed} failed in this flush")))
                details.append(f"pair={name} skipped")
                continue
            partitioner, batcher, coordinator = self._lanes[pair]
            for item in items:
                batcher.add(partitioner.route(item.record, topic=item.topic, partition=item.partition,
                                              offset=item.offset))
            pair_results = coordinator.execute(batcher.drain_all())
            results.extend(pair_results)
            details.append(f"pair={name} records={len(items)}")
            if any(not result.success for result in pair_results):
                failed = name
        self._last_flush = " | ".join(details)
        return tuple(results)

    def describe_last_flush(self) -> str:
        return self._last_flush

    def prime(self) -> None:  # symmetry with the lease composite; lanes need no connections
        return None

    def close(self) -> None:
        return None


class PairSplitLeaseCoordinator:
    """Bucket-lease execution of a multi-pair type: one lease coordinator per label pair (v7-S7).

    Each sub-coordinator is an unchanged :class:`BucketLeaseExecutionCoordinator` with its own
    ``BucketCellRouter(S, T, B, keyed)`` and its own lease-client pool, all talking to the same,
    unchanged lease server. Resources are ``Label:bucket`` / ``Label#<key digest>`` tokens, so a
    Person->Post and a Person->Comment cell that share a Person bucket are serialised by the server,
    while ``Post:5`` and ``Comment:5`` are different resources. Every running transaction in the
    fleet therefore stays node-disjoint (zero Neo4j lock waits), a transaction never mixes label
    pairs, and EXT merges and flush-scoped ``ParkedPairs`` state stay within one pair.

    ``execute_pending`` splits a flush by ``pair_of(item)`` (stable) and runs the pairs concurrently
    with the same ``budget_seconds``; results are concatenated in pair order. ``lease_wait`` is
    observed once per outer flush with the sum of the pairs' waits. A replica therefore opens up to
    ``pairs x execution.worker_count`` write sessions and lease connections."""

    routes_records = True

    def __init__(self, coordinators: dict[int, "BucketLeaseExecutionCoordinator"],
                 pair_of: Callable[[object], int], *, edge_type: str, pair_names: dict[int, str],
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None, kind: str = "edge",
                 executor_factory=ThreadPoolExecutor, observe_lease_wait: bool = True) -> None:
        if not coordinators:
            raise ValueError("PairSplitLeaseCoordinator needs at least one pair coordinator")
        self._coordinators = dict(sorted(coordinators.items()))
        self._pair_of = pair_of
        self._edge_type = edge_type
        self._pair_names = dict(pair_names)
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self._kind = kind
        self._executor_factory = executor_factory
        self._observe_lease_wait = observe_lease_wait
        self._last_flush = ""
        self.last_lease_wait_seconds = 0.0

    @property
    def coordinators(self) -> dict[int, "BucketLeaseExecutionCoordinator"]:
        return dict(self._coordinators)

    def prime(self, count: int | None = None) -> None:
        for coordinator in self._coordinators.values():
            coordinator.prime(count)

    def close(self) -> None:
        errors = []
        for coordinator in self._coordinators.values():
            try:
                coordinator.close()
            except Exception as exc:  # close every pool, then report
                errors.append(exc)
        if errors:
            raise errors[0]

    def execute_pending(self, pending: Sequence, *, budget_seconds: float | None = None,
                        ) -> tuple[LaneExecutionResult, ...]:
        groups = _group_by_pair(pending, self._pair_of)
        self.last_lease_wait_seconds = 0.0
        if not groups:
            self._last_flush = ""
            return ()
        outcomes: dict[int, tuple] = {}
        errors: dict[int, BaseException] = {}
        if len(groups) == 1:
            [(pair, items)] = groups.items()
            try:
                outcomes[pair] = self._coordinators[pair].execute_pending(tuple(items), budget_seconds=budget_seconds)
            except Exception as exc:
                errors[pair] = exc
        else:
            with self._executor_factory(max_workers=len(groups)) as executor:
                futures = {
                    pair: executor.submit(self._coordinators[pair].execute_pending, tuple(items),
                                          budget_seconds=budget_seconds)
                    for pair, items in groups.items()
                }
                for pair, future in futures.items():
                    try:
                        outcomes[pair] = future.result()
                    except Exception as exc:  # wait for every pair before reporting
                        errors[pair] = exc
        wait = sum(self._coordinators[pair].last_lease_wait_seconds for pair in outcomes)
        self.last_lease_wait_seconds = wait
        self._last_flush = " | ".join(
            f"pair={self._pair_names[pair]} "
            + (self._coordinators[pair].describe_last_flush() if pair in outcomes else "failed")
            for pair in groups
        )
        if self._observe_lease_wait:
            metrics_module.safe_emit(lambda: self._metrics.lease_wait.labels(
                kind=self._kind, name=self._edge_type
            ).observe(wait))
        if errors:
            for pair, exc in list(errors.items())[1:]:
                logger.error("edge=%s pair=%s also failed in this flush: %s: %s", self._edge_type,
                             self._pair_names[pair], type(exc).__name__, exc)
            raise next(iter(errors.values()))
        results = [result for pair in groups for result in outcomes[pair]]
        # One failure per outer flush, as in _LeaseFlush.record_failure: a real error beats a
        # stop-drain abandon, so the loader logs it and fails instead of exiting as drained.
        real = next((r.exception for r in results if not r.success and r.exception is not None
                     and not isinstance(r.exception, LeaseDrainAbandoned)), None)
        if real is not None:
            results = [r if r.success else dataclass_replace(r, exception=real) for r in results]
        return tuple(results)

    def describe_last_flush(self) -> str:
        return self._last_flush


@dataclass
class _LeaseFlush:
    """Mutable per-flush cell state; every access holds ``lock``."""
    cells: dict[tuple, deque]
    totals: dict[tuple, int]
    resources: dict[tuple, tuple[str, ...]]
    by_resource: dict[str, list[tuple]]
    nonempty: set[tuple]
    lock: Lock
    written: dict[tuple, int]
    busy: set[tuple]
    failure: Exception | None = None
    transactions: int = 0
    merged_cells: int = 0
    lease_wait_seconds: float = 0.0
    timeouts: int = 0
    # P5-S3 Story 4 review finding: pairs already parked by an earlier
    # transaction in this same flush (ParkedPairs protocol) -- a cell can
    # be drained across several sequential transactions, and a pair always
    # routes to the same cell, so this must span all of them, not just one
    # write_provenanced_batch call.
    parked_pairs: set = field(default_factory=set)
    # P5-S4 Story 3's second half: the durable, cross-flush index, when
    # this edge type has one (streaming, DLQ-enabled, not order_by --
    # BucketLeaseExecutionCoordinator decides). None preserves this
    # class's pre-P5-S4 behavior byte for byte.
    durable_index: "DurableParkedPairIndex | None" = None
    # Clock reading (the coordinator's clock) after which workers stop asking
    # for leases and abandon the flush; None means no time bound.
    abandon_at: float | None = None
    # Cells put back because too little of their lease deadline was left to
    # begin a fenced transaction (fenced_tx.LeaseDeadlineExceeded).
    deadline_requeues: int = 0

    def pop(self, key: tuple, count: int) -> list:
        queue = self.cells[key]
        rows = [queue.popleft() for _ in range(min(count, len(queue)))]
        if not queue:
            self.nonempty.discard(key)
        return rows

    def requeue(self, taken: dict) -> None:
        """Put popped rows back at the front of their cells, in their original order."""
        for key, rows in taken.items():
            self.cells[key].extendleft(reversed(rows))
            if rows:
                self.nonempty.add(key)

    def record_failure(self, exc: Exception) -> None:
        # The first failure wins, except that a real error replaces a mere
        # stop-drain abandon: it must still fail the loader (exit 1) and be logged.
        if self.failure is None or (
            isinstance(self.failure, LeaseDrainAbandoned) and not isinstance(exc, LeaseDrainAbandoned)
        ):
            self.failure = exc

    # -- ParkedPairs protocol (thread-safe: shared across worker threads) --

    def is_parked(self, pair: tuple) -> bool:
        with self.lock:
            if pair in self.parked_pairs:
                return True
        # P5-S4: fall back to the durable, cross-flush index -- a pair
        # parked by an *earlier flush* (not just an earlier transaction of
        # THIS flush) must also block here. is_blocked() is a cheap local
        # dict read (no Kafka/broker call), so this costs nothing extra
        # when nothing is blocked.
        if self.durable_index is not None:
            return self.durable_index.is_blocked(*pair)
        return False

    def mark_parked(self, pairs) -> None:
        with self.lock:
            self.parked_pairs.update(pairs)


class LeaseDrainAbandoned(ExecutionModeError):
    """A loader gave up waiting for a bucket lease.

    Raised (as a flush failure) once the stop drain budget, or a caller's
    per-flush time budget (``execute_pending(budget_seconds=...)``), is
    spent: nothing of the abandoned work was written without a lease, so the
    caller commits no offsets for it and the records are redelivered (MERGE
    is idempotent).
    """


DEFAULT_DRAIN_BUDGET_SECONDS = 5.0


class _DrainDeadline:
    """The stop-drain deadline: set on the first check after the stop, then fixed.

    One instance per coordinator by default. The per-pair coordinators of a multi-pair type share
    one (v7-S7), so the whole loader's stop drain stays bounded by one ``drain_budget_seconds``
    however its flushes are split across pairs."""

    def __init__(self) -> None:
        self._lock = Lock()
        self.deadline: float | None = None

    def expired(self, now: float, budget: float) -> bool:
        with self._lock:
            if self.deadline is None:
                self.deadline = now + budget
            return now >= self.deadline


class BucketLeaseExecutionCoordinator:
    """Write one outer flush as node-disjoint cell transactions under global leases.

    Each record goes to its cell (source resource x target resource, see
    :class:`~src.loader.lease_client.BucketCellRouter`).  A pool of
    ``worker_count`` threads repeatedly leases the resource set of a nonempty
    cell -- one ``ANY`` request listing up to ``candidate_limit`` pending
    cells, largest first, that waits server-side for releases -- takes up to
    ``batch_size`` of that cell's oldest rows, optionally merges small
    neighbouring cells (``EXT``: cells sharing a held resource, while the
    transaction stays under ``merge_target_rows``), writes them in ONE
    transaction, and releases the lease only after the commit.  Only one
    holder can own a cell's resources, so a cell's rows are written in FIFO
    order and every concurrently running transaction in the whole fleet is
    node-disjoint.  No write starts without a lease: any coordinator error
    fails the flush, and the caller then commits no offsets.
    """

    routes_records = True
    # Re-grants per flush of cells whose lease deadline was too short to begin
    # a fenced transaction (fenced_tx.LeaseDeadlineExceeded); past this the
    # flush fails as before, and the caller commits no offsets.
    _MAX_DEADLINE_REQUEUES = 16

    def __init__(
        self, writer, *, router, worker_count: int, batch_size: int,
        client_factory: Callable[[], object], wait_ms: int = 200, candidate_limit: int = 128,
        merge_target_rows: int = 200, merge_limit: int = 32,
        backoff_seconds: tuple[float, float] = (0.001, 0.002),
        sleep: Callable[[float], None] = time.sleep,
        executor_factory=ThreadPoolExecutor, event_logger: logging.Logger = logger,
        edge_type: str = "",
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
        tx_timeout_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        parked_pair_index: "DurableParkedPairIndex | None" = None,
        kind: str = "edge",
        stop_event: Event | None = None,
        drain_budget_seconds: float = DEFAULT_DRAIN_BUDGET_SECONDS,
        observe_lease_wait: bool = True,
        drain_deadline: "_DrainDeadline | None" = None,
    ) -> None:
        if (
            worker_count < 1 or batch_size < 1 or candidate_limit < 1 or wait_ms < 0
            or merge_target_rows < 0 or merge_limit < 1
        ):
            raise ValueError("lease coordinator sizes must be positive")
        self._writer, self._router = writer, router
        # P5-S4 Story 3's second half: threaded into every flush's
        # _LeaseFlush as its durable_index fallback (see that class's
        # is_parked) -- None (the default) preserves this coordinator's
        # pre-P5-S4 behavior byte for byte.
        self._parked_pair_index = parked_pair_index
        self._workers, self._batch_size = worker_count, batch_size
        self._client_factory = client_factory
        self._wait_ms, self._candidate_limit = wait_ms, candidate_limit
        self._merge_target, self._merge_limit = min(merge_target_rows, batch_size), merge_limit
        self._backoff, self._sleep = backoff_seconds, sleep
        self._executor_factory, self._logger, self._edge_type = executor_factory, event_logger, edge_type
        # Metrics ``kind`` label: "edge", or "node" when a stream node loader
        # reuses this coordinator for its node-disjoint cell writes.
        self._kind = kind
        self._idle_clients: list = []
        self._clients_lock = Lock()
        self._last_flush = "cells=0 transactions=0"
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        # P5-S1 Story 4 / P5-Q1 fencing amendment: the Neo4j transaction
        # timeout T. Each cell's deadline (grant time + T) is computed
        # fresh per grant in _run_worker, not stored here -- this is only
        # the constant T itself, shared by every grant this coordinator's
        # workers make.
        self._tx_timeout_seconds = tx_timeout_seconds
        self._clock = clock
        # Bounded stop drain: once ``stop_event`` is set (a SIGTERM), a flush
        # may keep waiting for leases for at most ``drain_budget_seconds``
        # in total -- well inside docker's stop grace. A lease held by a dead
        # writer stays fenced for T + margin, far longer than that. Past the
        # budget no new lease is requested and the flush fails with
        # LeaseDrainAbandoned (nothing is written without a lease).
        self._stop_event = stop_event
        self._drain_budget = drain_budget_seconds
        self._drain = drain_deadline if drain_deadline is not None else _DrainDeadline()
        # v7-S7: a per-pair sub-coordinator of PairSplitLeaseCoordinator does not
        # observe lease_wait itself; the composite observes the sum once per outer
        # flush (P6-S2 Story 5), reading last_lease_wait_seconds.
        self._observe_lease_wait = observe_lease_wait
        self.last_lease_wait_seconds = 0.0

    def _drain_expired(self) -> bool:
        if self._stop_event is None or not self._stop_event.is_set():
            return False
        return self._drain.expired(self._clock(), self._drain_budget)

    def _record_worker_failure(self, state: _LeaseFlush, exc: Exception) -> None:
        """Record a worker failure; a lease connect abandoned by a stop is a drain abandon."""
        if (
            isinstance(exc, LeaseCoordinatorError) and "shutdown requested" in str(exc)
            and self._stop_event is not None and self._stop_event.is_set()
        ):
            exc = LeaseDrainAbandoned(f"stop requested while reconnecting to the lease coordinator: {exc}")
        with state.lock:
            state.record_failure(exc)

    # -- lease client pool ---------------------------------------------------

    def _checkout(self):
        with self._clients_lock:
            if self._idle_clients:
                return self._idle_clients.pop()
        client = self._client_factory()
        client.connect()
        return client

    def _checkin(self, client) -> None:
        if getattr(client, "broken", False) is True:
            client.close()
            return
        with self._clients_lock:
            self._idle_clients.append(client)

    def prime(self, count: int | None = None) -> None:
        """Connect the full client pool (``worker_count`` by default) up front.

        A bad coordinator fails at startup, and a client created lazily after
        a stop request cannot connect (its reconnect loop honours the stop
        event), so a draining loader needs every client already pooled. If
        any connect fails, the clients opened so far are closed and the error
        is re-raised.
        """
        wanted = self._workers if count is None else count
        clients = []
        try:
            for _ in range(max(1, wanted)):
                client = self._client_factory()
                clients.append(client)
                client.connect()
        except BaseException:
            for client in clients:
                try:
                    client.close()
                except Exception:
                    pass
            raise
        for client in clients:
            self._checkin(client)

    def close(self) -> None:
        with self._clients_lock:
            clients, self._idle_clients = self._idle_clients, []
        for client in clients:
            client.close()

    # -- execution -------------------------------------------------------------

    def execute_pending(
        self, pending: Sequence, *, budget_seconds: float | None = None,
    ) -> tuple[LaneExecutionResult, ...]:
        """Durably write every record of one flush, or report the failure.

        ``budget_seconds`` bounds how long the flush may keep waiting for
        leases: once spent, no new lease is requested and the flush fails
        with :class:`LeaseDrainAbandoned` (rows already committed keep their
        results; the rest were never written)."""
        self.last_lease_wait_seconds = 0.0
        cells: dict[tuple, deque] = {}
        cell = self._router.cell
        for item in pending:
            key = cell(item.record.source_key, item.record.target_key)
            queue = cells.get(key)
            if queue is None:
                queue = cells[key] = deque()
            queue.append(item)
        if not cells:
            return ()
        cell_items = {key: tuple(queue) for key, queue in cells.items()}
        resources = {key: self._router.resources(key) for key in cells}
        by_resource: dict[str, list[tuple]] = {}
        for key, tokens in resources.items():
            for token in tokens:
                by_resource.setdefault(token, []).append(key)
        state = _LeaseFlush(
            cells=cells, totals={key: len(queue) for key, queue in cells.items()},
            resources=resources, by_resource=by_resource, nonempty=set(cells),
            lock=Lock(), written={key: 0 for key in cells}, busy=set(),
            durable_index=self._parked_pair_index,
            abandon_at=None if budget_seconds is None else self._clock() + budget_seconds,
        )
        workers = min(self._workers, len(cells))
        started = time.perf_counter()
        with self._executor_factory(max_workers=workers) as executor:
            futures = [executor.submit(self._run_worker, state) for _ in range(workers)]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:  # defensive: workers record their own failures
                    with state.lock:
                        state.record_failure(exc)
        results = []
        for index, key in enumerate(cells):
            complete = state.failure is None and state.written[key] == state.totals[key]
            error = None if complete else (
                state.failure if state.failure is not None
                else ExecutionModeError("lease cell was not completely written")
            )
            results.append(LaneExecutionResult(index, cell_items[key], complete, error))
        self._last_flush = (
            f"cells={len(cells)} transactions={state.transactions} merged_cells={state.merged_cells} "
            f"workers={workers} lease_wait_ms={state.lease_wait_seconds * 1000:.3f} "
            f"wait_timeouts={state.timeouts}"
        )
        self._logger.debug(
            "stage=edge-lease-flush edge=%s records=%s %s elapsed_ms=%.3f outcome=%s",
            self._edge_type, len(pending), self._last_flush, (time.perf_counter() - started) * 1000,
            "succeeded" if state.failure is None else "failed",
        )
        # One observation per outer flush (never per lease grant/cell) -
        # P6-S2 Story 5. Routed through safe_emit so a metrics-library
        # exception can never turn an already-completed flush into a
        # reported failure.
        lease_wait_seconds = state.lease_wait_seconds
        self.last_lease_wait_seconds = lease_wait_seconds
        if self._observe_lease_wait:
            metrics_module.safe_emit(lambda: self._metrics.lease_wait.labels(
                kind=self._kind, name=self._edge_type
            ).observe(lease_wait_seconds))
        return tuple(results)

    def describe_last_flush(self) -> str:
        """Return ``key=value`` counters of the most recent flush."""
        return self._last_flush

    def _candidates(self, state: _LeaseFlush) -> list:
        """Largest pending cells first; locally busy cells stay eligible.

        A busy cell's lease is held by a sibling worker, so the coordinator
        hands it over only after that worker commits and releases it.
        """
        with state.lock:
            if state.failure is not None or not state.nonempty:
                return []
            cells = state.cells
            if len(state.nonempty) <= self._candidate_limit:
                return sorted(state.nonempty, key=lambda key: -len(cells[key]))
            return heapq.nlargest(self._candidate_limit, state.nonempty, key=lambda key: len(cells[key]))

    def _run_worker(self, state: _LeaseFlush) -> None:
        try:
            client = self._checkout()
        except Exception as exc:
            self._record_worker_failure(state, exc)
            return
        denials = 0
        try:
            while True:
                candidates = self._candidates(state)
                if not candidates:
                    return
                if self._drain_expired():
                    with state.lock:
                        state.record_failure(LeaseDrainAbandoned(
                            f"stop drain budget of {self._drain_budget:g}s spent waiting for bucket leases"
                        ))
                    return
                if state.abandon_at is not None and self._clock() >= state.abandon_at:
                    with state.lock:
                        state.record_failure(LeaseDrainAbandoned(
                            "flush time budget spent waiting for bucket leases"
                        ))
                    return
                asked = time.perf_counter()
                try:
                    grant = client.acquire_any(
                        [state.resources[key] for key in candidates], wait_ms=self._wait_ms,
                    )
                except LeaseCoordinatorError:
                    # P5-S1 Story 4, review finding S3-3 phase (a): the
                    # connection broke before any Neo4j transaction opened
                    # -- nothing was written, so this is safe to ride out
                    # within the same flush rather than failing it. Drop
                    # the broken client and check out a fresh
                    # (reconnecting) one instead.
                    with state.lock:
                        state.lease_wait_seconds += time.perf_counter() - asked
                    client.close()
                    client = self._checkout()
                    continue
                with state.lock:
                    state.lease_wait_seconds += time.perf_counter() - asked
                if grant is None:
                    denials += 1
                    with state.lock:
                        state.timeouts += 1
                    if self._wait_ms == 0:
                        # Non-waiting mode: jittered 1-2 ms back-off, doubling
                        # to at most 4x while every candidate stays denied.
                        low, high = self._backoff
                        self._sleep(random.uniform(low, high) * min(2 ** (denials - 1), 4))
                    continue
                denials = 0
                lease_id, index = grant
                grant_time = self._clock()
                self._write_leased(state, candidates[index], client, lease_id, grant_time)
                if getattr(client, "broken", False):
                    # P5-S1 Story 4, review finding S3-3 phase (c): a REL
                    # (or any other request) failure inside _write_leased
                    # marks the client broken but does not itself fail the
                    # flush -- discard it and check out a fresh
                    # (reconnecting) one immediately, rather than
                    # continuing to issue requests on a known-dead
                    # connection until this worker's loop happens to exit.
                    client.close()
                    client = self._checkout()
        except Exception as exc:
            self._record_worker_failure(state, exc)
        finally:
            self._checkin(client)

    def _merge_plan(self, state: _LeaseFlush, primary: tuple, taken: int) -> tuple[list, list, int]:
        """Rank pending cells sharing a leased resource for an ``EXT`` merge."""
        budget = self._merge_target - taken
        if budget <= 0:
            return [], [], 0
        held = set(state.resources[primary])
        neighbours = {
            key for token in held for key in state.by_resource[token]
            if key != primary and key in state.nonempty and key not in state.busy
        }
        if not neighbours:
            return [], [], 0
        cells = state.cells
        ranked = sorted(
            neighbours,
            key=lambda key: (len(set(state.resources[key]) - held), -len(cells[key])),
        )[:self._merge_limit]
        weighted = [(min(len(cells[key]), budget), state.resources[key]) for key in ranked]
        return ranked, weighted, budget

    @staticmethod
    def _outcome_is_confirmed(exc: Exception | None) -> bool:
        """Whether it is safe to REL this lease immediately (review finding
        B4-2): a confirmed commit (``exc is None``), or a pre-commit raise
        where Neo4j itself is known to have never applied anything --
        never for an ambiguous failure (a dropped connection, a lost
        commit reply, a driver-level ServiceUnavailable/SessionExpired,
        or anything else this coordinator cannot positively identify).
        For anything not confirmed, the lease is left for the server's own
        disconnect quarantine (B4-1) to expire once T + margin has passed
        since the disconnect, instead of handing the same nodes to another
        writer while the outcome of the first transaction is still
        unknown.
        """
        if exc is None:
            return True
        from src.loader.edge_loader import MissingRelationshipEndpointError
        return isinstance(exc, (MissingRelationshipEndpointError, LeaseCoordinatorError, ClientError, TransientError))

    def _write_leased(self, state: _LeaseFlush, primary: tuple, client, lease_id: str, grant_time: float) -> None:
        """Write the leased cell(s) in one transaction, then release the lease."""
        taken: dict[tuple, list] = {}
        deadline = None if self._tx_timeout_seconds is None else grant_time + self._tx_timeout_seconds
        outcome_exc: Exception | None = None

        def liveness_check() -> bool:
            # P5-S1 Story 4: the pre-commit PING optimisation. Its cost is
            # accounted as lease-wait time (same flush-level metric as the
            # ANY/EXT round trips), per this story's AC. Not the fencing
            # guarantee -- see ApocLockedEdgeWriter's docstring and
            # plan/v5-v6-orchestration.md's P5-Q1 amendment; ``deadline``
            # above (Neo4j's own server-enforced transaction timeout) is.
            started = time.perf_counter()
            try:
                return not client.broken and client.ping()
            finally:
                with state.lock:
                    state.lease_wait_seconds += time.perf_counter() - started

        try:
            with state.lock:
                if state.failure is not None:
                    return
                if primary in state.busy:
                    raise ExecutionModeError("lease coordinator granted a cell another local writer holds")
                rows = state.pop(primary, self._batch_size)
                if not rows:
                    return
                taken[primary] = rows
                state.busy.add(primary)
                ranked, weighted, budget = self._merge_plan(state, primary, len(rows))
            if ranked:
                added = client.extend(lease_id, weighted, budget=budget)
                with state.lock:
                    for index in added:
                        key = ranked[index]
                        if key in state.busy:
                            raise ExecutionModeError("lease coordinator extended onto a cell another local writer holds")
                        extra = state.pop(key, weighted[index][0])
                        if extra:
                            taken[key] = extra
                            state.busy.add(key)
            # P5-S3 Story 4: row-level missing-endpoint DLQ, opt-in per
            # writer instance (never re-derived from environment/mode
            # here). getattr with a default: NativeDisjointEdgeWriter has
            # no row_level_dlq_enabled at all, and this coordinator only
            # ever wraps ApocLockedEdgeWriter, but this stays defensive
            # rather than assuming that forever.
            flattened = [item for rows in taken.values() for item in rows]
            if getattr(self._writer, "row_level_dlq_enabled", False):
                self._writer.write_provenanced_batch(
                    flattened, deadline=deadline, lease_liveness_check=liveness_check,
                    # P5-S3 Story 4 review finding: `state` implements the
                    # ParkedPairs protocol (is_parked/mark_parked), carrying
                    # the same-pair ordering guard across every sequential
                    # transaction this cell (or a merged neighbour) is
                    # drained through within this one flush.
                    parked_pairs=state,
                )
            else:
                self._writer.write_batch(
                    [item.record for item in flattened],
                    deadline=deadline, lease_liveness_check=liveness_check,
                )
            with state.lock:
                for key, rows in taken.items():
                    state.written[key] += len(rows)
                state.transactions += 1
                state.merged_cells += len(taken) - 1
        except LeaseDeadlineExceeded as exc:
            # Raised before any statement of this write call ran (a slow
            # connection acquisition or BEGIN, or retries that used up the
            # deadline): nothing was written and no transaction is open. Put
            # the rows back and write them under a fresh grant (a fresh deadline).
            outcome_exc = exc  # confirmed: the lease is released at once below
            with state.lock:
                requeue = state.deadline_requeues < self._MAX_DEADLINE_REQUEUES
                if requeue:
                    state.deadline_requeues += 1
                    state.requeue(taken)
                else:
                    state.record_failure(exc)
            if not requeue:
                raise
            self._logger.info(
                "stage=lease-fence edge=%s reason=deadline_requeue cells=%s requeues=%s detail=%s",
                self._edge_type, len(taken), state.deadline_requeues, exc,
            )
        except Exception as exc:
            outcome_exc = exc
            with state.lock:
                state.record_failure(exc)
            raise
        finally:
            # No `return`/`raise` here: this must never suppress an
            # exception already propagating from the `try`/`except`
            # above (a bare `return` inside `finally` would silently
            # swallow it -- a classic Python trap this coordinator's
            # correctness contract cannot afford).
            with state.lock:
                state.busy.difference_update(taken)
            if self._outcome_is_confirmed(outcome_exc):
                try:
                    client.release(lease_id)
                except LeaseCoordinatorError:
                    # P5-S1 Story 4, review finding S3-3 phase (c): whatever
                    # happened above (a successful commit, or an already-
                    # recorded failure) has already been decided; a REL
                    # failure here only means this client is broken (the
                    # pool discards it on checkin) and this lease's
                    # bookkeeping is lost -- the server's own disconnect-
                    # release covers it anyway. Never let this mask or
                    # replace the original outcome, and never turn an
                    # already-durable commit into a reported failure.
                    pass
            else:
                # P5-S1 Story 4, review finding B4-2: the Neo4j outcome is
                # not positively known (e.g. the commit reply was lost, or
                # the connection died mid-transaction) -- releasing now
                # would hand these exact nodes to another writer while our
                # transaction might still be open or committing. Leave
                # them held; closing this connection lets the server's own
                # disconnect quarantine (B4-1) free them once fence_seconds
                # after the disconnect has safely passed.
                client.close()


def lease_mode_settings(environ=os.environ) -> tuple[str, int, frozenset[str]] | None:
    """Return ``(address, bucket_count, keyed_labels)`` when bucket-lease mode is on."""
    from src.loader.lease_client import (
        LEASE_BUCKETS_ENV, LEASE_KEYED_LABELS_ENV,
        parse_keyed_labels, parse_lease_address, parse_lease_bucket_count,
        resolve_bucket_lease_address,
    )
    address = resolve_bucket_lease_address(environ).strip()
    if not address:
        return None
    parse_lease_address(address)
    return (
        address, parse_lease_bucket_count(environ.get(LEASE_BUCKETS_ENV)),
        parse_keyed_labels(environ.get(LEASE_KEYED_LABELS_ENV)),
    )


LEASE_MERGE_ROWS_ENV = "GRAPH_LOADER_LEASE_MERGE_ROWS"
DEFAULT_LEASE_MERGE_ROWS = 200


def streaming_lease_client_factory(
    environ, bucket_lease_config, shutdown_requested,
) -> Callable[[], object] | None:
    """Build a streaming-flavored ``BucketLeaseClient`` factory (P5-S1
    Story 7), or ``None`` when lease mode is not active at all.

    Unlike bulk's default factory (a fixed 30s connect deadline, no
    backoff -- the coordinator is assumed already up before any bulk
    writer starts), a streaming client rides out a supervised lease-
    server restart: ``backoff_ms`` from the schema's streaming supervision
    settings (Story 3's capability), and ``connect_timeout_seconds``
    derived from ``bucket_lease_config.reconnect_timeout_ms`` -- ``0``
    means unbounded (an explicit, documented-risk override), any other
    value is a bounded number of seconds (review finding B7-1: streaming's
    poll thread blocks synchronously on this reconnect and the write in
    this slice, so an unbounded budget by default could exceed
    ``max.poll.interval.ms`` across a few lease-server restarts;
    ``LoadingConfig``'s own validator keeps the bounded default safely
    under that). ``stop_event=shutdown_requested`` so the reconnect loop
    is interruptible by SIGTERM either way.
    """
    leases = lease_mode_settings(environ)
    if leases is None:
        return None
    from src.loader.lease_client import BucketLeaseClient

    address, bucket_count, keyed_labels = leases
    backoff_ms = (bucket_lease_config.restart_backoff_base_ms, bucket_lease_config.restart_backoff_max_ms)
    reconnect_ms = bucket_lease_config.reconnect_timeout_ms
    connect_timeout_seconds = None if reconnect_ms == 0 else reconnect_ms / 1000
    return lambda: BucketLeaseClient(
        address, bucket_count=bucket_count, keyed_labels=keyed_labels,
        connect_timeout_seconds=connect_timeout_seconds, backoff_ms=backoff_ms,
        stop_event=shutdown_requested,
    )


def build_edge_execution(edge_config: EdgeConfig, driver: Driver, *, executor_factory=ThreadPoolExecutor,
                         environ=os.environ, lease_client_factory=None,
                         metrics: "metrics_module.GraphLoaderMetrics | None" = None,
                         kind: str = "edge",
                         bucket_lease_tx_timeout_seconds: float | None = None,
                         stop_event: Event | None = None,
                         mode: str = "bulk",
                         missing_endpoint_sink: "MissingEndpointSink | None" = None,
                         parked_pair_index: "DurableParkedPairIndex | None" = None,
                         observe_lease_wait: bool = True,
                         drain_deadline: "_DrainDeadline | None" = None):
    """Construct the selected writer before the loader subscribes to Kafka.

    ``bucket_lease_tx_timeout_seconds`` (P5-S1 Story 4 / the P5-Q1 fencing
    amendment) is only used by the lease branch below -- classic mode (no
    lease, real Neo4j locking) has no lease-server-restart exposure to
    fence against, so it stays unbounded/server-default regardless of what
    the caller passes here.

    ``mode`` (P5-S3 Stories 4-5) is the caller's own ``args.mode``
    (``"bulk"`` or ``"stream"``) -- the *only* signal, together with the
    ``leases is not None``/schema ``dead_letter`` state below, that decides
    whether row-level missing-endpoint DLQ activates for this writer.
    Defaults to ``"bulk"`` so every existing call site (every test, and any
    caller that never passes it) keeps today's byte-identical behavior --
    row-level DLQ requires an explicit opt-in in bulk mode regardless of
    ``missing_endpoint_sink`` being supplied. This module never re-derives
    "am I streaming, bulk, or the deprecated clock path" from environment
    state itself; the caller (``edge_loader.py:main()``) always already
    knows ``args.mode``.
      | branch reached            | mode     | row-level DLQ active when |
      | ------------------------- | -------- | -------------------------- |
      | lease (``leases is not None``) | "stream" | a DLQ topic resolves (unconditional default) |
      | lease                     | "bulk"   | a DLQ topic resolves AND ``dead_letter.route_missing_endpoints_in_bulk`` |
      | classic (``execution.mode != "native_disjoint"``) | "bulk" | same opt-in flag |
      | classic                   | "stream" | never (the deprecated ``--stream-clock-rotation`` path) |
      | classic, ``execution.mode == "native_disjoint"`` | any | never (structurally different mechanism; out of scope) |

    ``native_disjoint`` can only ever reach the *classic* branch above:
    the lease branch raises ``ExecutionModeError`` at the top of this
    function for any ``execution.mode`` other than ``"python_apoc"``
    (pre-existing, P5-S1 -- bucket-lease mode requires ``python_apoc``),
    so ``native_disjoint`` is rejected outright before this slice's
    row-level-DLQ logic is ever reached, rather than silently running
    without it.

    ``parked_pair_index`` (P5-S4 Story 3's second half): the durable,
    cross-flush/cross-process ``ParkedPairIndex`` for this edge type, or
    ``None``. Only used on the **lease** branch when row-level DLQ is
    active (``row_level_active``) -- bulk's classic/lease-bulk paths never
    use it (P5-S4 Decisions §1: bulk has no live retry consumer to close
    the cross-flush gap against, so there is nothing for a durable index
    to protect there) -- and never when ``edge_config.order_by`` is set
    (Decisions §10: an ``order_by``-configured type uses a conditional
    ``SET`` instead, Story 4, and skips this mechanism by construction,
    enforced here rather than left to the caller). When active,
    ``missing_endpoint_sink`` is wrapped in ``IndexedMissingEndpointSink``
    so every unresolved row is durably recorded before its DLQ publish,
    and the coordinator's every flush consults it as a fallback through
    the *existing* ``ParkedPairs`` protocol (``_LeaseFlush.is_parked``) --
    not a second, parallel blocking mechanism.
    """
    if mode not in ("bulk", "stream"):
        raise ValueError(f"mode must be 'bulk' or 'stream', got {mode!r}")
    if edge_config.is_multi_pair:
        return _build_multi_pair_execution(
            edge_config, driver, executor_factory=executor_factory, environ=environ,
            lease_client_factory=lease_client_factory, metrics=metrics, kind=kind,
            bucket_lease_tx_timeout_seconds=bucket_lease_tx_timeout_seconds, stop_event=stop_event, mode=mode,
            missing_endpoint_sink=missing_endpoint_sink, parked_pair_index=parked_pair_index,
            observe_lease_wait=observe_lease_wait, drain_deadline=drain_deadline,
        )
    from src.loader.edge_loader import MissingRelationshipEndpointError
    from src.loader.mix_and_batch import LaneBatcher, MixAndBatchPartitioner
    dlq_topic = edge_dead_letter_topic_name(edge_config)
    bulk_opt_in = bool(edge_config.dead_letter is not None and edge_config.dead_letter.route_missing_endpoints_in_bulk)
    leases = lease_mode_settings(environ)
    if leases is not None:
        from src.loader.lease_client import BucketCellRouter, BucketLeaseClient
        if edge_config.execution.mode != "python_apoc":
            raise ExecutionModeError(
                f"bucket-lease mode requires python_apoc execution; edge={edge_config.type} "
                f"mode={edge_config.execution.mode}"
            )
        address, bucket_count, keyed_labels = leases
        merge_raw = environ.get(LEASE_MERGE_ROWS_ENV, "").strip()
        if merge_raw and not merge_raw.isdigit():
            raise ValueError(f"{LEASE_MERGE_ROWS_ENV} must be a nonnegative integer")
        merge_rows = int(merge_raw) if merge_raw else DEFAULT_LEASE_MERGE_ROWS
        # Leases make concurrent transactions node-disjoint, so the explicit
        # APOC lock step is pure overhead; the endpoint validation guard stays.
        # sort_rows=False (P6-S4 finding, fixed here since lease mode becomes
        # the streaming default in this slice): the default sort_rows=True
        # adds "ORDER BY elementId(s), elementId(t)", which is NOT a stable
        # sort for repeated (source,target) pairs within one transaction --
        # it can reorder them, silently breaking last-write-wins. That sort
        # exists only to reduce deadlocks under Neo4j's own MERGE locking; a
        # lease-mode transaction is already node-disjoint by construction
        # (the same guarantee planned mode relies on, which already passes
        # sort_rows=False), so the sort is both unnecessary and actively
        # harmful here.
        # tx_timeout_seconds is NOT set on the writer itself here: every real
        # write call supplies its own per-grant `deadline` (see
        # BucketLeaseExecutionCoordinator._write_leased), which takes
        # priority over a flat per-instance timeout.
        # P5-S3 Story 4: streaming activates row-level DLQ unconditionally
        # whenever a DLQ topic resolves (dead_letter.enabled=True); lease
        # BULK (--edge-leases) needs the explicit opt-in too, per the table
        # in this function's docstring.
        row_level_active = dlq_topic is not None and (mode == "stream" or bulk_opt_in)
        # P5-S4 Story 3's second half / Decisions §10: the durable index is
        # only ever wired in for streaming's row-level-DLQ-active,
        # non-order_by case -- never bulk (Decisions §1: no live retry
        # consumer there) and never an order_by-configured type
        # (Decisions §10: uses a conditional SET instead, Story 4).
        active_parked_pair_index = (
            parked_pair_index
            if (row_level_active and mode == "stream" and edge_config.order_by is None)
            else None
        )
        effective_sink = missing_endpoint_sink
        if row_level_active and active_parked_pair_index is not None and missing_endpoint_sink is not None:
            effective_sink = IndexedMissingEndpointSink(missing_endpoint_sink, active_parked_pair_index)
        writer = ApocLockedEdgeWriter(driver, edge_config, explicit_locks=False, sort_rows=False,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
            metrics=metrics, kind=kind,
            missing_endpoint_sink=effective_sink if row_level_active else None)
        factory = lease_client_factory or (
            lambda: BucketLeaseClient(address, bucket_count=bucket_count, keyed_labels=keyed_labels)
        )
        coordinator = BucketLeaseExecutionCoordinator(
            writer,
            router=BucketCellRouter(
                edge_config.nodes.source, edge_config.nodes.target, bucket_count, keyed_labels,
            ),
            worker_count=edge_config.execution.worker_count,
            batch_size=edge_config.mix_and_batch.batch_size,
            merge_target_rows=merge_rows,
            client_factory=factory, executor_factory=executor_factory, edge_type=edge_config.type,
            metrics=metrics, tx_timeout_seconds=bucket_lease_tx_timeout_seconds,
            parked_pair_index=active_parked_pair_index, stop_event=stop_event,
            observe_lease_wait=observe_lease_wait, drain_deadline=drain_deadline,
        )
        coordinator.prime()
        logger.info(
            "stage=edge-lease-mode edge=%s address=%s buckets=%s keyed_labels=%s workers=%s "
            "batch_size=%s merge_rows=%s",
            edge_config.type, address, bucket_count, ",".join(sorted(keyed_labels)) or "-",
            edge_config.execution.worker_count, edge_config.mix_and_batch.batch_size, merge_rows,
        )
        return writer, None, None, coordinator
    if edge_config.execution.mode == "native_disjoint":
        capabilities = probe_server_capabilities(driver)
        require_execution_mode(edge_config.execution.mode, capabilities)
        writer = NativeDisjointEdgeWriter(driver, edge_config, capabilities,
            missing_endpoint_error_factory=MissingRelationshipEndpointError)
    else:
        # P5-S3 Story 5: the classic-bulk opt-in only -- never "stream",
        # which always reaches this branch through the deprecated
        # --stream-clock-rotation path (P5-S1's clock retirement), not the
        # streaming default; row-level DLQ is new behavior this slice must
        # not silently graft onto a path scheduled for removal.
        row_level_active = mode == "bulk" and dlq_topic is not None and bulk_opt_in
        writer = ApocLockedEdgeWriter(driver, edge_config,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
            metrics=metrics, kind=kind,
            missing_endpoint_sink=missing_endpoint_sink if row_level_active else None)
    return writer, MixAndBatchPartitioner(edge_config), LaneBatcher(edge_config, clock=__import__("time").monotonic), LaneExecutionCoordinator(
        writer, mode=edge_config.execution.mode, worker_count=edge_config.execution.worker_count,
        executor_factory=executor_factory
    )


def _build_multi_pair_execution(edge_config: EdgeConfig, driver: Driver, *, missing_endpoint_sink,
                                parked_pair_index, observe_lease_wait: bool = True, drain_deadline=None,
                                **kwargs):
    """v7-S7: build one single-pair execution per label pair and compose them.

    Each pair is built by :func:`build_edge_execution` itself from ``edge_config.pair_view(pair)``,
    so its writer, queries, router, lanes and DLQ behaviour are exactly a single-pair type's. A flush
    is split by the pair its records' topics bind (decision 3: never by the record) and no Neo4j
    transaction ever mixes label pairs. Returns ``(guard writer, None, None, composite coordinator)``.
    """
    pairs = edge_config.endpoint_pairs
    pair_of_topic = {topic: pair.index for pair in pairs for topic in pair.topics}
    pair_names = {pair.index: pair.name for pair in pairs}
    parts = {}
    shared_drain = drain_deadline if drain_deadline is not None else _DrainDeadline()
    scoped = getattr(parked_pair_index, "scoped", None)
    if parked_pair_index is not None and not callable(scoped):
        raise ExecutionModeError(
            f"edge={edge_config.type} binds several label pairs; its parked-pair index must be pair-scoped"
        )
    try:
        for pair in pairs:
            # v7-S7 7.7: the DLQ key carries the pair's labels and the live index check asks only
            # about this pair, so a parked Comment (1,1) never blocks a live Post (1,1).
            parts[pair.index] = build_edge_execution(
                edge_config.pair_view(pair), driver,
                missing_endpoint_sink=(None if missing_endpoint_sink is None
                                       else LabelledEndpointSink(missing_endpoint_sink, pair.labels)),
                parked_pair_index=None if parked_pair_index is None else scoped(pair.labels),
                observe_lease_wait=False, drain_deadline=shared_drain, **kwargs,
            )
    except BaseException:
        for _writer, _partitioner, _batcher, coordinator in parts.values():
            close = getattr(coordinator, "close", None)
            if callable(close):
                close()
        raise
    guard = _MultiPairWriterGuard(edge_config.type, {index: part[0] for index, part in parts.items()})

    def pair_of(item) -> int:
        return pair_of_topic[item.topic]

    if all(isinstance(part[3], BucketLeaseExecutionCoordinator) for part in parts.values()):
        logger.info("stage=edge-multi-pair edge=%s mode=lease pairs=%s workers_per_pair=%s", edge_config.type,
                    ",".join(pair.name for pair in pairs), edge_config.execution.worker_count)
        return guard, None, None, PairSplitLeaseCoordinator(
            {index: part[3] for index, part in parts.items()}, pair_of, edge_type=edge_config.type,
            pair_names=pair_names, metrics=kwargs.get("metrics"), kind=kwargs.get("kind", "edge"),
            executor_factory=kwargs.get("executor_factory", ThreadPoolExecutor),
            observe_lease_wait=observe_lease_wait,
        )
    lanes = {index: (part[1], part[2], part[3]) for index, part in parts.items()}
    logger.info("stage=edge-multi-pair edge=%s mode=lanes pairs=%s", edge_config.type,
                ",".join(pair.name for pair in pairs))
    return guard, None, None, PairSplitLaneExecution(lanes, pair_of, edge_type=edge_config.type,
                                                     pair_names=pair_names)
