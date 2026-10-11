"""Library-level global edge planning for planned bulk loads (``--edge-plan``).

The planner turns every relationship row of a bulk run into the node-disjoint
chunk plan of :mod:`src.loader.edge_plan`.  It is schema-driven and works for
any graph model: endpoint keys may be any schema scalar type (string,
integer, float, date, datetime), types may be self-referencing, several types
may connect the same labels, and labels without relationships are simply not
planned.

Rows come from *tasks*, each one ordered stream of rows of one edge type:

* :class:`KafkaReadTask` -- a group of partitions of the schema-declared
  relationship topic, read from the low watermark up to the bulk boundary
  (the high watermarks captured when planning starts).  This is the library
  input contract: the same canonical envelopes the per-type edge loaders
  consume, validated by the same normalization
  (:func:`~src.loader.planned_writer.compile_record_normalizer`, identical to
  :func:`~src.loader.edge_records.normalize_edge_record`).  Planned mode is
  fail-closed: an invalid record fails the plan with its topic, partition
  and offset instead of being skipped.
* :class:`SourceTask` -- rows a producer supplies from memory or files
  (:class:`EdgeRecordSource`), for producers that plan while publishing and
  skip the per-type topics altogether (:func:`plan_and_publish_edges`).

``W`` persistent worker processes run three rounds (the parent only
exchanges small messages with them):

1. **scan** -- read and validate rows once, and append each row to its
   ``(source bucket, target bucket)`` cell in compact column buffers; endpoint
   keys are interned per label in bounded *windows* that count degrees.
2. **hubs** -- a node's total degree over all types decides whether it is a
   hub (``>= H``).  Every worker bounds each key's degree from above with
   fixed-size counters (indexed by ``crc32(token)``); the parent sums them,
   and only keys in counter slots of at least ``H`` are summed exactly by
   token over every worker.  The hub set is exact and independent of the
   worker count.
3. **emit** -- with the resource table, workers split each cell into units
   ``(type, source resource, target resource)``, cut each task's share of a
   unit into chunks of at most ``C`` rows (and a byte budget), produce one
   Kafka message per chunk and report its delivered partition and offset.

**Bounded memory.**  Each worker keeps at most ``PlanSettings.memory_mb``
(default: a quarter of the available RAM over the workers, 256 MiB - 4 GiB)
of rows, keys and Kafka client queues in memory.  While everything fits, it
plans purely in memory; beyond that its row buffers and key windows spill to
append-only files in the spill directory (``PlanSettings.spill_dir``,
default beside the plan file), and cells larger than the emit budget are
split by unit on disk and streamed chunk by chunk.  Spill files are deleted
when planning ends.  The plan -- chunks, keys, chain order, ranks, messages --
does not depend on the memory budget (see :mod:`src.loader.edge_spill`).

The parent ranks all chunks (bottleneck-resource LPT), writes the plan file
atomically, and returns; a failure writes ``<plan>.failed`` instead so a
waiting loader fails closed.  While planning, the parent rewrites
``<plan>.progress`` whenever the workers made progress, so a waiting loader
times out only when planning stalls.

**Ordering.**  Chunks of one unit form one chain ordered by
``(task order, row order within the task)``.  A Kafka task's rows are kept
in ``(partition, offset)`` order and its task order is ``(topic index,
first partition)``, where the topic index is the topic's position in
``EdgeConfig.topics``.  Every partition's records keep their offset order
along the chain, and all records of an earlier-listed topic precede those
of a later-listed one -- repeated rows of one ``(source, target)`` pair
(which a source-keyed producer puts in one partition of one topic) apply
``SET r += props`` in this order and the last one wins, exactly like the
per-type loader.  ``SET r += props`` *merges* properties: a later row
overwrites only the properties it carries, it does not replace the whole
relationship.  For a multi-topic type with ``topic_conflict: precedence``
the later-listed topic therefore wins; under ``arrival`` the same
deterministic order applies, but it is documented as arbitrary.  The chain
of a unit is the same for any worker count.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import json
import logging
import math
import multiprocessing
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import traceback
from typing import Callable, Iterable, Mapping, Sequence
import uuid
import zlib

import numpy as np

from src.loader.edge_plan import (
    DEFAULT_CHUNK_ROWS, DEFAULT_COST_FIXED_SECONDS, DEFAULT_COST_ROW_SECONDS, DEFAULT_HUB_DEGREE,
    DEFAULT_MAX_CHUNK_BYTES, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_PLAN_BUCKETS, MAX_CHUNK_ROWS, MAX_PLAN_BUCKETS,
    ChunkSpec, EdgePlan, EdgePlanError, ResourceTable, assemble_plan, decode_token, encode_chunk_message_parts,
    key_token, plan_failure_marker, plan_progress_path, resource_pair, write_plan_file,
)
from src.loader.edge_spill import (
    WINDOW_ENTRY_BYTES, GroupStore, KeyWindows, SpillFile, contiguous_body, counter_bits_for, gather_body,
    list_body, record_offsets, route, unit_chunks, window_slices,
)
from src.models.schema import EdgeConfig
from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)

DEFAULT_PLAN_WORKERS = 4
DEFAULT_PLAN_PARTITIONS = 4
DEFAULT_READ_TIMEOUT_SECONDS = 120.0
_CONSUME_BATCH = 10_000
_ORDER_BITS = 32
# A Kafka task's order is ``(topic index << _PARTITION_BITS) | first partition``.
_PARTITION_BITS = 20
DEFAULT_PLAN_MEMORY_MB = 1024
MIN_AUTO_PLAN_MEMORY_MB, MAX_AUTO_PLAN_MEMORY_MB = 256, 4096
# Shares of a worker's memory budget: open key windows, buffered rows (scan),
# one cell loaded for chunking (emit), the Kafka consumer's prefetch queue.
# The producer's queue gets a fifth of the budget (``queue.buffering.max.kbytes``).
_WINDOW_SHARE, _ROWS_SHARE, _EMIT_SHARE, _CONSUMER_SHARE = 0.15, 0.35, 0.40, 0.10
# Memory of one buffered row beyond its record bytes: the bytes object header,
# its list slot and the two endpoint ids.
_ROW_OVERHEAD = 49


def available_memory_bytes() -> int | None:
    """``MemAvailable`` (Linux), else free physical pages, else ``None``."""
    try:
        with open("/proc/meminfo", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        return os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None


def default_plan_memory_mb(workers: int, available: int | None = None) -> int:
    """Per-worker budget: a quarter of the available RAM over ``workers``, 256 MiB - 4 GiB."""
    available = available_memory_bytes() if available is None else available
    if not available:
        return DEFAULT_PLAN_MEMORY_MB
    share = available // 4 // max(1, workers) >> 20
    return int(max(MIN_AUTO_PLAN_MEMORY_MB, min(MAX_AUTO_PLAN_MEMORY_MB, share)))

# One chunk per record batch: a writer's fetch at a chunk's offset then
# returns just that chunk, never neighbouring chunks it does not need.
PLAN_PRODUCER_CONFIG: dict[str, object] = {
    "batch.size": 1,
    "linger.ms": 5,
    "compression.type": "lz4",
    "enable.idempotence": True,
    "queue.buffering.max.kbytes": 1_048_576,
    "queue.buffering.max.messages": 100_000,
}

# Sequential high-throughput reads of the relationship topics.
PLAN_CONSUMER_CONFIG: dict[str, object] = {
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "enable.partition.eof": True,
    "auto.offset.reset": "error",
    "fetch.max.bytes": 64 << 20,
    "max.partition.fetch.bytes": 16 << 20,
    "receive.message.max.bytes": (64 << 20) + (1 << 20),
    "queued.max.messages.kbytes": 131_072,
    "fetch.wait.max.ms": 100,
}


class EdgePlanInputError(EdgePlanError):
    """A relationship row cannot be planned (invalid record, missing input)."""


@dataclass(frozen=True)
class PlanSettings:
    """Planner knobs; ``topic`` is the chunk topic every chunk message goes to."""

    buckets: int = DEFAULT_PLAN_BUCKETS
    hub_degree: int = DEFAULT_HUB_DEGREE
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES
    topic: str = ""
    partitions: int = DEFAULT_PLAN_PARTITIONS
    workers: int = DEFAULT_PLAN_WORKERS
    c0: float = DEFAULT_COST_FIXED_SECONDS
    c1: float = DEFAULT_COST_ROW_SECONDS
    # Memory budget per planner process in MiB (0: derived from the available RAM).
    memory_mb: int = 0
    # Directory for the planner's spill files ("": beside the plan file, else the temp directory).
    spill_dir: str = ""

    def validate(self) -> "PlanSettings":
        for name in ("buckets", "hub_degree", "chunk_rows", "max_chunk_bytes", "max_message_bytes",
                     "partitions", "workers", "memory_mb"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"plan {name.replace('_', ' ')} must be an integer")
        if not 1 <= self.buckets <= MAX_PLAN_BUCKETS:
            raise ValueError(f"plan buckets must be 1-{MAX_PLAN_BUCKETS}")
        if self.hub_degree < 0:
            raise ValueError("plan hub degree must be nonnegative (0 disables hubs)")
        if not 1 <= self.chunk_rows <= MAX_CHUNK_ROWS:
            raise ValueError(f"plan chunk rows must be 1-{MAX_CHUNK_ROWS}")
        if not 1024 <= self.max_chunk_bytes <= self.max_message_bytes - 4096:
            raise ValueError("plan chunk bytes must be at least 1 KiB and below the message limit")
        if self.partitions < 1 or self.workers < 1:
            raise ValueError("plan partitions and workers must be positive")
        if not isinstance(self.topic, str) or not self.topic.strip():
            raise ValueError("plan topic must be nonblank")
        if self.memory_mb < 0:
            raise ValueError("plan memory must be nonnegative MiB (0 derives it from the available RAM)")
        if not isinstance(self.spill_dir, str):
            raise ValueError("plan spill directory must be a string")
        return self

    def as_plan_settings(self, workers: int) -> dict:
        return {
            "buckets": self.buckets, "hub_degree": self.hub_degree, "chunk_rows": self.chunk_rows,
            "max_chunk_bytes": self.max_chunk_bytes, "max_message_bytes": self.max_message_bytes,
            "c0": self.c0, "c1": self.c1, "planner_workers": workers,
        }


# -- record sources ---------------------------------------------------------------


class EdgeRecordSource:
    """One ordered stream of relationship rows of one type, supplied by a producer.

    Subclasses must be picklable (they are sent to planner processes) and
    implement :meth:`records`.  Each yielded item is one row, either

    * a canonical envelope -- a mapping or its JSON text (``str``/``bytes``)
      ``{"source": {key: value}, "target": {key: value}, "properties": {...}}``
      in the Kafka wire format; it is fully validated now; or
    * a *prepared* row ``(source_key, target_key, record_json)``: wire-format
      endpoint keys and the envelope's JSON text.  Only the keys are
      validated while planning (the JSON is not parsed); planned writers
      validate every record before writing, so an invalid record still
      fails the load closed, just later.

    ``order`` orders the sources of one edge type: a unit's rows follow
    ``(order, position in the source)``.  Rows of one ``(source, target)``
    pair that must keep their relative order (last write wins) must come
    from one source.  ``size_hint`` balances sources over processes.
    """

    # Subclasses provide ``edge_type: str`` and optionally ``order: int`` and
    # ``size_hint: int`` (both default 0) and ``topic: str`` (v7-S7: required for a type whose
    # topics bind several label pairs; the source's rows are planned with that topic's pair).

    def records(self, edge: EdgeConfig) -> Iterable[object]:  # pragma: no cover - interface
        raise NotImplementedError

    def describe(self) -> str:
        return f"{self.edge_type} source {getattr(self, 'order', 0)}"


@dataclass(frozen=True)
class IterableEdgeSource(EdgeRecordSource):
    """An in-memory list (or other iterable) of rows for one edge type."""

    edge_type: str
    rows: Sequence[object] = ()
    order: int = 0
    size_hint: int = 0
    topic: str | None = None

    def records(self, edge: EdgeConfig) -> Iterable[object]:
        return self.rows


# -- planner tasks --------------------------------------------------------------------


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON numeric constant {value}")


_JSON = json.JSONDecoder(parse_constant=_reject_json_constant)


@dataclass(frozen=True)
class KafkaReadTask:
    """Partitions ``ranges`` = ``((partition, start, end), ...)`` of one relationship topic."""

    edge: EdgeConfig
    topic: str
    ranges: tuple[tuple[int, int, int], ...]
    bootstrap_servers: str
    group_id: str
    read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS
    # Position of ``topic`` in ``edge.topics`` (later-listed topics apply later along the chain).
    topic_index: int = 0
    # v7-S7: the label pair ``topic`` binds. ``edge`` is then that pair's view (the parent type
    # is multi-pair); always 0 for a single-pair type.
    pair: int = 0

    @property
    def edge_type(self) -> str:
        return self.edge.type

    @property
    def order(self) -> int:
        first = min((partition for partition, _, _ in self.ranges), default=0)
        if not 0 <= first < 1 << _PARTITION_BITS or not 0 <= self.topic_index < 1 << (_ORDER_BITS - _PARTITION_BITS):
            raise EdgePlanError(
                f"{self.name}: topic index {self.topic_index} / partition {first} exceed the planner's order range"
            )
        return (self.topic_index << _PARTITION_BITS) | first

    @property
    def size_hint(self) -> int:
        return sum(end - start for _, start, end in self.ranges)

    @property
    def multipart(self) -> bool:
        return sum(1 for _, start, end in self.ranges if end > start) > 1

    @property
    def name(self) -> str:
        parts = ",".join(str(partition) for partition, _, _ in self.ranges)
        return f"{self.edge_type}@{self.topic}[{parts}]"

    def scan(self, add, consumer_factory=None, clock: Callable[[], float] = time.monotonic, *,
             progress: Callable[[int], None] | None = None) -> int:
        """Read every range, validate and ``add`` each record; return the row count."""
        return scan_kafka_tasks([(self, add)], consumer_factory, clock, progress=progress)[0]


def scan_kafka_tasks(tasks_and_adders: Sequence[tuple["KafkaReadTask", Callable]], consumer_factory=None,
                     clock: Callable[[], float] = time.monotonic, *, consumer_config: Mapping | None = None,
                     progress: Callable[[int], None] | None = None,
                     on_reject: Callable[..., None] | None = None) -> list[int]:
    """Read several tasks' ranges with ONE consumer; return each task's row count.

    With ``on_reject`` (called with ``edge_type``, ``topic``, ``partition``, ``offset``, ``key``,
    ``value`` and ``reason``), a malformed record -- the set the classic loaders reject to the
    rejection log: undecodable, bad JSON, or failing the edge normalization (all ``ValueError``)
    -- is handed to it and skipped instead of failing the plan. Without it every invalid record
    fails the plan. A rejected record never reaches ``add``, so the plan is exactly the plan of
    the valid records, whatever the worker count.

    One consumer per worker (instead of one per task) pays the connection
    and first-fetch latency once and fetches all partitions concurrently.
    Records are validated with the edge loaders' normalization and handed to
    their task's ``add(source_key, target_key, value, partition)``.
    """
    from src.loader.planned_writer import compile_record_normalizer

    if consumer_factory is None:
        from confluent_kafka import Consumer as consumer_factory
    from confluent_kafka import KafkaError, TopicPartition

    rows = [0] * len(tasks_and_adders)
    # (topic, partition) -> [task index, end offset, normalize, add, edge type]
    handlers: dict[tuple[str, int], list] = {}
    assignment = []
    for index, (task, add) in enumerate(tasks_and_adders):
        normalize = compile_record_normalizer(task.edge)
        for partition, start, end in task.ranges:
            if end <= start:
                continue
            key = (task.topic, partition)
            if key in handlers:
                raise EdgePlanError(f"{task.topic} partition {partition} is assigned to two planner tasks")
            handlers[key] = [index, end, normalize, add, task.edge_type]
            assignment.append(TopicPartition(task.topic, partition, start))
    if not handlers:
        return rows
    first = tasks_and_adders[0][0]
    read_timeout = max(task.read_timeout_seconds for task, _ in tasks_and_adders)
    consumer = consumer_factory({
        **PLAN_CONSUMER_CONFIG, **(consumer_config or {}),
        "bootstrap.servers": first.bootstrap_servers, "group.id": first.group_id,
    })
    decode = _JSON.decode
    eof_code = KafkaError._PARTITION_EOF
    try:
        consumer.assign(assignment)

        def finish(key: tuple[str, int]) -> None:
            del handlers[key]
            consumer.pause([TopicPartition(key[0], key[1])])

        last_progress = clock()
        while handlers:
            messages = consumer.consume(num_messages=_CONSUME_BATCH, timeout=0.5)
            if not messages:
                if clock() - last_progress > read_timeout:
                    raise EdgePlanInputError(
                        f"no relationship records within {read_timeout:.0f}s; unfinished partitions "
                        + ", ".join(f"{topic}[{partition}] up to offset {handler[1]}"
                                    for (topic, partition), handler in sorted(handlers.items()))
                    )
                continue
            last_progress = clock()
            for message in messages:
                key = (message.topic(), message.partition())
                error = message.error()
                if error is not None:
                    if error.code() == eof_code:
                        # Everything before the partition's current end (the EOF
                        # offset) was delivered; control records and compacted
                        # offsets below the boundary never arrive.
                        handler = handlers.get(key)
                        eof = message.offset()
                        if handler is not None and (eof is None or eof < 0 or eof >= handler[1]):
                            finish(key)
                        continue
                    raise EdgePlanInputError(f"topic {key[0]} partition {key[1]}: Kafka error {error}")
                handler = handlers.get(key)
                if handler is None:
                    continue
                index, end, normalize, add, edge_type = handler
                offset = message.offset()
                if offset >= end:
                    finish(key)
                    continue
                value = message.value()
                try:
                    if value is None:
                        raise ValueError("message payload is empty")
                    source_key, target_key, _ = normalize(decode(value.decode("utf-8")))
                except Exception as exc:
                    if on_reject is None or not isinstance(exc, ValueError):
                        raise EdgePlanInputError(
                            f"invalid {edge_type} record at {key[0]} partition {key[1]} offset {offset}: "
                            f"{type(exc).__name__}: {exc}"
                        ) from None
                    on_reject(edge_type=edge_type, topic=key[0], partition=key[1], offset=offset,
                              key=message.key(), value=value, reason=str(exc))
                    # The boundary check still applies to a rejected last record.
                    if offset >= end - 1:
                        finish(key)
                    continue
                add(source_key, target_key, value, key[1])
                rows[index] += 1
                if offset >= end - 1:
                    finish(key)
            if progress is not None:
                progress(sum(rows))
    finally:
        consumer.close()
    return rows


@dataclass(frozen=True)
class SourceTask:
    """A producer-supplied :class:`EdgeRecordSource` bound to its schema edge."""

    edge: EdgeConfig
    source: EdgeRecordSource
    pair: int = 0  # v7-S7: the label pair of the source's topic; ``edge`` is that pair's view
    multipart = False

    @property
    def edge_type(self) -> str:
        return self.edge.type

    @property
    def order(self) -> int:
        return getattr(self.source, "order", 0)

    @property
    def size_hint(self) -> int:
        return getattr(self.source, "size_hint", 0)

    @property
    def name(self) -> str:
        # v7-S7: sources of different label pairs may share an order (they never share a unit).
        return f"{self.edge_type}#{self.order}" if not self.pair else f"{self.edge_type}#{self.order}.p{self.pair}"

    def scan(self, add, consumer_factory=None, clock=None, *, progress: Callable[[int], None] | None = None) -> int:
        from src.loader.planned_writer import _FAIL, _fast_value, compile_record_normalizer

        edge = self.edge
        normalize = compile_record_normalizer(edge)
        source_value = _fast_value(edge.source_key_config.type)
        target_value = _fast_value(edge.target_key_config.type)
        decode, dumps = _JSON.decode, json.dumps
        rows = 0
        for rows, item in enumerate(self.source.records(edge), start=1):
            try:
                if type(item) is tuple:
                    if len(item) != 3:
                        raise ValueError("prepared rows must be (source_key, target_key, record_json)")
                    source_key, target_key, record = item
                    source_key, target_key = source_value(source_key), target_value(target_key)
                    if source_key is _FAIL or target_key is _FAIL or source_key is None or target_key is None:
                        raise ValueError("prepared row endpoint keys do not match the schema key types")
                    if type(record) is not str and type(record) is not bytes:
                        raise ValueError("prepared row record must be JSON text")
                else:
                    if type(item) is bytes:
                        record, raw = item, decode(item.decode("utf-8"))
                    elif type(item) is str:
                        record, raw = item, decode(item)
                    else:
                        raw, record = item, dumps(item, allow_nan=False, separators=(",", ":"))
                    source_key, target_key, _ = normalize(raw)
            except Exception as exc:
                raise EdgePlanInputError(
                    f"invalid {edge.type} row {rows} of {self.source.describe()}: {type(exc).__name__}: {exc}"
                ) from None
            add(source_key, target_key, record, 0)
            if progress is not None and not rows & 0x3FFF:
                progress(rows)
        return rows


# -- worker side ------------------------------------------------------------------------


@dataclass
class _ScannedTask:
    name: str
    edge_type: str
    source_label: str
    target_label: str
    order: int
    multipart: bool
    rows: int
    groups: dict
    seconds: float
    pair: int = 0  # v7-S7


def _peak_rss_mb() -> float:
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except (ImportError, OSError):  # pragma: no cover - non-Unix
        return 0.0


class PlannerWorkerState:
    """Everything one planner worker keeps between rounds, within ``memory_bytes``.

    Rows are grouped by ``(source bucket, target bucket)`` cell per task in
    column buffers (:class:`~src.loader.edge_spill.GroupStore`) and endpoint
    keys are interned in bounded windows (:class:`~src.loader.edge_spill.KeyWindows`);
    both spill to files in ``spill_dir`` when they outgrow their share of the
    budget, so a worker's memory does not grow with its input.
    """

    def __init__(self, buckets: int, *, hub_degree: int = DEFAULT_HUB_DEGREE,
                 memory_bytes: int = DEFAULT_PLAN_MEMORY_MB << 20, spill_dir: str | Path | None = None,
                 worker_index: int = 0, consumer_factory=None, progress=None,
                 rejection_path: str | Path | None = None) -> None:
        self.buckets = buckets
        # Malformed Kafka records go to this JSONL file (same entries as the classic loaders'
        # rejection log) and are skipped; ``None`` keeps the scan fail-closed.
        self.rejection_path = None if rejection_path is None else Path(rejection_path)
        self.rejected = 0
        self._rejection_sink = None
        self.hub_degree = hub_degree
        self.memory_bytes = max(1, int(memory_bytes))
        self.worker_index = worker_index
        self.consumer_factory = consumer_factory
        self.progress = progress if progress is not None else [0, 0, 0]
        self._own_spill_dir = spill_dir is None
        self.spill_dir = Path(spill_dir) if spill_dir is not None else Path(
            tempfile.mkdtemp(prefix="graph-loader-plan-spill-"))
        prefix = self.spill_dir / f"worker-{worker_index}"
        budget = self.memory_bytes
        self.windows = KeyWindows(
            cap=max(256, int(budget * _WINDOW_SHARE) // WINDOW_ENTRY_BYTES),
            counter_bits=counter_bits_for(budget), spill=SpillFile(f"{prefix}-keys.bin"),
        )
        self.store = GroupStore(SpillFile(f"{prefix}-rows.bin"), int(budget * _ROWS_SHARE))
        self.emit_bytes = max(1, int(budget * _EMIT_SHARE))
        self.consumer_config = {
            "queued.max.messages.kbytes": max(16_384, min(131_072, int(budget * _CONSUMER_SHARE) >> 10)),
            "fetch.max.bytes": max(8 << 20, min(64 << 20, int(budget * _CONSUMER_SHARE) // 2)),
        }
        self.scanned: list[_ScannedTask] = []
        self.rows = 0
        self._bucket_cache: dict[str, dict] = {}
        self.streamed_cells = 0
        # Buffered bytes are accounted in steps (one check per ~step bytes of rows).
        self._step = max(1, min(1 << 20, self.store.flush_bytes // 16))
        self.windows.on_seal = self._mark_groups

    # -- scan ---------------------------------------------------------------------

    def _reject(self, *, edge_type: str, topic: str, partition: int, offset: int, key, value, reason: str) -> None:
        """Durably log one malformed record (fsync'd JSONL entry like the classic rejection log)."""
        try:
            if self._rejection_sink is None:
                from src.loader.dead_letter import RejectionSink
                self._rejection_sink = RejectionSink(self.rejection_path)
            self._rejection_sink.route(topic=topic, partition=partition, offset=offset, key=key, value=value,
                                       reason=reason, edge_type=edge_type)
        except OSError as exc:
            raise EdgePlanInputError(
                f"cannot write the planner rejection file {self.rejection_path} for the invalid {edge_type} record "
                f"at {topic} partition {partition} offset {offset}: {type(exc).__name__}: {exc}"
            ) from None
        self.rejected += 1
        if self.rejected <= 5 or self.rejected % 1000 == 0:
            logger.warning(
                "Malformed relationship record rejected by the planner edge=%s topic=%s partition=%s offset=%s "
                "reason=%s rejected_so_far=%s file=%s", edge_type, topic, partition, offset, reason,
                self.rejected, self.rejection_path,
            )

    def _mark_groups(self, window: int) -> None:
        for group in self.store.groups:
            group.mark(window)

    def _account(self, amount: int) -> None:
        """Every ~``_step`` buffered bytes: flush the row buffers or seal the key window when full."""
        store = self.store
        store.buffered += amount
        if store.buffered >= store.limit:
            store.flush()
        windows = self.windows
        if self.hub_degree > 0 and windows.entries >= windows.cap:
            windows.seal()

    def _adder(self, source_label: str, target_label: str, groups: dict, multipart: bool):
        """Return ``add(source key, target key, record, partition)`` for one task (the hot path)."""
        if self.hub_degree <= 0:
            return self._bucket_adder(source_label, target_label, groups, multipart)
        windows, store = self.windows, self.store
        source, target = windows.label(source_label), windows.label(target_label)
        bucket_count, step, account = self.buckets, self._step, self._account
        crc32, token = zlib.crc32, key_token
        cells: list = [None] * (bucket_count * bucket_count)
        overhead = _ROW_OVERHEAD + (4 if multipart else 0)
        source_ids, source_tokens, source_crcs = source.ids, source.tokens, source.crcs
        source_buckets, source_counts = source.buckets, source.counts
        target_ids, target_tokens, target_crcs = target.ids, target.tokens, target.crcs
        target_buckets, target_counts = target.buckets, target.counts
        pending = 0

        def intern(ids, tokens, crcs, buckets, counts, key) -> int:
            value = token(key)
            crc = crc32(value)
            local = ids[key] = len(tokens)
            tokens.append(value)
            crcs.append(crc)
            buckets.append(crc % bucket_count)
            counts.append(1)
            windows.entries += 1
            return local

        def add(source_key: object, target_key: object, record, part: int) -> None:
            nonlocal pending
            source_id = source_ids.get(source_key)
            if source_id is None:
                source_id = intern(source_ids, source_tokens, source_crcs, source_buckets, source_counts, source_key)
            else:
                source_counts[source_id] += 1
            target_id = target_ids.get(target_key)
            if target_id is None:
                target_id = intern(target_ids, target_tokens, target_crcs, target_buckets, target_counts, target_key)
            else:
                target_counts[target_id] += 1
            cell = source_buckets[source_id] * bucket_count + target_buckets[target_id]
            group = cells[cell]
            if group is None:
                group = groups.get(cell)
                if group is None:
                    group = groups[cell] = store.new_group(True, multipart)
                cells[cell] = group
            if type(record) is str:
                record = record.encode("utf-8")
            group.recs.append(record)
            lids = group.lids
            lids.append(source_id)
            lids.append(target_id)
            if multipart:
                group.parts.append(part)
            pending += len(record) + overhead
            if pending >= step:
                account(pending)
                pending = 0

        return add

    def _bucket_adder(self, source_label: str, target_label: str, groups: dict, multipart: bool):
        """Hubs disabled: only each key's bucket is needed (bounded cache, no degrees)."""
        store = self.store
        bucket_count, step, account = self.buckets, self._step, self._account
        cap = max(256, int(self.memory_bytes * _WINDOW_SHARE) // WINDOW_ENTRY_BYTES)
        crc32, token = zlib.crc32, key_token
        source_cache = self._bucket_cache.setdefault(source_label, {})
        target_cache = self._bucket_cache.setdefault(target_label, {})
        cells: list = [None] * (bucket_count * bucket_count)
        overhead = _ROW_OVERHEAD - 8 + (4 if multipart else 0)
        pending = 0

        def add(source_key: object, target_key: object, record, part: int) -> None:
            nonlocal pending
            source_bucket = source_cache.get(source_key)
            if source_bucket is None:
                if len(source_cache) >= cap:
                    source_cache.clear()
                source_bucket = source_cache[source_key] = crc32(token(source_key)) % bucket_count
            target_bucket = target_cache.get(target_key)
            if target_bucket is None:
                if len(target_cache) >= cap:
                    target_cache.clear()
                target_bucket = target_cache[target_key] = crc32(token(target_key)) % bucket_count
            cell = source_bucket * bucket_count + target_bucket
            group = cells[cell]
            if group is None:
                group = groups.get(cell)
                if group is None:
                    group = groups[cell] = store.new_group(False, multipart)
                cells[cell] = group
            if type(record) is str:
                record = record.encode("utf-8")
            group.recs.append(record)
            if multipart:
                group.parts.append(part)
            pending += len(record) + overhead
            if pending >= step:
                account(pending)
                pending = 0

        return add

    def _record(self, task, groups: dict, rows: int, seconds: float) -> tuple[str, str, int, float]:
        edge = task.edge
        self.scanned.append(_ScannedTask(
            task.name, edge.type, edge.nodes.source, edge.nodes.target, task.order, task.multipart, rows,
            groups, seconds, getattr(task, "pair", 0),
        ))
        self.rows += rows
        return task.name, edge.type, rows, seconds

    def _report_rows(self, rows: int) -> None:
        self.progress[0] = self.rows + rows

    def scan(self, task) -> tuple[str, str, int, float]:
        """Validate one task's rows and append them to their cells' groups."""
        started = time.monotonic()
        edge = task.edge
        groups: dict = {}
        rows = task.scan(self._adder(edge.nodes.source, edge.nodes.target, groups, task.multipart),
                         self.consumer_factory, progress=self._report_rows)
        return self._record(task, groups, rows, time.monotonic() - started)

    def scan_lane(self, tasks) -> dict:
        """Scan a lane: every Kafka task through one consumer, then the other tasks."""
        started = time.monotonic()
        kafka = [task for task in tasks if isinstance(task, KafkaReadTask)]
        results = []
        if kafka:
            groups = [{} for _ in kafka]
            counts = scan_kafka_tasks(
                [(task, self._adder(task.edge.nodes.source, task.edge.nodes.target, groups[index], task.multipart))
                 for index, task in enumerate(kafka)],
                self.consumer_factory, consumer_config=self.consumer_config, progress=self._report_rows,
                on_reject=self._reject if self.rejection_path is not None else None,
            )
            seconds = time.monotonic() - started
            results.extend(self._record(task, groups[index], counts[index], seconds)
                           for index, task in enumerate(kafka))
        results.extend(self.scan(task) for task in tasks if not isinstance(task, KafkaReadTask))
        if self.rows >= 1 << 31:
            raise EdgePlanError(f"one planner worker read {self.rows} rows; use more planner workers")
        if self.hub_degree > 0:
            self.windows.seal(final=True)
        self._bucket_cache.clear()
        self.progress[0] = self.rows
        return {"tasks": results, "seconds": time.monotonic() - started, "rejected": self.rejected}

    # -- hubs -----------------------------------------------------------------------

    def degree_counter(self) -> np.ndarray | None:
        """This worker's slot counters (an upper bound of every key's local degree)."""
        return self.windows.counter if self.hub_degree > 0 else None

    def candidate_degrees(self, candidates: bytes) -> dict[str, dict[bytes, int]]:
        """Exact local degrees, by key token, of every key in a candidate counter slot."""
        size = len(self.windows.counter)
        flags = np.unpackbits(np.frombuffer(candidates, dtype=np.uint8), count=size).astype(bool)

        def progress() -> None:
            self.progress[2] += 1
        return self.windows.candidate_tokens(flags, progress)

    # -- emit -----------------------------------------------------------------------

    def emit(self, table: ResourceTable, *, producer_factory, **options) -> tuple[list[ChunkSpec], dict, dict]:
        """Chunk every unit, produce one message per chunk, return the delivered specs."""
        producer = producer_factory()
        try:
            return self._emit(table, producer, **options)
        except BaseException:
            _abandon(producer)
            raise

    def _emit(self, table: ResourceTable, producer, *, run: str, topic: str, partitions: int, chunk_rows: int,
              max_chunk_bytes: int, worker_index: int) -> tuple[list[ChunkSpec], dict, dict]:
        hub_tokens: dict[str, dict[bytes, int]] = {}
        hub_buckets: dict[str, set[int]] = {}
        for resource, (label, kind, value) in enumerate(table.names):
            if kind == "hub":
                token = key_token(value)
                hub_tokens.setdefault(label, {})[token] = resource
                hub_buckets.setdefault(label, set()).add(zlib.crc32(token) % self.buckets)
        maps = self.windows.hub_maps(hub_tokens) if self.hub_degree > 0 and hub_tokens else {}
        delivered: dict[int, tuple[int, int]] = {}
        failures: list[str] = []

        def report(error, message, index):
            if error is not None:
                failures.append(str(error))
            else:
                delivered[index] = (message.partition(), message.offset())

        pending: list[ChunkSpec] = []
        timings: dict[str, float] = {}
        next_partition = worker_index % partitions
        resource_count = table.count
        for scanned in self.scanned:
            started = time.monotonic()
            edge_type, order = scanned.edge_type, scanned.order
            if not 0 <= order < 1 << _ORDER_BITS:
                raise EdgePlanError(f"task order of {scanned.name} must be 0..2^32-1")
            source_base = table.bucket_resource(scanned.source_label, 0)
            target_base = table.bucket_resource(scanned.target_label, 0)
            source_hubs = hub_buckets.get(scanned.source_label, set())
            target_hubs = hub_buckets.get(scanned.target_label, set())
            for cell in sorted(scanned.groups):
                group = scanned.groups[cell]
                source_bucket, target_bucket = divmod(cell, self.buckets)
                sides = (
                    (scanned.source_label, source_base + source_bucket, source_bucket in source_hubs),
                    (scanned.target_label, target_base + target_bucket, target_bucket in target_hubs),
                )
                for (source, target), rows, pieces in self._units(group, sides, maps, resource_count):
                    unit = (edge_type, source, target)
                    resources = resource_pair(source, target)
                    for seq, (count, buffers) in enumerate(unit_chunks(rows, pieces, chunk_rows, max_chunk_bytes)):
                        key = f"{edge_type}:{source}:{target}:{order}:{seq}"
                        value = encode_chunk_message_parts(run, key, edge_type, count, buffers)
                        index = len(pending)
                        pending.append(ChunkSpec(
                            key, edge_type, unit, (order << _ORDER_BITS) | seq, resources,
                            count, next_partition, -1, len(value), scanned.pair,
                        ))
                        _produce(producer, topic, value, next_partition,
                                 lambda error, message, index=index: report(error, message, index))
                        next_partition = (next_partition + 1) % partitions
                        self.progress[1] += 1
                group.release()
            scanned.groups.clear()
            timings[scanned.name] = scanned.seconds + time.monotonic() - started
        outstanding = _flush(producer)
        if outstanding:
            raise RuntimeError(f"Kafka did not deliver {outstanding} planned chunk message(s)")
        if failures:
            raise RuntimeError(f"Kafka failed to deliver {len(failures)} planned chunk(s): {failures[0]}")
        if len(delivered) != len(pending):
            raise RuntimeError("planned chunk delivery reports are incomplete")
        specs = []
        for index, spec in enumerate(pending):
            partition, offset = delivered[index]
            specs.append(ChunkSpec(spec.key, spec.edge_type, spec.unit, spec.seq, spec.resources, spec.rows,
                                   partition, offset, spec.bytes, spec.pair))
        stats = {
            "spilled_bytes": self.store.spilled_bytes + self.windows.spill.size,
            "row_flushes": self.store.flushes, "key_windows": len(self.windows.windows),
            "keys_interned": self.windows.total_entries, "streamed_cells": self.streamed_cells,
            "peak_rss_mb": _peak_rss_mb(),
        }
        self.close_files()
        return specs, timings, stats

    def _units(self, group, sides, maps: dict, resource_count: int):
        """Yield ``((source, target), rows, pieces)`` per unit of one group, in unit order."""
        if GroupStore.total_bytes(group) + 48 * group.rows <= self.emit_bytes:
            yield from self._units_in_memory(group, sides, maps, resource_count)
        else:
            yield from self._units_streamed(group, sides, maps, resource_count)

    @staticmethod
    def _unit_keys(lids, slices, sides, maps, rows: int, resource_count: int) -> np.ndarray | None:
        (source_label, source_default, source_hub), (target_label, target_default, target_hub) = sides
        if not (source_hub or target_hub) or not maps:
            return None
        sources = route(lids[:, 0] if source_hub else None, slices, source_label, source_default, maps, rows)
        targets = route(lids[:, 1] if target_hub else None, slices, target_label, target_default, maps, rows)
        return sources * resource_count + targets

    def _units_in_memory(self, group, sides, maps: dict, resource_count: int):
        blen, lids, parts, records = self.store.load(group)
        group.release()  # the loaded rows replace the buffers
        rows = len(blen)
        if not rows:
            return
        if isinstance(records, list):  # never spilled: join the reader's record objects directly
            def gather(selected):
                return list_body(records, selected)
            whole = list_body(records, None)
        else:
            offsets = record_offsets(blen)
            view = memoryview(records)

            def gather(selected):
                return gather_body(view, offsets, selected)
            whole = contiguous_body(view, offsets)
        keys = self._unit_keys(lids, window_slices(group.marks, 0, rows), sides, maps, rows, resource_count)
        order = np.argsort(parts, kind="stable") if parts is not None else None
        default = (sides[0][1], sides[1][1])
        if keys is None or bool(np.all(keys == keys[0])):
            unit = default if keys is None else divmod(int(keys[0]), resource_count)
            if order is None:
                yield unit, rows, [(blen, whole)]
            else:
                yield unit, rows, [(blen[order], gather(order))]
            return
        sequence = keys if order is None else keys[order]
        positions = order if order is not None else np.arange(rows, dtype=np.int64)
        by_unit = np.argsort(sequence, kind="stable")
        sorted_keys = sequence[by_unit]
        members = positions[by_unit]
        starts = np.concatenate(([0], np.flatnonzero(np.diff(sorted_keys)) + 1))
        ends = np.concatenate((starts[1:], [rows]))
        for start, end in zip(starts.tolist(), ends.tolist()):
            unit_rows = members[start:end]
            yield divmod(int(sorted_keys[start]), resource_count), end - start, [
                (blen[unit_rows], gather(unit_rows)),
            ]

    def _units_streamed(self, group, sides, maps: dict, resource_count: int):
        """A group larger than the emit budget: split it by unit on disk, then stream each unit."""
        self.streamed_cells += 1
        units = GroupStore(SpillFile(self.spill_dir / f"worker-{self.worker_index}-units.bin"),
                           self.store.flush_bytes)
        unit_groups: dict[tuple[int, int], object] = {}
        default_key = sides[0][1] * resource_count + sides[1][1]
        try:
            for first, blen, lids, parts, records in self.store.segments(group):
                rows = len(blen)
                slices = window_slices(group.marks, first, first + rows)
                keys = self._unit_keys(lids, slices, sides, maps, rows, resource_count)
                if keys is None:
                    keys = np.full(rows, default_key, dtype=np.int64)
                if parts is None:
                    parts = np.zeros(rows, dtype=np.uint32)
                ordering = np.lexsort((parts, keys))
                sorted_keys, sorted_parts = keys[ordering], parts[ordering]
                change = np.flatnonzero((np.diff(sorted_keys) != 0) | (np.diff(sorted_parts) != 0)) + 1
                starts = np.concatenate(([0], change))
                ends = np.concatenate((starts[1:], [rows]))
                offsets = record_offsets(blen)
                view = memoryview(records)
                for start, end in zip(starts.tolist(), ends.tolist()):
                    selected = ordering[start:end]
                    # Row order is preserved: ``lexsort`` is stable within one (unit, partition).
                    name = (int(sorted_keys[start]), int(sorted_parts[start]))
                    target = unit_groups.get(name)
                    if target is None:
                        target = unit_groups[name] = units.new_group(False, False)
                    target.recs.extend(bytes(view[begin:stop - 1]) for begin, stop in zip(
                        offsets[selected].tolist(), offsets[selected + 1].tolist()))
                    units.buffered += int(blen[selected].sum()) + (_ROW_OVERHEAD - 8) * (end - start)
                    if units.buffered >= units.flush_bytes:
                        units.flush()
            by_key: dict[int, list[int]] = {}
            for key, part in sorted(unit_groups):
                by_key.setdefault(key, []).append(part)
            for key, unit_parts in sorted(by_key.items()):
                members = [unit_groups[(key, part)] for part in unit_parts]

                def pieces(members=members):
                    for member in members:
                        for _first, blen, _lids, _parts, records in units.segments(member):
                            yield blen, contiguous_body(memoryview(records), record_offsets(blen))
                yield divmod(key, resource_count), sum(member.rows for member in members), pieces()
        finally:
            units.remove()

    def close_files(self) -> None:
        self.windows.spill.close()
        self.store.spill.close()

    def close(self) -> None:
        self.windows.spill.remove()
        self.store.remove()
        if self._own_spill_dir:
            shutil.rmtree(self.spill_dir, ignore_errors=True)


def _produce(producer, topic: str, value: bytes, partition: int, callback) -> None:
    while True:
        try:
            producer.produce(topic, value=value, partition=partition, on_delivery=callback)
            break
        except BufferError:
            producer.poll(0.05)
    producer.poll(0)


def _flush(producer, stall_seconds: float = 120.0, clock: Callable[[], float] = time.monotonic) -> int:
    """Flush until every message is delivered; give up only after ``stall_seconds`` without progress."""
    outstanding = producer.flush(min(10.0, stall_seconds))
    last, progress_at = outstanding, clock()
    while outstanding:
        if outstanding < last:
            last, progress_at = outstanding, clock()
        elif clock() - progress_at >= stall_seconds:
            break
        outstanding = producer.flush(min(10.0, stall_seconds))
    return outstanding


def _abandon(producer) -> None:
    """Drop undelivered messages so a failed worker exits at once (a dead broker would stall it)."""
    purge = getattr(producer, "purge", None)
    if purge is not None:
        try:
            purge()
            producer.flush(0)
        except Exception:  # pragma: no cover - best effort
            pass


def _run_command(state: PlannerWorkerState, command: str, payload, producer_factory):
    if command == "scan":
        return state.scan_lane(payload)
    if command == "counter":
        return state.degree_counter()
    if command == "degrees":
        return state.candidate_degrees(payload)
    if command == "emit":
        payload = dict(payload)
        table = ResourceTable.from_json(payload.pop("resources"))
        return state.emit(table, producer_factory=producer_factory, **payload)
    raise ValueError(f"unknown planner command {command!r}")


def _default_producer_factory(bootstrap_servers: str, producer_config: dict):
    def factory():
        from confluent_kafka import Producer
        return Producer({**PLAN_PRODUCER_CONFIG, **producer_config, "bootstrap.servers": bootstrap_servers})
    return factory


def _planner_worker_main(connection, worker: dict, bootstrap_servers: str, producer_config: dict,
                         producer_class=None, consumer_factory=None, progress=None) -> None:
    """Serve planner commands from the parent until ``stop`` or EOF."""
    state = PlannerWorkerState(consumer_factory=consumer_factory, progress=progress, **worker)
    if producer_class is None:
        producer_factory = _default_producer_factory(bootstrap_servers, producer_config)
    else:
        def producer_factory():
            return producer_class({**PLAN_PRODUCER_CONFIG, **producer_config, "bootstrap.servers": bootstrap_servers})
    try:
        while True:
            try:
                command, payload = connection.recv()
            except (EOFError, OSError):
                return
            if command == "stop":
                try:
                    connection.send(("ok", None))
                except OSError:
                    pass
                return
            try:
                result = _run_command(state, command, payload, producer_factory)
                connection.send(("ok", result))
            except BaseException as exc:  # report every failure to the parent
                detail = "" if isinstance(exc, EdgePlanInputError) else "\n" + traceback.format_exc()
                connection.send(("error", f"{type(exc).__name__}: {exc}{detail}"))
    finally:
        state.close()


class _ProcessWorker:
    def __init__(self, context, worker: dict, bootstrap_servers: str, producer_config: dict,
                 producer_class, consumer_factory) -> None:
        parent, child = context.Pipe()
        self.progress = context.RawArray("q", 3)
        self.process = context.Process(
            target=_planner_worker_main,
            args=(child, worker, bootstrap_servers, producer_config, producer_class, consumer_factory, self.progress),
            daemon=True,
        )
        self.process.start()
        child.close()
        self.connection = parent
        self._stop_sent = False

    def send(self, command: str, payload) -> None:
        self.connection.send((command, payload))

    def recv(self):
        try:
            return self.connection.recv()
        except EOFError:
            return "error", f"planner process exited unexpectedly (exit code {self.process.exitcode})"

    def request_stop(self) -> None:
        if self._stop_sent:
            return
        self._stop_sent = True
        try:
            self.connection.send(("stop", None))
        except (OSError, ValueError):
            pass

    def close(self, terminate: bool = False) -> None:
        """Stop the worker; ``terminate`` kills a busy scan at once instead of waiting for it."""
        if terminate and self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5)
        self.request_stop()
        self.process.join(timeout=30)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=5)
        try:
            self.connection.close()
        except OSError:
            pass


class _InlineWorker:
    """Runs commands synchronously in the calling process (tests, generators)."""

    def __init__(self, worker: dict, producer_factory, consumer_factory) -> None:
        self.progress = [0, 0, 0]
        self.state = PlannerWorkerState(consumer_factory=consumer_factory, progress=self.progress, **worker)
        self.producer_factory = producer_factory
        self._reply = None

    def send(self, command: str, payload) -> None:
        try:
            self._reply = ("ok", _run_command(self.state, command, payload, self.producer_factory))
        except Exception as exc:
            self._reply = ("error", f"{type(exc).__name__}: {exc}")

    def recv(self):
        reply, self._reply = self._reply, None
        return reply

    def request_stop(self) -> None:
        pass

    def close(self, terminate: bool = False) -> None:
        if self.state is not None:
            self.state.close()
        self.state = None


# -- parent side ---------------------------------------------------------------------


def assign_lanes(tasks: Sequence, workers: int) -> list[list]:
    """Longest-first assignment of tasks to at most ``workers`` lanes (no empty lanes)."""
    lanes: list[list] = [[] for _ in range(max(1, workers))]
    loads = [0] * len(lanes)
    for task in sorted(tasks, key=lambda item: (-item.size_hint, item.name)):
        lane = min(range(len(lanes)), key=lambda index: (loads[index], index))
        lanes[lane].append(task)
        loads[lane] += max(1, task.size_hint)
    return [lane for lane in lanes if lane] or [[]]


@dataclass
class PlanResult:
    plan: EdgePlan
    digest: str
    path: Path
    scan_seconds: float
    hub_seconds: float
    emit_seconds: float
    total_seconds: float
    type_seconds: dict[str, float]
    rows_read: int = 0
    lanes: list = field(default_factory=list)

    @property
    def records_per_second(self) -> float:
        return self.rows_read / self.scan_seconds if self.scan_seconds > 0 else 0.0

    @property
    def peak_worker_rss_mb(self) -> float:
        return max((lane.get("peak_rss_mb", 0.0) for lane in self.lanes), default=0.0)


class _Heartbeat:
    """Rewrites the plan's progress file while the planner makes progress."""

    def __init__(self, path: Path, plan_id: str, read: Callable[[], dict], interval: float = 1.0) -> None:
        self.path, self.plan_id, self.read, self.interval = path, plan_id, read, interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="edge-plan-heartbeat", daemon=True)
        self._last = None
        self.sequence = 0

    def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.beat(force=True)
        self._thread.start()

    def beat(self, force: bool = False) -> None:
        state = self.read()
        if not force and state == self._last:
            return
        self._last = state
        self.sequence += 1
        data = json.dumps({"plan_id": self.plan_id, "sequence": self.sequence, "at": time.time(), **state},
                          separators=(",", ":"))
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        try:
            temporary.write_text(data + "\n", encoding="utf-8")
            os.replace(temporary, self.path)
        except OSError as exc:  # pragma: no cover - best effort
            logger.warning("stage=edge-plan could not write progress %s: %s", self.path, exc)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self.beat()

    def stop(self, *, remove: bool) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)
        if remove:
            self.path.unlink(missing_ok=True)


class ParallelEdgePlanner:
    """Run the planning rounds over ``tasks`` on ``settings.workers`` workers.

    ``types`` maps every planned edge type (including types without rows) to
    ``(source label, target label)``.  ``context`` is a multiprocessing
    context (default ``spawn``) or ``"inline"`` to run in this process.
    ``after_scan`` runs in the parent once every row was read (e.g. to prove
    the Kafka input did not change after its boundary).  ``plan_path`` (the
    file :meth:`finish` will write) lets :meth:`start` place the spill
    directory beside it and start the progress heartbeat early.
    """

    def __init__(self, tasks: Sequence, settings: PlanSettings, bootstrap_servers: str, *,
                 types: Mapping[str, tuple[str, str]], plan_id: str | None = None,
                 producer_config: dict | None = None, context=None, producer_class=None,
                 producer_factory=None, consumer_factory=None,
                 after_scan: Callable[[], None] | None = None, extra_stats: Mapping | None = None,
                 plan_path: Path | str | None = None,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None,
                 rejection_dir: Path | str | None = None) -> None:
        self.settings = settings.validate()
        # With a directory, malformed Kafka records are rejected to
        # ``<dir>/<plan id>-planner-<worker>.jsonl`` and skipped instead of failing the plan.
        self.rejection_dir = None if rejection_dir is None else Path(rejection_dir)
        self.tasks = list(tasks)
        self.types = dict(types)
        for task in self.tasks:
            if task.edge_type not in self.types:
                raise EdgePlanError(f"task {task.name} has unplanned edge type {task.edge_type}")
        orders = [(task.edge_type, getattr(task, "pair", 0), task.order) for task in self.tasks]
        if len(set(orders)) != len(orders):
            raise EdgePlanError("tasks of one edge type (and label pair) must have distinct orders")
        self.bootstrap_servers = bootstrap_servers
        self.lanes = assign_lanes(self.tasks, settings.workers)
        self.memory_mb = settings.memory_mb or default_plan_memory_mb(len(self.lanes))
        self.producer_config = {
            "queue.buffering.max.kbytes": max(16_384, min(1_048_576, (self.memory_mb << 10) // 5)),
            **(producer_config or {}), "message.max.bytes": settings.max_message_bytes + 65536,
        }
        self.context = context
        self.producer_class, self.producer_factory = producer_class, producer_factory
        self.consumer_factory = consumer_factory
        self.after_scan = after_scan
        self.extra_stats = dict(extra_stats or {})
        self.plan_id = plan_id or uuid.uuid4().hex
        self.plan_path = Path(plan_path) if plan_path is not None else None
        self.spill_dir: Path | None = None
        self._workers: list = []
        self._started: float | None = None
        self._heartbeat: _Heartbeat | None = None
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self._last_progress_totals = [0, 0, 0]
        self._phase = "starting"
        self._lock = threading.Lock()

    def _spill_root(self) -> Path:
        if self.settings.spill_dir:
            return Path(self.settings.spill_dir)
        if self.plan_path is not None:
            return self.plan_path.parent
        return Path(tempfile.gettempdir())

    def _progress(self) -> dict:
        totals = [0, 0, 0]
        for worker in list(self._workers):
            progress = getattr(worker, "progress", None)
            if progress is not None:
                for index in range(3):
                    totals[index] += progress[index]
        # This is the planner's own existing progress heartbeat call site
        # (P6-S2 Story 8), read at most once per _Heartbeat.interval
        # (1 second) - never per row. `totals` are running absolute counts
        # across every worker, so the Counter is advanced by the delta
        # since the last beat, not set to the absolute value (Counters
        # only support .inc()).
        rows_delta = totals[0] - self._last_progress_totals[0]
        chunks_delta = totals[1] - self._last_progress_totals[1]
        if rows_delta > 0:
            metrics_module.safe_emit(lambda delta=rows_delta: self._metrics.planner_rows_read.labels(
                kind="cli"
            ).inc(delta))
        if chunks_delta > 0:
            metrics_module.safe_emit(lambda delta=chunks_delta: self._metrics.planner_chunks_published.labels(
                kind="cli"
            ).inc(delta))
        self._last_progress_totals = list(totals)
        return {"phase": self._phase, "rows": totals[0], "chunks": totals[1], "steps": totals[2]}

    def _start_heartbeat(self) -> None:
        if self._heartbeat is None and self.plan_path is not None:
            self._heartbeat = _Heartbeat(plan_progress_path(self.plan_path), self.plan_id, self._progress)
            self._heartbeat.start()

    def start(self) -> None:
        """Start the workers and their scans (non-blocking for process workers)."""
        with self._lock:
            if self._started is not None:
                return
            self._started = time.monotonic()
            self.spill_dir = self._spill_root() / f".edge-plan-spill-{self.plan_id}"
            shutil.rmtree(self.spill_dir, ignore_errors=True)  # a crashed earlier run of this plan id
            self.spill_dir.mkdir(parents=True, exist_ok=True)
            if self.rejection_dir is not None:
                try:
                    self.rejection_dir.mkdir(parents=True, exist_ok=True)
                    if not os.access(self.rejection_dir, os.W_OK):
                        raise PermissionError(f"{self.rejection_dir} is not writable")
                except OSError as exc:
                    raise EdgePlanInputError(
                        f"planner rejection directory {self.rejection_dir} is unusable: {type(exc).__name__}: {exc}"
                    ) from None
            logger.info(
                "stage=edge-plan-memory plan_id=%s workers=%s memory_mb_per_worker=%s spill_dir=%s spill_free_gb=%.1f",
                self.plan_id, len(self.lanes), self.memory_mb, self.spill_dir,
                shutil.disk_usage(self.spill_dir).free / 1e9,
            )
            inline = self.context == "inline"
            context = None if inline else (self.context or multiprocessing.get_context("spawn"))
            for index, lane in enumerate(self.lanes):
                worker = {
                    "buckets": self.settings.buckets, "hub_degree": self.settings.hub_degree,
                    "memory_bytes": self.memory_mb << 20, "spill_dir": str(self.spill_dir), "worker_index": index,
                }
                if self.rejection_dir is not None:
                    worker["rejection_path"] = str(self.rejection_dir / f"{self.plan_id}-planner-{index}.jsonl")
                if inline:
                    factory = self.producer_factory or _default_producer_factory(
                        self.bootstrap_servers, self.producer_config,
                    )
                    handle = _InlineWorker(worker, factory, self.consumer_factory)
                else:
                    handle = _ProcessWorker(context, worker, self.bootstrap_servers, self.producer_config,
                                            self.producer_class, self.consumer_factory)
                self._workers.append(handle)
                handle.send("scan", lane)
            self._phase = "scan"
            self._start_heartbeat()

    def _gather(self) -> list:
        results, errors = [], []
        for index, worker in enumerate(self._workers):
            status, payload = worker.recv()
            if status != "ok":
                errors.append(f"planner worker {index} failed: {payload}")
            results.append(payload)
        if errors:
            raise RuntimeError(errors[0] if len(errors) == 1 else "; ".join(errors))
        return results

    def _broadcast(self, command: str, payloads) -> list:
        for worker, payload in zip(self._workers, payloads):
            worker.send(command, payload)
        return self._gather()

    def finish(self, plan_path: Path | str) -> PlanResult:
        """Complete planning and write the plan; on failure write the failure marker."""
        plan_path = Path(plan_path)
        marker = plan_failure_marker(plan_path)
        if self.plan_path is None:
            self.plan_path = plan_path
        try:
            return self._finish(plan_path)
        except BaseException as exc:
            try:
                plan_path.parent.mkdir(parents=True, exist_ok=True)
                marker.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
            except OSError:
                pass
            raise
        finally:
            self.close()

    def _hubs(self, workers: int) -> dict[str, list[object]]:
        """Exact hub selection: counter slots bound degrees; candidates are summed by token."""
        hub_degree = self.settings.hub_degree
        if hub_degree <= 0:
            return {}
        self._phase = "hubs"
        total = None
        for counter in self._broadcast("counter", [None] * workers):
            total = counter.astype(np.uint64) if total is None else total + counter
        candidates = total >= hub_degree
        if not candidates.any():
            return {}
        packed = np.packbits(candidates).tobytes()
        totals: dict[str, dict[bytes, int]] = {}
        for degrees in self._broadcast("degrees", [packed] * workers):
            for label, counts in degrees.items():
                bucket = totals.setdefault(label, {})
                for token, count in counts.items():
                    bucket[token] = bucket.get(token, 0) + count
        return {
            label: [decode_token(token) for token in sorted(token for token, count in counts.items()
                                                           if count >= hub_degree)]
            for label, counts in totals.items()
            if any(count >= hub_degree for count in counts.values())
        }

    def _finish(self, plan_path: Path) -> PlanResult:
        self.start()
        self._start_heartbeat()
        settings = self.settings
        scans = self._gather()
        rows_by_type = {name: 0 for name in self.types}
        pair_of_task = {task.name: getattr(task, "pair", 0) for task in self.tasks}
        rows_by_pair: dict[tuple[str, int], int] = {}
        lane_stats = []
        for lane in scans:
            lane_rows = 0
            for name, edge_type, rows, _seconds in lane["tasks"]:
                rows_by_type[edge_type] += rows
                key = (edge_type, pair_of_task.get(name, 0))
                rows_by_pair[key] = rows_by_pair.get(key, 0) + rows
                lane_rows += rows
            lane_stats.append({"rows": lane_rows, "seconds": round(lane["seconds"], 3), "memory_mb": self.memory_mb,
                               **({"rejected": lane["rejected"]} if lane.get("rejected") else {})})
        scan_done = time.monotonic()
        rows_read = sum(rows_by_type.values())
        rejected = sum(lane.get("rejected", 0) for lane in scans)
        scan_seconds = scan_done - self._started
        if rejected:
            logger.warning(
                "stage=edge-plan-rejections plan_id=%s rejected=%s dir=%s (malformed records were skipped; "
                "see the planner rejection files)", self.plan_id, rejected, self.rejection_dir,
            )
        logger.info(
            "stage=edge-plan-scan plan_id=%s workers=%s tasks=%s rows=%s scan_s=%.3f records_per_s=%.0f",
            self.plan_id, len(self._workers), len(self.tasks), rows_read, scan_seconds,
            rows_read / scan_seconds if scan_seconds > 0 else 0.0,
        )
        if self.after_scan is not None:
            self.after_scan()
        workers = len(self._workers)
        hubs = self._hubs(workers)
        labels = {label for value in self.types.values() for label in _type_labels(value)}
        table = ResourceTable(labels, settings.buckets, hubs)
        hubs_done = time.monotonic()
        self._phase = "emit"
        emit_payload = {
            "resources": table.to_json(), "run": self.plan_id, "topic": settings.topic,
            "partitions": settings.partitions, "chunk_rows": settings.chunk_rows,
            "max_chunk_bytes": settings.max_chunk_bytes,
        }
        emitted = self._broadcast("emit", [{**emit_payload, "worker_index": index} for index in range(workers)])
        specs: list[ChunkSpec] = []
        type_seconds: dict[str, float] = {}
        names = {task.name: task.edge_type for task in self.tasks}
        for index, (lane_specs, timings, stats) in enumerate(emitted):
            specs.extend(lane_specs)
            lane_stats[index].update(stats)
            for name, seconds in timings.items():
                edge_type = names.get(name, name)
                type_seconds[edge_type] = max(type_seconds.get(edge_type, 0.0), seconds)
        emit_done = time.monotonic()
        self._phase = "assemble"
        sizes = [spec.bytes for spec in specs]
        plan = assemble_plan(
            plan_id=self.plan_id, topic=settings.topic, partitions=settings.partitions, resources=table,
            types={name: _assembled_type(name, value, rows_by_type, rows_by_pair) for name, value in self.types.items()},
            specs=specs, c0=settings.c0, c1=settings.c1, settings=settings.as_plan_settings(workers),
            stats={
                "resources": table.count, "hubs": table.hub_count, "chunks": len(specs),
                "units": len({spec.unit for spec in specs}), "rows": sum(spec.rows for spec in specs),
                "max_chunk_message_bytes": max(sizes, default=0),
                "mean_chunk_message_bytes": round(sum(sizes) / len(sizes), 1) if sizes else 0,
                **({"rejected": rejected} if rejected else {}),  # only when records were skipped
                "scan_seconds": round(scan_seconds, 3),
                "scan_records_per_second": round(rows_read / scan_seconds, 1) if scan_seconds > 0 else 0,
                "hub_seconds": round(hubs_done - scan_done, 3),
                "emit_seconds": round(emit_done - hubs_done, 3),
                # Per worker: rows, seconds, memory budget, spilled bytes, peak RSS.
                "lanes": lane_stats,
                **self.extra_stats,
            },
        )
        digest = write_plan_file(plan_path, plan)
        finished = time.monotonic()
        logger.info(
            "stage=edge-plan-ready plan_id=%s chunks=%s rows=%s resources=%s hubs=%s scan_s=%.3f hub_s=%.3f "
            "emit_s=%.3f total_s=%.3f spilled_mb=%.1f peak_worker_rss_mb=%.1f path=%s",
            plan.plan_id, len(plan.chunks), plan.total_rows, table.count, table.hub_count, scan_seconds,
            hubs_done - scan_done, emit_done - hubs_done, finished - self._started,
            sum(lane.get("spilled_bytes", 0) for lane in lane_stats) / 1e6, max((lane.get("peak_rss_mb", 0) for lane in lane_stats), default=0),
            plan_path,
        )
        return PlanResult(plan, digest, plan_path, scan_seconds, hubs_done - scan_done, emit_done - hubs_done,
                          finished - self._started, type_seconds, rows_read, lane_stats)

    def close(self, *, terminate: bool = False) -> None:
        """Release the workers, spill files and heartbeat; ``terminate`` kills a still-scanning planner at once."""
        if self._heartbeat is not None:
            self._heartbeat.stop(remove=True)
            self._heartbeat = None
        # P6-S2 review finding S8-3: one final read after the heartbeat
        # thread has been joined (never concurrently with a beat - avoids
        # double-counting the same delta), before self._workers is cleared
        # below - otherwise up to one heartbeat interval (1s) of trailing
        # rows/chunks is never counted, and a plan that finishes in under a
        # second would read 0 forever. Guarded: an unexpected error reading
        # a dead spawn worker's RawArray must never block the
        # request_stop()/close() cleanup loop right below (same principle
        # as safe_emit - metrics must never block cleanup).
        try:
            self._progress()
        except Exception as exc:
            logger.warning("stage=edge-plan reason=final_progress_read_failed error=%s", exc)
        workers, self._workers = self._workers, []
        for worker in workers:
            worker.request_stop()
        for worker in workers:
            worker.close(terminate=terminate)
        if self.spill_dir is not None:
            shutil.rmtree(self.spill_dir, ignore_errors=True)


# -- Kafka input: boundary, tasks, run topic ---------------------------------------


def _metadata_partitions(consumer, topic: str, timeout: float) -> list[int]:
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    item = metadata.topics.get(topic) if metadata is not None else None
    if item is None or getattr(item, "error", None) is not None or not getattr(item, "partitions", None):
        error = getattr(item, "error", None) if item is not None else None
        raise EdgePlanInputError(f"relationship topic {topic} has no partition metadata"
                                 + (f": {error}" if error is not None else ""))
    return sorted(int(partition) for partition in item.partitions)


def capture_edge_boundary(edges: Sequence[EdgeConfig], bootstrap_servers: str, *, consumer_factory=None,
                          timeout: float = 10.0) -> dict[str, tuple[tuple[int, int, int], ...]]:
    """Return ``topic -> ((partition, low, high), ...)`` for every topic of every edge type."""
    if consumer_factory is None:
        from confluent_kafka import Consumer as consumer_factory
    from confluent_kafka import TopicPartition

    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-plan-boundary-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        boundary = {}
        for topic in sorted({topic for edge in edges for topic in edge.topics}):
            ranges = []
            for partition in _metadata_partitions(consumer, topic, timeout):
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
                if not isinstance(low, int) or not isinstance(high, int) or low < 0 or high < low:
                    raise EdgePlanInputError(f"invalid watermarks for {topic} partition {partition}")
                ranges.append((partition, low, high))
            boundary[topic] = tuple(ranges)
        return boundary
    finally:
        consumer.close()


def _type_labels(value) -> tuple[str, ...]:
    """Labels of one planned type: ``(S, T)``, or the union over ``((S, T, topics), ...)`` (v7-S7)."""
    if isinstance(value[0], str):
        return tuple(value)
    return tuple(label for source, target, _topics in value for label in (source, target))


def _assembled_type(name: str, value, rows_by_type: Mapping[str, int], rows_by_pair: Mapping) -> tuple:
    if isinstance(value[0], str):
        source, target = value
        return (source, target, rows_by_type[name])
    return (tuple(value), [rows_by_pair.get((name, index), 0) for index in range(len(value))])


def plan_types(edges: Sequence[EdgeConfig]) -> dict[str, tuple]:
    """``ParallelEdgePlanner.types`` for schema edges: ``(S, T)`` for a single-pair type (unchanged),
    ``((S, T, topics), ...)`` in pair order for a type of several label pairs (v7-S7)."""
    return {
        edge.type: (
            (edge.nodes.source, edge.nodes.target) if not edge.is_multi_pair
            else tuple((pair.source_label, pair.target_label, pair.topics) for pair in edge.endpoint_pairs)
        )
        for edge in edges
    }


def self_referencing_rows(edges: Sequence[EdgeConfig], rows_by_topic: Mapping[str, int]) -> int:
    """Rows of the same-label (self-referencing) pairs of ``edges``, from per-topic row counts.

    A single-pair type counts when its source and target labels are equal; a
    type of several label pairs counts only the topics of its same-label
    pairs (each topic binds exactly one pair), e.g. only the Person->Person
    topic of a type that also reads a Person->Company topic.  Such a pair's
    chunks hold two buckets of one label, so they matter for the bucket count
    (:func:`~src.orchestrator.auto_settings.derive_plan_buckets`).
    """
    return sum(
        max(0, int(rows_by_topic.get(topic, 0)))
        for edge in edges for pair in edge.endpoint_pairs if pair.is_self_referencing
        for topic in pair.topics
    )


def _accepts_keyword(function: Callable, name: str) -> bool:
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        return False
    return name in parameters or any(item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values())


def kafka_read_tasks(edges: Sequence[EdgeConfig], boundary: Mapping[str, Sequence[tuple[int, int, int]]],
                     workers: int, bootstrap_servers: str, group_id: str,
                     read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS) -> list[KafkaReadTask]:
    """Split each ``(edge type, topic)`` pair into contiguous partition groups of balanced size.

    A topic larger than a worker's fair share of all rows (over all topics of
    all types) is split into up to ``workers`` groups (never more than its
    non-empty partitions); everything else is one task.  Every task carries
    its ``edge.precedence_rank(topic)`` (the index in ``edge.topics``), which orders the plan chain.

    v7-S7: for a type whose topics bind several label pairs the rank is still the parent's, and
    each task carries the pair view of its topic and the pair index (decision 3: the topic decides
    the pair).
    """
    sized = [(edge, edge.precedence_rank(topic), topic, tuple(boundary[topic]))
             for edge in edges for topic in edge.topics]
    total = sum(end - start for _, _, _, ranges in sized for _, start, end in ranges)
    share = max(1.0, total / max(1, workers))
    tasks: list[KafkaReadTask] = []
    for parent, topic_index, topic, ranges in sized:
        pair = parent.pair_for_topic(topic).index if parent.is_multi_pair else 0
        edge = parent.pair_view(pair) if parent.is_multi_pair else parent
        rows = sum(end - start for _, start, end in ranges)
        nonempty = [item for item in ranges if item[2] > item[1]]
        groups = max(1, min(workers, len(nonempty), math.ceil(rows / share))) if rows else 1
        if groups == 1:
            tasks.append(KafkaReadTask(edge, topic, ranges, bootstrap_servers, group_id, read_timeout_seconds,
                                       topic_index, pair))
            continue
        target = rows / groups
        current: list[tuple[int, int, int]] = []
        used = 0
        remaining_groups = groups
        for index, item in enumerate(ranges):
            current.append(item)
            used += item[2] - item[1]
            left = len(ranges) - index - 1
            if remaining_groups > 1 and used >= target and left >= 1:
                tasks.append(KafkaReadTask(edge, topic, tuple(current), bootstrap_servers, group_id,
                                           read_timeout_seconds, topic_index, pair))
                current, used = [], 0
                remaining_groups -= 1
        if current:
            tasks.append(KafkaReadTask(edge, topic, tuple(current), bootstrap_servers, group_id,
                                       read_timeout_seconds, topic_index, pair))
    return tasks


def chunk_topic_config(max_message_bytes: int) -> dict[str, str]:
    """Topic configuration of a run's chunk topic.

    Retention is unlimited in time and size (``-1``), whatever the broker
    defaults are: a chunk deleted before its writer fetched it would fail the
    load, and a large run (tens of GB of chunks) may outlive a short
    ``log.retention.*`` default.  The topic is deleted after the run.  1 GiB
    segments keep the segment count small for large runs.
    """
    return {
        "max.message.bytes": str(max_message_bytes + 65536),
        "retention.ms": "-1",
        "retention.bytes": "-1",
        "cleanup.policy": "delete",
        "segment.bytes": str(1 << 30),
    }


def create_chunk_topic(bootstrap_servers: str, topic: str, partitions: int, max_message_bytes: int, *,
                       admin_factory=None, timeout: float = 30.0) -> None:
    """Create a new, empty chunk topic that accepts ``max_message_bytes`` messages (see :func:`chunk_topic_config`)."""
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    from confluent_kafka.admin import NewTopic

    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    if topic in metadata.topics:
        raise EdgePlanError(
            f"chunk topic {topic} already exists; planned runs need a new, empty topic "
            "(left over from an earlier run that was killed? use a new --run-id, or re-run with the same "
            "--run-id and --replace-stale-plan-topic to delete it when no container of that run is alive)"
        )
    futures = admin.create_topics([NewTopic(
        topic, num_partitions=partitions, replication_factor=-1, config=chunk_topic_config(max_message_bytes),
    )])
    for future in futures.values():
        future.result(timeout)


def delete_chunk_topic(bootstrap_servers: str, topic: str, *, admin_factory=None, timeout: float = 30.0) -> None:
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient as admin_factory
    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    for future in admin.delete_topics([topic], operation_timeout=timeout).values():
        future.result(timeout)


def run_scoped_chunk_topic(run_id: str) -> str:
    return f"graph-loader-edge-chunks-{run_id}"


class KafkaEdgePlanner:
    """Plan a bulk run's relationships from the schema-declared edge topics.

    ``start()`` creates the run's chunk topic, captures the bulk boundary
    (every edge topic partition's ``[low, high)`` watermarks) and starts the
    workers; ``finish(plan_path)`` completes the plan (or writes the failure
    marker).  :meth:`run_in_background` runs ``finish`` on a daemon thread so
    planning overlaps node loading.  Input produced after the boundary fails
    the plan (like the per-type bulk loaders' boundary check).

    ``settings`` is a :class:`PlanSettings` or a callable ``(records,
    edge_types) -> PlanSettings`` evaluated once the boundary is known
    (relationship records in total, and edge types with records).  A
    callable that accepts a ``self_ref_records`` keyword also gets the
    records of the self-referencing label pairs
    (:func:`self_referencing_rows`).
    """

    def __init__(self, schema, bootstrap_servers: str,
                 settings: PlanSettings | Callable[[int, int], PlanSettings], *, plan_id: str,
                 create_topic: bool = True, context=None, consumer_factory=None, producer_class=None,
                 producer_factory=None, admin_factory=None,
                 read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
                 plan_path: Path | str | None = None,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None,
                 rejection_dir: Path | str | None = None) -> None:
        self.schema = schema
        # Malformed records are rejected to ``<rejection_dir>/<plan id>-planner-<worker>.jsonl`` (classic
        # rejection-log entries) and skipped; ``None`` keeps the plan fail-closed on any invalid record.
        self.rejection_dir = rejection_dir
        self.plan_path = None if plan_path is None else Path(plan_path)
        self.edges = tuple(schema.edges)
        self.bootstrap_servers = bootstrap_servers
        self._settings_for = settings if callable(settings) else None
        self.settings = settings.validate() if not callable(settings) else None
        self.plan_id = plan_id
        self.create_topic = create_topic
        self.context, self.consumer_factory = context, consumer_factory
        self.producer_class, self.producer_factory, self.admin_factory = producer_class, producer_factory, admin_factory
        self.read_timeout_seconds = read_timeout_seconds
        self.boundary: dict | None = None
        self.topic_created = False
        self.planner: ParallelEdgePlanner | None = None
        self.result: PlanResult | None = None
        # N2: ``() -> bool``, true once the operator asked the run to stop. Only
        # when close() then aborted the plan (terminating the planner
        # processes, e.g. ``[Errno 104] Connection reset by peer``) is the
        # background failure logged as a WARNING; a failure of its own, stop
        # or not, stays an ERROR.
        self.stop_requested: Callable[[], bool] | None = None
        self._aborted = False  # set by close() just before it terminates an unfinished plan
        self.error: BaseException | None = None
        self._thread: threading.Thread | None = None
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()

    def start(self) -> None:
        self.boundary = capture_edge_boundary(self.edges, self.bootstrap_servers,
                                              consumer_factory=self.consumer_factory)
        per_topic = {topic: sum(end - start for _, start, end in ranges) for topic, ranges in self.boundary.items()}
        records = sum(per_topic.values())
        if self._settings_for is not None:
            types_with_records = sum(1 for edge in self.edges if any(per_topic.get(topic, 0) > 0 for topic in edge.topics))
            extra = ({"self_ref_records": self_referencing_rows(self.edges, per_topic)}
                     if _accepts_keyword(self._settings_for, "self_ref_records") else {})
            self.settings = self._settings_for(records, types_with_records, **extra).validate()
        if self.create_topic:
            create_chunk_topic(self.bootstrap_servers, self.settings.topic, self.settings.partitions,
                               self.settings.max_message_bytes, admin_factory=self.admin_factory)
            self.topic_created = True
        tasks = kafka_read_tasks(self.edges, self.boundary, self.settings.workers, self.bootstrap_servers,
                                 f"graph-loader-planner-{self.plan_id}", self.read_timeout_seconds)
        logger.info(
            "stage=edge-plan-start plan_id=%s topic=%s edge_topics=%s boundary_records=%s tasks=%s workers=%s",
            self.plan_id, self.settings.topic, len(self.boundary), records, len(tasks),
            min(self.settings.workers, max(1, len(tasks))),
        )
        self.planner = ParallelEdgePlanner(
            tasks, self.settings, self.bootstrap_servers, plan_id=self.plan_id,
            types=plan_types(self.edges),
            context=self.context, producer_class=self.producer_class, producer_factory=self.producer_factory,
            consumer_factory=self.consumer_factory, after_scan=self._verify_boundary_unchanged,
            extra_stats={"source": "kafka", "boundary_records": records,
                         "type_topics": {edge.type: list(edge.topics) for edge in self.edges}, "boundary": {
                topic: [list(item) for item in ranges] for topic, ranges in self.boundary.items()
            }},
            plan_path=self.plan_path, metrics=self._metrics, rejection_dir=self.rejection_dir,
        )
        self.planner.start()

    def _verify_boundary_unchanged(self) -> None:
        current = capture_edge_boundary(self.edges, self.bootstrap_servers, consumer_factory=self.consumer_factory)
        for topic, ranges in (self.boundary or {}).items():
            before = {partition: high for partition, _, high in ranges}
            after = {partition: high for partition, _, high in current.get(topic, ())}
            if before != after:
                raise EdgePlanInputError(
                    f"relationship topic {topic} changed after the bulk boundary was captured "
                    f"(end offsets {before} -> {after}); publish every relationship before starting the load"
                )

    def finish(self, plan_path: Path | str) -> PlanResult:
        if self.planner is None:
            marker = plan_failure_marker(Path(plan_path))
            try:
                self.start()
            except BaseException as exc:
                Path(plan_path).parent.mkdir(parents=True, exist_ok=True)
                marker.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")
                raise
        self.result = self.planner.finish(plan_path)
        return self.result

    @property
    def rejected(self) -> int:
        """Malformed records skipped while planning (0 until the plan is finished)."""
        return int(self.result.plan.stats.get("rejected", 0)) if self.result is not None else 0

    def run_in_background(self, plan_path: Path | str) -> threading.Thread:
        def target() -> None:
            try:
                self.finish(plan_path)
            except BaseException as exc:
                self.error = exc
                if getattr(self, "_aborted", False) and self.stop_requested is not None and self.stop_requested():
                    logger.warning("stage=edge-plan plan_id=%s stopped by operator: %s", self.plan_id, exc)
                else:
                    logger.error("stage=edge-plan plan_id=%s failed: %s", self.plan_id, exc)
        self._thread = threading.Thread(target=target, name="edge-planner", daemon=True)
        self._thread.start()
        return self._thread

    def close(self, *, delete_topic: bool = True) -> None:
        # A plan that has not finished (operator stop, failed run) is aborted
        # at once: terminating the planner processes beats waiting for the scan.
        aborting = self.result is None and self.planner is not None and self._thread is not None \
            and self._thread.is_alive()
        if aborting:
            # N2: the background plan's failure after this point is the abort itself.
            self._aborted = True
        if self.planner is not None:
            self.planner.close(terminate=aborting)
        if self._thread is not None:
            self._thread.join(timeout=10)
        if aborting and self.plan_path is not None:
            for leftover in (plan_failure_marker(self.plan_path), plan_progress_path(self.plan_path)):
                leftover.unlink(missing_ok=True)
        if delete_topic and self.topic_created:
            try:
                delete_chunk_topic(self.bootstrap_servers, self.settings.topic, admin_factory=self.admin_factory)
                self.topic_created = False
            except Exception as exc:
                logger.warning("stage=edge-plan could not delete chunk topic %s: %s", self.settings.topic, exc)


# -- producer API --------------------------------------------------------------------


def source_tasks(schema, sources: Sequence[EdgeRecordSource] | Mapping[str, Iterable[object]]) -> list[SourceTask]:
    """Bind producer sources to their schema edges (a mapping gives one source per type).

    v7-S7: a source of a type whose topics bind several label pairs must name one of the type's
    topics (``source.topic``); its rows are planned with that topic's pair. The mapping form, which
    cannot name a topic, is refused for such types."""
    edges = {edge.type: edge for edge in schema.edges}
    if isinstance(sources, Mapping):
        multi = sorted(name for name in sources if name in edges and edges[name].is_multi_pair)
        if multi:
            raise EdgePlanError(
                f"edge type(s) {multi} bind several label pairs; pass EdgeRecordSource objects naming their "
                "topic, not the {type: rows} mapping"
            )
        sources = [IterableEdgeSource(edge_type, rows) for edge_type, rows in sources.items()]
    tasks = []
    for source in sources:
        edge = edges.get(source.edge_type)
        if edge is None:
            raise EdgePlanError(f"source edge type {source.edge_type!r} is not in the schema")
        if edge.is_multi_pair:
            topic = getattr(source, "topic", None)
            if topic not in edge.topics:
                raise EdgePlanError(
                    f"source for {edge.type} must name its topic (each topic binds a label pair); got {topic!r}, "
                    f"expected one of {list(edge.topics)}"
                )
            pair = edge.pair_for_topic(topic).index
            tasks.append(SourceTask(edge.pair_view(pair), source, pair))
            continue
        tasks.append(SourceTask(edge, source))
    return tasks


def producer_edge_planner(schema, sources, settings: PlanSettings, bootstrap_servers: str, *,
                          plan_id: str | None = None, context=None, **options) -> ParallelEdgePlanner:
    """Return a :class:`ParallelEdgePlanner` over producer-supplied rows of every schema edge type.

    Call ``start()`` early (e.g. before publishing nodes) and ``finish(plan_path)`` later.  Pass
    ``plan_path=`` here too so the spill directory and the progress heartbeat (which lets a loader
    waiting for the plan time out only on a stall) exist from ``start()`` on.
    """
    tasks = source_tasks(schema, sources)
    return ParallelEdgePlanner(
        tasks, settings, bootstrap_servers, plan_id=plan_id, context=context,
        types=plan_types(schema.edges),
        extra_stats={"source": "producer"}, **options,
    )


def plan_and_publish_edges(schema, sources, *, bootstrap_servers: str, plan_path: Path | str,
                           settings: PlanSettings, plan_id: str | None = None, create_topic: bool = True,
                           context=None, admin_factory=None, **options) -> PlanResult:
    """Plan producer-supplied relationship rows, publish the chunks, and write the plan file.

    ``sources`` is a mapping ``edge type -> iterable of rows`` (planned in
    this process) or a list of picklable :class:`EdgeRecordSource` objects
    (planned on ``settings.workers`` processes).  Load the result with
    ``graph-loader start --mode bulk --edge-plan <plan_path>``.
    """
    if context is None and isinstance(sources, Mapping):
        context = "inline"
    if create_topic:
        create_chunk_topic(bootstrap_servers, settings.topic, settings.partitions, settings.max_message_bytes,
                           admin_factory=admin_factory)
    planner = producer_edge_planner(schema, sources, settings, bootstrap_servers, plan_id=plan_id,
                                    context=context, plan_path=plan_path, **options)
    return planner.finish(plan_path)
