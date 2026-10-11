"""Frozen copy of the in-memory edge planner (before external-memory planning).

Test reference only: :func:`plan_in_memory` runs the original three rounds
(scan, candidates/count, emit) of the in-memory planner over the same tasks in
this process, so tests can prove that the external-memory planner of
:mod:`src.loader.edge_planner` produces the identical plan.  The worker state
below is the original ``PlannerWorkerState`` verbatim.
"""
from __future__ import annotations

from array import array
from dataclasses import dataclass, field
import math
import time
from typing import Mapping, Sequence
import zlib

from src.loader.edge_plan import (
    ChunkSpec, EdgePlanError, ResourceTable, assemble_plan, chunk_boundaries, encode_chunk_message, key_token,
    resource_pair, select_hubs,
)
from src.loader.edge_planner import KafkaReadTask, PlanSettings, _produce, assign_lanes, scan_kafka_tasks

_ORDER_BITS = 32


class _LabelKeys:
    """Per-label key interning with cached bucket and local degree."""

    __slots__ = ("ids", "keys", "buckets", "degrees")

    def __init__(self) -> None:
        self.ids: dict[object, int] = {}
        self.keys: list[object] = []
        self.buckets = array("I")
        self.degrees = array("I")


@dataclass
class _Cell:
    sources: array = field(default_factory=lambda: array("I"))
    targets: array = field(default_factory=lambda: array("I"))
    parts: array = field(default_factory=lambda: array("I"))
    records: list = field(default_factory=list)


@dataclass
class _ScannedTask:
    name: str
    edge_type: str
    source_label: str
    target_label: str
    order: int
    multipart: bool
    rows: int
    cells: dict
    seconds: float


class PlannerWorkerState:
    """Everything one planner worker keeps between rounds."""

    def __init__(self, buckets: int, *, consumer_factory=None) -> None:
        self.buckets = buckets
        self.consumer_factory = consumer_factory
        self.labels: dict[str, _LabelKeys] = {}
        self.scanned: list[_ScannedTask] = []

    def _label(self, label: str) -> _LabelKeys:
        keys = self.labels.get(label)
        if keys is None:
            keys = self.labels[label] = _LabelKeys()
        return keys

    def _adder(self, source_label: str, target_label: str, cells: dict):
        source, target = self._label(source_label), self._label(target_label)
        bucket_count = self.buckets
        crc32, token = zlib.crc32, key_token

        def intern(keys: _LabelKeys, key: object) -> int:
            local = keys.ids[key] = len(keys.keys)
            keys.keys.append(key)
            keys.buckets.append(crc32(token(key)) % bucket_count)
            keys.degrees.append(0)
            return local

        source_ids, target_ids = source.ids, target.ids
        source_buckets, target_buckets = source.buckets, target.buckets
        source_degrees, target_degrees = source.degrees, target.degrees

        def add(source_key: object, target_key: object, record, part: int) -> None:
            source_id = source_ids.get(source_key)
            if source_id is None:
                source_id = intern(source, source_key)
            target_id = target_ids.get(target_key)
            if target_id is None:
                target_id = intern(target, target_key)
            source_degrees[source_id] += 1
            target_degrees[target_id] += 1
            cell_key = (source_buckets[source_id], target_buckets[target_id])
            cell = cells.get(cell_key)
            if cell is None:
                cell = cells[cell_key] = _Cell()
            cell.sources.append(source_id)
            cell.targets.append(target_id)
            cell.parts.append(part)
            cell.records.append(record)

        return add

    def _record(self, task, cells: dict, rows: int, seconds: float) -> tuple[str, str, int, float]:
        edge = task.edge
        self.scanned.append(_ScannedTask(
            task.name, edge.type, edge.nodes.source, edge.nodes.target, task.order, task.multipart, rows,
            cells, seconds,
        ))
        return task.name, edge.type, rows, seconds

    def scan(self, task) -> tuple[str, str, int, float]:
        """Validate, intern and cell-group one task's rows."""
        started = time.monotonic()
        edge = task.edge
        cells: dict = {}
        rows = task.scan(self._adder(edge.nodes.source, edge.nodes.target, cells), self.consumer_factory)
        return self._record(task, cells, rows, time.monotonic() - started)

    def scan_lane(self, tasks) -> dict:
        """Scan a lane: every Kafka task through one consumer, then the other tasks."""
        started = time.monotonic()
        kafka = [task for task in tasks if isinstance(task, KafkaReadTask)]
        results = []
        if kafka:
            cells = [{} for _ in kafka]
            counts = scan_kafka_tasks(
                [(task, self._adder(task.edge.nodes.source, task.edge.nodes.target, cells[index]))
                 for index, task in enumerate(kafka)],
                self.consumer_factory,
            )
            seconds = time.monotonic() - started
            results.extend(self._record(task, cells[index], counts[index], seconds)
                           for index, task in enumerate(kafka))
        results.extend(self.scan(task) for task in tasks if not isinstance(task, KafkaReadTask))
        return {"tasks": results, "seconds": time.monotonic() - started}

    def candidates(self, threshold: int) -> dict[str, list[object]]:
        if threshold <= 0:
            return {}
        return {
            label: [keys.keys[index] for index, degree in enumerate(keys.degrees) if degree >= threshold]
            for label, keys in self.labels.items()
        }

    def exact_degrees(self, candidates: Mapping[str, Sequence[object]]) -> dict[str, list[tuple[object, int]]]:
        result: dict[str, list[tuple[object, int]]] = {}
        for label, keys in candidates.items():
            local = self.labels.get(label)
            if local is None:
                continue
            found = [(key, local.degrees[local.ids[key]]) for key in keys if key in local.ids]
            if found:
                result[label] = found
        return result

    def emit(self, table: ResourceTable, *, run: str, topic: str, partitions: int, chunk_rows: int,
             max_chunk_bytes: int, producer_factory, worker_index: int) -> tuple[list[ChunkSpec], dict]:
        """Chunk every unit, produce one message per chunk, return the delivered specs."""
        hub_keys: dict[str, dict[object, int]] = {}
        for resource, (label, kind, value) in enumerate(table.names):
            if kind == "hub":
                hub_keys.setdefault(label, {})[value] = resource
        hub_ids: dict[str, dict[int, int]] = {}
        for label, keys in self.labels.items():
            mapping = {}
            for value, resource in hub_keys.get(label, {}).items():
                local = keys.ids.get(value)
                if local is not None:
                    mapping[local] = resource
            hub_ids[label] = mapping
        producer = producer_factory()
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
        for scanned in self.scanned:
            started = time.monotonic()
            edge_type, order = scanned.edge_type, scanned.order
            if not 0 <= order < 1 << _ORDER_BITS:
                raise EdgePlanError(f"task order of {scanned.name} must be 0..2^32-1")
            source_hubs, target_hubs = hub_ids.get(scanned.source_label, {}), hub_ids.get(scanned.target_label, {})
            source_base = table.bucket_resource(scanned.source_label, 0)
            target_base = table.bucket_resource(scanned.target_label, 0)
            for (source_bucket, target_bucket), cell in sorted(scanned.cells.items()):
                units: dict[tuple[int, int], list[int]] = {}
                default = (source_base + source_bucket, target_base + target_bucket)
                sources, targets = cell.sources, cell.targets
                if not source_hubs and not target_hubs:
                    units[default] = list(range(len(cell.records)))
                else:
                    for index in range(len(cell.records)):
                        source = source_hubs.get(sources[index], default[0])
                        target = target_hubs.get(targets[index], default[1])
                        members = units.get((source, target))
                        if members is None:
                            members = units[(source, target)] = []
                        members.append(index)
                for (source, target), members in sorted(units.items()):
                    if scanned.multipart:
                        # (partition, offset) order: rows arrive in offset order per partition.
                        members.sort(key=cell.parts.__getitem__)
                    records = [cell.records[index] for index in members]
                    unit = (edge_type, source, target)
                    boundaries = chunk_boundaries([len(record) for record in records], chunk_rows, max_chunk_bytes)
                    for seq, (start, end) in enumerate(boundaries):
                        key = f"{edge_type}:{source}:{target}:{order}:{seq}"
                        value = encode_chunk_message(run, key, edge_type, records[start:end])
                        index = len(pending)
                        pending.append(ChunkSpec(
                            key, edge_type, unit, (order << _ORDER_BITS) | seq, resource_pair(source, target),
                            end - start, next_partition, -1, len(value),
                        ))
                        _produce(producer, topic, value, next_partition,
                                 lambda error, message, index=index: report(error, message, index))
                        next_partition = (next_partition + 1) % partitions
            scanned.cells.clear()
            timings[scanned.name] = scanned.seconds + time.monotonic() - started
        outstanding = producer.flush(120)
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
                                   partition, offset, spec.bytes))
        return specs, timings


def plan_in_memory(tasks: Sequence, settings: PlanSettings, *, types: Mapping[str, tuple[str, str]], plan_id: str,
                   producer_factory, consumer_factory=None, extra_stats: Mapping | None = None):
    """Plan ``tasks`` with the original in-memory algorithm (inline workers, one per lane)."""
    settings = settings.validate()
    lanes = assign_lanes(list(tasks), settings.workers)
    states = [PlannerWorkerState(settings.buckets, consumer_factory=consumer_factory) for _ in lanes]
    rows_by_type = {name: 0 for name in types}
    for state, lane in zip(states, lanes):
        for _name, edge_type, rows, _seconds in state.scan_lane(lane)["tasks"]:
            rows_by_type[edge_type] += rows
    workers = len(states)
    threshold = math.ceil(settings.hub_degree / workers) if settings.hub_degree > 0 else 0
    union: dict[str, dict[bytes, object]] = {}
    for state in states:
        for label, keys in state.candidates(threshold).items():
            bucket = union.setdefault(label, {})
            for key in keys:
                bucket.setdefault(key_token(key), key)
    request = {label: [keys[token] for token in sorted(keys)] for label, keys in union.items()}
    totals: dict[str, dict[object, int]] = {}
    for state in states:
        for label, pairs in state.exact_degrees(request).items():
            bucket = totals.setdefault(label, {})
            for key, degree in pairs:
                bucket[key] = bucket.get(key, 0) + degree
    hubs = select_hubs(totals, settings.hub_degree)
    labels = {label for pair in types.values() for label in pair}
    table = ResourceTable(labels, settings.buckets, hubs)
    specs: list[ChunkSpec] = []
    for index, state in enumerate(states):
        lane_specs, _timings = state.emit(
            table, run=plan_id, topic=settings.topic, partitions=settings.partitions,
            chunk_rows=settings.chunk_rows, max_chunk_bytes=settings.max_chunk_bytes,
            producer_factory=producer_factory, worker_index=index,
        )
        specs.extend(lane_specs)
    sizes = [spec.bytes for spec in specs]
    return assemble_plan(
        plan_id=plan_id, topic=settings.topic, partitions=settings.partitions, resources=table,
        types={name: (source, target, rows_by_type[name]) for name, (source, target) in types.items()},
        specs=specs, c0=settings.c0, c1=settings.c1, settings=settings.as_plan_settings(workers),
        stats={
            "resources": table.count, "hubs": table.hub_count, "chunks": len(specs),
            "units": len({spec.unit for spec in specs}), "rows": sum(spec.rows for spec in specs),
            "max_chunk_message_bytes": max(sizes, default=0),
            "mean_chunk_message_bytes": round(sum(sizes) / len(sizes), 1) if sizes else 0,
            **dict(extra_stats or {}),
        },
    )
