"""Globally planned bulk relationship load: resources, units, chunks, plan file.

Planned mode loads every relationship of a bulk run from one *plan*, computed
after every relationship row of the run was read -- by the library planner
from the schema's per-type topics (:mod:`src.loader.edge_planner`), or by a
producer from its in-memory rows before publishing:

* **Resources.**  Every endpoint node maps to exactly one resource: its
  label's hash bucket ``crc32(token) % B`` (``B`` buckets per label), unless
  its total degree over *all* edge types is at least ``H`` -- then it is a
  *hub* with a resource of its own.  Concurrent transactions whose resource
  sets are disjoint are node-disjoint, so they never wait on each other's
  Neo4j locks (MERGE of a relationship locks exactly its two endpoints).
* **Units.**  A unit is ``(edge type, source resource, target resource)``:
  every row of one type whose endpoints map to that resource pair.  One
  transaction MERGEs one relationship type, so units are type-homogeneous.
  All rows of one ``(type, source, target)`` pair fall into one unit.
* **Chunks.**  A unit's rows, in their original order, are split into chunks
  of at most ``C`` rows (and a byte budget).  Each chunk is one Kafka message
  and exactly one Neo4j transaction.  A unit's chunks form a chain: chunk
  ``i`` may only start after chunk ``i-1`` committed, so repeated rows of one
  pair keep their ``SET r += props`` (last row wins) order.
* **Priority.**  Longest-processing-time first on the busiest resource: with
  ``cost(chunk) = c0 + c1 * rows`` and ``work(r)`` the summed cost of every
  chunk using resource ``r``, units are ranked by ``max work(r)`` over their
  resources, then by their own cost (both descending).

The plan file lists every chunk (id, type, resources, rows, Kafka partition
and offset, predecessor, rank).  The run-scoped dispatcher hands chunks out
at run time (highest rank whose resources are all free); writers fetch the
chunk message by offset and write it as one transaction.  Every component
loads the same file and checks its SHA-256 digest.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Iterable, Mapping, Sequence
import zlib

from src.loader.mix_and_batch import canonical_endpoint_token


PLAN_FORMAT = "graph-loader-edge-plan/1"
CHUNK_MESSAGE_FORMAT = 1
DEFAULT_PLAN_BUCKETS = 8
DEFAULT_HUB_DEGREE = 5000
DEFAULT_CHUNK_ROWS = 5000
# Chunk payloads are ~100 bytes/row (canonical edge envelopes), so a
# 5000-row chunk is ~0.5 MB; the byte cap only guards unusual rows.
DEFAULT_MAX_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024
# Server cost model fitted from benchmark lock_waits.txt (seconds).
DEFAULT_COST_FIXED_SECONDS = 0.003
DEFAULT_COST_ROW_SECONDS = 15e-6
MAX_PLAN_BUCKETS = 4096
MAX_CHUNK_ROWS = 100_000
_LABEL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PLAN_ID = re.compile(r"[A-Za-z0-9_.-]{1,64}")


class EdgePlanError(ValueError):
    """A plan, plan file, or chunk message is invalid or inconsistent."""


# -- resource mapping ----------------------------------------------------------


def key_token(value: object) -> bytes:
    """Return the typed, process-independent byte token of one node key.

    Keys are the *normalized* endpoint values of
    :func:`~src.loader.edge_loader.normalize_edge_record` (``str``, ``int``,
    ``float``, ``datetime.date`` or aware ``datetime.datetime``, fixed per
    label by the schema).  Distinct values give distinct tokens (``"1"`` and
    ``1`` differ), and values that compare equal in Python give equal tokens
    in every process: ``-0.0`` and ``0.0`` share one token, and datetimes are
    tokenized by their UTC instant (``12:00+01:00 == 11:00Z``).  Mapping
    Python-equal keys to one resource is always safe; it can only make two
    nodes share a resource, never split one node over two.
    """
    kind = type(value)
    if kind is str:
        return b"s:" + value.encode("utf-8")
    if kind is int:
        return b"i:" + str(value).encode("ascii")
    if kind is float:
        return b"f:" + float.__repr__(value + 0.0).encode("ascii")
    if kind is date:
        return b"d:" + value.isoformat().encode("ascii")
    if kind is datetime:
        if value.utcoffset() is None:
            raise EdgePlanError("datetime keys must be timezone-aware")
        return b"t:" + value.astimezone(timezone.utc).isoformat().encode("ascii")
    return b"j:" + canonical_endpoint_token(value).encode("utf-8")


def decode_token(token: bytes) -> object:
    """Return the canonical key of a :func:`key_token` (``key_token(decode_token(t)) == t``).

    Canonical means Python-equal keys decode to one value: floats as
    ``float(repr)`` (``-0.0`` becomes ``0.0``) and datetimes in UTC.
    """
    kind, text = token[:2], token[2:]
    try:
        if kind == b"s:":
            return text.decode("utf-8")
        if kind == b"i:":
            return int(text)
        if kind == b"f:":
            return float(text)
        if kind == b"d:":
            return date.fromisoformat(text.decode("ascii"))
        if kind == b"t:":
            value = datetime.fromisoformat(text.decode("ascii"))
            if value.utcoffset() is not None:
                return value
    except (UnicodeDecodeError, ValueError):
        pass
    raise EdgePlanError(f"key token {token[:40]!r} is not a plannable key")


def encode_key(value: object) -> object:
    """Return the plan-file JSON form of one node key (typed for temporal keys)."""
    kind = type(value)
    if kind is str or kind is int:
        return value
    if kind is float:
        if not math.isfinite(value):
            raise EdgePlanError("float keys must be finite")
        return value
    if kind is date:
        return {"date": value.isoformat()}
    if kind is datetime:
        if value.utcoffset() is None:
            raise EdgePlanError("datetime keys must be timezone-aware")
        return {"datetime": value.isoformat()}
    raise EdgePlanError("node keys must be strings, integers, finite floats, dates, or aware datetimes")


def decode_key(value: object) -> object:
    """Inverse of :func:`encode_key`."""
    if type(value) in (str, int) or (type(value) is float and math.isfinite(value)):
        return value
    if isinstance(value, dict) and len(value) == 1:
        (kind, text), = value.items()
        try:
            if kind == "date" and isinstance(text, str):
                return date.fromisoformat(text)
            if kind == "datetime" and isinstance(text, str):
                parsed = datetime.fromisoformat(text)
                if parsed.utcoffset() is not None:
                    return parsed
        except ValueError:
            pass
    raise EdgePlanError(f"invalid plan key {value!r}")


def planned_bucket(value: object, bucket_count: int) -> int:
    """Return the stable bucket in ``[0, bucket_count)`` of one node key."""
    return zlib.crc32(key_token(value)) % bucket_count


def _validate_bucket_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_PLAN_BUCKETS:
        raise EdgePlanError(f"plan bucket count must be an integer from 1 through {MAX_PLAN_BUCKETS}")
    return value


def _validate_hub_key(value: object) -> object:
    encode_key(value)
    return value


class ResourceTable:
    """Deterministic ``node -> resource id`` map shared by planner, dispatcher and writers.

    Ids are dense: for each label in sorted order, its ``B`` buckets, then
    its hubs in token order.  ``resource(label, key)`` is a hub's own id when
    ``key`` is a hub of ``label``, else the label's bucket id.
    """

    def __init__(self, labels: Iterable[str], bucket_count: int,
                 hubs: Mapping[str, Iterable[object]] | None = None) -> None:
        self.bucket_count = _validate_bucket_count(bucket_count)
        label_set = set(labels)
        hubs = {label: list(keys) for label, keys in (hubs or {}).items()}
        unknown = set(hubs) - label_set
        if unknown:
            raise EdgePlanError(f"hub labels are not plan labels: {sorted(unknown)}")
        for label in label_set:
            if not isinstance(label, str) or not _LABEL.fullmatch(label):
                raise EdgePlanError(f"invalid plan label: {label!r}")
        self.labels = tuple(sorted(label_set))
        self.names: list[tuple[str, str, object]] = []
        self._base: dict[str, int] = {}
        self._hubs: dict[tuple[str, bytes], int] = {}
        for label in self.labels:
            self._base[label] = len(self.names)
            self.names.extend((label, "bucket", index) for index in range(self.bucket_count))
            tokens = {}
            for key in hubs.get(label, ()):
                token = key_token(_validate_hub_key(key))
                if token in tokens:
                    raise EdgePlanError(f"duplicate hub key for label {label}")
                tokens[token] = key
            for token in sorted(tokens):
                self._hubs[(label, token)] = len(self.names)
                self.names.append((label, "hub", tokens[token]))

    @property
    def count(self) -> int:
        return len(self.names)

    @property
    def hub_count(self) -> int:
        return len(self._hubs)

    def bucket_resource(self, label: str, bucket: int) -> int:
        return self._base[label] + bucket

    def hub_resource(self, label: str, key: object) -> int | None:
        return self._hubs.get((label, key_token(key)))

    def resource(self, label: str, key: object) -> int:
        """Return the resource id of the node ``(label, key)``."""
        token = key_token(key)
        hub = self._hubs.get((label, token))
        if hub is not None:
            return hub
        try:
            base = self._base[label]
        except KeyError:
            raise EdgePlanError(f"label {label!r} is not in the plan") from None
        return base + zlib.crc32(token) % self.bucket_count

    def label_of(self, resource: int) -> str:
        return self.names[resource][0]

    def describe(self, resource: int) -> str:
        label, kind, value = self.names[resource]
        return f"{label}:{value}" if kind == "bucket" else f"{label}#hub={value!r}"

    def to_json(self) -> dict:
        return {
            "buckets": self.bucket_count,
            "labels": list(self.labels),
            "hubs": {
                label: [encode_key(value) for name, kind, value in self.names if name == label and kind == "hub"]
                for label in self.labels
                if any(name == label and kind == "hub" for name, kind, _ in self.names)
            },
        }

    @classmethod
    def from_json(cls, payload: object) -> "ResourceTable":
        if not isinstance(payload, dict):
            raise EdgePlanError("plan resources must be an object")
        hubs = payload.get("hubs", {})
        if not isinstance(hubs, dict) or not all(isinstance(keys, list) for keys in hubs.values()):
            raise EdgePlanError("plan hubs must map labels to key lists")
        labels = payload.get("labels")
        if not isinstance(labels, list):
            raise EdgePlanError("plan resource labels must be a list")
        return cls(labels, payload.get("buckets"),
                   {label: [decode_key(key) for key in keys] for label, keys in hubs.items()})


def select_hubs(degrees: Mapping[str, Mapping[object, int]], threshold: int) -> dict[str, list[object]]:
    """Return, per label, the keys whose total degree is at least ``threshold``.

    ``threshold <= 0`` disables hubs.
    """
    if isinstance(threshold, bool) or not isinstance(threshold, int):
        raise EdgePlanError("hub degree threshold must be an integer")
    if threshold <= 0:
        return {}
    hubs = {}
    for label, counts in degrees.items():
        keys = [key for key, count in counts.items() if count >= threshold]
        if keys:
            hubs[label] = sorted(keys, key=key_token)
    return hubs


# -- units, chunks, priorities -------------------------------------------------


def split_sizes(count: int, max_rows: int) -> list[int]:
    """Split ``count`` rows into ``ceil(count / max_rows)`` near-equal chunk sizes."""
    if count <= 0:
        return []
    parts = -(-count // max_rows)
    base, extra = divmod(count, parts)
    return [base + 1 if index < extra else base for index in range(parts)]


def chunk_boundaries(sizes_bytes: Sequence[int], max_rows: int, max_bytes: int) -> list[tuple[int, int]]:
    """Return ``[start, end)`` row ranges of at most ``max_rows`` rows and ``max_bytes`` bytes.

    Rows are first split into near-equal counts; any range whose payload
    exceeds ``max_bytes`` is split greedily (a single larger row stays alone).
    """
    if max_rows < 1 or max_bytes < 1:
        raise EdgePlanError("chunk limits must be positive")
    ranges: list[tuple[int, int]] = []
    start = 0
    for size in split_sizes(len(sizes_bytes), max_rows):
        end = start + size
        used, left = 0, start
        for index in range(start, end):
            row_bytes = sizes_bytes[index] + 1
            if index > left and used + row_bytes > max_bytes:
                ranges.append((left, index))
                left, used = index, 0
            used += row_bytes
        ranges.append((left, end))
        start = end
    return ranges


def chunk_cost(rows: int, c0: float = DEFAULT_COST_FIXED_SECONDS, c1: float = DEFAULT_COST_ROW_SECONDS) -> float:
    return c0 + c1 * rows


@dataclass(frozen=True)
class ChunkSpec:
    """A chunk before ranking: its unit, position in the unit, and Kafka location."""

    key: str
    edge_type: str
    unit: tuple
    seq: int
    resources: tuple[int, ...]
    rows: int
    partition: int
    offset: int
    bytes: int = 0
    # v7-S7: the label pair (index into the type's pairs) this chunk writes; 0 for single-pair types.
    pair: int = 0


def rank_chunks(specs: Sequence[ChunkSpec], *, c0: float = DEFAULT_COST_FIXED_SECONDS,
                c1: float = DEFAULT_COST_ROW_SECONDS) -> list[int]:
    """Return each spec's dispatch rank (0 = first) by bottleneck-resource LPT.

    Units are ordered by the largest total work of any of their resources,
    then by their own cost, both descending (ties by unit key); a unit's
    chunks take consecutive ranks in chain order.
    """
    unit_cost: dict[tuple, float] = {}
    unit_resources: dict[tuple, tuple[int, ...]] = {}
    for spec in specs:
        unit_cost[spec.unit] = unit_cost.get(spec.unit, 0.0) + chunk_cost(spec.rows, c0, c1)
        known = unit_resources.setdefault(spec.unit, spec.resources)
        if known != spec.resources:
            raise EdgePlanError(f"unit {spec.unit} chunks disagree on resources")
    work: dict[int, float] = {}
    for unit, cost in unit_cost.items():
        for resource in unit_resources[unit]:
            work[resource] = work.get(resource, 0.0) + cost
    order = sorted(
        unit_cost,
        key=lambda unit: (
            -max(work[resource] for resource in unit_resources[unit]), -unit_cost[unit], repr(unit),
        ),
    )
    unit_rank = {unit: index for index, unit in enumerate(order)}
    indexed = sorted(range(len(specs)), key=lambda index: (unit_rank[specs[index].unit], specs[index].seq))
    ranks = [0] * len(specs)
    for rank, index in enumerate(indexed):
        ranks[index] = rank
    return ranks


def resource_pair(source: int, target: int) -> tuple[int, ...]:
    """Return a unit's sorted, de-duplicated resource set."""
    return (source,) if source == target else tuple(sorted((source, target)))


# -- the plan ------------------------------------------------------------------


@dataclass(frozen=True)
class PlannedChunk:
    id: int
    edge_type: str
    resources: tuple[int, ...]
    rows: int
    partition: int
    offset: int
    after: int
    rank: int
    key: str
    bytes: int = 0
    pair: int = 0  # v7-S7: index into the type's pairs (always 0 for a single-pair type)


@dataclass
class PlanPair:
    """One label pair of a multi-pair type in a plan (v7-S7): its topics and its share of the type."""

    source: str
    target: str
    topics: tuple[str, ...]
    rows: int
    chunks: int = 0
    units: int = 0

    @property
    def labels(self) -> tuple[str, str]:
        return (self.source, self.target)


@dataclass
class EdgeTypePlan:
    """A planned type. Single-pair: ``source_label``/``target_label`` set, ``pairs`` empty (the
    pre-S7 shape and JSON). Multi-pair (v7-S7): both labels ``None`` and ``pairs`` lists every pair
    in the schema's pair order; each chunk records its ``pair``."""

    edge_type: str
    source_label: str | None
    target_label: str | None
    rows: int
    chunks: int = 0
    units: int = 0
    pairs: tuple[PlanPair, ...] = ()

    @property
    def is_multi_pair(self) -> bool:
        return bool(self.pairs)

    def pair_labels(self, pair: int) -> tuple[str, str]:
        """``(source, target)`` of the type's pair ``pair``."""
        if self.pairs:
            if isinstance(pair, bool) or not isinstance(pair, int) or not 0 <= pair < len(self.pairs):
                raise EdgePlanError(f"edge type {self.edge_type} has no label pair {pair!r}")
            return self.pairs[pair].labels
        if pair != 0:
            raise EdgePlanError(f"edge type {self.edge_type} has one label pair; chunk pair is {pair!r}")
        return self.source_label, self.target_label


@dataclass
class EdgePlan:
    """A validated plan; ``chunks[i].id == i``."""

    plan_id: str
    topic: str
    partitions: int
    resources: ResourceTable
    types: dict[str, EdgeTypePlan]
    chunks: list[PlannedChunk]
    settings: dict = field(default_factory=dict)
    stats: dict = field(default_factory=dict)

    @property
    def total_rows(self) -> int:
        return sum(item.rows for item in self.types.values())

    def labels_of(self, chunk: PlannedChunk) -> tuple[str, str]:
        """The ``(source, target)`` labels ``chunk`` writes (v7-S7: its own pair's)."""
        return self.types[chunk.edge_type].pair_labels(chunk.pair)

    def validate(self) -> "EdgePlan":
        """Check every structural invariant the dispatcher and writers rely on."""
        if not isinstance(self.plan_id, str) or not _PLAN_ID.fullmatch(self.plan_id):
            raise EdgePlanError("plan id must be 1-64 letters, digits, dot, underscore, or hyphen")
        if not isinstance(self.topic, str) or not self.topic.strip():
            raise EdgePlanError("plan topic must be nonblank")
        if isinstance(self.partitions, bool) or not isinstance(self.partitions, int) or self.partitions < 1:
            raise EdgePlanError("plan partitions must be positive")
        labels = set(self.resources.labels)
        for name, item in self.types.items():
            if name != item.edge_type or not _LABEL.fullmatch(name):
                raise EdgePlanError(f"invalid plan edge type {name!r}")
            if isinstance(item.rows, bool) or not isinstance(item.rows, int) or item.rows < 0:
                raise EdgePlanError(f"edge type {name} row count must be nonnegative")
            if item.pairs:
                if item.source_label is not None or item.target_label is not None or len(item.pairs) < 2:
                    raise EdgePlanError(f"edge type {name}: a multi-pair type lists 2+ pairs and no top-level labels")
                if len({pair.labels for pair in item.pairs}) != len(item.pairs):
                    raise EdgePlanError(f"edge type {name} lists a label pair twice")
                pair_topics = [topic for pair in item.pairs for topic in pair.topics]
                if len(set(pair_topics)) != len(pair_topics):
                    raise EdgePlanError(f"edge type {name} lists a topic under two label pairs")
                for pair in item.pairs:
                    for value in (pair.rows, pair.chunks, pair.units):
                        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                            raise EdgePlanError(f"edge type {name} pair counts must be nonnegative integers")
                    if pair.source not in labels or pair.target not in labels:
                        raise EdgePlanError(f"edge type {name} endpoint labels are not plan labels")
                    if isinstance(pair.rows, bool) or not isinstance(pair.rows, int) or pair.rows < 0:
                        raise EdgePlanError(f"edge type {name} pair row count must be nonnegative")
                    if not pair.topics or not all(isinstance(topic, str) and topic for topic in pair.topics):
                        raise EdgePlanError(f"edge type {name} pair {pair.source}->{pair.target} names no topics")
                if sum(pair.rows for pair in item.pairs) != item.rows:
                    raise EdgePlanError(f"edge type {name} pair rows do not add up to its rows")
                if sum(pair.chunks for pair in item.pairs) != item.chunks:
                    raise EdgePlanError(f"edge type {name} pair chunks do not add up to its chunks")
            elif item.source_label not in labels or item.target_label not in labels:
                raise EdgePlanError(f"edge type {name} endpoint labels are not plan labels")
        rows_by_type = {name: 0 for name in self.types}
        chunks_by_type = {name: 0 for name in self.types}
        rows_by_pair: dict[tuple[str, int], int] = {}
        chunks_by_pair: dict[tuple[str, int], int] = {}
        locations: set[tuple[int, int]] = set()
        keys: set[str] = set()
        predecessors: set[int] = set()
        ranks = set()
        for index, chunk in enumerate(self.chunks):
            if chunk.id != index:
                raise EdgePlanError("plan chunk ids must be 0..n-1 in order")
            if chunk.edge_type not in self.types:
                raise EdgePlanError(f"chunk {index} has unknown edge type {chunk.edge_type}")
            if chunk.rows < 1:
                raise EdgePlanError(f"chunk {index} is empty")
            if not chunk.resources or list(chunk.resources) != sorted(set(chunk.resources)) or len(chunk.resources) > 2:
                raise EdgePlanError(f"chunk {index} resources must be 1-2 sorted distinct ids")
            if any(not 0 <= resource < self.resources.count for resource in chunk.resources):
                raise EdgePlanError(f"chunk {index} references an unknown resource")
            item = self.types[chunk.edge_type]
            chunk_labels = {self.resources.label_of(resource) for resource in chunk.resources}
            if chunk_labels != set(item.pair_labels(chunk.pair)):
                raise EdgePlanError(f"chunk {index} resources do not match {chunk.edge_type} endpoint labels")
            if not 0 <= chunk.partition < self.partitions or chunk.offset < 0:
                raise EdgePlanError(f"chunk {index} has an invalid Kafka location")
            if (chunk.partition, chunk.offset) in locations:
                raise EdgePlanError(f"chunk {index} shares a Kafka location with another chunk")
            locations.add((chunk.partition, chunk.offset))
            if chunk.key in keys:
                raise EdgePlanError(f"chunk {index} key is not unique")
            keys.add(chunk.key)
            if chunk.after != -1:
                if not 0 <= chunk.after < index:
                    raise EdgePlanError(f"chunk {index} predecessor must be an earlier chunk")
                previous = self.chunks[chunk.after]
                if (previous.edge_type != chunk.edge_type or previous.resources != chunk.resources
                        or previous.pair != chunk.pair):
                    raise EdgePlanError(f"chunk {index} predecessor is not in the same unit")
                if chunk.after in predecessors:
                    raise EdgePlanError(f"chunk {chunk.after} has two successors")
                predecessors.add(chunk.after)
            ranks.add(chunk.rank)
            rows_by_type[chunk.edge_type] += chunk.rows
            chunks_by_type[chunk.edge_type] += 1
            rows_by_pair[(chunk.edge_type, chunk.pair)] = rows_by_pair.get((chunk.edge_type, chunk.pair), 0) + chunk.rows
            chunks_by_pair[(chunk.edge_type, chunk.pair)] = chunks_by_pair.get((chunk.edge_type, chunk.pair), 0) + 1
        if ranks != set(range(len(self.chunks))):
            raise EdgePlanError("plan chunk ranks must be a permutation of 0..n-1")
        for name, item in self.types.items():
            if rows_by_type[name] != item.rows:
                raise EdgePlanError(
                    f"edge type {name} chunks hold {rows_by_type[name]} rows; plan declares {item.rows}"
                )
            if chunks_by_type[name] != item.chunks:
                raise EdgePlanError(f"edge type {name} chunk count does not match its chunks")
            for index, pair in enumerate(item.pairs):
                if rows_by_pair.get((name, index), 0) != pair.rows or chunks_by_pair.get((name, index), 0) != pair.chunks:
                    raise EdgePlanError(
                        f"edge type {name} pair {pair.source}->{pair.target} chunks do not match its rows/chunks"
                    )
        return self

    def to_json(self) -> dict:
        return {
            "format": PLAN_FORMAT,
            "plan_id": self.plan_id,
            "topic": self.topic,
            "partitions": self.partitions,
            "settings": self.settings,
            "resources": self.resources.to_json(),
            "types": {
                name: (
                    {
                        "source": item.source_label, "target": item.target_label,
                        "rows": item.rows, "chunks": item.chunks, "units": item.units,
                    }
                    if not item.pairs else {
                        # v7-S7: no top-level source/target, so a pre-S7 reader fails closed.
                        "pairs": [
                            {"source": pair.source, "target": pair.target, "topics": list(pair.topics),
                             "rows": pair.rows, "chunks": pair.chunks, "units": pair.units}
                            for pair in item.pairs
                        ],
                        "rows": item.rows, "chunks": item.chunks, "units": item.units,
                    }
                )
                for name, item in sorted(self.types.items())
            },
            "chunks": [
                [chunk.id, chunk.edge_type, list(chunk.resources), chunk.rows, chunk.partition,
                 chunk.offset, chunk.after, chunk.rank, chunk.key, chunk.bytes]
                + ([chunk.pair] if self.types[chunk.edge_type].pairs else [])
                for chunk in self.chunks
            ],
            "stats": self.stats,
        }

    @classmethod
    def from_json(cls, payload: object) -> "EdgePlan":
        if not isinstance(payload, dict) or payload.get("format") != PLAN_FORMAT:
            raise EdgePlanError(f"not a {PLAN_FORMAT} plan")
        try:
            types = {}
            for name, item in payload["types"].items():
                if "pairs" in item:
                    if not isinstance(item["pairs"], list) or not all(
                        isinstance(pair, dict) and isinstance(pair.get("topics"), list) for pair in item["pairs"]
                    ):
                        raise EdgePlanError(f"edge type {name} pairs are malformed")
                    pairs = tuple(
                        PlanPair(pair["source"], pair["target"], tuple(pair["topics"]), pair["rows"],
                                 pair.get("chunks", 0), pair.get("units", 0))
                        for pair in item["pairs"]
                    )
                    types[name] = EdgeTypePlan(name, None, None, item["rows"], item.get("chunks", 0),
                                               item.get("units", 0), pairs)
                else:
                    types[name] = EdgeTypePlan(name, item["source"], item["target"], item["rows"],
                                               item.get("chunks", 0), item.get("units", 0))
            chunks = []
            for entry in payload["chunks"]:
                multi = isinstance(entry, list) and len(entry) > 1 and isinstance(types.get(entry[1]), EdgeTypePlan) \
                    and types[entry[1]].is_multi_pair
                if not isinstance(entry, list) or len(entry) != (11 if multi else 10):
                    raise EdgePlanError(
                        "plan chunk entries must have 10 fields (11, with the pair, for a type of several "
                        "label pairs)"
                    )
                chunk_id, edge_type, resources, rows, partition, offset, after, rank, key, size = entry[:10]
                pair = entry[10] if multi else 0
                for value in (chunk_id, rows, partition, offset, after, rank, size, pair):
                    if isinstance(value, bool) or not isinstance(value, int):
                        raise EdgePlanError("plan chunk numeric fields must be integers")
                if not isinstance(resources, list) or not all(
                    isinstance(item, int) and not isinstance(item, bool) for item in resources
                ):
                    raise EdgePlanError("plan chunk resources must be integers")
                if not isinstance(edge_type, str) or not isinstance(key, str):
                    raise EdgePlanError("plan chunk type and key must be strings")
                chunks.append(PlannedChunk(chunk_id, edge_type, tuple(resources), rows, partition,
                                           offset, after, rank, key, size, pair))
            plan = cls(
                plan_id=payload["plan_id"], topic=payload["topic"], partitions=payload["partitions"],
                resources=ResourceTable.from_json(payload["resources"]), types=types, chunks=chunks,
                settings=dict(payload.get("settings") or {}), stats=dict(payload.get("stats") or {}),
            )
        except (KeyError, TypeError, AttributeError) as exc:
            raise EdgePlanError(f"malformed plan: {exc}") from exc
        return plan.validate()


def plan_digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_plan(plan: EdgePlan) -> bytes:
    return (json.dumps(plan.validate().to_json(), separators=(",", ":"), sort_keys=False) + "\n").encode("utf-8")


def write_plan_file(path: str | os.PathLike, plan: EdgePlan) -> str:
    """Atomically write ``plan`` (fsync'd); return its digest."""
    data = encode_plan(plan)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".edge-plan-", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, target)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return plan_digest(data)


def read_plan_file(path: str | os.PathLike) -> tuple[EdgePlan, str]:
    """Load and validate a plan file; return ``(plan, sha256 digest)``."""
    data = Path(path).read_bytes()
    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EdgePlanError(f"plan file is not JSON: {exc}") from exc
    return EdgePlan.from_json(payload), plan_digest(data)


def plan_failure_marker(path: str | os.PathLike) -> Path:
    """Return the file a failed planner writes instead of the plan."""
    target = Path(path)
    return target.with_name(target.name + ".failed")


def plan_progress_path(path: str | os.PathLike) -> Path:
    """Return the heartbeat file a running planner rewrites whenever it made progress."""
    target = Path(path)
    return target.with_name(target.name + ".progress")


def assemble_plan(
    *, plan_id: str, topic: str, partitions: int, resources: ResourceTable,
    types: Mapping[str, tuple], specs: Sequence[ChunkSpec],
    settings: Mapping | None = None, stats: Mapping | None = None,
    c0: float = DEFAULT_COST_FIXED_SECONDS, c1: float = DEFAULT_COST_ROW_SECONDS,
) -> EdgePlan:
    """Rank ``specs``, assign ids in rank order, chain each unit, and validate.

    ``types`` maps every planned edge type (including empty ones) to
    ``(source label, target label, published rows)``, or, for a type of several label pairs
    (v7-S7), to ``(pairs, rows_by_pair)`` with ``pairs`` a sequence of ``(source, target, topics)``.
    """
    ranks = rank_chunks(specs, c0=c0, c1=c1)
    order = sorted(range(len(specs)), key=ranks.__getitem__)
    chunks: list[PlannedChunk] = []
    last_in_unit: dict[tuple, tuple[int, int]] = {}
    for chunk_id, index in enumerate(order):
        spec = specs[index]
        previous = last_in_unit.get(spec.unit)
        if previous is not None and previous[1] >= spec.seq:
            raise EdgePlanError(f"unit {spec.unit} chunk sequence is not increasing")
        chunks.append(PlannedChunk(
            chunk_id, spec.edge_type, tuple(spec.resources), spec.rows, spec.partition, spec.offset,
            -1 if previous is None else previous[0], ranks[index], spec.key, spec.bytes, spec.pair,
        ))
        last_in_unit[spec.unit] = (chunk_id, spec.seq)
    type_plans: dict[str, EdgeTypePlan] = {}
    for name, value in types.items():
        if len(value) == 3:
            source, target, rows = value
            type_plans[name] = EdgeTypePlan(name, source, target, rows)
        else:
            pairs, rows_by_pair = value
            pair_plans = tuple(PlanPair(source, target, tuple(topics), rows)
                               for (source, target, topics), rows in zip(pairs, rows_by_pair, strict=True))
            type_plans[name] = EdgeTypePlan(name, None, None, sum(rows_by_pair), pairs=pair_plans)
    units: dict[str, set] = {name: set() for name in type_plans}
    pair_units: dict[tuple[str, int], set] = {}
    for spec in specs:
        if spec.edge_type not in type_plans:
            raise EdgePlanError(f"chunk of unplanned edge type {spec.edge_type}")
        item = type_plans[spec.edge_type]
        item.chunks += 1
        units[spec.edge_type].add(spec.unit)
        if item.pairs:
            item.pair_labels(spec.pair)  # range check
            item.pairs[spec.pair].chunks += 1
            pair_units.setdefault((spec.edge_type, spec.pair), set()).add(spec.unit)
    for name, item in type_plans.items():
        item.units = len(units[name])
        for index, pair in enumerate(item.pairs):
            pair.units = len(pair_units.get((name, index), ()))
    plan = EdgePlan(
        plan_id=plan_id, topic=topic, partitions=partitions, resources=resources, types=type_plans,
        chunks=chunks, settings=dict(settings or {}), stats=dict(stats or {}),
    )
    return plan.validate()


# -- chunk messages -------------------------------------------------------------


def encode_chunk_message(plan_run: str, key: str, edge_type: str, records: Sequence[str | bytes]) -> bytes:
    """Return one chunk message: a header plus the record JSON texts.

    ``records`` are exactly the JSON objects the per-type topic carries
    (``{"source": {...}, "target": {...}, "properties": {...}}``), as ``str``
    or UTF-8 ``bytes`` (e.g. Kafka message values, embedded verbatim).
    """
    header = json.dumps(
        {"format": CHUNK_MESSAGE_FORMAT, "run": plan_run, "chunk": key, "type": edge_type, "count": len(records)},
        separators=(",", ":"),
    ).encode("ascii")
    body = b",".join(record if type(record) is bytes else record.encode("utf-8") for record in records)
    return header[:-1] + b',"records":[' + body + b"]}"


def encode_chunk_message_parts(plan_run: str, key: str, edge_type: str, count: int, parts: Sequence) -> bytes:
    """:func:`encode_chunk_message` of ``count`` records given as buffers of comma-terminated records.

    ``b"".join(parts)`` must be the records' JSON texts, each followed by one
    comma; the result is byte-identical to :func:`encode_chunk_message`.
    """
    header = json.dumps(
        {"format": CHUNK_MESSAGE_FORMAT, "run": plan_run, "chunk": key, "type": edge_type, "count": count},
        separators=(",", ":"),
    ).encode("ascii")
    body = b"".join(parts)
    if count and body[-1:] != b",":
        raise EdgePlanError("chunk record buffers must end with a comma")
    return b"".join((header[:-1], b',"records":[', memoryview(body)[:-1] if count else b"", b"]}"))


def decode_chunk_message(value: bytes | None) -> tuple[dict, list]:
    """Decode a chunk message into ``(header, records)``; malformed raises."""
    if value is None:
        raise EdgePlanError("chunk message is empty")
    try:
        payload = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EdgePlanError(f"chunk message is not JSON: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("format") != CHUNK_MESSAGE_FORMAT:
        raise EdgePlanError("chunk message has an unknown format")
    records = payload.pop("records", None)
    if not isinstance(records, list) or payload.get("count") != len(records):
        raise EdgePlanError("chunk message record count does not match its header")
    return payload, records
