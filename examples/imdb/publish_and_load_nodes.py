#!/usr/bin/env python3
"""Publish an IMDb graph subset's node and relationship TSVs for bulk loading."""
from __future__ import annotations

import argparse
import logging
import csv
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import json.encoder
import math
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import zlib

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import (
    AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewPartitions, NewTopic,
)
from dotenv import load_dotenv
import yaml

# Permit the documented ``python examples/imdb/publish_and_load_nodes.py``
# invocation without requiring callers to export PYTHONPATH first.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.schema import EdgeConfig, PropertyConfig
from src.loader.edge_planner import chunk_topic_config
from examples.imdb.planned_edges import (
    DEFAULT_PLAN_TOPIC, DEFAULT_PLAN_WRITERS, PlanSettings, imdb_edge_planner,
)
from src.orchestrator.auto_settings import default_plan_writers, derive_plan_parameters, probe_neo4j_cores
from src.orchestrator.docker_service import DockerService
from src.utils.schema_loader import load_schema

# Full IMDb exports contain quoted fields longer than csv's 128 KiB default.
csv.field_size_limit(sys.maxsize)


SCHEMA_PATH = Path(__file__).with_name("graph_schema.yaml")
NODE_SOURCES = {
    "title": ("imdb-title", "title.tsv"),
    "person": ("imdb-person", "person.tsv"),
    "genre": ("imdb-genre", "genre.tsv"),
    "profession": ("imdb-profession", "profession.tsv"),
    "character": ("imdb-character", "character.tsv"),
    "title_type": ("imdb-title-type", "title_type.tsv"),
    "region": ("imdb-region", "region.tsv"),
    "language": ("imdb-language", "language.tsv"),
    "alternative_title": ("imdb-alternative-title", "alternative_title.tsv"),
    "rating": ("imdb-rating", "rating.tsv"),
}

# (Kafka topic, TSV filename, source-column, target-column).  The column
# values become the endpoint keys declared by graph_schema.yaml; remaining
# schema-declared fields become relationship properties.
EDGE_SOURCES = {
    "HAS_GENRE": ("imdb-has-genre", "has_genre.tsv", "title_id", "genre"),
    "HAS_TITLE_TYPE": ("imdb-has-title-type", "has_title_type.tsv", "title_id", "title_type"),
    "HAS_PROFESSION": ("imdb-has-profession", "has_profession.tsv", "person_id", "profession"),
    "KNOWN_FOR": ("imdb-known-for", "known_for.tsv", "person_id", "title_id"),
    "CREDITED_ON": ("imdb-credited-on", "credited_on.tsv", "person_id", "title_id"),
    "ACTED_IN": ("imdb-acted-in", "acted_in.tsv", "person_id", "title_id"),
    "PLAYED": ("imdb-played", "played.tsv", "person_id", "character"),
    "DIRECTED": ("imdb-directed", "directed.tsv", "person_id", "title_id"),
    "WROTE": ("imdb-wrote", "wrote.tsv", "person_id", "title_id"),
    "EPISODE_OF": ("imdb-episode-of", "episode_of.tsv", "episode_id", "series_id"),
    "HAS_ALTERNATIVE_TITLE": ("imdb-has-alternative-title", "has_alternative_title.tsv", "title_id", "aka_id"),
    "AVAILABLE_IN_REGION": ("imdb-available-in-region", "available_in_region.tsv", "aka_id", "region"),
    "IN_LANGUAGE": ("imdb-in-language", "in_language.tsv", "aka_id", "language"),
    "HAS_RATING": ("imdb-has-rating", "has_rating.tsv", "title_id", "rating_id"),
}


@dataclass(frozen=True)
class WriteTimingSummary:
    batches: int = 0
    records: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0


@dataclass(frozen=True)
class EdgeStageTiming:
    edge_type: str
    started_utc: str
    completed_utc: str
    elapsed_seconds: float
    succeeded: bool


def planned_timing_key(topic: str, edge_type: str) -> str:
    """Write-timing key of one edge type's chunks in the planned topic."""
    return f"{topic}#{edge_type}"


def read_write_timings(directory: Path) -> dict[str, WriteTimingSummary]:
    """Sum successful Neo4j batch timings captured by the loader replicas.

    Planned-mode writes (``planned=1``) share one topic, so they are keyed
    per edge type with :func:`planned_timing_key`.
    """
    summaries: dict[str, WriteTimingSummary] = {}
    for path in sorted(directory.glob("*.log")):
        if not path.name.startswith(("node-", "edge-")):
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            if fields.get("stage") not in {"node-write", "edge-write"}:
                continue
            try:
                topic = fields["topic"]
                if fields.get("planned") == "1" and fields.get("edge"):
                    topic = planned_timing_key(topic, fields["edge"])
                records = int(fields["records"])
                write_ms = float(fields["write_ms"])
            except (KeyError, ValueError) as exc:
                raise ValueError(f"invalid write timing in {path}: {line}") from exc
            if records < 0 or not math.isfinite(write_ms) or write_ms < 0:
                raise ValueError(f"invalid write timing in {path}: {line}")
            previous = summaries.get(topic, WriteTimingSummary())
            summaries[topic] = WriteTimingSummary(
                previous.batches + 1,
                previous.records + records,
                previous.total_ms + write_ms,
                max(previous.max_ms, write_ms),
            )
    return summaries


def read_edge_stage_timings(directory: Path) -> dict[str, EdgeStageTiming]:
    """Read scheduler wall times for each finite bulk edge stage."""
    timings: dict[str, EdgeStageTiming] = {}
    for path in sorted(directory.glob("stage-*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        timing = EdgeStageTiming(
            edge_type=payload["edge_type"],
            started_utc=payload["started_utc"],
            completed_utc=payload["completed_utc"],
            elapsed_seconds=float(payload["elapsed_seconds"]),
            succeeded=payload["succeeded"],
        )
        timings[timing.edge_type] = timing
    return timings


def read_edge_substage_timings(directory: Path) -> dict[str, list[tuple[int, str, EdgeStageTiming]]]:
    """Read per-topic precedence sub-stage rows (``substage-<type>.t<rank>.json``), by edge type.

    They are kept apart from the type's own ``stage-<type>.json`` row, which carries the type total.
    """
    rows: dict[str, list[tuple[int, str, EdgeStageTiming]]] = {}
    for path in sorted(directory.glob("substage-*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        timing = EdgeStageTiming(
            edge_type=payload["edge_type"],
            started_utc=payload["started_utc"],
            completed_utc=payload["completed_utc"],
            elapsed_seconds=float(payload["elapsed_seconds"]),
            succeeded=payload["succeeded"],
        )
        rows.setdefault(timing.edge_type, []).append((int(payload["rank"]), str(payload["topic"]), timing))
    return {edge_type: sorted(items, key=lambda item: item[0]) for edge_type, items in rows.items()}


def read_metrics_snapshots(directory: Path) -> dict[str, float]:
    """Sum every ``metrics-*.prom`` snapshot's samples across every replica.

    P6-S2 Story 9: each process kind writes its final metric values to
    ``metrics-<kind>-<name-or-replica-id>.prom`` in the run's timing
    directory on exit (normal or SIGTERM) when
    ``GRAPH_LOADER_METRICS_SNAPSHOT_PATH``/``GRAPH_LOADER_TIMING_DIR`` is
    set - the durable record for a run shorter than one Prometheus scrape
    interval. Parses with ``prometheus_client``'s own text parser (already
    a transitive dependency of this repo - no new one) and sums every
    sample by its full metric name (e.g.
    ``graph_loader_records_consumed_total``) across every replica's file -
    deliberately coarse (not broken out by label), since this is a
    run-report *total*, not a live per-label dashboard. Returns an empty
    dict, not an error, when no snapshot files exist (e.g.
    ``GRAPH_LOADER_METRICS_ENABLED=0`` for the whole run).
    """
    from prometheus_client.parser import text_string_to_metric_families

    totals: dict[str, float] = {}
    for path in sorted(directory.glob("metrics-*.prom")):
        try:
            text = path.read_text(encoding="utf-8")
            families = list(text_string_to_metric_families(text))
        except (OSError, ValueError) as exc:
            # A hand-edited, foreign, or partially copied file must not
            # crash the whole report over this optional section (P6-S2
            # review finding S9-2) - text_string_to_metric_families raises
            # ValueError on malformed/truncated exposition text.
            print(f"warning: skipping unparseable metrics snapshot {path}: {exc}")
            continue
        for family in families:
            for sample in family.samples:
                totals[sample.name] = totals.get(sample.name, 0.0) + sample.value
    return totals


# The CLI's own graceful stop (bounded container drain) takes about 5-10 s;
# allow it a generous margin before the child is killed.
CHILD_STOP_TIMEOUT_SECONDS = 60.0


class ChildSignalForwarder:
    """Forward SIGINT/SIGTERM to the loader CLI child and wait for it to stop.

    The CLI installs its own idempotent handlers (the first signal starts a
    graceful stop; a second one changes nothing), so every signal received is
    forwarded as-is and never escalates.  Only if the child is still alive
    ``timeout`` seconds after the first signal is it killed.  Off the main
    thread ``signal.signal`` is unavailable and nothing is installed.
    """

    def __init__(self, process: subprocess.Popen, timeout: float = CHILD_STOP_TIMEOUT_SECONDS) -> None:
        self.process = process
        self.timeout = timeout
        self.first_signal_at: float | None = None
        self._previous: list[tuple[int, object]] = []

    def __enter__(self) -> "ChildSignalForwarder":
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                self._previous.append((signum, signal.signal(signum, self._forward)))
        return self

    def __exit__(self, *_exc) -> None:
        for signum, previous in self._previous:
            signal.signal(signum, previous)
        self._previous.clear()

    def _forward(self, signum: int, _frame=None) -> None:
        if self.first_signal_at is None:
            self.first_signal_at = time.monotonic()
        if self.process.poll() is None:
            try:
                self.process.send_signal(signum)
            except ProcessLookupError:
                pass

    def wait(self) -> int:
        """Return the child's exit code, killing it if a stop overruns ``timeout``."""
        while True:
            try:
                return self.process.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                if self.first_signal_at is not None and time.monotonic() - self.first_signal_at > self.timeout:
                    print(f"warning: loader CLI did not stop within {self.timeout:.0f}s of the signal; killing it")
                    self.process.kill()
                    return self.process.wait()


def start_bulk_loader(
    command: list[str], environment: dict[str, str], timing_directory: Path,
) -> subprocess.Popen:
    """Start the loader CLI in the background with its timing directory."""
    timing_directory.mkdir(parents=True, exist_ok=True)
    environment = {**environment, "GRAPH_LOADER_TIMING_DIR": str(timing_directory)}
    return subprocess.Popen(command, cwd=PROJECT_ROOT, env=environment)


def ensure_plan_topic(bootstrap_servers: str, topic: str, partitions: int, max_message_bytes: int) -> None:
    """Create the planned chunk topic (or check it) with a large ``max.message.bytes``."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=20)
    # Unlimited retention (the broker default could delete chunks of a long run), large segments.
    config = {**chunk_topic_config(max_message_bytes), "max.message.bytes": str(max_message_bytes)}
    if topic not in metadata.topics:
        futures = admin.create_topics([
            NewTopic(topic, num_partitions=partitions, replication_factor=1, config=config),
        ])
        for future in futures.values():
            future.result(20)
        print(f"Created planned edge topic {topic} with {partitions} partition(s), max.message.bytes={max_message_bytes}")
        return
    existing = len(metadata.topics[topic].partitions)
    if existing != partitions:
        raise ValueError(
            f"planned edge topic {topic} has {existing} partition(s); requested {partitions}. "
            "Delete it (scripts/cleanup_imdb_local_load.sh) before retrying."
        )
    resource = ConfigResource("topic", topic, incremental_configs=[
        ConfigEntry(name, value, incremental_operation=AlterConfigOpType.SET) for name, value in config.items()
    ])
    for future in admin.incremental_alter_configs([resource]).values():
        future.result(20)


def run_bulk_loader_with_timings(
    command: list[str], environment: dict[str, str], report_directory: Path,
) -> tuple[int, dict[str, WriteTimingSummary], Path]:
    """Run the loader and retain its Docker-mounted logs beside the report."""
    report_directory.mkdir(parents=True, exist_ok=True)
    timing_directory = Path(tempfile.mkdtemp(prefix="imdb-write-timings-", dir=report_directory))
    environment = {**environment, "GRAPH_LOADER_TIMING_DIR": str(timing_directory)}
    process = subprocess.Popen(command, cwd=PROJECT_ROOT, env=environment)
    with ChildSignalForwarder(process) as forwarder:
        returncode = forwarder.wait()
    return returncode, read_write_timings(timing_directory), timing_directory


def ensure_topics(bootstrap_servers: str, topics: list[str], partitions: int) -> None:
    """Create or expand node topics to the requested partition count.

    Kafka can only increase a topic's partition count.  Refuse a smaller
    requested count rather than silently producing a topology the caller did
    not ask for.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=20)
    missing = [topic for topic in topics if topic not in metadata.topics]
    if missing:
        futures = admin.create_topics([
            NewTopic(topic, num_partitions=partitions, replication_factor=1) for topic in missing
        ])
        for topic, future in futures.items():
            future.result(20)
            print(f"Created Kafka topic {topic} with {partitions} partition(s)")

    too_large = [
        topic for topic in topics
        if topic in metadata.topics and len(metadata.topics[topic].partitions) > partitions
    ]
    if too_large:
        raise ValueError(
            "cannot reduce existing Kafka topic partitions; requested "
            f"{partitions}, but these topics have more: {', '.join(too_large)}"
        )
    expandable = [
        topic for topic in topics
        if topic in metadata.topics and len(metadata.topics[topic].partitions) < partitions
    ]
    if expandable:
        futures = admin.create_partitions([
            NewPartitions(topic, new_total_count=partitions) for topic in expandable
        ])
        for topic, future in futures.items():
            future.result(20)
            print(f"Expanded Kafka topic {topic} to {partitions} partition(s)")


def require_empty_topics(bootstrap_servers: str, topics: list[str]) -> None:
    """Refuse a new finite load if a fresh consumer group would replay old data."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "imdb-example-topic-preflight",
        "enable.auto.commit": False,
    })
    try:
        deadline = time.monotonic() + 30
        while True:
            metadata = consumer.list_topics(timeout=20)
            unavailable = [
                topic for topic in topics
                if topic not in metadata.topics or metadata.topics[topic].error is not None
            ]
            if not unavailable:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"cannot inspect Kafka topic {unavailable[0]}")
            time.sleep(0.25)
        occupied: list[str] = []
        for topic in topics:
            topic_metadata = metadata.topics.get(topic)
            retained = 0
            for partition in topic_metadata.partitions:
                while True:
                    try:
                        low, high = consumer.get_watermark_offsets(
                            TopicPartition(topic, partition), timeout=5,
                        )
                        break
                    except KafkaException as exc:
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"cannot read Kafka topic {topic} partition {partition} watermark") from exc
                        time.sleep(0.25)
                retained += high - low
            if retained:
                occupied.append(f"{topic} ({retained} record(s))")
        if occupied:
            raise ValueError(
                "IMDb Kafka topics already contain data: " + ", ".join(occupied)
                + ". Clear the IMDb input topics on the configured Kafka broker before retrying."
            )
    finally:
        consumer.close()


def typed_payload(row: dict[str, str | None], properties: dict[str, PropertyConfig]) -> dict[str, object]:
    """Convert TSV text to the exact scalar types required by the node schema."""
    payload: dict[str, object] = {}
    for field, value in row.items():
        if value in (None, ""):
            continue
        property_config = properties[field]
        try:
            if property_config.type == "integer":
                payload[field] = int(value)
            elif property_config.type == "float":
                payload[field] = float(value)
            else:
                # Dates and datetimes are already ISO-8601 strings, which is
                # the loader's deliberate Kafka-wire representation.
                payload[field] = value
        except ValueError as exc:
            raise ValueError(f"invalid {property_config.type} value for field '{field}': {value!r}") from exc
    # The main project already has a non-null :Person.name constraint.  Preserve
    # IMDb's explicit ``primary_name`` while supplying the compatible alias.
    if "name" in properties and "name" not in payload and "primary_name" in payload:
        payload["name"] = payload["primary_name"]
    return payload


# ---------------------------------------------------------------------------
# Fast row encoding.
#
# ``typed_payload``/``edge_payload`` + ``json.dumps`` define the wire format.
# The encoders below produce byte-identical JSON text directly from
# ``csv.reader`` rows (no per-row dict, no pydantic attribute lookups).  Any
# row they do not recognise as the simple case (width mismatch, empty key,
# conversion error, ...) returns ``None`` or raises, and the caller re-encodes
# it through the original reference path so values and errors stay exact.
# ---------------------------------------------------------------------------

_encode_json_string = json.encoder.encode_basestring_ascii


def _json_float(value: float) -> str:
    """Match ``json.dumps`` float rendering (``allow_nan=True``)."""
    if value != value:
        return "NaN"
    if value == math.inf:
        return "Infinity"
    if value == -math.inf:
        return "-Infinity"
    return float.__repr__(value)


def _json_integer(value: str) -> str:
    return int.__repr__(int(value))


def _json_float_text(value: str) -> str:
    return _json_float(float(value))


def _json_scalar_encoder(property_type: str):
    """Return ``text -> JSON text`` matching ``typed_payload`` then ``json.dumps``."""
    if property_type == "integer":
        return _json_integer
    if property_type == "float":
        return _json_float_text
    return _encode_json_string


def _key_text(property_type: str):
    """Return ``text -> str(typed value)`` (``None`` means identity)."""
    if property_type == "integer":
        return lambda value: str(int(value))
    if property_type == "float":
        return lambda value: str(float(value))
    return None


def _unknown_field(field: str):
    def fail(_value: str) -> str:
        raise KeyError(field)
    return fail


def _dict_row(fieldnames: list[str], row: list[str]) -> dict[str | None, object]:
    """Rebuild exactly what ``csv.DictReader`` would yield for ``row``."""
    mapped: dict[str | None, object] = dict(zip(fieldnames, row))
    if len(fieldnames) < len(row):
        mapped[None] = row[len(fieldnames):]
    elif len(fieldnames) > len(row):
        for field in fieldnames[len(row):]:
            mapped[field] = None
    return mapped


def shard_of(key: str, shard_count: int) -> int:
    """Deterministic, process-independent shard for a Kafka message key.

    Every record for one key lands in one shard, so splitting a topic across
    producers keeps each key's records in their original file order.
    """
    return zlib.crc32(key.encode("utf-8")) % shard_count


class NodeRowEncoder:
    """Encode simple node rows to ``(key, json)`` without building dicts."""

    def __init__(self, fieldnames: list[str], properties: dict[str, PropertyConfig], key_property: str) -> None:
        self.width = len(fieldnames)
        self.columns = [
            (
                index,
                _encode_json_string(field) + ": ",
                _json_scalar_encoder(properties[field].type) if field in properties else _unknown_field(field),
            )
            for index, field in enumerate(fieldnames)
        ]
        # ``typed_payload`` appends ``name`` from ``primary_name`` when absent.
        self.alias_index = None
        self.alias_encoder = None
        self.name_index = fieldnames.index("name") if "name" in fieldnames else None
        if "name" in properties and "primary_name" in fieldnames and "primary_name" in properties:
            self.alias_index = fieldnames.index("primary_name")
            self.alias_encoder = _json_scalar_encoder(properties["primary_name"].type)
        self.key_index = fieldnames.index(key_property) if key_property in fieldnames else None
        self.key_cast = _key_text(properties[key_property].type) if key_property in properties else None

    def key(self, row: list[str]) -> str | None:
        """The Kafka key for ``row`` when it comes straight from the key column."""
        index = self.key_index
        if index is None or index >= len(row) or not row[index]:
            return None
        value = row[index]
        return value if self.key_cast is None else self.key_cast(value)

    def encode(self, row: list[str]) -> tuple[str, str] | None:
        if len(row) != self.width:
            return None
        key = self.key(row)
        if key is None:
            return None
        parts = [prefix + encode(row[index]) for index, prefix, encode in self.columns if row[index]]
        alias_index = self.alias_index
        if alias_index is not None and row[alias_index] and (self.name_index is None or not row[self.name_index]):
            parts.append('"name": ' + self.alias_encoder(row[alias_index]))
        return key, "{" + ", ".join(parts) + "}"


class EdgeRowEncoder:
    """Encode simple relationship rows to ``(key, json)`` without building dicts."""

    def __init__(self, fieldnames: list[str], edge: EdgeConfig, source_column: str, target_column: str) -> None:
        self.width = len(fieldnames)
        self.source_index = fieldnames.index(source_column) if source_column in fieldnames else None
        self.target_index = fieldnames.index(target_column) if target_column in fieldnames else None
        self.source_prefix = '{"source": {' + _encode_json_string(edge.source_key_property) + ": "
        self.target_prefix = '}, "target": {' + _encode_json_string(edge.target_key_property) + ": "
        # ``edge_payload`` emits properties in schema order, not column order.
        self.properties = []
        self.usable = self.source_index is not None and self.target_index is not None
        for name, config in edge.properties.items():
            if name not in fieldnames:
                if config.required:
                    self.usable = False
                continue
            self.properties.append((
                fieldnames.index(name), _encode_json_string(name) + ": ",
                _json_scalar_encoder(config.type), config.required,
            ))

    def key(self, row: list[str]) -> str | None:
        index = self.source_index
        if index is None or index >= len(row) or not row[index]:
            return None
        return row[index]

    def encode(self, row: list[str]) -> tuple[str, str] | None:
        if not self.usable or len(row) != self.width:
            return None
        source, target = row[self.source_index], row[self.target_index]
        if not source or not target:
            return None
        parts = []
        for index, prefix, encode, required in self.properties:
            value = row[index]
            if value:
                parts.append(prefix + encode(value))
            elif required:
                return None
        return source, (
            self.source_prefix + _encode_json_string(source) + self.target_prefix
            + _encode_json_string(target) + '}, "properties": {' + ", ".join(parts) + "}}"
        )


_POLL_EVERY = 10_000


def _produce_rows(producer: Producer, topic: str, encoded_rows) -> int:
    """Produce ``(key, value)`` text pairs, polling in bulk instead of per record."""
    count = 0
    produce, poll = producer.produce, producer.poll
    for key, value in encoded_rows:
        key_bytes, value_bytes = key.encode("utf-8"), value.encode("utf-8")
        while True:
            try:
                produce(topic, key=key_bytes, value=value_bytes)
                break
            except BufferError:
                # Local queue full: serve delivery reports and let librdkafka drain.
                poll(0.05)
        count += 1
        if count % _POLL_EVERY == 0:
            poll(0)
    outstanding = producer.flush(60)
    if outstanding:
        raise RuntimeError(f"Kafka did not deliver {outstanding} records for {topic}")
    return count


def _tsv_rows(source: Path, shard_index: int, shard_count: int, key_of):
    """Yield ``(line_number, fieldnames, row)`` for this shard's rows.

    Line numbers match the original ``enumerate(csv.DictReader(...), start=2)``
    (blank rows are skipped and not counted, exactly like ``DictReader``).
    """
    with source.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.reader(input_file, delimiter="\t")
        fieldnames = next(reader, None)
        if fieldnames is None:
            return
        line_number = 1
        for row in reader:
            if not row:
                continue
            line_number += 1
            if shard_count > 1:
                try:
                    key = key_of(row)
                except Exception:
                    key = None
                # Rows without a direct key are rare error/alias cases; keep them
                # in shard 0 so they are still encoded (or rejected) exactly once.
                if (0 if key is None else shard_of(key, shard_count)) != shard_index:
                    continue
            yield line_number, fieldnames, row


def publish_tsv(
    producer: Producer,
    topic: str,
    source: Path,
    properties: dict[str, PropertyConfig],
    key_property: str,
    *,
    shard_index: int = 0,
    shard_count: int = 1,
) -> int:
    """Publish one graph-node TSV (or one key-hash shard of it) as schema-typed JSON records."""
    with source.open("r", encoding="utf-8", newline="") as header_file:
        fieldnames = next(csv.reader(header_file, delimiter="\t"), None) or []
    encoder = NodeRowEncoder(fieldnames, properties, key_property) if len(set(fieldnames)) == len(fieldnames) else None

    def key_of(row: list[str]) -> str | None:
        return encoder.key(row) if encoder is not None else None

    def encoded():
        for line_number, names, row in _tsv_rows(source, shard_index, shard_count, key_of):
            result = None
            if encoder is not None:
                try:
                    result = encoder.encode(row)
                except Exception:
                    result = None
            if result is None:
                try:
                    payload = typed_payload(_dict_row(names, row), properties)
                except ValueError as exc:
                    raise ValueError(f"{source}:{line_number}: {exc}") from exc
                result = str(payload[key_property]), json.dumps(payload)
            yield result

    return _produce_rows(producer, topic, encoded())


def edge_payload(
    row: dict[str, str | None], edge: EdgeConfig, *, source_column: str, target_column: str,
) -> dict[str, object]:
    """Convert one generated relationship TSV row to the public edge wire format."""
    source, target = row.get(source_column), row.get(target_column)
    if not source or not target:
        raise ValueError(f"missing endpoint columns '{source_column}' or '{target_column}'")
    properties = typed_payload(
        {name: row.get(name) for name in edge.properties}, edge.properties,
    )
    missing = [name for name, config in edge.properties.items() if config.required and name not in properties]
    if missing:
        raise ValueError(f"missing required relationship properties: {', '.join(sorted(missing))}")
    return {
        "source": {edge.source_key_property: source},
        "target": {edge.target_key_property: target},
        "properties": properties,
    }


def publish_edge_tsv(
    producer: Producer, topic: str, source: Path, edge: EdgeConfig, *, source_column: str, target_column: str,
    shard_index: int = 0, shard_count: int = 1,
) -> int:
    """Publish one generated relationship TSV (or one key-hash shard of it) as canonical edge events."""
    with source.open("r", encoding="utf-8", newline="") as header_file:
        fieldnames = next(csv.reader(header_file, delimiter="\t"), None) or []
    encoder = (
        EdgeRowEncoder(fieldnames, edge, source_column, target_column)
        if len(set(fieldnames)) == len(fieldnames) else None
    )

    def key_of(row: list[str]) -> str | None:
        if encoder is not None:
            return encoder.key(row)
        value = _dict_row(fieldnames, row).get(source_column)
        return value if isinstance(value, str) and value else None

    def encoded():
        for line_number, names, row in _tsv_rows(source, shard_index, shard_count, key_of):
            result = None
            if encoder is not None:
                try:
                    result = encoder.encode(row)
                except Exception:
                    result = None
            if result is None:
                mapped = _dict_row(names, row)
                try:
                    payload = edge_payload(mapped, edge, source_column=source_column, target_column=target_column)
                except ValueError as exc:
                    raise ValueError(f"{source}:{line_number}: {exc}") from exc
                result = str(mapped[source_column]), json.dumps(payload)
            yield result

    return _produce_rows(producer, topic, encoded())


# ---------------------------------------------------------------------------
# Multi-process publication.
# ---------------------------------------------------------------------------

# Producer settings used by the publisher processes.  Messages, keys and the
# default key partitioner are unchanged; these only affect batching on the
# wire.  Consumers decompress lz4 transparently.
DEFAULT_PRODUCER_CONFIG: dict[str, object] = {
    "linger.ms": 50,
    "batch.size": 1_000_000,
    "batch.num.messages": 100_000,
    "queue.buffering.max.messages": 500_000,
    "queue.buffering.max.kbytes": 1_048_576,
    "compression.type": "lz4",
}

# Topics smaller than this are never split: re-reading the file per shard is
# cheaper than the extra producer and process start-up.
MIN_SHARD_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class PublishTask:
    """One unit of publisher work: a topic, or one key-hash shard of it."""
    label: str
    topic: str
    source: Path
    size_bytes: int
    shard_index: int = 0
    shard_count: int = 1
    # Nodes carry (properties, key_property); edges carry (edge, source_column, target_column).
    properties: dict[str, PropertyConfig] | None = None
    key_property: str | None = None
    edge: EdgeConfig | None = None
    source_column: str | None = None
    target_column: str | None = None


@dataclass(frozen=True)
class PublishTaskResult:
    label: str
    topic: str
    shard_index: int
    count: int
    started: float
    completed: float
    shard_count: int = 1


def plan_publish_tasks(tasks: list[PublishTask], workers: int, max_shards: int = 0) -> list[PublishTask]:
    """Split large topics into key-hash shards and order work largest first.

    ``max_shards`` caps shards per topic (0 = automatic: up to ``workers``).
    A topic is split so no shard exceeds roughly half a worker's fair share
    of the bytes, which keeps the biggest relationship topic off the critical
    path.  Longest-first ordering then packs the pool well.
    """
    limit = max(1, workers if max_shards <= 0 else max_shards)
    total = sum(task.size_bytes for task in tasks)
    target = max(MIN_SHARD_BYTES, total / max(1, 2 * workers))
    planned: list[PublishTask] = []
    for task in tasks:
        shard_count = 1 if max_shards == 1 else min(limit, max(1, math.ceil(task.size_bytes / target)))
        planned.extend(
            PublishTask(**{**task.__dict__, "shard_index": index, "shard_count": shard_count})
            for index in range(shard_count)
        )
    planned.sort(key=lambda task: (-task.size_bytes / task.shard_count, task.topic, task.shard_index))
    return planned


class _DeliveryFailures:
    """Producer-wide delivery report callback (errors only)."""

    def __init__(self) -> None:
        self.count = 0
        self.first: object | None = None

    def __call__(self, error, _message) -> None:
        if error is not None:
            self.count += 1
            if self.first is None:
                self.first = error


def run_publish_task(
    task: PublishTask, bootstrap_servers: str, producer_config: dict[str, object] | None = None,
) -> PublishTaskResult:
    """Publish one task with a producer owned by this worker process."""
    started = time.monotonic()
    failures = _DeliveryFailures()
    producer = Producer({
        **DEFAULT_PRODUCER_CONFIG,
        **(producer_config or {}),
        "bootstrap.servers": bootstrap_servers,
        "delivery.report.only.error": True,
        "on_delivery": failures,
    })
    if task.edge is not None:
        count = publish_edge_tsv(
            producer, task.topic, task.source, task.edge,
            source_column=task.source_column, target_column=task.target_column,
            shard_index=task.shard_index, shard_count=task.shard_count,
        )
    else:
        count = publish_tsv(
            producer, task.topic, task.source, task.properties, task.key_property,
            shard_index=task.shard_index, shard_count=task.shard_count,
        )
    if failures.count:
        raise RuntimeError(f"Kafka failed to deliver {failures.count} records for {task.topic}: {failures.first}")
    return PublishTaskResult(
        task.label, task.topic, task.shard_index, count, started, time.monotonic(), task.shard_count,
    )


def merge_publish_results(results: list[PublishTaskResult]) -> list[tuple[str, str, int, float]]:
    """Combine shard results into one ``(label, topic, count, seconds)`` per topic.

    A topic's time spans its first shard's start to its last shard's flush.
    """
    by_topic: dict[tuple[str, str], list[PublishTaskResult]] = {}
    for result in results:
        by_topic.setdefault((result.label, result.topic), []).append(result)
    return [
        (label, topic, sum(part.count for part in parts),
         max(part.completed for part in parts) - min(part.started for part in parts))
        for (label, topic), parts in by_topic.items()
    ]


def build_loader_images() -> float:
    """Build every image required by the public node-plus-edge bulk CLI."""
    started = time.monotonic()
    service = DockerService()
    service.build_image()
    service.build_edge_image()
    return time.monotonic() - started


from src.orchestrator.auto_settings import DEFAULT_NODE_REPLICAS_MIN_RECORDS  # noqa: E402  (library policy)
# With --edge-leases each edge outer flush is split into up to B*B bucket
# cells, one Neo4j transaction per cell (capped by mix_and_batch.batch_size),
# so lease mode uses a much larger default flush than lane mode.
DEFAULT_LEASE_BUCKETS = 16
DEFAULT_LEASE_EDGE_BATCH_SIZE = 32_000
DEFAULT_LEASE_KEYED_MAX_NODES = 1_000
MAX_EDGE_BATCH_SIZE = 1_000_000


def read_lease_settings(directory: Path | None) -> dict | None:
    """Return the lease settings the CLI recorded for this run, if any."""
    if directory is None or not (directory / "lease-settings.json").is_file():
        return None
    try:
        return json.loads((directory / "lease-settings.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def read_lease_server_stats(directory: Path | None) -> str | None:
    """Return the coordinator's final ``key=value`` counters, if recorded."""
    if directory is None:
        return None
    path = directory / "lease-server.log"
    if not path.is_file():
        return None
    marker = "stage=lease-server-stats "
    for line in reversed(path.read_text(encoding="utf-8", errors="replace").splitlines()):
        if marker in line:
            return line.split(marker, 1)[1].strip()
    return None


def plan_node_replicas(
    topic_results: list[tuple[str, str, int, float]], consumer_workers: int, min_records: int,
) -> dict[str, int]:
    """Map each node topic to ``consumer_workers`` replicas, or 1 if it published < ``min_records``.

    A tiny node type (IMDb genres: 28 records) gains nothing from four loader
    containers, but each one costs a container launch, a consumer-group
    rebalance while it joins, and a Docker stop. ``min_records=0`` disables
    the policy.  This is the library's policy
    (:func:`src.orchestrator.auto_settings.node_replicas_for`, which the
    loader also applies to any schema whose YAML omits ``replicas``) with
    ``consumer_workers`` as the per-label maximum.
    """
    from src.orchestrator.auto_settings import node_replicas_for

    published = {topic: count for source, topic, count, _ in topic_results if source in NODE_SOURCES}
    return {
        topic: node_replicas_for(published.get(topic, 0), consumer_workers, min_records=min_records,
                                 max_replicas=consumer_workers)
        for topic, _ in NODE_SOURCES.values()
    }


def create_runtime_schema(
    source: Path, consumer_workers: int, edge_writers: int = 4, edge_consumers: int = 1,
    edge_batch_size: int | None = None, node_batch_size: int | None = None,
    node_replicas: dict[str, int] | None = None,
) -> Path:
    """Create an ephemeral schema with worker counts and independent batch overrides.

    ``node_replicas`` overrides ``consumer_workers`` for the node topics it names.
    """
    with source.open("r", encoding="utf-8") as input_file:
        raw_schema = yaml.safe_load(input_file)
    if node_batch_size is not None:
        # Preserve the original edge fallback if this schema has no explicit edge size.
        if edge_batch_size is None and raw_schema["loading"].get("edge_unwind_batch_size") is None:
            raw_schema["loading"]["edge_unwind_batch_size"] = raw_schema["loading"].get("unwind_batch_size", 500)
        raw_schema["loading"]["unwind_batch_size"] = node_batch_size
    if edge_batch_size is not None:
        raw_schema["loading"]["edge_unwind_batch_size"] = edge_batch_size
    for node in raw_schema["nodes"]:
        node["replicas"] = (node_replicas or {}).get(node["topic"], consumer_workers)
    for edge in raw_schema["edges"]:
        edge["replicas"] = edge_consumers
        edge.setdefault("execution", {})["worker_count"] = edge_writers
        edge.setdefault("mix_and_batch", {})["lane_count"] = edge_writers

    runtime_directory = PROJECT_ROOT / "var" / "runtime_schemas"
    runtime_directory.mkdir(parents=True, exist_ok=True)
    descriptor, path_string = tempfile.mkstemp(
        prefix="imdb-", suffix=".yaml", dir=runtime_directory
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as output_file:
        yaml.safe_dump(raw_schema, output_file, sort_keys=False)
    return Path(path_string)


def read_planned_summary(directory: Path | None) -> dict | None:
    """Return the CLI's planned-phase summary, if recorded."""
    if directory is None or not (directory / "planned-summary.json").is_file():
        return None
    try:
        return json.loads((directory / "planned-summary.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def kafka_plan_report(timing_directory: Path | None, writers: int) -> dict:
    """Report data for ``--plan-source kafka``: the plan the loader wrote from the edge topics."""
    from src.loader.edge_plan import read_plan_file

    path = None if timing_directory is None else timing_directory / "edge-plan.json"
    if path is None or not path.is_file():
        marker = None if path is None else path.with_name(path.name + ".failed")
        error = marker.read_text(encoding="utf-8").strip() if marker is not None and marker.is_file() else (
            "the loader wrote no edge plan")
        return {"settings": {}, "stats": {}, "types": {}, "error": error, "writers": writers,
                "topic": None, "partitions": None, "source": "kafka"}
    plan, _digest = read_plan_file(path)
    return {
        "settings": plan.settings, "stats": plan.stats, "error": None, "writers": writers,
        "types": {name: {"rows": item.rows, "chunks": item.chunks, "units": item.units}
                  for name, item in plan.types.items()},
        "topic": plan.topic, "partitions": plan.partitions, "source": "kafka",
        "ready_seconds": plan.stats.get("scan_seconds", 0) + plan.stats.get("hub_seconds", 0)
        + plan.stats.get("emit_seconds", 0),
    }


def planned_report_lines(edge_plan: dict, timing_directory: Path | None) -> list[str]:
    """Report header lines describing the planned edge load."""
    settings, stats = edge_plan.get("settings") or {}, edge_plan.get("stats") or {}
    chunks, rows = stats.get("chunks") or 0, stats.get("rows") or 0
    lines = [
        "- Edge plan: enabled ("
        f"{settings.get('buckets')} buckets/label, hub degree >= {settings.get('hub_degree')} "
        f"({stats.get('hubs')} hubs), <= {settings.get('chunk_rows')} rows/chunk; "
        f"{stats.get('resources')} resources, {stats.get('units')} units, {chunks} chunks, "
        f"{rows / chunks if chunks else 0:.0f} rows/chunk; {edge_plan.get('writers')} writers; "
        f"topic `{edge_plan.get('topic')}` with {edge_plan.get('partitions')} partition(s))",
    ]
    if edge_plan.get("error"):
        lines.append(f"- Edge planning failed: {edge_plan['error']}")
    else:
        kafka = edge_plan.get("source") == "kafka"
        lines.append(
            f"- Edge planning ({settings.get('planner_workers')} processes, "
            + ("by the loader from the per-type edge topics, overlapped with node loading): "
               f"read {stats.get('rows', 0)} records at {stats.get('scan_records_per_second', 0):.0f} records/s, "
               if kafka else "from the relationship files, overlapped with node publish/load): ")
            + f"scan {stats.get('scan_seconds', 0):.3f}s, hubs {stats.get('hub_seconds', 0):.3f}s, "
            f"chunk + publish {stats.get('emit_seconds', 0):.3f}s, ready after {edge_plan.get('ready_seconds', 0):.3f}s"
            + (" of planning; " if kafka else "; ") +
            f"chunk messages max {stats.get('max_chunk_message_bytes')} B, mean {stats.get('mean_chunk_message_bytes')} B"
        )
    summary = read_planned_summary(timing_directory)
    if summary is not None:
        dispatch = summary.get("dispatch_seconds")
        dispatcher_stats = summary.get("dispatcher_stats") or {}
        lines.append(
            "- Planned edge dispatch: "
            + (f"first grant to last commit {dispatch:.3f}s" if dispatch is not None else "no chunks")
            + f" ({summary.get('first_grant_utc')} to {summary.get('last_done_utc')}); "
            + f"phase incl. plan wait and container lifecycle {summary.get('phase_seconds', 0):.3f}s; "
            + " ".join(f"{key}={value}" for key, value in sorted(dispatcher_stats.items()))
        )
    return lines


def planned_type_table(edge_plan: dict, timing_directory: Path | None) -> list[str]:
    """Per-edge-type planned rows, chunks and dispatch spans."""
    summary = read_planned_summary(timing_directory) or {}
    types = summary.get("types") or {}
    planned_types = edge_plan.get("types") or {}
    lines = [
        "## Planned edge load by type",
        "",
        "| Edge type | Planned rows | Committed rows | Chunks | Units | Sum of write time | First grant (UTC) | Last commit (UTC) | Span |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for edge_type in sorted(planned_types):
        item = planned_types[edge_type]
        done = types.get(edge_type) or {}
        first, last = done.get("first_grant"), done.get("last_done")
        lines.append(
            f"| {edge_type} | {item.get('rows')} | {done.get('rows', 'not recorded')} | {item.get('chunks')} | "
            f"{item.get('units')} | "
            + (f"{done['write_ms'] / 1000:.3f}s" if "write_ms" in done else "—")
            + f" | {datetime.fromtimestamp(first, timezone.utc).isoformat() if first else '—'}"
            + f" | {datetime.fromtimestamp(last, timezone.utc).isoformat() if last else '—'}"
            + f" | {f'{last - first:.3f}s' if first and last else '—'} |"
        )
    lines.extend([
        "",
        "Planned rows come from the publisher's pass over the relationship files; committed rows are the "
        "dispatcher's completion ledger (every chunk exactly once, verified by the CLI before success). "
        "Spans overlap: all edge types share one node-disjoint writer pool.",
        "",
    ])
    return lines


def write_run_report(
    report_directory: Path,
    *,
    started_at: datetime,
    total_seconds: float,
    partitions: int,
    publish_workers: int,
    consumer_workers: int,
    topic_setup_seconds: float,
    parallel_phase_seconds: float,
    image_build_seconds: float | None,
    topic_results: list[tuple[str, str, int, float]],
    consumer_lifecycle_seconds: float | None,
    consumer_exit_code: int | None,
    write_timings: dict[str, WriteTimingSummary] | None = None,
    edge_writers: int | None = 1,  # None (planned mode): no per-type edge loaders, so the lines are omitted
    edge_consumers: int | None = 1,
    node_batch_size: int = 500,
    edge_batch_size: int = 500,
    timing_directory: Path | None = None,
    bulk_edge_overlap: str | None = "labels",  # None: not classic, so the line is omitted
    max_concurrent_bulk_edges: int = 0,
    publish_summary: str | None = None,
    node_replicas: dict[str, int] | None = None,
    edge_leases: bool = False,
    lease_buckets: int | None = None,
    edge_plan: dict | None = None,
) -> Path:
    """Write the one human-readable timing record for this example run."""
    report_directory.mkdir(parents=True, exist_ok=True)
    completed_at = datetime.now(timezone.utc)
    total_nodes = sum(count for label, _, count, _ in topic_results if label in NODE_SOURCES)
    total_relationships = sum(count for label, _, count, _ in topic_results if label in EDGE_SOURCES)
    consumer_count = (
        sum(node_replicas.values()) if node_replicas else len(NODE_SOURCES) * consumer_workers
    )
    outcome = "published only" if consumer_exit_code is None else (
        "succeeded" if consumer_exit_code == 0 else f"failed (exit code {consumer_exit_code})"
    )
    lines = [
        "# IMDb Bulk Load Performance Report",
        "",
        f"- Started (UTC): {started_at.isoformat()}",
        f"- Completed (UTC): {completed_at.isoformat()}",
        f"- Outcome: {outcome}",
        f"- Kafka partitions per node topic: {partitions}",
        f"- Publisher threads: {publish_workers}",
        *([f"- Publisher layout: {publish_summary}"] if publish_summary else []),
        f"- Node-loader containers per node type: {consumer_workers}",
        *([
            "- Node-loader replicas by topic: "
            + ", ".join(f"{topic}={count}" for topic, count in sorted(node_replicas.items()))
        ] if node_replicas else []),
        *([f"- Edge writer threads per edge type: {edge_writers}"] if edge_writers is not None else []),
        *([f"- Edge consumer containers per edge type: {edge_consumers}"] if edge_consumers is not None else []),
        f"- Node outer batch size: {node_batch_size}",
        f"- Edge outer batch size: {edge_batch_size}",
        *([f"- Bulk edge overlap: {bulk_edge_overlap} (max concurrent edge types: {max_concurrent_bulk_edges or 'unlimited'})"]
          if bulk_edge_overlap is not None else []),
        (
            f"- Edge bucket leases: enabled ({lease_buckets} buckets per label; one transaction per leased cell, no APOC lock step)"
            if edge_leases else "- Edge bucket leases: disabled"
        ),
        *([
            "- Lease keyed labels (one resource per node): "
            + (", ".join(lease_settings.get("keyed_labels") or []) or "none")
            + f"; lease outer batch size: {lease_settings.get('outer_batch_size')}"
        ] if edge_leases and (lease_settings := read_lease_settings(timing_directory)) else []),
        *([f"- Lease coordinator stats: {lease_stats}"] if (lease_stats := read_lease_server_stats(timing_directory)) else []),
        *(planned_report_lines(edge_plan, timing_directory) if edge_plan is not None else []),
        f"- Node-loader containers started: {consumer_count if consumer_exit_code == 0 else 'not confirmed'}",
        f"- Node records sent to Kafka: {total_nodes}",
        f"- Relationship records sent to Kafka: {total_relationships}",
        f"- Graph records sent to Kafka: {total_nodes + total_relationships}",
        *([f"- Raw Neo4j write timing logs: `{timing_directory.name}/`"] if timing_directory else []),
        "",
        "## Timings",
        "",
        "| Step | Wall-clock time |",
        "| --- | ---: |",
        f"| Topic create/partition expansion | {topic_setup_seconds:.3f}s |",
        f"| Parallel publish + image-build phase | {parallel_phase_seconds:.3f}s |",
        f"| Docker image build | {'not run' if image_build_seconds is None else f'{image_build_seconds:.3f}s'} |",
        f"| Kafka consumption + Neo4j graph loading + drain verification | {'not run' if consumer_lifecycle_seconds is None else f'{consumer_lifecycle_seconds:.3f}s'} |",
        f"| Total end-to-end | {total_seconds:.3f}s |",
        "",
        "## Kafka publication by graph type",
        "",
        "| Graph type | Topic | Records flushed to Kafka | Time |",
        "| --- | --- | ---: | ---: |",
    ]
    lines.extend(
        f"| {label} | `{topic}` | {count} | {duration:.3f}s |"
        for label, topic, count, duration in sorted(topic_results)
    )
    lines.extend(["", "## Neo4j writes by graph type", ""])
    if write_timings is None:
        lines.extend(["Not run or write timings unavailable.", ""])
    else:
        lines.extend([
            "| Graph type | Topic | Records in successful writes | Batches | Total write time | Mean batch | Max batch |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ])
        for label, topic, published, _ in sorted(topic_results):
            planned_edge = edge_plan is not None and label in EDGE_SOURCES
            summary = write_timings.get(
                planned_timing_key(edge_plan.get("topic") or topic, label) if planned_edge else topic
            )
            if summary is None:
                if published:
                    lines.append(f"| {label} | `{topic}` | not recorded | not recorded | not recorded | not recorded | not recorded |")
                else:
                    lines.append(f"| {label} | `{topic}` | 0 | 0 | 0.000s | — | — |")
                continue
            lines.append(
                f"| {label} | `{topic}` | {summary.records} | {summary.batches} | "
                f"{summary.total_ms / 1000:.3f}s | {summary.total_ms / summary.batches:.1f}ms | "
                f"{summary.max_ms:.1f}ms |"
            )
        lines.extend([
            "",
            "Write times sum successful node batch calls and edge flushes. An edge flush can run several lane writes concurrently, and replicas can overlap; these totals are not end-to-end wall-clock durations. Records count batch inputs and can include replay after a failed offset commit.",
            "",
        ])
    if edge_plan is not None:
        lines.extend(planned_type_table(edge_plan, timing_directory))
    edge_stages = read_edge_stage_timings(timing_directory) if timing_directory else {}
    edge_substages = read_edge_substage_timings(timing_directory) if timing_directory else {}
    if edge_stages:
        lines.extend([
            "## Bulk edge lifecycle by graph type",
            "",
            "| Edge type | Started (UTC) | Completed (UTC) | Elapsed | Outcome |",
            "| --- | --- | --- | ---: | --- |",
        ])
        for edge_type, stage in sorted(edge_stages.items()):
            lines.append(
                f"| {edge_type} | {stage.started_utc} | {stage.completed_utc} | "
                f"{stage.elapsed_seconds:.3f}s | {'succeeded' if stage.succeeded else 'failed'} |"
            )
            for rank, topic, sub in edge_substages.get(edge_type, ()):
                lines.append(
                    f"| &nbsp;&nbsp;topic {rank} `{topic}` | {sub.started_utc} | {sub.completed_utc} | "
                    f"{sub.elapsed_seconds:.3f}s | {'succeeded' if sub.succeeded else 'failed'} |"
                )
        lines.extend([
            "",
            "Each elapsed time includes the edge container lifecycle, Kafka consumption, drain acknowledgement, and zero-lag verification. Different non-conflicting edge types can overlap.",
            "",
        ])
    metrics_totals = read_metrics_snapshots(timing_directory) if timing_directory else {}
    if metrics_totals:
        lines.extend([
            "## Metrics totals",
            "",
            "Summed across every process replica's final Prometheus snapshot "
            "(P6-S2 Story 9) - the durable record for a run shorter than one "
            "scrape interval. These are at-least-once counters (P5-Q5): a "
            "crash between a durable write/DLQ-publish and the counter's "
            "increment can double-count on restart; the graph itself stays "
            "exact via idempotent MERGE. Do not treat a delta here as an "
            "exact retry/DLQ count.",
            "",
            "| Series | Total |",
            "| --- | ---: |",
        ])
        for series, label in (
            ("graph_loader_records_consumed_total", "Records consumed"),
            ("graph_loader_dlq_routed_total", "DLQ routed"),
            ("graph_loader_deadlocks_recovered_total", "Deadlocks recovered"),
            ("graph_loader_retry_total", "Retries"),
        ):
            if series in metrics_totals:
                lines.append(f"| {label} | {metrics_totals[series]:.0f} |")
        lines.append("")
    lines.extend([
        "",
        "The publication and image-build durations overlap; the parallel-phase duration is the elapsed wall time before consumers can start. The final lifecycle duration includes Docker consumer launch, Kafka consumption, Neo4j node and relationship writes, drain, and zero-lag verification.",
        "",
    ])
    report_path = report_directory / f"imdb-load-{started_at.strftime('%Y%m%dT%H%M%SZ')}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def _relationship_rows(relationship_dir: Path) -> dict[str, int]:
    """Data rows per relationship TSV (newline count minus the header)."""
    counts = {}
    for path in sorted(relationship_dir.glob("*.tsv")):
        newlines = 0
        with open(path, "rb") as handle:
            while block := handle.read(1 << 24):
                newlines += block.count(b"\n")
        counts[path.name] = max(0, newlines - 1)
    return counts


def _derive_plan_settings(args: argparse.Namespace) -> dict[str, int]:
    """Fill unset plan settings with the library's defaults for this Neo4j server and input.

    Writers follow Neo4j's available processors (probed), and buckets, chunk
    size and hub degree follow the writers and the relationship volume, so the
    files path scales with the server exactly like the library's Kafka path.
    """
    if args.plan_writers is None:
        cores = None
        uri = os.environ.get("NEO4J_URI")
        if uri:
            from neo4j import GraphDatabase
            try:
                with GraphDatabase.driver(uri, auth=(
                    os.environ.get("NEO4J_USERNAME", "neo4j"), os.environ.get("NEO4J_PASSWORD", ""),
                )) as driver:
                    cores = probe_neo4j_cores(driver)
            except Exception as exc:  # unreachable server: fall back to the documented default
                logging.getLogger(__name__).info("Neo4j core probe failed: %s", exc)
        args.plan_writers = default_plan_writers(cores) if cores else DEFAULT_PLAN_WRITERS
    rows = _relationship_rows(args.subset_dir / "graph" / "relationships")
    # Same-label types (EPISODE_OF, a 0.3% share) count like the Kafka path's self-referencing pairs.
    edge_by_file = {source[1]: edge_type for edge_type, source in EDGE_SOURCES.items()}
    self_ref_types = {edge.type for edge in load_schema(SCHEMA_PATH).edges
                      if any(pair.is_self_referencing for pair in edge.endpoint_pairs)}
    return derive_plan_parameters(
        sum(rows.values()), args.plan_writers, edge_types=max(1, sum(1 for count in rows.values() if count)),
        buckets=args.plan_buckets, hub_degree=args.plan_hub_degree, chunk_rows=args.plan_chunk_rows,
        self_ref_records=sum(count for name, count in rows.items() if edge_by_file.get(name) in self_ref_types),
    )


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subset-dir", type=Path, required=True, help="directory created by build_subset.py")
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    default_network = (
        "bridge" if os.environ.get("LOADER_NEO4J_URI")
        and os.environ.get("LOADER_KAFKA_BOOTSTRAP_SERVERS")
        else "graph-loader-net"
    )
    parser.add_argument("--network", default=default_network, help="Docker network for the loader fleet")
    parser.add_argument("--bulk-timeout-seconds", type=float, default=600.0)
    parser.add_argument(
        "--partitions",
        type=int,
        default=4,
        help="Kafka partitions per IMDb node topic (default: 4; may only increase existing topics)",
    )
    parser.add_argument(
        "--consumer-workers",
        type=int,
        help="node-loader containers per node type (default: --partitions)",
    )
    parser.add_argument(
        "--node-replicas-min-records", type=int, default=DEFAULT_NODE_REPLICAS_MIN_RECORDS,
        help="node types that published fewer records use 1 loader container instead of "
        f"--consumer-workers (default: {DEFAULT_NODE_REPLICAS_MIN_RECORDS}; 0 = always --consumer-workers)",
    )
    parser.add_argument(
        "--publish-workers",
        type=int,
        default=4,
        help="publisher worker processes shared by all node and relationship topics (default: 4)",
    )
    parser.add_argument(
        "--publish-shards", type=int, default=0,
        help="maximum key-hash shards per large topic (default: 0 = automatic, up to --publish-workers; 1 disables splitting)",
    )
    parser.add_argument(
        "--publish-compression", choices=("none", "lz4", "snappy", "zstd", "gzip"),
        default=DEFAULT_PRODUCER_CONFIG["compression.type"],
        help=f"Kafka producer compression.type (default: {DEFAULT_PRODUCER_CONFIG['compression.type']})",
    )
    parser.add_argument(
        "--publish-linger-ms", type=int, default=DEFAULT_PRODUCER_CONFIG["linger.ms"],
        help=f"Kafka producer linger.ms (default: {DEFAULT_PRODUCER_CONFIG['linger.ms']})",
    )
    parser.add_argument(
        "--edge-writers", type=int, default=4,
        help="concurrent Neo4j writer threads and Mix-and-Batch lanes per edge type (default: 4)",
    )
    parser.add_argument(
        "--edge-consumers", type=int, default=1,
        help="Kafka consumer containers per edge type (default: 1)",
    )
    parser.add_argument(
        "--node-batch-size", type=int,
        help="node outer flush size (default: schema setting, 500 for IMDb)",
    )
    parser.add_argument(
        "--edge-batch-size", type=int,
        help="edge outer flush size (default: schema setting, 2000 for IMDb; "
        f"{DEFAULT_LEASE_EDGE_BATCH_SIZE} with --edge-leases)",
    )
    parser.add_argument(
        "--edge-leases", action="store_true",
        help="use the global bucket-lease coordinator: node-disjoint relationship "
        "transactions across all edge types (implies --bulk-edge-overlap shared)",
    )
    parser.add_argument(
        "--lease-buckets", type=int, default=DEFAULT_LEASE_BUCKETS,
        help=f"with --edge-leases: buckets per node label (default: {DEFAULT_LEASE_BUCKETS})",
    )
    parser.add_argument(
        "--lease-keyed-max-nodes", type=int, default=DEFAULT_LEASE_KEYED_MAX_NODES,
        help="with --edge-leases: labels with at most this many nodes are leased per node key "
        f"(default: {DEFAULT_LEASE_KEYED_MAX_NODES}; 0 = never)",
    )
    parser.add_argument(
        "--edge-plan", action="store_true",
        help="load every relationship as globally planned node-disjoint chunks of all edge types, "
        "run-time dispatched to one writer pool (exclusive with --edge-leases and --edge-classic); "
        "see --plan-source. A run with no edge-mode flag does the same with --plan-source kafka "
        "(the bulk default of the CLI)",
    )
    parser.add_argument(
        "--edge-classic", action="store_true",
        help="use the classic per-type APOC-locked relationship loaders (the one mode that can wait on "
        "Neo4j locks); the only mode that honors --bulk-edge-overlap and --max-concurrent-bulk-edges",
    )
    parser.add_argument(
        "--plan-source", choices=("files", "kafka"), default=None,
        help="with --edge-plan: 'files' (default, fastest) plans in the publisher from the relationship "
        "TSVs with the library producer API and publishes only chunks; 'kafka' publishes the per-type "
        "edge topics like the normal mode and lets the loader plan from them (the generic library path)",
    )
    parser.add_argument(
        "--plan-buckets", type=int, default=None,
        help="with --edge-plan: hash buckets per node label (default: derived from the writers and the "
        "relationship volume, as the library does)",
    )
    parser.add_argument(
        "--plan-hub-degree", type=int, default=None,
        help="with --edge-plan: nodes with at least this total degree get their own resource "
        "(default: the chunk size, as the library derives it; 0 = no hubs)",
    )
    parser.add_argument(
        "--plan-chunk-rows", type=int, default=None,
        help="with --edge-plan: maximum rows per chunk transaction (default: derived from the relationship "
        "volume and writers, as the library does)",
    )
    parser.add_argument(
        "--plan-writers", type=int, default=None,
        help="with --edge-plan: writer containers shared by all edge types (default: Neo4j available "
        f"processors + 2, probed from the server; {DEFAULT_PLAN_WRITERS} if the probe fails)",
    )
    parser.add_argument(
        "--plan-workers", type=int,
        help="with --edge-plan: planner processes (default: --publish-workers)",
    )
    parser.add_argument(
        "--plan-topic", default=None,
        help=f"with --edge-plan: planned chunk topic (default: {DEFAULT_PLAN_TOPIC} for files, "
        "a run-scoped topic for kafka)",
    )
    parser.add_argument(
        "--plan-partitions", type=int,
        help="with --edge-plan: partitions of the planned chunk topic (default: --partitions)",
    )
    parser.add_argument(
        "--plan-memory-mb", type=int, default=None,
        help="with --edge-plan: memory budget per planner process in MiB; beyond it the planner spills "
        "to --plan-spill-dir (default: a quarter of the available RAM over the planner processes)",
    )
    parser.add_argument(
        "--plan-spill-dir", default=None,
        help="with --edge-plan: directory for the planner's temporary spill files (default: the plan directory)",
    )
    parser.add_argument(
        "--bulk-edge-overlap", choices=("labels", "shared"), default=None,
        help="with --edge-classic: edge types sharing an endpoint label: 'labels' serializes them, 'shared' "
        "overlaps them (default: labels); ignored with a warning in the other modes",
    )
    parser.add_argument(
        "--max-concurrent-bulk-edges", type=int, default=None,
        help="with --edge-classic: maximum concurrently running edge types (default: 0 = unlimited); "
        "ignored with a warning in the other modes",
    )
    parser.add_argument("--publish-only", action="store_true", help="do not start the Neo4j bulk loader")
    parser.add_argument(
        "--skip-image-build",
        action="store_true",
        help="use existing graph-loader-node and graph-loader-edge images instead of building them",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="directory for the generated Markdown timing report (default: <subset-dir>/reports)",
    )
    args = parser.parse_args()
    started_at = datetime.now(timezone.utc)
    overall_started = time.monotonic()
    node_dir = args.subset_dir / "graph" / "nodes"
    relationship_dir = args.subset_dir / "graph" / "relationships"
    missing = [filename for _, filename in NODE_SOURCES.values() if not (node_dir / filename).is_file()]
    missing.extend(filename for _, filename, _, _ in EDGE_SOURCES.values() if not (relationship_dir / filename).is_file())
    if missing:
        parser.error(f"subset is missing generated graph TSVs: {', '.join(missing)}")
    if args.publish_workers < 1 or args.partitions < 1 or not 1 <= args.edge_writers <= 64 or not 1 <= args.edge_consumers <= 64:
        parser.error("--publish-workers and --partitions must be positive; --edge-writers and --edge-consumers must be 1–64")
    if args.edge_batch_size is not None and not 1 <= args.edge_batch_size <= MAX_EDGE_BATCH_SIZE:
        parser.error(f"--edge-batch-size must be 1–{MAX_EDGE_BATCH_SIZE}")
    if not 1 <= args.lease_buckets <= 4096:
        parser.error("--lease-buckets must be 1–4096")
    if args.lease_keyed_max_nodes < 0:
        parser.error("--lease-keyed-max-nodes must be nonnegative")
    chosen = [flag for flag, on in (("--edge-plan", args.edge_plan), ("--edge-leases", args.edge_leases),
                                    ("--edge-classic", args.edge_classic)) if on]
    if len(chosen) > 1:
        parser.error(f"{' and '.join(chosen)} are mutually exclusive")
    if not chosen:
        if args.plan_source == "files":
            parser.error("--plan-source files needs --edge-plan (with no edge-mode flag the runner plans from "
                         "the Kafka edge topics, the CLI's bulk default)")
        # The bulk default of the CLI: planned from the per-type edge topics (Kafka planning).
        args.edge_plan, args.plan_source = True, "kafka"
    elif args.plan_source is None:
        args.plan_source = "files"
    if not args.edge_classic and (args.bulk_edge_overlap is not None or args.max_concurrent_bulk_edges is not None):
        print("warning: --bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored "
              "(add --edge-classic to use classic)", file=sys.stderr)
    if args.bulk_edge_overlap is None:
        args.bulk_edge_overlap = "labels"
    if args.max_concurrent_bulk_edges is None:
        args.max_concurrent_bulk_edges = 0
    # Per-type edge loaders (and so their consumers and writer threads) exist only in classic and lease mode.
    args.per_type_edges = bool(args.edge_classic or args.edge_leases)
    plan_settings = None
    kafka_plan = bool(args.edge_plan) and args.plan_source == "kafka"
    if args.edge_plan:
        derived = _derive_plan_settings(args)
        if not 1 <= args.plan_writers <= 64:
            parser.error("--plan-writers must be 1-64")
        try:
            settings = PlanSettings(
                buckets=derived["buckets"],
                hub_degree=derived["hub_degree"],
                chunk_rows=derived["chunk_rows"],
                topic=args.plan_topic or DEFAULT_PLAN_TOPIC, partitions=args.plan_partitions or args.partitions,
                workers=args.plan_workers or args.publish_workers,
                memory_mb=args.plan_memory_mb or 0, spill_dir=args.plan_spill_dir or "",
            ).validate()
        except ValueError as exc:
            parser.error(str(exc))
        # 'kafka': the edges go to their per-type topics and the loader plans.
        plan_settings = None if kafka_plan else settings
    args.kafka_plan = kafka_plan
    if args.node_batch_size is not None and not 1 <= args.node_batch_size <= 10_000:
        parser.error("--node-batch-size must be 1–10000")
    if args.max_concurrent_bulk_edges < 0:
        parser.error("--max-concurrent-bulk-edges must be nonnegative")
    if args.publish_shards < 0 or args.publish_linger_ms < 0:
        parser.error("--publish-shards and --publish-linger-ms must be nonnegative")
    consumer_workers = args.partitions if args.consumer_workers is None else args.consumer_workers
    if consumer_workers < 1:
        parser.error("--consumer-workers must be positive")
    if args.node_replicas_min_records < 0:
        parser.error("--node-replicas-min-records must be nonnegative")

    schema = load_schema(SCHEMA_PATH)
    node_batch_size = args.node_batch_size or schema.loading.unwind_batch_size
    edge_batch_size = args.edge_batch_size or (
        DEFAULT_LEASE_EDGE_BATCH_SIZE if args.edge_leases
        else schema.loading.edge_unwind_batch_size or schema.loading.unwind_batch_size
    )
    if not args.publish_only and args.bulk_timeout_seconds <= 0:
        parser.error("--bulk-timeout-seconds must be positive")
    node_config_by_topic = {node.topic: node for node in schema.nodes}
    edge_config_by_type = {edge.type: edge for edge in schema.edges}
    node_topics = [topic for topic, _ in NODE_SOURCES.values()]
    topics = node_topics + (
        [] if plan_settings is not None else [topic for topic, _, _, _ in EDGE_SOURCES.values()]
    )
    topic_setup_started = time.monotonic()
    ensure_topics(args.kafka_bootstrap, topics, args.partitions)
    if plan_settings is not None:
        try:
            ensure_plan_topic(
                args.kafka_bootstrap, plan_settings.topic, plan_settings.partitions,
                plan_settings.max_message_bytes,
            )
        except ValueError as exc:
            parser.error(str(exc))
        topics = topics + [plan_settings.topic]
    try:
        require_empty_topics(args.kafka_bootstrap, topics)
    except ValueError as exc:
        parser.error(str(exc))
    topic_setup_seconds = time.monotonic() - topic_setup_started
    report_directory = args.report_dir or args.subset_dir / "reports"
    planner = None
    plan_path = None
    timing_directory = None
    if plan_settings is not None:
        # The plan lives beside the loader timing logs (mounted into the
        # dispatcher and writer containers); planning starts before node
        # publication and finishes while the node loaders run.
        report_directory.mkdir(parents=True, exist_ok=True)
        timing_directory = Path(tempfile.mkdtemp(prefix="imdb-write-timings-", dir=report_directory))
        plan_path = timing_directory / "edge-plan.json"
        planner = imdb_edge_planner(
            schema,
            {
                edge_type: (str(relationship_dir / filename), source_column, target_column)
                for edge_type, (_, filename, source_column, target_column) in EDGE_SOURCES.items()
            },
            plan_settings, args.kafka_bootstrap, plan_path=plan_path,
        )
        print(
            f"Planning {len(EDGE_SOURCES)} relationship types on {len(planner.lanes)} planner process(es): "
            f"{plan_settings.buckets} buckets/label, hubs >= {plan_settings.hub_degree}, "
            f"<= {plan_settings.chunk_rows} rows/chunk, topic {plan_settings.topic}"
        )
        planner.start()
    try:
        return _publish_and_load(
            args, parser, schema, started_at=started_at, overall_started=overall_started,
            node_dir=node_dir, relationship_dir=relationship_dir, node_batch_size=node_batch_size,
            edge_batch_size=edge_batch_size, node_config_by_topic=node_config_by_topic,
            edge_config_by_type=edge_config_by_type, consumer_workers=consumer_workers,
            topic_setup_seconds=topic_setup_seconds, report_directory=report_directory,
            planner=planner, plan_path=plan_path, plan_settings=plan_settings,
            timing_directory=timing_directory,
        )
    finally:
        if planner is not None:
            planner.close()


def _publish_and_load(
    args, parser, schema, *, started_at, overall_started, node_dir, relationship_dir, node_batch_size,
    edge_batch_size, node_config_by_topic, edge_config_by_type, consumer_workers, topic_setup_seconds,
    report_directory, planner, plan_path, plan_settings, timing_directory,
) -> int:
    """Publish (nodes, and edges unless planned) and run the bulk loader."""
    plan_state: dict = {}

    def finish_plan() -> dict:
        """Complete the edge plan once; record its outcome for the report."""
        if planner is None or plan_state:
            return plan_state
        try:
            result = planner.finish(plan_path)
            plan_state.update({
                "result": result, "settings": result.plan.settings, "stats": result.plan.stats,
                "ready_seconds": time.monotonic() - overall_started,
                "types": {
                    name: {"rows": item.rows, "chunks": item.chunks, "units": item.units}
                    for name, item in result.plan.types.items()
                },
            })
            print(
                f"Edge plan ready: {len(result.plan.chunks)} chunks, {result.plan.total_rows} rows, "
                f"{result.plan.resources.count} resources ({result.plan.resources.hub_count} hubs), "
                f"planning {result.total_seconds:.3f}s -> {result.path}"
            )
        except Exception as exc:
            plan_state.update({"error": f"{type(exc).__name__}: {exc}", "settings": {}, "stats": {}, "types": {}})
            print(f"Edge planning failed; the bulk loader fails closed: {exc}", file=sys.stderr)
        return plan_state

    kafka_state: dict = {}

    def plan_report() -> dict | None:
        if getattr(args, "kafka_plan", False):
            if kafka_state.get("timing_directory") is None:
                return None  # the loader did not run (publish only, or failed before starting)
            return kafka_plan_report(kafka_state["timing_directory"], args.plan_writers)
        if plan_settings is None:
            return None
        state = finish_plan()
        return {
            "settings": state.get("settings") or {}, "stats": state.get("stats") or {},
            "types": state.get("types") or {}, "error": state.get("error"),
            "ready_seconds": state.get("ready_seconds", 0.0), "writers": args.plan_writers,
            "topic": plan_settings.topic, "partitions": plan_settings.partitions,
        }

    def planned_topic_results() -> list[tuple[str, str, int, float]]:
        state = finish_plan() if plan_settings is not None else {}
        result = state.get("result")
        if result is None:
            return []
        return [
            (name, result.plan.topic, item.rows, result.type_seconds.get(name, 0.0))
            for name, item in sorted(result.plan.types.items())
        ]

    unit_tasks = [
        PublishTask(
            label, topic, node_dir / filename, (node_dir / filename).stat().st_size,
            properties=node_config_by_topic[topic].properties,
            key_property=node_config_by_topic[topic].key_property,
        )
        for label, (topic, filename) in NODE_SOURCES.items()
    ] + ([] if plan_settings is not None else [
        PublishTask(
            edge_type, topic, relationship_dir / filename, (relationship_dir / filename).stat().st_size,
            edge=edge_config_by_type[edge_type], source_column=source_column, target_column=target_column,
        )
        for edge_type, (topic, filename, source_column, target_column) in EDGE_SOURCES.items()
    ])
    worker_count = args.publish_workers
    publish_tasks = plan_publish_tasks(unit_tasks, worker_count, args.publish_shards)
    sharded_topics = sorted({task.topic for task in publish_tasks if task.shard_count > 1})
    producer_config = {
        "compression.type": args.publish_compression,
        "linger.ms": args.publish_linger_ms,
    }
    build_future = None
    build_requested = not args.publish_only and not args.skip_image_build
    parallel_phase_started = time.monotonic()
    print(
        f"Publishing {len(NODE_SOURCES)} node"
        + ("" if plan_settings is not None else f" and {len(EDGE_SOURCES)} relationship")
        + f" topics as {len(publish_tasks)} task(s) on {worker_count} worker process(es)"
        + (f"; key-hash sharded: {', '.join(sharded_topics)}" if sharded_topics else "")
    )
    shard_results: list[PublishTaskResult] = []
    # Worker *processes* sidestep the GIL; ``spawn`` avoids forking a parent
    # that already started librdkafka threads for the topic preflight.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="imdb-image-build") as build_executor, \
            ProcessPoolExecutor(max_workers=worker_count, mp_context=multiprocessing.get_context("spawn")) as executor:
        if build_requested:
            # Start the expensive build now, but do not start any consumers
            # until every producer has flushed and this future succeeds.
            print("Building node and edge images in parallel with publishing")
            build_future = build_executor.submit(build_loader_images)
        futures = [
            executor.submit(run_publish_task, task, args.kafka_bootstrap, producer_config)
            for task in publish_tasks
        ]
        try:
            for future in as_completed(futures):
                result = future.result()
                shard_results.append(result)
                shard = f" (shard {result.shard_index + 1}/{result.shard_count})" if result.shard_count > 1 else ""
                print(
                    f"Published {result.count} {result.label} record(s) to {result.topic}{shard} "
                    f"in {result.completed - result.started:.3f}s"
                )
        except BaseException:
            executor.shutdown(wait=True, cancel_futures=True)
            raise
    parallel_phase_seconds = time.monotonic() - parallel_phase_started
    topic_results = merge_publish_results(shard_results)
    total = sum(count for _, _, count, _ in topic_results)
    node_total = sum(count for label, _, count, _ in topic_results if label in NODE_SOURCES)
    if plan_settings is not None:
        print(f"Published {node_total} node record(s); relationship chunks are being planned")
    else:
        print(f"Published {node_total} node and {total - node_total} relationship record(s) in total")
    publish_summary = (
        f"{worker_count} process(es), {len(publish_tasks)} task(s), "
        f"sharded topics: {', '.join(sharded_topics) or 'none'}, "
        f"compression={args.publish_compression}, linger.ms={args.publish_linger_ms}"
    )

    if args.publish_only:
        topic_results = topic_results + planned_topic_results()
        report_path = write_run_report(
            report_directory,
            started_at=started_at,
            total_seconds=time.monotonic() - overall_started,
            partitions=args.partitions,
            publish_workers=worker_count,
            consumer_workers=consumer_workers,
            topic_setup_seconds=topic_setup_seconds,
            parallel_phase_seconds=parallel_phase_seconds,
            image_build_seconds=None,
            topic_results=topic_results,
            consumer_lifecycle_seconds=None,
            consumer_exit_code=None,
            edge_writers=args.edge_writers if args.per_type_edges else None,
            edge_consumers=args.edge_consumers if args.per_type_edges else None,
            node_batch_size=node_batch_size,
            edge_batch_size=edge_batch_size,
            publish_summary=publish_summary,
            bulk_edge_overlap=args.bulk_edge_overlap if args.edge_classic else None,
            edge_plan=plan_report(),
        )
        print(f"Wrote timing report to {report_path}")
        return 0 if not plan_state.get("error") else 1
    image_build_seconds = None
    if build_future is not None:
        try:
            image_build_seconds = build_future.result()
        except Exception as exc:
            report_path = write_run_report(
                report_directory,
                started_at=started_at,
                total_seconds=time.monotonic() - overall_started,
                partitions=args.partitions,
                publish_workers=worker_count,
                consumer_workers=consumer_workers,
                topic_setup_seconds=topic_setup_seconds,
                parallel_phase_seconds=parallel_phase_seconds,
                image_build_seconds=None,
                topic_results=topic_results,
                consumer_lifecycle_seconds=None,
                consumer_exit_code=1,
                edge_writers=args.edge_writers if args.per_type_edges else None,
                edge_consumers=args.edge_consumers if args.per_type_edges else None,
                node_batch_size=node_batch_size,
                edge_batch_size=edge_batch_size,
                publish_summary=publish_summary,
                bulk_edge_overlap=args.bulk_edge_overlap if args.edge_classic else None,
                edge_plan=None if planner is None else {
                    "settings": {}, "stats": {}, "types": {}, "error": "image build failed before planning finished",
                    "writers": args.plan_writers, "topic": plan_settings.topic,
                    "partitions": plan_settings.partitions,
                },
            )
            print(f"Wrote timing report to {report_path}")
            print(f"Loader image build failed; consumer fleet will not start: {exc}", file=sys.stderr)
            return 1
    else:
        print("Using existing node and edge images; image build skipped")

    consumer_lifecycle_started = time.monotonic()
    node_replicas = plan_node_replicas(topic_results, consumer_workers, args.node_replicas_min_records)
    runtime_schema = create_runtime_schema(
        SCHEMA_PATH, consumer_workers, args.edge_writers, args.edge_consumers,
        edge_batch_size=args.edge_batch_size, node_batch_size=args.node_batch_size,
        node_replicas=node_replicas,
    )
    try:
        environment = os.environ.copy()
        environment["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka_bootstrap
        command = [
            sys.executable, "-m", "src.cli", "start", "--mode", "bulk",
            "--config", str(runtime_schema), "--network", args.network,
            "--bulk-timeout-seconds", str(args.bulk_timeout_seconds),
            "--skip-image-build",
        ]
        if args.edge_classic:
            command.extend([
                "--edge-classic", "--bulk-edge-overlap", args.bulk_edge_overlap,
                "--max-concurrent-bulk-edges", str(args.max_concurrent_bulk_edges),
            ])
        if args.edge_leases:
            # P5-S1 Story 1: use the current --bucket-lease-* CLI flag
            # names, not the deprecated --lease-* aliases, so a run of this
            # example doesn't log three deprecation warnings on every start.
            command.extend([
                "--edge-leases", "--bucket-lease-buckets", str(args.lease_buckets),
                "--bucket-lease-outer-batch-size", str(edge_batch_size),
                "--bucket-lease-keyed-max-nodes", str(args.lease_keyed_max_nodes),
            ])
        if plan_settings is not None:
            command.extend(["--edge-plan", str(plan_path), "--plan-writers", str(args.plan_writers)])
        elif getattr(args, "kafka_plan", False):
            # The library plans from the per-type topics while the nodes load.
            command.extend(["--edge-plan", "--plan-writers", str(args.plan_writers),
                            "--plan-workers", str(args.plan_workers or args.publish_workers),
                            "--plan-partitions", str(args.plan_partitions or args.partitions)])
            for flag, value in (("--plan-buckets", args.plan_buckets), ("--plan-hub-degree", args.plan_hub_degree),
                                ("--plan-chunk-rows", args.plan_chunk_rows), ("--plan-topic", args.plan_topic),
                                ("--plan-memory-mb", args.plan_memory_mb), ("--plan-spill-dir", args.plan_spill_dir)):
                if value is not None:
                    command.extend([flag, str(value)])
        print(
            "Starting the public node-plus-edge bulk loader with "
            f"{consumer_workers} node consumer container(s) per type "
            f"(1 for types under {args.node_replicas_min_records} records: "
            f"{', '.join(sorted(topic for topic, count in node_replicas.items() if count != consumer_workers)) or 'none'}), "
            + (
                f"{args.edge_consumers} edge consumer(s) per type, "
                f"{args.edge_writers} writer thread(s) per edge type, " if args.per_type_edges else ""
            )
            + (
                f"edge overlap={args.bulk_edge_overlap}, "
                f"max concurrent edge types={args.max_concurrent_bulk_edges or 'unlimited'}"
                if args.edge_classic else "no classic edge scheduling"
            )
            + (
                f", bucket leases on ({args.lease_buckets} buckets/label, edge outer batch {edge_batch_size})."
                if args.edge_leases else
                f", planned edges on ({args.plan_writers} writers, planned from "
                f"{'Kafka by the loader' if getattr(args, 'kafka_plan', False) else 'the files'})."
                if args.edge_plan else "."
            )
        )
        if plan_settings is None:
            consumer_exit_code, write_timings, timing_directory = run_bulk_loader_with_timings(
                command, environment, report_directory,
            )
            kafka_state["timing_directory"] = timing_directory
        else:
            process = start_bulk_loader(command, environment, timing_directory)
            with ChildSignalForwarder(process) as forwarder:
                try:
                    finish_plan()
                    consumer_exit_code = forwarder.wait()
                except BaseException:
                    process.terminate()
                    forwarder.wait()
                    raise
            if plan_state.get("error") and consumer_exit_code == 0:
                consumer_exit_code = 1
            write_timings = read_write_timings(timing_directory)
            topic_results = topic_results + planned_topic_results()
        consumer_lifecycle_seconds = time.monotonic() - consumer_lifecycle_started
        report_path = write_run_report(
            report_directory,
            started_at=started_at,
            total_seconds=time.monotonic() - overall_started,
            partitions=args.partitions,
            publish_workers=worker_count,
            consumer_workers=consumer_workers,
            topic_setup_seconds=topic_setup_seconds,
            parallel_phase_seconds=parallel_phase_seconds,
            image_build_seconds=image_build_seconds,
            topic_results=topic_results,
            consumer_lifecycle_seconds=consumer_lifecycle_seconds,
            consumer_exit_code=consumer_exit_code,
            write_timings=write_timings,
            edge_writers=args.edge_writers if args.per_type_edges else None,
            edge_consumers=args.edge_consumers if args.per_type_edges else None,
            node_batch_size=node_batch_size,
            edge_batch_size=edge_batch_size,
            publish_summary=publish_summary,
            timing_directory=timing_directory,
            bulk_edge_overlap=args.bulk_edge_overlap if args.edge_classic else None,
            max_concurrent_bulk_edges=args.max_concurrent_bulk_edges,
            node_replicas=node_replicas,
            edge_leases=args.edge_leases,
            lease_buckets=args.lease_buckets,
            edge_plan=plan_report(),
        )
        print(f"Wrote timing report to {report_path}")
        return consumer_exit_code
    finally:
        runtime_schema.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
