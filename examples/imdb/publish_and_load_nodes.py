#!/usr/bin/env python3
"""Publish an IMDb graph subset's node and relationship TSVs for bulk loading."""
from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from dotenv import load_dotenv
import yaml

# Permit the documented ``python examples/imdb/publish_and_load_nodes.py``
# invocation without requiring callers to export PYTHONPATH first.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.schema import EdgeConfig, PropertyConfig
from src.orchestrator.docker_service import DockerService
from src.utils.schema_loader import load_schema


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


def read_write_timings(directory: Path) -> dict[str, WriteTimingSummary]:
    """Sum successful Neo4j batch timings captured by the loader replicas."""
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


def run_bulk_loader_with_timings(
    command: list[str], environment: dict[str, str], report_directory: Path,
) -> tuple[int, dict[str, WriteTimingSummary], Path]:
    """Run the loader and retain its Docker-mounted logs beside the report."""
    report_directory.mkdir(parents=True, exist_ok=True)
    timing_directory = Path(tempfile.mkdtemp(prefix="imdb-write-timings-", dir=report_directory))
    environment = {**environment, "GRAPH_LOADER_TIMING_DIR": str(timing_directory)}
    completed = subprocess.run(command, cwd=PROJECT_ROOT, env=environment, check=False)
    return completed.returncode, read_write_timings(timing_directory), timing_directory


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


def publish_tsv(
    producer: Producer,
    topic: str,
    source: Path,
    properties: dict[str, PropertyConfig],
    key_property: str,
) -> int:
    """Publish one graph-node TSV as schema-typed JSON records."""
    count = 0
    with source.open("r", encoding="utf-8", newline="") as input_file:
        for line_number, row in enumerate(csv.DictReader(input_file, delimiter="\t"), start=2):
            try:
                payload = typed_payload(row, properties)
            except ValueError as exc:
                raise ValueError(f"{source}:{line_number}: {exc}") from exc
            producer.produce(
                topic,
                key=str(payload[key_property]).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            )
            producer.poll(0)
            count += 1
    outstanding = producer.flush(60)
    if outstanding:
        raise RuntimeError(f"Kafka did not deliver {outstanding} records for {topic}")
    return count


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
) -> int:
    """Publish one generated relationship TSV as canonical edge events."""
    count = 0
    with source.open("r", encoding="utf-8", newline="") as input_file:
        for line_number, row in enumerate(csv.DictReader(input_file, delimiter="\t"), start=2):
            try:
                payload = edge_payload(row, edge, source_column=source_column, target_column=target_column)
            except ValueError as exc:
                raise ValueError(f"{source}:{line_number}: {exc}") from exc
            producer.produce(topic, key=str(row[source_column]).encode("utf-8"), value=json.dumps(payload).encode("utf-8"))
            producer.poll(0)
            count += 1
    outstanding = producer.flush(60)
    if outstanding:
        raise RuntimeError(f"Kafka did not deliver {outstanding} records for {topic}")
    return count


def publish_topic(
    label: str,
    topic: str,
    source: Path,
    properties: dict[str, PropertyConfig],
    key_property: str,
    bootstrap_servers: str,
) -> tuple[str, str, int, float]:
    """Publish one node type using a producer owned by the calling worker."""
    started = time.monotonic()
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    count = publish_tsv(producer, topic, source, properties, key_property)
    return label, topic, count, time.monotonic() - started


def publish_edge_topic(
    edge_type: str, topic: str, source: Path, edge: EdgeConfig, source_column: str, target_column: str,
    bootstrap_servers: str,
) -> tuple[str, str, int, float]:
    """Publish one relationship type using a producer owned by this worker."""
    started = time.monotonic()
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    count = publish_edge_tsv(producer, topic, source, edge, source_column=source_column, target_column=target_column)
    return edge_type, topic, count, time.monotonic() - started


def build_loader_images() -> float:
    """Build every image required by the public node-plus-edge bulk CLI."""
    started = time.monotonic()
    service = DockerService()
    service.build_image()
    service.build_edge_image()
    return time.monotonic() - started


def create_runtime_schema(
    source: Path, consumer_workers: int, edge_writers: int = 4, edge_consumers: int = 1,
    edge_batch_size: int | None = None, node_batch_size: int | None = None,
) -> Path:
    """Create an ephemeral schema with worker counts and independent batch overrides."""
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
        node["replicas"] = consumer_workers
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
    edge_writers: int = 1,
    edge_consumers: int = 1,
    node_batch_size: int = 500,
    edge_batch_size: int = 500,
    timing_directory: Path | None = None,
) -> Path:
    """Write the one human-readable timing record for this example run."""
    report_directory.mkdir(parents=True, exist_ok=True)
    completed_at = datetime.now(timezone.utc)
    total_nodes = sum(count for label, _, count, _ in topic_results if label in NODE_SOURCES)
    total_relationships = sum(count for label, _, count, _ in topic_results if label in EDGE_SOURCES)
    consumer_count = len(NODE_SOURCES) * consumer_workers
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
        f"- Node-loader containers per node type: {consumer_workers}",
        f"- Edge writer threads per edge type: {edge_writers}",
        f"- Edge consumer containers per edge type: {edge_consumers}",
        f"- Node outer batch size: {node_batch_size}",
        f"- Edge outer batch size: {edge_batch_size}",
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
            summary = write_timings.get(topic)
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
    edge_stages = read_edge_stage_timings(timing_directory) if timing_directory else {}
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
        lines.extend([
            "",
            "Each elapsed time includes the edge container lifecycle, Kafka consumption, drain acknowledgement, and zero-lag verification. Different non-conflicting edge types can overlap.",
            "",
        ])
    lines.extend([
        "",
        "The publication and image-build durations overlap; the parallel-phase duration is the elapsed wall time before consumers can start. The final lifecycle duration includes Docker consumer launch, Kafka consumption, Neo4j node and relationship writes, drain, and zero-lag verification.",
        "",
    ])
    report_path = report_directory / f"imdb-load-{started_at.strftime('%Y%m%dT%H%M%SZ')}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


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
        "--publish-workers",
        type=int,
        default=4,
        help="maximum node-topic publisher threads (default: 4)",
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
        help="edge outer flush size (default: schema setting, 2000 for IMDb)",
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
    if args.edge_batch_size is not None and not 1 <= args.edge_batch_size <= 10_000:
        parser.error("--edge-batch-size must be 1–10000")
    if args.node_batch_size is not None and not 1 <= args.node_batch_size <= 10_000:
        parser.error("--node-batch-size must be 1–10000")
    consumer_workers = args.partitions if args.consumer_workers is None else args.consumer_workers
    if consumer_workers < 1:
        parser.error("--consumer-workers must be positive")

    schema = load_schema(SCHEMA_PATH)
    node_batch_size = args.node_batch_size or schema.loading.unwind_batch_size
    edge_batch_size = args.edge_batch_size or schema.loading.edge_unwind_batch_size or schema.loading.unwind_batch_size
    if not args.publish_only and args.bulk_timeout_seconds <= 0:
        parser.error("--bulk-timeout-seconds must be positive")
    node_config_by_topic = {node.topic: node for node in schema.nodes}
    edge_config_by_type = {edge.type: edge for edge in schema.edges}
    topics = [topic for topic, _ in NODE_SOURCES.values()] + [topic for topic, _, _, _ in EDGE_SOURCES.values()]
    topic_setup_started = time.monotonic()
    ensure_topics(args.kafka_bootstrap, topics, args.partitions)
    try:
        require_empty_topics(args.kafka_bootstrap, topics)
    except ValueError as exc:
        parser.error(str(exc))
    topic_setup_seconds = time.monotonic() - topic_setup_started
    total = 0
    topic_results: list[tuple[str, str, int, float]] = []
    worker_count = min(args.publish_workers, len(NODE_SOURCES) + len(EDGE_SOURCES))
    build_future = None
    build_requested = not args.publish_only and not args.skip_image_build
    parallel_phase_started = time.monotonic()
    print(f"Publishing {len(NODE_SOURCES)} node and {len(EDGE_SOURCES)} relationship topics with {worker_count} worker thread(s)")
    with ThreadPoolExecutor(max_workers=worker_count + build_requested, thread_name_prefix="imdb-publisher") as executor:
        if build_requested:
            # Start the expensive build now, but do not start any consumers
            # until every producer has flushed and this future succeeds.
            print("Building node and edge images in parallel with publishing")
            build_future = executor.submit(build_loader_images)
        futures = [
            executor.submit(
                publish_topic,
                label,
                topic,
                node_dir / filename,
                node_config_by_topic[topic].properties,
                node_config_by_topic[topic].key_property,
                args.kafka_bootstrap,
            )
            for label, (topic, filename) in NODE_SOURCES.items()
        ]
        for future in as_completed(futures):
            label, topic, count, duration = future.result()
            total += count
            topic_results.append((label, topic, count, duration))
            print(f"Published {count} {label} record(s) to {topic} in {duration:.3f}s")
        edge_futures = [
            executor.submit(
                publish_edge_topic, edge_type, topic, relationship_dir / filename,
                edge_config_by_type[edge_type], source_column, target_column, args.kafka_bootstrap,
            )
            for edge_type, (topic, filename, source_column, target_column) in EDGE_SOURCES.items()
        ]
        for future in as_completed(edge_futures):
            edge_type, topic, count, duration = future.result()
            total += count
            topic_results.append((edge_type, topic, count, duration))
            print(f"Published {count} {edge_type} relationship record(s) to {topic} in {duration:.3f}s")
    parallel_phase_seconds = time.monotonic() - parallel_phase_started
    node_total = sum(count for label, _, count, _ in topic_results if label in NODE_SOURCES)
    print(f"Published {node_total} node and {total - node_total} relationship record(s) in total")

    report_directory = args.report_dir or args.subset_dir / "reports"
    if args.publish_only:
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
            edge_writers=args.edge_writers,
            edge_consumers=args.edge_consumers,
            node_batch_size=node_batch_size,
            edge_batch_size=edge_batch_size,
        )
        print(f"Wrote timing report to {report_path}")
        return 0
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
                edge_writers=args.edge_writers,
                edge_consumers=args.edge_consumers,
                node_batch_size=node_batch_size,
                edge_batch_size=edge_batch_size,
            )
            print(f"Wrote timing report to {report_path}")
            print(f"Loader image build failed; consumer fleet will not start: {exc}", file=sys.stderr)
            return 1
    else:
        print("Using existing node and edge images; image build skipped")

    consumer_lifecycle_started = time.monotonic()
    runtime_schema = create_runtime_schema(
        SCHEMA_PATH, consumer_workers, args.edge_writers, args.edge_consumers,
        edge_batch_size=args.edge_batch_size, node_batch_size=args.node_batch_size,
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
        print(
            "Starting the public node-plus-edge bulk loader with "
            f"{consumer_workers} node consumer container(s) per type, {args.edge_consumers} edge consumer(s) per type, "
            f"and {args.edge_writers} writer thread(s) per edge type."
        )
        consumer_exit_code, write_timings, timing_directory = run_bulk_loader_with_timings(
            command, environment, report_directory,
        )
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
            edge_writers=args.edge_writers,
            edge_consumers=args.edge_consumers,
            node_batch_size=node_batch_size,
            edge_batch_size=edge_batch_size,
            timing_directory=timing_directory,
        )
        print(f"Wrote timing report to {report_path}")
        return consumer_exit_code
    finally:
        runtime_schema.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
