#!/usr/bin/env python3
"""Publish an IMDb graph subset's node TSVs and run the Phase 2 bulk loader.

Relationship TSVs deliberately remain local artifacts. They document the full
IMDb graph model and are ready for the Phase 3 relationship loader, but this
example verifies only the current production node-loading path.
"""
from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
import yaml

# Permit the documented ``python examples/imdb/publish_and_load_nodes.py``
# invocation without requiring callers to export PYTHONPATH first.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.schema import PropertyConfig
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


def build_node_loader_image() -> float:
    """Build the image and return the wall-clock time consumed by that process."""
    started = time.monotonic()
    DockerService().build_image()
    return time.monotonic() - started


def create_runtime_schema(source: Path, consumer_workers: int) -> Path:
    """Create an ephemeral schema that starts the requested replica count."""
    with source.open("r", encoding="utf-8") as input_file:
        raw_schema = yaml.safe_load(input_file)
    for node in raw_schema["nodes"]:
        node["replicas"] = consumer_workers

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
) -> Path:
    """Write the one human-readable timing record for this example run."""
    report_directory.mkdir(parents=True, exist_ok=True)
    completed_at = datetime.now(timezone.utc)
    total_nodes = sum(count for _, _, count, _ in topic_results)
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
        f"- Node-loader containers started: {consumer_count if consumer_exit_code == 0 else 'not confirmed'}",
        f"- Node records sent to Kafka: {total_nodes}",
        "",
        "## Timings",
        "",
        "| Step | Wall-clock time |",
        "| --- | ---: |",
        f"| Topic create/partition expansion | {topic_setup_seconds:.3f}s |",
        f"| Parallel publish + image-build phase | {parallel_phase_seconds:.3f}s |",
        f"| Docker image build | {'not run' if image_build_seconds is None else f'{image_build_seconds:.3f}s'} |",
        f"| Kafka consumption + Neo4j node loading + drain verification | {'not run' if consumer_lifecycle_seconds is None else f'{consumer_lifecycle_seconds:.3f}s'} |",
        f"| Total end-to-end | {total_seconds:.3f}s |",
        "",
        "## Kafka publication by node type",
        "",
        "| Node type | Topic | Records flushed to Kafka | Time |",
        "| --- | --- | ---: | ---: |",
    ]
    lines.extend(
        f"| {label} | `{topic}` | {count} | {duration:.3f}s |"
        for label, topic, count, duration in sorted(topic_results)
    )
    lines.extend([
        "",
        "The publication and image-build durations overlap; the parallel-phase duration is the elapsed wall time before consumers can start. The final lifecycle duration includes Docker consumer launch, Kafka consumption, Neo4j node upserts, drain, and zero-lag verification.",
        "",
    ])
    report_path = report_directory / f"imdb-load-{started_at.strftime('%Y%m%dT%H%M%SZ')}.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subset-dir", type=Path, required=True, help="directory created by build_subset.py")
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--network", default="graph-loader-net", help="Docker network for the loader fleet")
    parser.add_argument("--bulk-timeout-seconds", type=float, default=300.0)
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
    parser.add_argument("--publish-only", action="store_true", help="do not start the Neo4j bulk loader")
    parser.add_argument(
        "--skip-image-build",
        action="store_true",
        help="use an existing graph-loader-node image instead of building one",
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
    missing = [filename for _, filename in NODE_SOURCES.values() if not (node_dir / filename).is_file()]
    if missing:
        parser.error(f"subset is missing generated node TSVs: {', '.join(missing)}")
    if args.publish_workers < 1 or args.partitions < 1:
        parser.error("--publish-workers and --partitions must be positive")
    consumer_workers = args.partitions if args.consumer_workers is None else args.consumer_workers
    if consumer_workers < 1:
        parser.error("--consumer-workers must be positive")

    schema = load_schema(SCHEMA_PATH)
    node_config_by_topic = {node.topic: node for node in schema.nodes}
    topics = [topic for topic, _ in NODE_SOURCES.values()]
    topic_setup_started = time.monotonic()
    ensure_topics(args.kafka_bootstrap, topics, args.partitions)
    topic_setup_seconds = time.monotonic() - topic_setup_started
    total = 0
    topic_results: list[tuple[str, str, int, float]] = []
    worker_count = min(args.publish_workers, len(NODE_SOURCES))
    build_future = None
    build_requested = not args.publish_only and not args.skip_image_build
    parallel_phase_started = time.monotonic()
    print(f"Publishing {len(NODE_SOURCES)} node topics with {worker_count} worker thread(s)")
    with ThreadPoolExecutor(max_workers=worker_count + build_requested, thread_name_prefix="imdb-publisher") as executor:
        if build_requested:
            # Start the expensive build now, but do not start any consumers
            # until every producer has flushed and this future succeeds.
            print("Building the node-loader image in parallel with publishing")
            build_future = executor.submit(build_node_loader_image)
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
    parallel_phase_seconds = time.monotonic() - parallel_phase_started
    print(f"Published {total} node record(s) in total")

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
            )
            print(f"Wrote timing report to {report_path}")
            print(f"Node-loader image build failed; consumer fleet will not start: {exc}", file=sys.stderr)
            return 1
    else:
        print("Using the existing node-loader image; image build skipped")

    consumer_lifecycle_started = time.monotonic()
    runtime_schema = create_runtime_schema(SCHEMA_PATH, consumer_workers)
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
            "Starting the Phase 2 node bulk loader with "
            f"{consumer_workers} consumer container(s) per node type; relationship TSVs are not published."
        )
        completed = subprocess.run(command, cwd=PROJECT_ROOT, env=environment, check=False)
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
            consumer_exit_code=completed.returncode,
        )
        print(f"Wrote timing report to {report_path}")
        return completed.returncode
    finally:
        runtime_schema.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
