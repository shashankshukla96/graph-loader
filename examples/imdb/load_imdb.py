#!/usr/bin/env python3
"""Load an IMDb graph subset into Neo4j with the installed neo4j-graph-loader.

Needs only ``pip install neo4j-graph-loader``, Docker with the four loader images (see the
project's Docker execution guide), a reachable Kafka broker and Neo4j 5 Enterprise.

    export NEO4J_PASSWORD=...        # also read: NEO4J_URI, NEO4J_USER, NEO4J_DATABASE, KAFKA_BOOTSTRAP_SERVERS
    python load_imdb.py graph_schema.yaml data/subsets/titles-10000-depth-4 --neo4j-database imdb

The script (1) validates the schema, (2) publishes every node and relationship TSV of the
subset to Kafka topics that carry a unique prefix, (3) runs ``graph-loader start --mode bulk``
(planned relationships by default) and (4) prints the elapsed time of each phase. Topics are kept
unless you pass ``--delete-topics``; ``--cleanup RUN_DIR`` deletes exactly the topics and consumer
groups a finished run recorded. Options the script does not know are passed to ``graph-loader start``.
"""
from __future__ import annotations

import argparse
import csv
import json
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from neo4j_graph_loader.schema import load_schema

# Full IMDb exports contain quoted fields longer than csv's 128 KiB default.
csv.field_size_limit(sys.maxsize)

PRODUCER_CONFIG = {
    "linger.ms": 50,
    "batch.num.messages": 100_000,
    "queue.buffering.max.messages": 500_000,
    "queue.buffering.max.kbytes": 1_048_576,
    "compression.type": "lz4",
}


def snake(name: str) -> str:
    """``AlternativeTitle`` -> ``alternative_title``."""
    return "".join(f"_{c.lower()}" if c.isupper() and i else c.lower() for i, c in enumerate(name))


def scalar(property_type: str):
    """Text -> the JSON value the schema type expects (dates stay ISO-8601 strings)."""
    return {"integer": int, "float": float}.get(property_type, str)


def publish_file(task: dict) -> dict:
    """Publish one TSV as JSON records keyed by the source (relationships) or key (nodes) column."""
    started = time.monotonic()
    failures = []

    def delivered(error, message):
        if error:
            failures.append(str(error))

    producer = Producer({"bootstrap.servers": task["bootstrap"], **PRODUCER_CONFIG})
    properties = {name: scalar(kind) for name, kind in task["properties"].items()}
    required = set(task["required"])
    produced = 0
    with open(task["path"], encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        header = next(reader)
        # Columns the schema does not declare (for example ``ordering``) are not published.
        columns = [(i, name, properties[name]) for i, name in enumerate(header) if name in properties]
        index = {name: i for i, name in enumerate(header)}
        for line, row in enumerate(reader, start=2):
            if not row:
                continue
            values = {name: cast(row[i]) for i, name, cast in columns if row[i] != ""}
            if task["kind"] == "node":
                if "name" in properties and "name" not in values and "primary_name" in values:
                    values["name"] = values["primary_name"]  # the example schema's Person.name alias
                missing = required - values.keys()
                key = row[index[task["key"]]]
                value = values
            else:
                source, target = row[index[task["source"]]], row[index[task["target"]]]
                missing = required - values.keys()
                if not source or not target:
                    missing = missing | {"endpoint"}
                key = source
                value = {"source": {task["source_key"]: source}, "target": {task["target_key"]: target},
                         "properties": values}
            if missing:
                raise ValueError(f"{task['path']}:{line}: missing {sorted(missing)}")
            while True:
                try:
                    producer.produce(task["topic"], key=key.encode(), value=json.dumps(value).encode(),
                                     on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(0.05)
            produced += 1
            if produced % 10_000 == 0:
                producer.poll(0)
    if producer.flush(120) or failures:
        raise RuntimeError(f"{task['topic']}: undelivered records or delivery errors {failures[:3]}")
    return {"topic": task["topic"], "file": Path(task["path"]).name, "records": produced,
            "seconds": round(time.monotonic() - started, 1)}


def build_run_schema(source: Path, prefix: str, destination: Path) -> Path:
    """Copy the schema with every topic and the consumer group prefixed, so a run owns its topics."""
    document = yaml.safe_load(source.read_text(encoding="utf-8"))
    for entry in [*document.get("nodes", []), *document.get("edges", [])]:
        entry["topic"] = prefix + entry["topic"]
    loading = document.setdefault("loading", {})
    loading["consumer_group_id"] = prefix + loading.get("consumer_group_id", "imdb")
    if "coordination" in loading:
        loading["coordination"]["topic"] = prefix + loading["coordination"]["topic"]
    destination.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    load_schema(destination)  # the run schema must be valid too
    return destination


def publish_tasks(schema, graph: Path, bootstrap: str) -> list[dict]:
    tasks = []
    for node in schema.nodes:
        properties = {name: config.type for name, config in node.properties.items()}
        tasks.append({"kind": "node", "topic": node.topic, "path": str(graph / "nodes" / f"{snake(node.label)}.tsv"),
                      "properties": properties, "required": [n for n, c in node.properties.items() if c.required],
                      "key": node.key_property, "bootstrap": bootstrap})
    for edge in schema.edges:
        properties = {name: config.type for name, config in edge.properties.items()}
        path = graph / "relationships" / f"{edge.type.lower()}.tsv"
        with open(path, encoding="utf-8", newline="") as handle:
            source, target = next(csv.reader(handle, delimiter="\t"))[:2]  # first two columns are the endpoints
        tasks.append({"kind": "edge", "topic": edge.topic, "path": str(path), "properties": properties,
                      "required": [n for n, c in edge.properties.items() if c.required], "source": source,
                      "target": target, "source_key": edge.source_key_property,
                      "target_key": edge.target_key_property, "bootstrap": bootstrap})
    return sorted(tasks, key=lambda t: -Path(t["path"]).stat().st_size)  # largest files first


def delete_run(admin: AdminClient, topics: list[str], group_prefix: str) -> dict:
    """Delete exactly the recorded topics (when present) and this run's consumer groups."""
    present = set(admin.list_topics(timeout=30).topics)
    doomed = [t for t in topics if t in present]
    for future in admin.delete_topics(doomed, operation_timeout=60).values():
        future.result(60)
    groups = [g.group_id for g in admin.list_consumer_groups().result(30).valid if g.group_id.startswith(group_prefix)]
    if groups:
        for future in admin.delete_consumer_groups(groups).values():
            try:
                future.result(30)
            except Exception as error:  # a group that still has members is reported, not forced
                print(f"consumer group not deleted: {error}", file=sys.stderr)
    return {"topics_deleted": doomed, "topics_absent": [t for t in topics if t not in present], "groups": groups}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("schema", nargs="?", type=Path, help="schema YAML (graph_schema.yaml)")
    parser.add_argument("subset_dir", nargs="?", type=Path, help="directory created by build_subset.py")
    parser.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-database", default=os.environ.get("NEO4J_DATABASE"),
                        help="target database (the password is read only from NEO4J_PASSWORD)")
    parser.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--loader-neo4j-uri", help="Bolt URI as the loader containers see it (default: the CLI's)")
    parser.add_argument("--loader-kafka-bootstrap", help="Kafka as the loader containers see it (default: the CLI's)")
    parser.add_argument("--network", default="graph-loader-net", help="Docker network the loader containers join")
    parser.add_argument("--topic-prefix", help="default: imdb-load-<UTC timestamp>-")
    parser.add_argument("--partitions", type=int, default=4, help="partitions per topic")
    parser.add_argument("--publish-workers", type=int, default=min(8, os.cpu_count() or 1))
    parser.add_argument("--run-dir", type=Path, default=Path("runs"), help="where this run's files are kept")
    parser.add_argument("--delete-topics", action="store_true", help="after a successful load, delete this run's topics")
    parser.add_argument("--cleanup", type=Path, metavar="RUN_DIR", help="delete the topics a finished run recorded")
    args, passthrough = parser.parse_known_args()

    if args.cleanup:
        record = json.loads((args.cleanup / "run.json").read_text())
        admin = AdminClient({"bootstrap.servers": record["kafka_bootstrap"]})
        receipt = delete_run(admin, record["topics"], record["group_prefix"])
        (args.cleanup / "cleanup.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt, indent=2))
        return 0
    if not args.schema or not args.subset_dir:
        parser.error("schema and subset_dir are required")
    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        parser.error("set NEO4J_PASSWORD in the environment")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = args.topic_prefix or f"imdb-load-{stamp}-"
    run_dir = (args.run_dir / f"imdb-load-{stamp}").resolve()
    run_dir.mkdir(parents=True)
    schema_path = build_run_schema(args.schema, prefix, run_dir / "schema.yaml")
    schema = load_schema(schema_path)
    graph = args.subset_dir / "graph"
    tasks = publish_tasks(schema, graph, args.kafka_bootstrap)
    chunk_topic = f"{prefix}edge-chunks"
    topics = [t["topic"] for t in tasks]
    record = {"prefix": prefix, "group_prefix": schema.loading.consumer_group_id, "kafka_bootstrap": args.kafka_bootstrap,
              "topics": [*topics, chunk_topic], "subset": str(args.subset_dir), "neo4j_database": args.neo4j_database}
    (run_dir / "run.json").write_text(json.dumps(record, indent=2) + "\n")

    admin = AdminClient({"bootstrap.servers": args.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=30).topics)
    if clash := [t for t in topics if t in existing]:
        raise SystemExit(f"topics already exist, choose another --topic-prefix: {clash[:3]}")
    for future in admin.create_topics([NewTopic(t, num_partitions=args.partitions, replication_factor=1)
                                       for t in topics]).values():
        future.result(60)
    print(f"created {len(topics)} topics with prefix {prefix}", flush=True)

    started = time.monotonic()
    with ProcessPoolExecutor(args.publish_workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(publish_file, tasks))
    published = time.monotonic() - started
    for result in sorted(results, key=lambda r: r["file"]):
        print(f"published {result['records']:>10,} records to {result['topic']} in {result['seconds']}s", flush=True)
    print(f"publish phase: {published:.1f}s, {sum(r['records'] for r in results):,} records", flush=True)

    environment = {k: v for k, v in os.environ.items() if not k.startswith(("NEO4J_", "KAFKA_", "LOADER_"))}
    environment.update(NEO4J_URI=args.neo4j_uri, NEO4J_USERNAME=args.neo4j_user, NEO4J_PASSWORD=password,
                       KAFKA_BOOTSTRAP_SERVERS=args.kafka_bootstrap)
    if args.neo4j_database:
        environment["NEO4J_DATABASE"] = args.neo4j_database
    if args.loader_neo4j_uri:
        environment["LOADER_NEO4J_URI"] = args.loader_neo4j_uri
    if args.loader_kafka_bootstrap:
        environment["LOADER_KAFKA_BOOTSTRAP_SERVERS"] = args.loader_kafka_bootstrap
    executable = shutil.which("graph-loader") or str(Path(sys.executable).parent / "graph-loader")
    command = [executable, "start", "--mode", "bulk", "--config", str(schema_path), "--network", args.network,
               "--run-dir", str(run_dir), "--plan-topic", chunk_topic, "--skip-image-build", *passthrough]
    print("running:", " ".join(command), flush=True)
    started = time.monotonic()
    with (run_dir / "graph-loader.log").open("w") as log:
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            sys.stdout.write(line)
            log.write(line)
        code = process.wait()
    loaded = time.monotonic() - started
    summary = {"publish_seconds": round(published, 1), "graph_loader_seconds": round(loaded, 1), "exit_code": code,
               "records": {r["topic"]: r["records"] for r in results}}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"load phase: {loaded:.1f}s (graph-loader exit {code}); run files in {run_dir}", flush=True)
    if code == 0 and args.delete_topics:
        print(json.dumps(delete_run(admin, record["topics"], record["group_prefix"])))
    return code


if __name__ == "__main__":
    sys.exit(main())
