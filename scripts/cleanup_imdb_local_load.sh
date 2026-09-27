#!/usr/bin/env bash
# Reset only the IMDb example's Kafka inputs. Existing Neo4j nodes are kept;
# the public loader upserts nodes and relationships on the next run.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

active_fleet="$(docker ps -q --filter 'label=app=graph-loader')"
if [[ -n "$active_fleet" ]]; then
  echo "Refusing cleanup: a Graph Loader fleet is active. Stop that exact run first." >&2
  exit 1
fi

"${PYTHON:-python}" - "$@" <<'PY'
import argparse
import os
from pathlib import Path
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

from examples.imdb.publish_and_load_nodes import EDGE_SOURCES, NODE_SOURCES, SCHEMA_PATH
from src.utils.schema_loader import load_schema

parser = argparse.ArgumentParser(description="Clear only IMDb Kafka inputs and node consumer groups")
parser.add_argument("--dry-run", action="store_true", help="show what would be deleted")
args = parser.parse_args()
load_dotenv(Path(".env"), override=False)
bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
admin = AdminClient({"bootstrap.servers": bootstrap})
topics = sorted(
    {topic for topic, _ in NODE_SOURCES.values()}
    | {topic for topic, *_ in EDGE_SOURCES.values()}
)
schema = load_schema(SCHEMA_PATH)
groups = [f"{schema.loading.consumer_group_id}-{node.label}" for node in schema.nodes]
existing = sorted(set(topics) & set(admin.list_topics(timeout=20).topics))
if args.dry_run:
    print(f"Kafka broker: {bootstrap}")
    print(f"IMDb topics to delete ({len(existing)}): {', '.join(existing) or 'none'}")
    print(f"Node consumer groups to delete if present: {', '.join(groups)}")
    raise SystemExit(0)

if existing:
    for future in admin.delete_topics(existing, operation_timeout=30).values():
        future.result(45)
    deadline = time.monotonic() + 60
    while set(topics) & set(admin.list_topics(timeout=15).topics):
        if time.monotonic() >= deadline:
            raise RuntimeError("IMDb topic deletion did not finish within 60 seconds")
        time.sleep(0.5)

for group, future in admin.delete_consumer_groups(groups, request_timeout=30).items():
    try:
        future.result(35)
    except KafkaException as exc:
        if exc.args[0].code() not in {KafkaError.GROUP_ID_NOT_FOUND, KafkaError._UNKNOWN_GROUP}:
            raise RuntimeError(f"could not delete IMDb node consumer group {group}") from exc

print(f"Cleared {len(existing)} IMDb Kafka input topic(s) and existing node consumer groups on {bootstrap}.")
print("Neo4j data and shared control topics were preserved.")
PY
