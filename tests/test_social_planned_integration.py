"""End-to-end: a non-IMDb schema loaded with ``--edge-plan`` (library planning) and classic mode.

Skipped unless a disposable Kafka + Neo4j Enterprise environment is named by
environment variables (the run creates topics and databases and launches
loader containers on ``GRAPH_LOADER_IT_NETWORK``)::

    GRAPH_LOADER_IT_KAFKA=localhost:19092            # host-side bootstrap
    GRAPH_LOADER_IT_LOADER_KAFKA=scratch-kafka:29092  # bootstrap inside the network
    GRAPH_LOADER_IT_NEO4J_URI=bolt://localhost:17687
    GRAPH_LOADER_IT_LOADER_NEO4J_URI=bolt://scratch-neo4j:7687
    GRAPH_LOADER_IT_NEO4J_PASSWORD=...
    GRAPH_LOADER_IT_NETWORK=scratch-gl-net
    GRAPH_LOADER_NODE_IMAGE=... GRAPH_LOADER_EDGE_IMAGE=...   # images built from this tree
    GRAPH_LOADER_IT_PERSONS=3000                      # optional scale
    GRAPH_LOADER_IT_DATABASE=glit                     # scratch database NAME PREFIX (never neo4j/latesttesttemp)

The run creates (replacing) the databases ``<prefix>planned``, ``<prefix>classic`` and
``<prefix>invalid`` on the server, never the default database.

The planned graph must equal the independently computed expected graph
exactly (every node and relationship with every property, last write wins
for repeated pairs), the classic-mode graph must be identical, and a
malformed relationship record must be rejected row-level to the planner
rejection file without failing the planned run.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from examples.social import social_graph  # noqa: E402
from it_scratch_db import check_database_name, explicit_neo4j_auth  # noqa: E402

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.environ.get("GRAPH_LOADER_IT_KAFKA"), reason="GRAPH_LOADER_IT_* environment not set"),
]


@pytest.fixture(scope="module")
def environment():
    env = os.environ
    # Never the default password: skip before any connection when it is not exported.
    _uri, _user, password = explicit_neo4j_auth(
        password_var="GRAPH_LOADER_IT_NEO4J_PASSWORD", uri_var="GRAPH_LOADER_IT_NEO4J_URI",
        opted_in_by="GRAPH_LOADER_IT_KAFKA",
    )
    return {
        "kafka": env["GRAPH_LOADER_IT_KAFKA"],
        "loader_kafka": env.get("GRAPH_LOADER_IT_LOADER_KAFKA", env["GRAPH_LOADER_IT_KAFKA"]),
        "neo4j": env.get("GRAPH_LOADER_IT_NEO4J_URI", "bolt://localhost:7687"),
        "loader_neo4j": env.get("GRAPH_LOADER_IT_LOADER_NEO4J_URI", env.get("GRAPH_LOADER_IT_NEO4J_URI")),
        "password": password,
        "network": env.get("GRAPH_LOADER_IT_NETWORK", "graph-loader-net"),
        "persons": int(env.get("GRAPH_LOADER_IT_PERSONS", "3000")),
    }


@pytest.fixture(scope="module")
def driver(environment):
    from neo4j import GraphDatabase

    with GraphDatabase.driver(environment["neo4j"], auth=("neo4j", environment["password"])) as driver:
        yield driver


@pytest.fixture(scope="module")
def dataset(environment):
    social_graph.delete_topics(environment["kafka"])
    data = social_graph.generate(environment["persons"], seed=11)
    social_graph.publish(data, environment["kafka"], partitions=3)
    yield data
    social_graph.delete_topics(environment["kafka"])


def _database(driver, prefix: str, suffix: str) -> str:
    """Create (replacing) the scratch database ``<prefix><suffix>``; ``prefix`` is GRAPH_LOADER_IT_DATABASE."""
    name = check_database_name(f"{prefix}{suffix}")
    with driver.session(database="system") as session:
        session.run(f"CREATE OR REPLACE DATABASE `{name}` WAIT").consume()
    return name


def _run_cli(environment, tmp_path, database: str, *extra: str, run_id: str | None = None) -> subprocess.CompletedProcess:
    raw = yaml.safe_load(social_graph.SCHEMA_PATH.read_text(encoding="utf-8"))
    # Node loaders resume their consumer group, so every run gets its own.
    raw["loading"]["consumer_group_id"] = f"social-it-{uuid.uuid4().hex[:8]}"
    config = tmp_path / f"schema-{database}.yaml"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    env = {
        **os.environ, "KAFKA_BOOTSTRAP_SERVERS": environment["kafka"], "NEO4J_URI": environment["neo4j"],
        "NEO4J_USERNAME": "neo4j", "NEO4J_PASSWORD": environment["password"], "NEO4J_DATABASE": database,
        "LOADER_KAFKA_BOOTSTRAP_SERVERS": environment["loader_kafka"],
        "LOADER_NEO4J_URI": environment["loader_neo4j"], "GRAPH_LOADER_TIMING_DIR": str(tmp_path / database),
    }
    return subprocess.run(
        [sys.executable, "-m", "src.cli", "start", "--mode", "bulk", "--config", str(config),
         "--network", environment["network"], "--skip-image-build", "--bulk-timeout-seconds", "300",
         *(["--run-id", run_id] if run_id else []), *extra],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=1200,
    )


def test_planned_load_matches_the_expected_graph_and_classic_mode(environment, driver, dataset, tmp_path, it_database):
    expected = social_graph.expected_graph(dataset)
    planned_db = _database(driver, it_database, "planned")
    planned = _run_cli(environment, tmp_path, planned_db, "--edge-plan")
    assert planned.returncode == 0, planned.stderr[-4000:]
    assert "stage=edge-plan-ready" in planned.stderr and "stage=planned-edges-complete" in planned.stderr
    planned_graph = social_graph.read_graph(driver, planned_db)
    assert social_graph.compare(expected, planned_graph) == []
    evidence = social_graph.last_write_evidence(dataset)
    assert sum(item["pairs_overwritten"] for item in evidence.values()) > 0
    # The plan ran through the library planner, from the per-type topics.
    plan = json.loads((tmp_path / planned_db / "edge-plan.json").read_text())
    assert plan["stats"]["source"] == "kafka" and plan["stats"]["rows"] == dataset.edge_count
    assert plan["stats"]["hubs"] > 0

    classic_db = _database(driver, it_database, "classic")
    classic = _run_cli(environment, tmp_path, classic_db, "--edge-classic")
    assert classic.returncode == 0, classic.stderr[-4000:]
    classic_graph = social_graph.read_graph(driver, classic_db)
    assert social_graph.compare(expected, classic_graph) == []
    assert social_graph.compare(planned_graph[:2], classic_graph) == []


def test_a_malformed_relationship_record_is_rejected_row_level_and_the_planned_run_succeeds(
    environment, driver, dataset, tmp_path, it_database,
):
    """Since v7 (56af85f) a ValueError record goes to ``var/rejections/<run>-planner-<n>.jsonl``, not fail-closed."""
    from confluent_kafka import Producer

    delivered: list[tuple[int, int]] = []

    def on_delivery(error, message) -> None:
        assert error is None, error
        delivered.append((message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": environment["kafka"]})
    producer.produce("social-visited", key=b"1", value=b'{"source": {"person_id": "1"}, "target": {"name": "City A"},'
                                                     b' "properties": {"times": 1}}', partition=2,
                     on_delivery=on_delivery)
    assert producer.flush(30) == 0
    assert len(delivered) == 1
    partition, offset = delivered[0]
    database = _database(driver, it_database, "invalid")
    run_id = f"socialit{uuid.uuid4().hex[:10]}"
    rejection_dir = ROOT / "var" / "rejections"
    files: list[Path] = []
    try:
        result = _run_cli(environment, tmp_path, database, "--edge-plan", run_id=run_id)
        assert result.returncode == 0, result.stderr[-4000:]
        outcome = [line for line in result.stderr.splitlines() if "stage=outcome rejected=1 " in line]
        assert outcome and "WARNING" in outcome[0], result.stderr[-4000:]
        # Rejection files are named after this exact (unique) run id; no other run's files match.
        files = sorted(rejection_dir.glob(f"{run_id}-planner-*.jsonl"))
        entries = [json.loads(line) for path in files for line in path.read_text().splitlines()]
        assert len(entries) == 1, entries
        assert entries[0]["topic"] == "social-visited" and entries[0]["partition"] == partition
        assert entries[0]["offset"] == offset and entries[0]["edge_type"] == "VISITED"
        assert entries[0]["reason"]
        # The rest of the graph is exactly the expected one: the rejected record created no relationship.
        assert social_graph.compare(social_graph.expected_graph(dataset), social_graph.read_graph(driver, database)) == []
    finally:
        for path in files:
            path.unlink(missing_ok=True)
