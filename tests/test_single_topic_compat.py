"""v7-S6 story 6.6: backward compatibility of every shipped single-topic schema.

``tests/golden/single_topic_compat.json`` was recorded with THIS module's ``fingerprint`` on the tree before
v7-S1 (feat/multi-topic 4382960) and re-recorded unchanged on the multi-topic tree, so the comparison below is a
true before/after golden: for each shipped schema, the DLQ / parked / parked-pair-index topic names, the
consumer group ids, the edge-loader container commands, the lag series and the planned-mode chunk messages
(byte-identical) must not change when a schema only uses ``topic:``. ``plan_hub`` repeats the planned fingerprint
with ``hub_degree=4`` (the hub path; final follow-ups nit), recorded on 4382960 like the rest: every other key of
the golden was byte-identical in that re-recording.  The plan JSON itself is excluded because
S3 intentionally added ``stats.type_topics``.

What it does NOT pin: the plan JSON's ``stats`` (timings, and ``type_topics`` added by S3); the rest of the plan JSON is hashed. The chunk
hash depends on ``test_edge_planner.FakeCluster`` / ``FakeProducer`` (the synthetic partitioning and the fake
Kafka); changing those helpers legitimately changes the hash.  The node-loader consumer group id
(``<prefix>-<Label>``) is computed inline in ``node_loader.main``, so only what the CLI hands the node container is
pinned here.

One intentional change is applied on top of the golden, never written into it: stable stream consumer groups
(fix/stream-stable-groups). A stream relationship loader's group is ``<prefix>-<type>``, no longer
``<prefix>-<type>-<run id>``, so a stream restart resumes. :func:`_expected` replaces exactly the ``stream.group``
of every edge; every other key, every bulk key included, is compared with the untouched golden.

Regenerate (only for an intentional change, with the reason in the commit):
``python tests/test_single_topic_compat.py --dump tests/golden/single_topic_compat.json``
"""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests" / "golden" / "single_topic_compat.json"
SCHEMAS = [
    "config/graph_schema.yaml",
    "examples/imdb/graph_schema.yaml",
    "examples/social/graph_schema.yaml",
    # No shipped schema enables dead_letter on a type or an order_by edge, so this fixture is what actually pins
    # the DLQ / parked / parked-pair-index names (default and overridden) and an order_by edge.
    "tests/golden/fixtures/single_topic_dlq.yaml",
]
RUN_ID = "run-A"
HUB_DEGREE = 4


def _sample(prop_type: str, index: int):
    return {
        "string": f"s{index}", "integer": index, "float": index + 0.5,
        "date": f"2020-01-{1 + index % 28:02d}", "datetime": f"2020-01-01T00:00:{index % 60:02d}+00:00",
    }[prop_type]


def _node_property_type(schema, label: str) -> str:
    node = next(n for n in schema.nodes if n.label == label)
    return node.properties[node.key_property].type


def _edge_records(schema, edge, count: int = 40) -> list[bytes]:
    source_type = _node_property_type(schema, edge.nodes.source)
    target_type = _node_property_type(schema, edge.nodes.target)
    records = []
    for i in range(count):
        properties = {name: _sample(prop.type, i) for name, prop in edge.properties.items() if prop.required}
        records.append(json.dumps({
            "source": {edge.source_key_property: _sample(source_type, i % 7)},
            "target": {edge.target_key_property: _sample(target_type, i % 11)},
            "properties": properties,
        }, sort_keys=True).encode())
    return records


def _plan_fingerprint(schema, workdir: Path, *, hub_degree: int = 0) -> dict:
    from types import SimpleNamespace

    from src.loader.edge_planner import KafkaEdgePlanner, PlanSettings
    from test_edge_planner import FakeCluster, FakeProducer

    edges = list(schema.edges)
    cluster = FakeCluster()
    for edge in edges:
        topic = edge.topic
        cluster.topic(topic, 2)
        for index, value in enumerate(_edge_records(schema, edge)):
            cluster.append(topic, index % 2, value)
    settings = PlanSettings(buckets=3, hub_degree=hub_degree, chunk_rows=7, topic="chunks", partitions=2, workers=1,
                            memory_mb=0, spill_dir=str(workdir / "spill"))
    planner = KafkaEdgePlanner(SimpleNamespace(nodes=schema.nodes, edges=edges), "kafka:1", settings, plan_id="run-1",
                               context="inline", consumer_factory=cluster.consumer,
                               producer_factory=lambda: FakeProducer(cluster), admin_factory=cluster.admin)
    planner.start()
    result = planner.finish(workdir / "plan.json")
    digest = hashlib.sha256()
    for partition in sorted(cluster.logs[result.plan.topic]):
        for offset, message in sorted(cluster.logs[result.plan.topic][partition].items()):
            digest.update(f"{partition}:{offset}:".encode())
            digest.update(message)
    plan_json = json.loads((workdir / "plan.json").read_text(encoding="utf-8"))
    # ``stats`` holds timings (nondeterministic) and, since v7-S3, ``type_topics``: leave it out entirely.
    hub_count = (plan_json.pop("stats", None) or {}).get("hubs")
    plan_hash = hashlib.sha256(json.dumps(plan_json, sort_keys=True).encode()).hexdigest()
    out = {"chunks": len(result.plan.chunks), "chunk_messages_sha256": digest.hexdigest(),
           "plan_json_without_stats_sha256": plan_hash}
    if hub_degree:
        out["hubs"] = hub_count  # the hub path must actually be taken (not vacuous)
    return out


def _names(entity_topic: str, dead_letter) -> list:
    from src.models import schema as schema_module

    return [
        schema_module.dead_letter_topic_name(entity_topic, dead_letter),
        schema_module.parked_topic_name(entity_topic, dead_letter),
        schema_module.parked_pair_index_topic_name(entity_topic, dead_letter),
    ]


def _launch_env(edge, mode: str, config_path: str) -> dict:
    from src.orchestrator.docker_service import DockerService

    client = MagicMock()
    client.containers.run.return_value = MagicMock()
    service = DockerService(client=client)
    with tempfile.TemporaryDirectory() as rejection_dir:
        common = dict(edge_type=edge.type, mode=mode, config_path=config_path, network="net",
                      rejection_dir=rejection_dir, run_id=RUN_ID, consumer_group_prefix="prefix")
        if hasattr(edge, "topics"):
            service.run_edge_loader(topics=edge.topics, **common)  # what the CLI passes since v7-S2
        else:  # the pre-v7 tree only knows ``topic=``
            service.run_edge_loader(topic=edge.topic, **common)
    call = client.containers.run.call_args.kwargs
    return {
        "group": call["environment"].get("KAFKA_GROUP_ID"),
        "command": call["command"],
        "rejection_file": Path(call["environment"]["REJECTION_LOG_PATH"]).name,
    }


def _node_launch(node, mode: str, config_path: str) -> dict:
    from src.orchestrator.docker_service import DockerService

    client = MagicMock()
    client.containers.run.return_value = MagicMock()
    with tempfile.TemporaryDirectory() as rejection_dir:
        DockerService(client=client).run_node_loader(
            node_label=node.label, topic=node.topic, mode=mode, config_path=config_path, replicas=1, network="net",
            rejection_dir=rejection_dir, run_id=RUN_ID,
        )
    call = client.containers.run.call_args.kwargs
    environment = call["environment"]
    return {
        "command": call["command"],
        "group_env": environment.get("KAFKA_GROUP_ID"),
        "rejection_file": Path(environment["REJECTION_LOG_PATH"]).name,
    }


def _lag_series(edge) -> list:
    """Lag series the real monitor code creates for one single-topic type (labels and value), via a refresh."""
    from prometheus_client import CollectorRegistry

    from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
    from src.utils.metrics import GraphLoaderMetrics

    from types import SimpleNamespace

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(
        topics={edge.topic: SimpleNamespace(error=None, partitions={0: SimpleNamespace()})})
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    monitor = RelationshipBulkMonitor(
        [edge], RUN_ID, {(edge.type, "0")}, {(edge.type, "0"): container}, bootstrap_servers="kafka:9092",
        consumer_group_prefix="prefix", admin_client=admin, watermark_consumer=MagicMock(),
        control_consumer=MagicMock(), metrics=metrics,
    )
    # the same fake broker state in every tree: watermarks (0, 10), 4 records committed
    monitor._watermarks_low_high = lambda timeout=None: {(edge.type, edge.topic, 0): (0, 10)}
    monitor._commits = lambda timeout=None: {(edge.type, edge.topic, 0): 4}
    monitor._refresh_consumer_lag()
    return sorted(
        [dict(sample.labels), sample.value]
        for family in metrics.registry.collect() if family.name == "graph_loader_consumer_lag_records"
        for sample in family.samples
    )


def fingerprint(relative_path: str) -> dict:
    from src.utils.schema_loader import load_schema

    schema = load_schema(ROOT / relative_path)
    out: dict = {"prefix": schema.loading.consumer_group_id, "nodes": {}, "edges": {}}
    for node in schema.nodes:
        out["nodes"][node.label] = {
            "topic": node.topic, "dlq": _names(node.topic, node.dead_letter),
            "stream": _node_launch(node, "stream", relative_path), "bulk": _node_launch(node, "bulk", relative_path),
        }
    for edge in schema.edges:
        with tempfile.TemporaryDirectory() as workdir:
            entry = {
                "topic": edge.topic,
                "nodes": [edge.nodes.source, edge.nodes.target],
                "order_by": edge.order_by,
                "dlq": _names(edge.topic, edge.dead_letter),
                "lag_series": _lag_series(edge),
                "stream": _launch_env(edge, "stream", relative_path),
                "bulk": _launch_env(edge, "bulk", relative_path),
            }
        out["edges"][edge.type] = entry
    with tempfile.TemporaryDirectory() as workdir:
        out["plan"] = _plan_fingerprint(schema, Path(workdir)) if schema.edges else None
    with tempfile.TemporaryDirectory() as workdir:
        # Final follow-ups nit: the planned hub path (hub_degree > 0; sources repeat every 7 records).
        out["plan_hub"] = _plan_fingerprint(schema, Path(workdir), hub_degree=HUB_DEGREE) if schema.edges else None
    return out


def fingerprint_all() -> dict:
    return {path: fingerprint(path) for path in SCHEMAS}


@pytest.fixture(scope="module")
def golden() -> dict:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def _expected(recorded: dict) -> dict:
    """The golden with the one intentional change: stable stream relationship groups ``<prefix>-<type>``."""
    expected = json.loads(json.dumps(recorded))
    for edge_type, entry in expected["edges"].items():
        assert entry["stream"]["group"] == f"prefix-{edge_type}-{RUN_ID}"  # what the golden recorded
        entry["stream"]["group"] = f"prefix-{edge_type}"
    return expected


@pytest.mark.parametrize("path", SCHEMAS)
def test_shipped_single_topic_schema_matches_the_pre_v7_golden(path, golden) -> None:
    assert fingerprint(path) == _expected(golden[path])


@pytest.mark.parametrize("path", SCHEMAS)
def test_bulk_launches_are_byte_identical_to_the_golden(path, golden) -> None:
    """Bulk is untouched by stable stream groups: every bulk key equals the golden as recorded."""
    current = fingerprint(path)
    assert current["edges"], path
    for edge_type, entry in current["edges"].items():
        assert entry["bulk"] == golden[path]["edges"][edge_type]["bulk"]
        assert entry["bulk"]["group"] == f"prefix-{edge_type}-{RUN_ID}"
        assert entry["lag_series"] == golden[path]["edges"][edge_type]["lag_series"]
    for label, entry in current["nodes"].items():
        assert entry["bulk"] == golden[path]["nodes"][label]["bulk"]
    assert current["plan"] == golden[path]["plan"]
    assert current["plan_hub"] == golden[path]["plan_hub"]


@pytest.mark.parametrize("path", SCHEMAS)
def test_new_naming_api_agrees_with_the_old_topic_based_names(path, golden) -> None:
    """``edge_*_name(edge)`` (the multi-topic API) must give the single-topic names the golden recorded."""
    from src.models.schema import (
        edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name,
    )
    from src.utils.schema_loader import load_schema

    schema = load_schema(ROOT / path)
    for edge in schema.edges:
        assert edge.is_multi_topic is False
        assert edge.topics == [edge.topic]
        assert edge.dead_letter_entity == edge.topic
        assert edge.conflict_rule in ("single", "order_by")
        assert [edge_dead_letter_topic_name(edge), edge_parked_topic_name(edge),
                edge_parked_pair_index_topic_name(edge)] == golden[path]["edges"][edge.type]["dlq"]


def test_the_golden_pins_real_dlq_names_and_an_order_by_edge(golden) -> None:
    """Guard against a vacuous golden: None == None would pass the naming comparisons."""
    fixture = golden["tests/golden/fixtures/single_topic_dlq.yaml"]
    assert fixture["nodes"]["Person"]["dlq"] == ["compat-person.dlq", "compat-person.dlq.parked",
                                                 "compat-person.dlq.parked-pairs"]
    assert fixture["nodes"]["Company"]["dlq"][0] == "compat-company-rejects"
    assert fixture["edges"]["WORKS_AT"]["dlq"][0] == "compat-works-at.dlq"
    assert fixture["edges"]["EMPLOYED_BY"]["dlq"][:2] == ["compat-employed-by-failures", "compat-employed-by-parked"]
    assert fixture["edges"]["FOLLOWS"]["order_by"] == "updated_at"
    assert fixture["edges"]["KNOWS"]["dlq"] == [None, None, None]
    non_null = [e["dlq"] for g in golden.values() for e in g["edges"].values() if e["dlq"][0] is not None]
    assert len(non_null) >= 3
    series = [e["lag_series"] for g in golden.values() for e in g["edges"].values()]
    assert all(len(item) == 1 and item[0][1] == 6.0 for item in series)  # one series per type, lag 10 - 4
    # Final follow-ups nit: the planned hub path is taken for every schema with relationships.
    assert all(g["plan_hub"]["hubs"] > 0 and g["plan_hub"] != g["plan"] for g in golden.values() if g["plan"])


def test_every_tracked_schema_with_only_topic_keys_is_covered() -> None:
    """A new shipped single-topic schema must be added to ``SCHEMAS`` (and the golden re-recorded)."""
    import subprocess

    tracked = subprocess.run(["git", "ls-files", "config", "examples"], cwd=ROOT, capture_output=True, text=True,
                             check=True).stdout.split()
    shipped = {p for p in tracked if p.endswith("graph_schema.yaml")}
    multi_topic_variants = {p for p in shipped if "multi_topic" in p}
    assert shipped - multi_topic_variants <= set(SCHEMAS), sorted(shipped - set(SCHEMAS))


if __name__ == "__main__":
    # `--dump FILE`: record the golden (run with PYTHONPATH=<tree>:<tree>/tests, also on the pre-v7 tree).
    if len(sys.argv) == 3 and sys.argv[1] == "--dump":
        Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
        Path(sys.argv[2]).write_text(json.dumps(fingerprint_all(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        raise SystemExit(__doc__)
