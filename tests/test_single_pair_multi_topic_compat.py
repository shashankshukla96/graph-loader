"""v7-S7 (review S3): byte-level compatibility of multi-topic types that bind ONE label pair.

S1-S6 made a relationship type read several topics. S7 lets those topics bind different label pairs, and
it changes code that multi-topic single-pair types also go through: the DLQ key, the sequenced parked-pair
index, the precedence sub-edge (now ``EdgeConfig.topic_view``) and the planner's read tasks. This golden
pins, for every type and topic of ``tests/golden/fixtures/single_pair_multi_topic.yaml``:

* the precedence sub-stage plan (``cli._precedence_substage_plan``) and every sub-edge
  (``cli._single_topic_sub_edge``): its dump (minus S7's new ``topic_endpoints`` key), DLQ names, and the
  SHA-256 of every Cypher text built from it;
* ``DeadLetterRecordSink.route_unresolved_endpoint`` key/value bytes for a row of each topic;
* the wire key and value bytes of a sequenced ``ParkedPairIndex`` for one pair added from both topics;
* the planned-mode plan JSON (minus ``stats``) and chunk messages (``test_edge_planner.FakeCluster``), without
  and (``plan_hub``, final follow-ups nit) with the hub path (``hub_degree=3``).

``tests/golden/single_pair_multi_topic_compat.json`` was recorded with this module on feat/multi-topic
8748274 (the S7 base, before any S7 change); the S7 tree must reproduce it byte for byte.

Record: ``git archive 8748274 | tar -x -C /tmp/base``; copy this module and the fixture into it; then
``cd /tmp/base && PYTHONPATH=.:tests python tests/test_single_pair_multi_topic_compat.py --dump <golden>``.
"""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests" / "golden" / "single_pair_multi_topic_compat.json"
FIXTURE = "tests/golden/fixtures/single_pair_multi_topic.yaml"
FIXED_NOW_MS = 1_700_000_000_000
HUB_DEGREE = 3


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _key(schema, label: str, index: int):
    node = next(n for n in schema.nodes if n.label == label)
    return index if node.properties[node.key_property].type == "integer" else f"k{index}"


def _raw(schema, edge, index: int) -> dict:
    samples = {"date": f"2020-01-{1 + index % 28:02d}", "string": f"v{index}", "float": index + 0.25}
    return {
        "source": {edge.source_key_property: _key(schema, edge.nodes.source, index % 5)},
        "target": {edge.target_key_property: _key(schema, edge.nodes.target, index % 3)},
        "properties": {name: samples[prop.type] for name, prop in edge.properties.items()},
    }


def _queries(edge) -> dict:
    from src.loader import edge_execution

    return {
        "apoc_locked": _sha(edge_execution.build_apoc_locked_upsert_query(edge)),
        "apoc_lease": _sha(edge_execution.build_apoc_locked_upsert_query(edge, explicit_locks=False,
                                                                         sort_rows=False)),
        "apoc_collect": _sha(edge_execution.build_apoc_locked_upsert_query(edge, explicit_locks=False)),
        "preflight": _sha(edge_execution._preflight_query(edge)),
        "classification": _sha(edge_execution._endpoint_classification_query(edge)),
    }


def _names(edge) -> list:
    from src.models.schema import (
        edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name,
    )

    return [edge_dead_letter_topic_name(edge), edge_parked_topic_name(edge), edge_parked_pair_index_topic_name(edge)]


def _sub_edges(edge) -> dict:
    from src import cli

    out = {}
    for topic in edge.topics:
        sub = cli._single_topic_sub_edge(edge, topic)
        dump = sub.model_dump(mode="json")
        dump.pop("topic_endpoints", None)
        out[topic] = {"dump": dump, "names": _names(sub), "queries": _queries(sub),
                      "conflict_rule": sub.conflict_rule, "is_multi_topic": sub.is_multi_topic}
    return out


class _CapturingProducer:
    def __init__(self) -> None:
        self.published: list = []

    def publish(self, topic, envelope, **_kwargs) -> None:
        self.published.append((topic, envelope))


def _dead_letter(schema, edge) -> dict:
    from src.loader.dead_letter import DeadLetterRecordSink, encode_envelope
    from src.loader.edge_records import normalize_edge_record

    producer = _CapturingProducer()
    sink = DeadLetterRecordSink(producer=producer, topic=_names(edge)[0], run_id="run-A", replica_id="0",
                                kind="edge", clock=lambda: FIXED_NOW_MS)
    for offset, topic in enumerate(edge.topics):
        record = normalize_edge_record(_raw(schema, edge, 4), edge)
        sink.route_unresolved_endpoint(
            topic=topic, partition=1, offset=10 + offset, record=record, edge_type=edge.type,
            source_key_property=edge.source_key_property, target_key_property=edge.target_key_property,
            failure_class="missing_endpoint", reason_code="missing_target", error_text="target not found",
            retry_after_seconds=2.0,
        )
    return {topic: {"dlq": dlq, "key": envelope.key_bytes.decode(), "value": envelope.value_bytes.decode(),
                    "encoded_sha256": hashlib.sha256(encode_envelope(envelope)).hexdigest()}
            for topic, (dlq, envelope) in zip(edge.topics, producer.published)}


def _index(schema, edge) -> list:
    from src.loader.edge_records import normalize_edge_record
    from src.loader.parked_pair_index import ParkedPairIndex

    record = normalize_edge_record(_raw(schema, edge, 4), edge)
    index = ParkedPairIndex(topic=_names(edge)[2], bootstrap_servers="kafka:1", writable=True,
                            sequenced=edge.is_multi_topic)
    published: list = []
    index._publish = lambda key, value: published.append((key, value))
    for offset, topic in enumerate(reversed(edge.topics)):  # the later-listed topic first
        index.add(record.source_key, record.target_key, original_topic=topic, original_partition=1,
                  original_offset=10 + offset, first_failure_at_ms=FIXED_NOW_MS)
    return [[key if isinstance(key, str) else key.decode(), value.decode()] for key, value in published]


def _plan(schema, workdir: Path, *, hub_degree: int = 0) -> dict:
    from types import SimpleNamespace

    from src.loader.edge_planner import KafkaEdgePlanner, PlanSettings
    from test_edge_planner import FakeCluster, FakeProducer

    cluster = FakeCluster()
    for edge in schema.edges:
        for rank, topic in enumerate(edge.topics):
            cluster.topic(topic, 2)
            for index in range(12):
                cluster.append(topic, index % 2, json.dumps(_raw(schema, edge, index + rank), sort_keys=True).encode())
    settings = PlanSettings(buckets=3, hub_degree=hub_degree, chunk_rows=5, topic="chunks", partitions=2, workers=1,
                            memory_mb=0, spill_dir=str(workdir / "spill"))
    planner = KafkaEdgePlanner(SimpleNamespace(nodes=schema.nodes, edges=list(schema.edges)), "kafka:1", settings,
                               plan_id="run-1", context="inline", consumer_factory=cluster.consumer,
                               producer_factory=lambda: FakeProducer(cluster), admin_factory=cluster.admin)
    planner.start()
    result = planner.finish(workdir / "plan.json")
    digest = hashlib.sha256()
    for partition in sorted(cluster.logs[result.plan.topic]):
        for offset, message in sorted(cluster.logs[result.plan.topic][partition].items()):
            digest.update(f"{partition}:{offset}:".encode())
            digest.update(message)
    plan_json = json.loads((workdir / "plan.json").read_text(encoding="utf-8"))
    hub_count = (plan_json.pop("stats", None) or {}).get("hubs")
    out = {"chunks": len(result.plan.chunks), "chunk_messages_sha256": digest.hexdigest(),
           "plan_json_without_stats_sha256": _sha(json.dumps(plan_json, sort_keys=True))}
    if hub_degree:
        out["hubs"] = hub_count  # the hub path must actually be taken (not vacuous)
    return out


def fingerprint() -> dict:
    from src import cli
    from src.utils.schema_loader import load_schema

    schema = load_schema(ROOT / FIXTURE)
    out: dict = {"edges": {}}
    for edge in schema.edges:
        entry = {"topics": list(edge.topics), "conflict_rule": edge.conflict_rule, "names": _names(edge),
                 "queries": _queries(edge), "sub_edges": _sub_edges(edge), "dead_letter": _dead_letter(schema, edge),
                 "index": _index(schema, edge)}
        if edge.conflict_rule == "precedence":
            entry["substages"] = [[rank, topic, run_id] for rank, topic, run_id, _sub in
                                  cli._precedence_substage_plan(edge, "run-A")]
        out["edges"][edge.type] = entry
    with tempfile.TemporaryDirectory() as workdir:
        out["plan"] = _plan(schema, Path(workdir))
    with tempfile.TemporaryDirectory() as workdir:
        # Final follow-ups nit: the planned hub path (hub_degree > 0), recorded on 8748274 like the rest.
        out["plan_hub"] = _plan(schema, Path(workdir), hub_degree=HUB_DEGREE)
    return out


def test_multi_topic_single_pair_types_keep_the_s7_base_bytes() -> None:
    assert fingerprint() == json.loads(GOLDEN.read_text(encoding="utf-8"))


def test_the_golden_is_not_vacuous() -> None:
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    edges = golden["edges"]
    assert {entry["conflict_rule"] for entry in edges.values()} == {"order_by", "precedence", "arrival"}
    assert len(edges["EMPLOYED_BY"]["substages"]) == 2
    assert all(json.loads(item["key"]) and len(json.loads(item["key"])) == 2
               for entry in edges.values() for item in entry["dead_letter"].values())
    assert any('"seq":2' in value for entry in edges.values() for _key, value in entry["index"])
    assert golden["plan"]["chunks"] > 0
    assert golden["plan_hub"]["hubs"] > 0 and golden["plan_hub"] != golden["plan"]


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--dump":
        Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
        Path(sys.argv[2]).write_text(json.dumps(fingerprint(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        raise SystemExit(__doc__)
