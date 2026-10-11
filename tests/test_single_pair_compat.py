"""v7-S7 story 7.1: byte-level compatibility of every single-pair relationship type.

S7 lets one relationship type bind several label pairs (one per topic). Every type that binds ONE pair (all shipped
schemas) must keep the exact bytes it had before v7. ``tests/golden/single_pair_compat.json`` pins them. It was
recorded with THIS module's ``fingerprint`` on the tree before v7-S1 (``git archive 4382960``) and the dump from the
S7 base (feat/multi-topic d334cdd, before any S7 change) was byte-identical. For each edge of each schema it pins:

* SHA-256 of every Cypher text built from the endpoint labels and key properties:
  ``build_apoc_locked_upsert_query`` for (explicit_locks, sort_rows) = (True, True), (False, True), (False, False);
  ``_preflight_query``; ``_endpoint_classification_query``; ``edge_loader.build_edge_upsert_query``;
  ``edge_loader._build_endpoint_preflight_query``; the ``NativeDisjointEdgeWriter`` query (stub capabilities);
* ``normalize_edge_record`` and ``planned_writer.compile_record_normalizer`` outputs (``repr``) for 5 records whose
  keys and properties cover string, integer, float, date and datetime values;
* ``DeadLetterRecordSink.route_unresolved_endpoint``: the envelope ``key_bytes``, ``value_bytes`` and the SHA-256 of
  the encoded envelope, with a fixed clock and a capturing producer;
* ``parked_pair_index._record_wire_key`` and the value bytes of one unsequenced ``ParkedPairIndex.add``;
* ``BucketCellRouter(S, T, 64, keyed).cell`` / ``.resources`` for 5 key pairs, with keyed ``{}`` and ``{T}``.

``tests/golden/single_topic_compat.json`` (S6) is a separate golden and is never re-recorded by S7.

Record (only on the pre-v7 tree, or for an intentional change with its reason in the commit)::

    git archive 4382960 | tar -x -C /tmp/pre_v7
    cp tests/test_single_pair_compat.py /tmp/pre_v7/tests/
    cp tests/golden/fixtures/single_topic_dlq.yaml /tmp/pre_v7/tests/golden/fixtures/   # S6 fixture, newer
    cd /tmp/pre_v7 && PYTHONPATH=.:tests python tests/test_single_pair_compat.py --dump <repo>/tests/golden/single_pair_compat.json
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests" / "golden" / "single_pair_compat.json"
SCHEMAS = [
    "config/graph_schema.yaml",
    "examples/imdb/graph_schema.yaml",
    "examples/social/graph_schema.yaml",
    "tests/golden/fixtures/single_topic_dlq.yaml",
]
FIXED_NOW_MS = 1_700_000_000_000


def _sample(prop_type: str, index: int):
    return {
        "string": f"s{index}", "integer": index, "float": index + 0.5,
        "date": f"2020-01-{1 + index % 28:02d}", "datetime": f"2020-01-01T00:00:{index % 60:02d}+00:00",
    }[prop_type]


def _node_property_type(schema, label: str) -> str:
    node = next(n for n in schema.nodes if n.label == label)
    return node.properties[node.key_property].type


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _raw_records(schema, edge, count: int = 5) -> list[dict]:
    source_type = _node_property_type(schema, edge.nodes.source)
    target_type = _node_property_type(schema, edge.nodes.target)
    return [
        {
            "source": {edge.source_key_property: _sample(source_type, i)},
            "target": {edge.target_key_property: _sample(target_type, i + 1)},
            "properties": {name: _sample(prop.type, i) for name, prop in edge.properties.items()},
        }
        for i in range(count)
    ]


def _queries(edge) -> dict:
    from src.loader import edge_execution, edge_loader

    out = {}
    for explicit_locks, sort_rows in ((True, True), (False, True), (False, False)):
        out[f"apoc_locked_{explicit_locks}_{sort_rows}"] = _sha(edge_execution.build_apoc_locked_upsert_query(
            edge, explicit_locks=explicit_locks, sort_rows=sort_rows))
    out["preflight"] = _sha(edge_execution._preflight_query(edge))
    out["classification"] = _sha(edge_execution._endpoint_classification_query(edge))
    out["legacy_upsert"] = _sha(edge_loader.build_edge_upsert_query(edge))
    out["legacy_preflight"] = _sha(edge_loader._build_endpoint_preflight_query(edge))
    writer = edge_execution.NativeDisjointEdgeWriter(
        object(), edge, edge_execution.ServerCapabilities(neo4j_version="5.26", cypher_25=True),
        missing_endpoint_error_factory=RuntimeError,
    )
    out["native_disjoint"] = _sha(writer._query)
    return out


def _normalized(schema, edge) -> dict:
    from src.loader.edge_records import normalize_edge_record
    from src.loader.planned_writer import compile_record_normalizer

    raws = _raw_records(schema, edge)
    compiled = compile_record_normalizer(edge)
    reference = []
    for raw in raws:
        record = normalize_edge_record(raw, edge)
        reference.append(repr((record.source_key, record.target_key, record.properties)))
    return {"normalize_edge_record": reference, "compile_record_normalizer": [repr(compiled(raw)) for raw in raws]}


class _CapturingProducer:
    def __init__(self) -> None:
        self.published: list = []

    def publish(self, topic, envelope, **_kwargs) -> None:
        self.published.append((topic, envelope))


def _dead_letter(schema, edge) -> dict:
    from src.loader.dead_letter import DeadLetterRecordSink, encode_envelope
    from src.loader.edge_records import normalize_edge_record

    producer = _CapturingProducer()
    sink = DeadLetterRecordSink(producer=producer, topic="compat.dlq", run_id="run-A", replica_id="0", kind="edge",
                                clock=lambda: FIXED_NOW_MS)
    record = normalize_edge_record(_raw_records(schema, edge)[3], edge)
    sink.route_unresolved_endpoint(
        topic=edge.topic, partition=2, offset=17, record=record, edge_type=edge.type,
        source_key_property=edge.source_key_property, target_key_property=edge.target_key_property,
        failure_class="missing_endpoint", reason_code="missing_endpoint", error_text="target not found",
        retry_after_seconds=1.5,
    )
    [(topic, envelope)] = producer.published
    return {
        "topic": topic,
        "key_bytes": envelope.key_bytes.decode("utf-8"),
        "value_bytes": envelope.value_bytes.decode("utf-8"),
        "encoded_envelope_sha256": hashlib.sha256(encode_envelope(envelope)).hexdigest(),
    }


def _index(schema, edge) -> dict:
    from src.loader import parked_pair_index
    from src.loader.edge_records import normalize_edge_record

    record = normalize_edge_record(_raw_records(schema, edge)[3], edge)
    index = parked_pair_index.ParkedPairIndex(topic="compat.dlq.parked-pairs", bootstrap_servers="kafka:1",
                                              writable=True)
    published: list = []
    index._publish = lambda key, value: published.append((key, value))
    index.add(record.source_key, record.target_key, original_topic=edge.topic, original_partition=2,
              original_offset=17, first_failure_at_ms=FIXED_NOW_MS)
    [(add_key, add_value)] = published
    return {
        "wire_key": parked_pair_index._record_wire_key(record.source_key, record.target_key, edge.topic, 2, 17),
        "add_key": add_key if isinstance(add_key, str) else add_key.decode("utf-8"),
        "add_value": add_value.decode("utf-8"),
    }


def _router(schema, edge) -> dict:
    from src.loader.edge_records import normalize_edge_record
    from src.loader.lease_client import BucketCellRouter

    keys = [(r.source_key, r.target_key) for r in (normalize_edge_record(raw, edge)
                                                    for raw in _raw_records(schema, edge))]
    out = {}
    for name, keyed in (("unkeyed", frozenset()), ("keyed_target", frozenset({edge.nodes.target}))):
        router = BucketCellRouter(edge.nodes.source, edge.nodes.target, 64, keyed)
        cells = [router.cell(source_key, target_key) for source_key, target_key in keys]
        out[name] = {"cells": [list(cell) for cell in cells],
                     "resources": [list(BucketCellRouter.resources(cell)) for cell in cells]}
    return out


def fingerprint(relative_path: str) -> dict:
    from src.utils.schema_loader import load_schema

    schema = load_schema(ROOT / relative_path)
    out: dict = {}
    for edge in schema.edges:
        out[edge.type] = {
            "topic": edge.topic,
            "nodes": [edge.nodes.source, edge.nodes.target],
            "keys": [edge.source_key_property, edge.target_key_property],
            "order_by": edge.order_by,
            "queries": _queries(edge),
            "normalized": _normalized(schema, edge),
            "dead_letter": _dead_letter(schema, edge),
            "index": _index(schema, edge),
            "router": _router(schema, edge),
        }
    return out


def fingerprint_all() -> dict:
    return {path: fingerprint(path) for path in SCHEMAS}


@pytest.fixture(scope="module")
def golden() -> dict:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", SCHEMAS)
def test_single_pair_types_keep_the_pre_v7_bytes(path, golden) -> None:
    assert fingerprint(path) == golden[path]


def test_shipped_types_are_single_pair() -> None:
    from src.utils.schema_loader import load_schema

    for path in SCHEMAS:
        for edge in load_schema(ROOT / path).edges:
            assert getattr(edge, "is_multi_pair", False) is False, (path, edge.type)


def test_the_golden_is_not_vacuous(golden) -> None:
    """At least one order_by edge, one DLQ'd edge, one self-reference and one keyed-label case are pinned."""
    edges = [entry for schema in golden.values() for entry in schema.values()]
    assert any(entry["order_by"] for entry in edges)
    assert any(entry["nodes"][0] == entry["nodes"][1] for entry in edges)
    assert golden["examples/social/graph_schema.yaml"]["FRIEND_OF"]["nodes"] == ["Person", "Person"]
    assert all(entry["dead_letter"]["key_bytes"].startswith("[") for entry in edges)
    assert all(len(json.loads(entry["dead_letter"]["key_bytes"])) == 2 for entry in edges)
    assert any("#" in token for entry in edges for cell in entry["router"]["keyed_target"]["cells"] for token in cell)
    order_by = [entry for entry in edges if entry["order_by"]]
    assert all(entry["queries"]["apoc_locked_False_False"] != entry["queries"]["apoc_locked_True_True"]
               for entry in order_by)
    assert all(len(entry["normalized"]["normalize_edge_record"]) == 5 for entry in edges)
    assert len(edges) >= 10
    from src.models.schema import dead_letter_topic_name
    from src.utils.schema_loader import load_schema

    fixture = load_schema(ROOT / "tests/golden/fixtures/single_topic_dlq.yaml")
    assert set(golden["tests/golden/fixtures/single_topic_dlq.yaml"]) == {edge.type for edge in fixture.edges}
    assert any(dead_letter_topic_name(edge.topic, edge.dead_letter) for edge in fixture.edges)


if __name__ == "__main__":
    # `--dump FILE`: record the golden (run with PYTHONPATH=<tree>:<tree>/tests, also on the pre-v7 tree).
    if len(sys.argv) == 3 and sys.argv[1] == "--dump":
        Path(sys.argv[2]).parent.mkdir(parents=True, exist_ok=True)
        Path(sys.argv[2]).write_text(json.dumps(fingerprint_all(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        raise SystemExit(__doc__)
