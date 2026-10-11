"""Deterministic star-topology datasets and their independently-computed expected graphs.

Mirrors ``examples/social/social_graph.py``'s shape (``Dataset``, ``generate``,
``expected_graph``, ``wire_value``, ``_canonical``) but is scoped to one
deliberate hub, parameterized by scale (``hub_edges``) and by which endpoint
the hub sits at (``topology``). This module is imported by every star-topology
stress test in this package (Stories 3-7) and must stay Docker-free at import
time: only :func:`publish` needs Kafka, and it imports ``confluent_kafka``
inside its own body, never at module scope.

Topologies
----------
* ``target_hub``: one ``Hub`` node is the **target** of every ``POINTS_AT``
  relationship from ``hub_edges`` distinct ``Leaf`` nodes (integer keys).
* ``source_hub``: the reverse direction -- one ``Hub`` node is the
  **source** of every ``POINTS_FROM`` relationship to ``hub_edges`` distinct
  ``Leaf`` nodes.
* ``multi_type_hub``: five relationship types (``LIKES``, ``FOLLOWS``,
  ``RATED``, ``VIEWED``, ``SHARED``), each ``Leaf -> Hub``, together summing
  to ``hub_edges`` records -- proves the planner's hub rule keys on a node's
  *total* degree across types, not per type.
"""
from __future__ import annotations

from dataclasses import dataclass
import random
import time
from typing import Literal

Topology = Literal["target_hub", "source_hub", "multi_type_hub"]

HUB_LABEL = "Hub"
LEAF_LABEL = "Leaf"
HUB_KEY_PROPERTY = "hub_id"
LEAF_KEY_PROPERTY = "leaf_id"
HUB_KEY = "H"
KEY_PROPERTY = {HUB_LABEL: HUB_KEY_PROPERTY, LEAF_LABEL: LEAF_KEY_PROPERTY}

MULTI_TYPES = ("LIKES", "FOLLOWS", "RATED", "VIEWED", "SHARED")

# topology -> list of (edge_type, source_label, target_label)
_TOPOLOGY_EDGES: dict[str, list[tuple[str, str, str]]] = {
    "target_hub": [("POINTS_AT", LEAF_LABEL, HUB_LABEL)],
    "source_hub": [("POINTS_FROM", HUB_LABEL, LEAF_LABEL)],
    "multi_type_hub": [(name, LEAF_LABEL, HUB_LABEL) for name in MULTI_TYPES],
}


class StarTopologyError(ValueError):
    """Raised for an unknown topology name or an invalid scale."""


@dataclass
class Dataset:
    """Typed Python values; ``edges[type]`` is in publication order."""

    topology: str
    nodes: dict[str, list[dict]]
    edges: dict[str, list[tuple[object, object, dict]]]

    @property
    def edge_count(self) -> int:
        return sum(len(rows) for rows in self.edges.values())


def _validate(hub_edges: int, topology: str) -> None:
    if topology not in _TOPOLOGY_EDGES:
        raise StarTopologyError(f"unknown topology {topology!r}; expected one of {sorted(_TOPOLOGY_EDGES)}")
    if not isinstance(hub_edges, int) or isinstance(hub_edges, bool) or hub_edges < 1:
        raise StarTopologyError("hub_edges must be a positive integer")
    if topology == "multi_type_hub" and hub_edges < len(MULTI_TYPES):
        raise StarTopologyError(
            f"multi_type_hub needs hub_edges >= {len(MULTI_TYPES)} so every type gets at least one record"
        )


def schema_dict(topology: str, run_id: str) -> dict:
    """Return the loading/nodes/edges YAML dict for ``topology`` (``load_schema``-compatible).

    ``run_id`` is embedded in every topic name and the consumer group id.
    Kafka fixtures in this package are session-scoped, and more than one
    test publishes the same ``topology`` per session (different tiers,
    different execution modes) -- without a per-call ``run_id``, a second
    publish would append to the first test's topic and reuse its committed
    offsets instead of starting clean. Callers should pass something like
    ``uuid.uuid4().hex[:8]``, matching ``tests/conftest.py``'s own
    ``kafka_topic`` fixture convention.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise StarTopologyError("run_id must be a nonblank string")
    if topology not in _TOPOLOGY_EDGES:
        raise StarTopologyError(f"unknown topology {topology!r}; expected one of {sorted(_TOPOLOGY_EDGES)}")
    edges = _TOPOLOGY_EDGES[topology]
    return {
        "loading": {
            "mode": "bulk",
            "consumer_group_id": f"stress-{topology}-{run_id}",
            "unwind_batch_size": 500,
            "edge_unwind_batch_size": 5000,
        },
        "nodes": [
            {
                "label": HUB_LABEL, "topic": f"stress-{topology}-{run_id}-hub", "key_property": HUB_KEY_PROPERTY,
                "properties": {HUB_KEY_PROPERTY: {"type": "string", "required": True,
                                                   "constraint": {"type": "uniqueness"}}},
            },
            {
                "label": LEAF_LABEL, "topic": f"stress-{topology}-{run_id}-leaf", "key_property": LEAF_KEY_PROPERTY,
                "properties": {LEAF_KEY_PROPERTY: {"type": "integer", "required": True,
                                                    "constraint": {"type": "uniqueness"}}},
            },
        ],
        "edges": [
            {
                "type": edge_type,
                "topic": f"stress-{topology}-{run_id}-{edge_type.lower()}",
                "nodes": {"source": source_label, "target": target_label},
                "source_key_property": KEY_PROPERTY[source_label],
                "target_key_property": KEY_PROPERTY[target_label],
                "properties": {"seq": {"type": "integer", "required": True}},
            }
            for edge_type, source_label, target_label in edges
        ],
    }


def _rows_for_pair_direction(
    count: int, *, hub_is_source: bool, rng: random.Random, repeat_fraction: float, id_offset: int = 0,
) -> list[tuple[object, object, dict]]:
    """``count`` rows of one leaf<->hub pair per leaf (ids ``id_offset..id_offset+count-1``).

    Plus a ``repeat_fraction`` of those pairs sent a second time with a
    higher ``seq``, so last-write-wins collapse always has something real to
    prove.
    """
    rows: list[tuple[object, object, dict]] = []
    for index in range(count):
        leaf_id = id_offset + index
        pair = (HUB_KEY, leaf_id) if hub_is_source else (leaf_id, HUB_KEY)
        rows.append((pair[0], pair[1], {"seq": index}))
    repeats = round(count * repeat_fraction)
    if repeats > 0:
        chosen = rng.sample(range(count), min(repeats, count))
        next_seq = count
        for index in sorted(chosen):
            leaf_id = id_offset + index
            pair = (HUB_KEY, leaf_id) if hub_is_source else (leaf_id, HUB_KEY)
            rows.append((pair[0], pair[1], {"seq": next_seq}))
            next_seq += 1
    return rows


def generate(hub_edges: int, topology: str, seed: int = 13, *, repeat_fraction: float = 0.01) -> Dataset:
    """Deterministic dataset: one hub, ``hub_edges`` distinct leaf/pair endpoints for ``topology``.

    ``hub_edges`` is the number of *distinct* ``(leaf, hub)`` pairs (and the
    number of ``Leaf`` nodes), not the raw record count: ``repeat_fraction``
    of those pairs additionally get a second record with a higher ``seq``,
    so last-write-wins collapse (Story 4/6's exact-count assertion) always
    has something real to prove, even at small scale. The raw record count
    is therefore ``hub_edges + round(hub_edges * repeat_fraction)``, and the
    *distinct*-pair count (what Neo4j ends up holding) is exactly
    ``hub_edges`` (summed over the five types for ``multi_type_hub``).
    """
    _validate(hub_edges, topology)
    if not isinstance(repeat_fraction, (int, float)) or isinstance(repeat_fraction, bool) or not 0.0 <= repeat_fraction <= 1.0:
        raise StarTopologyError("repeat_fraction must be a number in [0, 1]")
    rng = random.Random(seed)
    nodes = {
        HUB_LABEL: [{HUB_KEY_PROPERTY: HUB_KEY}],
        LEAF_LABEL: [{LEAF_KEY_PROPERTY: leaf_id} for leaf_id in range(hub_edges)],
    }
    edges: dict[str, list[tuple[object, object, dict]]] = {}
    if topology == "target_hub":
        edges["POINTS_AT"] = _rows_for_pair_direction(
            hub_edges, hub_is_source=False, rng=rng, repeat_fraction=repeat_fraction,
        )
    elif topology == "source_hub":
        edges["POINTS_FROM"] = _rows_for_pair_direction(
            hub_edges, hub_is_source=True, rng=rng, repeat_fraction=repeat_fraction,
        )
    else:  # multi_type_hub
        base, extra = divmod(hub_edges, len(MULTI_TYPES))
        counts = [base + (1 if index < extra else 0) for index in range(len(MULTI_TYPES))]
        offset = 0
        for edge_type, count in zip(MULTI_TYPES, counts):
            edges[edge_type] = _rows_for_pair_direction(
                count, hub_is_source=False, rng=rng, repeat_fraction=repeat_fraction, id_offset=offset,
            ) if count > 0 else []
            offset += count
    return Dataset(topology, nodes, edges)


# -- wire format -----------------------------------------------------------------


def wire_value(value: object) -> object:
    return value


def node_record(record: dict) -> dict:
    return {name: wire_value(value) for name, value in record.items()}


def edge_record(source_key_property: str, target_key_property: str, source: object, target: object,
                 properties: dict) -> dict:
    return {
        "source": {source_key_property: wire_value(source)},
        "target": {target_key_property: wire_value(target)},
        "properties": {name: wire_value(value) for name, value in properties.items()},
    }


def ensure_constraints(driver, schema) -> None:
    """Create every schema-declared constraint/index and wait until ONLINE.

    Reuses ``src.orchestrator.schema_initializer.apply_schema`` (production
    DDL generation and index-online polling) rather than reinventing
    constraint Cypher here. Without this, ``MERGE`` over a 100k/1M-node
    ``Leaf`` label and every writer's endpoint ``MATCH`` become label scans
    (blowing up runtime), and lock/plan behaviour no longer matches
    production, which always has these constraints -- directly undermining
    this slice's own lock-wait claims.
    """
    from src.orchestrator.schema_initializer import apply_schema

    apply_schema(driver, schema)


def seed_nodes(driver, dataset: Dataset, schema, *, batch_size: int = 20_000) -> None:
    """Ensure constraints, then seed every node with one bounded ``UNWIND ... MERGE`` per label per batch.

    Decision 5 (``plan/v6/slice-4/stories.md``): node-loading correctness is
    already covered by ``test_node_loader_integration.py``, so the
    star-topology tests bypass ``NodeLoader``/Kafka entirely for their
    (small, bounded) node set. This keeps setup time out of every stress
    test's measurement window.
    """
    ensure_constraints(driver, schema)
    for label, records in dataset.nodes.items():
        key_property = KEY_PROPERTY[label]
        for start in range(0, len(records), batch_size):
            batch = records[start:start + batch_size]
            with driver.session() as session:
                session.run(
                    f"UNWIND $rows AS row MERGE (n:`{label}` {{`{key_property}`: row.`{key_property}`}})",
                    rows=batch,
                ).consume()


def distinct_pairs(dataset: Dataset, edge_type: str) -> set[tuple[object, object]]:
    """Return the unique ``(source, target)`` pairs of one edge type (last-write-wins collapse)."""
    return {(source, target) for source, target, _ in dataset.edges[edge_type]}


# -- expected graph ---------------------------------------------------------------


def _canonical(value: object) -> object:
    if isinstance(value, float) and value == 0.0:
        return 0.0
    return value


def expected_graph(dataset: Dataset) -> tuple[dict, dict]:
    """``(nodes, rels)``: ``nodes[label][key] = props``, ``rels[type][(s, t)] = props``.

    Computed directly from the generated Python values with the same
    last-write-wins merge rule as ``examples/social/social_graph.py``
    (``merged.update(...)``, never ``merged = ...``): later records overwrite
    the properties they carry and keep the others.
    """
    nodes: dict[str, dict] = {label: {} for label in dataset.nodes}
    for label, records in dataset.nodes.items():
        key_property = KEY_PROPERTY[label]
        for record in records:
            merged = nodes[label].setdefault(_canonical(record[key_property]), {})
            merged.update({name: _canonical(value) for name, value in record.items()})
    rels: dict[str, dict] = {name: {} for name in dataset.edges}
    for edge_type, rows in dataset.edges.items():
        for source, target, properties in rows:
            merged = rels[edge_type].setdefault((_canonical(source), _canonical(target)), {})
            merged.update({name: _canonical(value) for name, value in properties.items()})
    return nodes, rels


def last_write_evidence(dataset: Dataset) -> dict[str, dict[str, int]]:
    """Per type: records, distinct pairs, and pairs whose final properties differ from their first record."""
    evidence = {}
    for edge_type, rows in dataset.edges.items():
        first: dict[tuple, dict] = {}
        final: dict[tuple, dict] = {}
        for source, target, properties in rows:
            pair = (source, target)
            first.setdefault(pair, dict(properties))
            final.setdefault(pair, {}).update(properties)
        evidence[edge_type] = {
            "records": len(rows), "pairs": len(final),
            "pairs_overwritten": sum(1 for pair, props in final.items() if props != first[pair]),
        }
    return evidence


# -- Kafka publication (Docker/Kafka import only inside this function) -----------


def _kafka_key(edge_config, source: object, target: object) -> object:
    """Return the Kafka partition key: always the *leaf* endpoint.

    Keying by the endpoint that varies (the leaf) rather than always
    ``source`` matters for ``source_hub``, where the hub is the source of
    every record: keying by ``source`` there would put every record under
    one key (``"H"``), collapsing onto a single partition and defeating any
    parallel-consumer test. Keying by the leaf still keeps one pair's
    repeats on one partition (each pair has exactly one leaf value), so
    per-pair ordering (last-write-wins) is unaffected.
    """
    return source if edge_config.source_key_property == LEAF_KEY_PROPERTY else target


def edge_topic_names(schema) -> list[str]:
    return [edge.topic for edge in schema.edges]


def delete_topics(bootstrap: str, topics: list[str]) -> None:
    """Delete ``topics`` (only those that exist) and wait until they are gone."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=20).topics
    names = [topic for topic in topics if topic in existing]
    if not names:
        return
    for future in admin.delete_topics(names, operation_timeout=30).values():
        future.result(60)
    deadline = time.monotonic() + 60
    while any(topic in admin.list_topics(timeout=20).topics for topic in names):
        if time.monotonic() > deadline:
            raise RuntimeError(f"star-topology topics were not deleted: {names}")
        time.sleep(0.5)


def read_graph(driver, schema, database: str | None = None) -> tuple[dict, dict, dict]:
    """Return ``(nodes, rels, duplicates)`` from Neo4j in the :func:`expected_graph` shape.

    Driven entirely by ``schema`` (labels/types/key properties), the same
    pattern as ``examples/social/social_graph.py::read_graph`` generalized
    beyond one fixed topic table.
    """
    session_args = {"database": database} if database else {}
    nodes: dict[str, dict] = {node.label: {} for node in schema.nodes}
    rels: dict[str, dict] = {edge.type: {} for edge in schema.edges}
    duplicates: dict[str, int] = {}
    with driver.session(**session_args) as session:
        for node in schema.nodes:
            for record in session.run(f"MATCH (n:`{node.label}`) RETURN properties(n) AS p"):
                props = dict(record["p"])
                identity = _canonical(props[node.key_property])
                if identity in nodes[node.label]:
                    duplicates[node.label] = duplicates.get(node.label, 0) + 1
                nodes[node.label][identity] = props
        for edge in schema.edges:
            query = (
                f"MATCH (s:`{edge.nodes.source}`)-[r:`{edge.type}`]->(t:`{edge.nodes.target}`) "
                f"RETURN s.`{edge.source_key_property}` AS s, t.`{edge.target_key_property}` AS t, "
                "properties(r) AS p"
            )
            for record in session.run(query):
                pair = (_canonical(record["s"]), _canonical(record["t"]))
                if pair in rels[edge.type]:
                    duplicates[edge.type] = duplicates.get(edge.type, 0) + 1
                rels[edge.type][pair] = dict(record["p"])
        total = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        extra = total - sum(len(items) for items in rels.values()) - sum(
            count for name, count in duplicates.items() if name in rels
        )
        if extra:
            duplicates["<unexpected relationships>"] = extra
    return nodes, rels, duplicates


def compare(expected: tuple[dict, dict], actual: tuple[dict, dict, dict]) -> list[str]:
    """Return human-readable differences (empty = exact match)."""
    problems = []
    (want_nodes, want_rels), (got_nodes, got_rels, duplicates) = expected, actual
    for name, count in sorted(duplicates.items()):
        problems.append(f"{name}: {count} duplicate entities")
    for kind, want, got in (("node", want_nodes, got_nodes), ("relationship", want_rels, got_rels)):
        for name in sorted(set(want) | set(got)):
            left, right = want.get(name, {}), got.get(name, {})
            missing, extra = set(left) - set(right), set(right) - set(left)
            wrong = [key for key in set(left) & set(right) if left[key] != right[key]]
            if missing or extra or wrong:
                problems.append(
                    f"{kind} {name}: expected {len(left)} got {len(right)}; missing {len(missing)}, "
                    f"unexpected {len(extra)}, wrong properties {len(wrong)}"
                )
    return problems


def publish(dataset: Dataset, bootstrap: str, schema, *, partitions: int = 4, create: bool = True) -> dict[str, int]:
    """Publish every edge record of ``dataset`` to its schema-declared topic.

    Node seeding is deliberately *not* done here (Decision 5 in
    ``plan/v6/slice-4/stories.md``: the star-topology tests seed their small,
    bounded node set with one direct ``UNWIND ... MERGE`` Cypher statement,
    not through Kafka/NodeLoader). ``schema`` is a loaded ``GraphSchema``
    (``src.utils.schema_loader.load_schema``-compatible): topics and key
    property names come from it, never duplicated here.
    """
    import json

    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    edge_configs = {edge.type: edge for edge in schema.edges}
    unknown = set(dataset.edges) - set(edge_configs)
    if unknown:
        raise StarTopologyError(f"dataset has edge types not in the schema: {sorted(unknown)}")
    if create:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        existing = admin.list_topics(timeout=20).topics
        new = [
            NewTopic(edge_configs[edge_type].topic, num_partitions=partitions, replication_factor=1)
            for edge_type in dataset.edges if edge_configs[edge_type].topic not in existing
        ]
        for future in (admin.create_topics(new).values() if new else ()):
            future.result(30)
    producer = Producer({
        "bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 20,
        "compression.type": "lz4", "queue.buffering.max.messages": 2_000_000,
    })
    counts: dict[str, int] = {}
    errors: list[object] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(err)

    def produce(topic: str, key: object, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=str(key).encode("utf-8"), value=payload, callback=on_delivery)
                break
            except BufferError:
                producer.poll(0.05)
        counts[topic] = counts.get(topic, 0) + 1

    for edge_type, rows in dataset.edges.items():
        config = edge_configs[edge_type]
        for source, target, properties in rows:
            record = edge_record(config.source_key_property, config.target_key_property, source, target, properties)
            produce(config.topic, _kafka_key(config, source, target), record)
        producer.poll(0)
    if producer.flush(180):
        raise RuntimeError("Kafka did not deliver every star-topology record: producer still had a nonempty queue")
    if errors:
        raise RuntimeError(f"Kafka failed to deliver {len(errors)} star-topology record(s): {errors[:3]}")
    return counts
