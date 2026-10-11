#!/usr/bin/env python3
"""Two relationship types fed by two topics each: generate, publish, verify.

The library contract is the same as in ``examples/social``: ``graph_schema.yaml`` plus JSON records on the
schema-declared topics.  What is new is that a type lists several topics and declares how a pair that
appears in more than one of them is resolved:

* ``RATED`` (``order_by: rated_at``): the newest ``rated_at`` wins, whichever topic carried it.  The
  generator puts the newer value in the EARLIER topic for half of the shared pairs and in the LATER topic for
  the other half, so neither "first topic" nor "last topic" gives the right answer.
* ``WISHLISTED`` (``topic_conflict: precedence``): the later-listed topic wins, property by property
  (``SET r += props``).  The corrections topic omits ``note`` on some pairs; the base value must survive.

The expected graph is computed here from the generated Python values, without any library code.

Usage::

    python examples/multi_topic/multi_topic_graph.py publish --bootstrap localhost:9092 --customers 2000
    python -m src.cli start --mode bulk --config examples/multi_topic/graph_schema.yaml
    python examples/multi_topic/multi_topic_graph.py verify --customers 2000

``--variant multi-pair`` (v7-S7) uses ``graph_schema_multi_pair.yaml`` instead: ONE type, ``LIKES``, over the
label pairs Customer->Post (two topics) and Customer->Comment (one topic), with colliding Post and Comment ids;
``verify`` compares each labelled pair and counts cross-pair relationships.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import sys
import time

SCHEMA_PATH = Path(__file__).with_name("graph_schema.yaml")

NODE_TOPICS = {"Customer": ("mt-customer", "customer_id"), "Product": ("mt-product", "sku")}
# edge type -> (topics in precedence order, source label, target label, source key, target key)
EDGE_TOPICS = {
    "RATED": (("mt-rated-app", "mt-rated-import"), "Customer", "Product", "customer_id", "sku"),
    "WISHLISTED": (("mt-wishlist-base", "mt-wishlist-corrections"), "Customer", "Product", "customer_id", "sku"),
}
EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


@dataclass
class Dataset:
    """Typed Python values; ``edges[type][topic]`` is the list of ``(source, target, properties)`` rows."""

    nodes: dict[str, list[dict]] = field(default_factory=dict)
    edges: dict[str, dict[str, list[tuple[object, object, dict]]]] = field(default_factory=dict)

    @property
    def edge_count(self) -> int:
        return sum(len(rows) for topics in self.edges.values() for rows in topics.values())


def generate(customers: int = 2_000, seed: int = 7) -> Dataset:
    """Deterministic dataset: 3 ratings and 2 wishlist entries per customer, 30% of pairs in both topics."""
    rng = random.Random(seed)
    products = max(20, customers // 10)
    data = Dataset(nodes={
        "Customer": [{"customer_id": i, "name": f"customer-{i}"} for i in range(customers)],
        "Product": [{"sku": f"sku-{i:05d}", "title": f"product {i}"} for i in range(products)],
    })
    rated_app, rated_import = [], []
    base, corrections = [], []
    for customer in range(customers):
        for slot, sku_index in enumerate(rng.sample(range(products), 3)):
            sku = f"sku-{sku_index:05d}"
            # unique, strictly ordered timestamps: ties are not part of this example
            older = EPOCH + timedelta(seconds=2 * (customer * 3 + slot))
            newer = older + timedelta(seconds=1, days=30)

            def row(when, channel, customer=customer, sku=sku):
                return customer, sku, {"stars": rng.randint(1, 5), "rated_at": when, "channel": channel}

            shared = rng.random() < 0.3
            if shared:
                newer_in_app = rng.random() < 0.5
                rated_app.append(row(newer if newer_in_app else older, "app"))
                rated_import.append(row(older if newer_in_app else newer, "import"))
            elif rng.random() < 0.5:
                rated_app.append(row(older, "app"))
            else:
                rated_import.append(row(older, "import"))
        for sku_index in rng.sample(range(products), 2):
            sku = f"sku-{sku_index:05d}"
            base.append((customer, sku, {"priority": rng.randint(1, 3), "note": f"base-{customer}"}))
            if rng.random() < 0.3:
                # correction of the same pair: always a new priority, sometimes without a note
                properties = {"priority": rng.randint(4, 9)}
                if rng.random() < 0.5:
                    properties["note"] = f"corrected-{customer}"
                corrections.append((customer, sku, properties))
        if rng.random() < 0.2:  # an extra correction for a random product: usually a pair the base topic lacks (rarely one it has)
            corrections.append((customer, f"sku-{rng.randrange(products):05d}", {"priority": 9}))
    data.edges = {
        "RATED": {"mt-rated-app": rated_app, "mt-rated-import": rated_import},
        "WISHLISTED": {"mt-wishlist-base": base, "mt-wishlist-corrections": corrections},
    }
    return data


def wire_value(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


def edge_record(edge_type: str, source: object, target: object, properties: dict) -> dict:
    _, _, _, source_key, target_key = EDGE_TOPICS[edge_type]
    return {"source": {source_key: source}, "target": {target_key: target},
            "properties": {name: wire_value(value) for name, value in properties.items()}}


def all_topics() -> list[str]:
    return [topic for topic, _ in NODE_TOPICS.values()] + [t for item in EDGE_TOPICS.values() for t in item[0]]


def delete_topics(bootstrap: str, topics: list[str] | None = None) -> None:
    """Delete the example's topics (``topics``, default every topic of the main variant) and wait until gone."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = [topic for topic in (topics or all_topics()) if topic in admin.list_topics(timeout=20).topics]
    for future in (admin.delete_topics(existing, operation_timeout=30).values() if existing else ()):
        future.result(60)
    deadline = time.monotonic() + 60
    while any(topic in admin.list_topics(timeout=20).topics for topic in existing):
        if time.monotonic() > deadline:
            raise RuntimeError("topics were not deleted")
        time.sleep(0.5)


def publish(dataset: Dataset, bootstrap: str, *, partitions: int = 3, only: str = "all") -> dict[str, int]:
    """Create the topics and publish every record; return records per topic.

    Edge records are keyed by the source key, so every record of one pair stays in one partition of its topic.
    The record order across the two topics of a type is NOT defined by Kafka: the second topic is published
    first on purpose, to show that the result does not depend on it.
    """
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=20).topics
    new = [NewTopic(topic, num_partitions=partitions, replication_factor=1)
           for topic in all_topics() if topic not in existing]
    for future in (admin.create_topics(new).values() if new else ()):
        future.result(30)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 20})
    counts: dict[str, int] = {}

    def produce(topic: str, key: object, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=str(key).encode("utf-8"), value=payload)
                break
            except BufferError:
                producer.poll(0.05)
        counts[topic] = counts.get(topic, 0) + 1

    for label, records in (dataset.nodes.items() if only in ("all", "nodes") else ()):
        topic, key = NODE_TOPICS[label]
        for record in records:
            produce(topic, record[key], record)
    for edge_type, by_topic in (dataset.edges.items() if only in ("all", "edges") else ()):
        for topic in reversed(EDGE_TOPICS[edge_type][0]):
            for source, target, properties in by_topic[topic]:
                produce(topic, source, edge_record(edge_type, source, target, properties))
        producer.poll(0)
    if producer.flush(120):
        raise RuntimeError("Kafka did not deliver every record")
    return counts


# -- expected graph and Neo4j comparison ----------------------------------------


def _canonical(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return value


def expected_graph(dataset: Dataset) -> tuple[dict, dict]:
    """``(nodes, rels)``: ``nodes[label][key] = props``, ``rels[type][(source, target)] = props``."""
    nodes = {label: {record[key]: dict(record) for record in dataset.nodes[label]}
             for label, (_, key) in NODE_TOPICS.items()}
    rels: dict[str, dict] = {"RATED": {}, "WISHLISTED": {}}
    # order_by: the row with the greatest rated_at wins (its properties replace the stored ones)
    for rows in dataset.edges["RATED"].values():
        for source, target, properties in rows:
            current = rels["RATED"].get((source, target))
            if current is None or properties["rated_at"] > current["rated_at"]:
                rels["RATED"][(source, target)] = {name: _canonical(value) for name, value in properties.items()}
    # precedence: topics in declared order, SET r += props
    for topic in EDGE_TOPICS["WISHLISTED"][0]:
        for source, target, properties in dataset.edges["WISHLISTED"][topic]:
            rels["WISHLISTED"].setdefault((source, target), {}).update(properties)
    return nodes, rels


def _native(value: object) -> object:
    to_native = getattr(value, "to_native", None)
    return _canonical(to_native() if callable(to_native) else value)


def read_graph(driver, database: str | None) -> tuple[dict, dict, dict]:
    """Return ``(nodes, rels, duplicates)`` from Neo4j in the :func:`expected_graph` shape."""
    session_args = {"database": database} if database else {}
    nodes: dict[str, dict] = {label: {} for label in NODE_TOPICS}
    rels: dict[str, dict] = {name: {} for name in EDGE_TOPICS}
    duplicates: dict[str, int] = {}
    with driver.session(**session_args) as session:
        for label, (_, key) in NODE_TOPICS.items():
            for record in session.run(f"MATCH (n:`{label}`) RETURN properties(n) AS p"):
                props = {name: _native(value) for name, value in record["p"].items()}
                if props[key] in nodes[label]:
                    duplicates[label] = duplicates.get(label, 0) + 1
                nodes[label][props[key]] = props
        for edge_type, (_, source_label, target_label, source_key, target_key) in EDGE_TOPICS.items():
            query = (f"MATCH (s:`{source_label}`)-[r:`{edge_type}`]->(t:`{target_label}`) "
                     f"RETURN s.`{source_key}` AS s, t.`{target_key}` AS t, properties(r) AS p")
            for record in session.run(query):
                pair = (_native(record["s"]), _native(record["t"]))
                if pair in rels[edge_type]:
                    duplicates[edge_type] = duplicates.get(edge_type, 0) + 1
                rels[edge_type][pair] = {name: _native(value) for name, value in record["p"].items()}
    return nodes, rels, duplicates


def compare(expected: tuple[dict, dict], actual: tuple[dict, dict, dict], types=None) -> list[str]:
    """Return human-readable differences (empty = exact match); ``types`` limits the relationship types."""
    problems = []
    (want_nodes, want_rels), (got_nodes, got_rels, duplicates) = expected, actual
    if types is not None:
        want_rels = {name: rows for name, rows in want_rels.items() if name in types}
        got_rels = {name: rows for name, rows in got_rels.items() if name in types}
        duplicates = {name: n for name, n in duplicates.items() if name in NODE_TOPICS or name in types}
    for name, count in sorted(duplicates.items()):
        problems.append(f"{name}: {count} duplicate entities")
    for kind, want, got in (("node", want_nodes, got_nodes), ("relationship", want_rels, got_rels)):
        for name in sorted(set(want) | set(got)):
            left, right = want.get(name, {}), got.get(name, {})
            missing, extra = set(left) - set(right), set(right) - set(left)
            wrong = [key for key in set(left) & set(right) if left[key] != right[key]]
            if missing or extra or wrong:
                sample = next(iter(wrong), None)
                problems.append(
                    f"{kind} {name}: expected {len(left)} got {len(right)}; missing {len(missing)}, "
                    f"unexpected {len(extra)}, wrong properties {len(wrong)}"
                    + (f" e.g. {sample}: expected {left[sample]} got {right[sample]}" if sample is not None else "")
                )
    return problems


def conflict_evidence(dataset: Dataset) -> dict[str, dict[str, int]]:
    """Per type: records, distinct pairs and pairs present in both topics (so the rule is exercised)."""
    out = {}
    for edge_type, by_topic in dataset.edges.items():
        first, second = (({(s, t) for s, t, _ in by_topic[topic]}) for topic in EDGE_TOPICS[edge_type][0])
        out[edge_type] = {"records": sum(len(rows) for rows in by_topic.values()),
                          "pairs": len(first | second), "pairs_in_both_topics": len(first & second)}
    return out


def summary(expected: tuple[dict, dict], dataset: Dataset | None = None) -> dict:
    nodes, rels = expected
    return {"nodes": {label: len(items) for label, items in nodes.items()},
            "relationships": {name: len(items) for name, items in rels.items()},
            **({"conflicts": conflict_evidence(dataset)} if dataset is not None else {})}


# -- multi-pair variant (v7-S7): one type, two label pairs ---------------------------
#
# ``graph_schema_multi_pair.yaml``: ``LIKES`` Customer->Post from ``mt-likes-posts`` and ``mt-likes-posts-import``
# (one pair, two topics: ``order_by: liked_at`` decides) and Customer->Comment from ``mt-likes-comments``. Post and
# Comment ids overlap on purpose (both run 0..N-1), and customer 0 likes every post and every comment (a hub over
# both pairs). Each topic binds its pair; the record never says which pair it belongs to.

MULTI_PAIR_SCHEMA_PATH = Path(__file__).with_name("graph_schema_multi_pair.yaml")
MP_NODE_TOPICS = {"Customer": ("mtp-customer", "customer_id"), "Post": ("mtp-post", "post_id"),
                  "Comment": ("mtp-comment", "comment_id")}
# topic -> (target label, target key) of LIKES; the source is always Customer / customer_id
MP_EDGE_TOPICS = {"mt-likes-posts": ("Post", "post_id"), "mt-likes-posts-import": ("Post", "post_id"),
                  "mt-likes-comments": ("Comment", "comment_id")}
MP_PAIRS = (("Customer", "Post"), ("Customer", "Comment"))


def generate_multi_pair(customers: int = 2_000, seed: int = 7) -> Dataset:
    """Deterministic two-pair dataset; ``edges["LIKES"][topic]`` rows, unique ``liked_at`` values."""
    rng = random.Random(seed)
    items = max(20, customers // 10)
    data = Dataset(nodes={
        "Customer": [{"customer_id": i, "name": f"customer-{i}"} for i in range(customers)],
        "Post": [{"post_id": i, "title": f"post {i}"} for i in range(items)],
        "Comment": [{"comment_id": i, "text": f"comment {i}"} for i in range(items)],
    })
    rows: dict[str, list] = {topic: [] for topic in MP_EDGE_TOPICS}
    clock = [0]

    def when() -> datetime:
        clock[0] += 1
        return EPOCH + timedelta(seconds=clock[0])

    for customer in range(customers):
        posts = list(range(items)) if customer == 0 else rng.sample(range(items), 3)
        for post in posts:
            older, newer = when(), when()
            if rng.random() < 0.3:  # in both topics of the pair, the newer value in either
                live_newer = rng.random() < 0.5
                rows["mt-likes-posts"].append((customer, post, {"liked_at": newer if live_newer else older,
                                                                "reaction": "live"}))
                rows["mt-likes-posts-import"].append((customer, post, {"liked_at": older if live_newer else newer,
                                                                       "reaction": "import"}))
            else:
                topic = rng.choice(("mt-likes-posts", "mt-likes-posts-import"))
                rows[topic].append((customer, post, {"liked_at": older, "reaction": topic.rsplit("-", 1)[-1]}))
        # Half of the customers like the comments with the SAME ids as their posts: colliding keys.
        comments = posts if customer == 0 or rng.random() < 0.5 else rng.sample(range(items), 3)
        for comment in comments:
            rows["mt-likes-comments"].append((customer, comment, {"liked_at": when(), "reaction": "comment"}))
            if rng.random() < 0.2:  # liked again later in the same topic: the later value wins
                rows["mt-likes-comments"].append((customer, comment, {"liked_at": when(), "reaction": "again"}))
    data.edges = {"LIKES": rows}
    return data


def multi_pair_record(topic: str, source: object, target: object, properties: dict) -> dict:
    """The wire record of ``topic``: its pair's key property names."""
    _target_label, target_key = MP_EDGE_TOPICS[topic]
    return {"source": {"customer_id": source}, "target": {target_key: target},
            "properties": {name: wire_value(value) for name, value in properties.items()}}


def multi_pair_topics() -> list[str]:
    return [topic for topic, _ in MP_NODE_TOPICS.values()] + list(MP_EDGE_TOPICS)


def publish_multi_pair(dataset: Dataset, bootstrap: str, *, partitions: int = 3, only: str = "all") -> dict[str, int]:
    """Create the variant's topics and publish its records (edge topics in reverse order); records per topic."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=20).topics
    new = [NewTopic(topic, num_partitions=partitions, replication_factor=1)
           for topic in multi_pair_topics() if topic not in existing]
    for future in (admin.create_topics(new).values() if new else ()):
        future.result(30)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 20})
    counts: dict[str, int] = {}

    def produce(topic: str, key: object, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=str(key).encode("utf-8"), value=payload)
                break
            except BufferError:
                producer.poll(0.05)
        counts[topic] = counts.get(topic, 0) + 1

    for label, records in (dataset.nodes.items() if only in ("all", "nodes") else ()):
        topic, key = MP_NODE_TOPICS[label]
        for record in records:
            produce(topic, record[key], record)
    for topic in (reversed(list(MP_EDGE_TOPICS)) if only in ("all", "edges") else ()):
        for source, target, properties in dataset.edges["LIKES"][topic]:
            produce(topic, source, multi_pair_record(topic, source, target, properties))
        producer.poll(0)
    if producer.flush(120):
        raise RuntimeError("Kafka did not deliver every record")
    return counts


def expected_multi_pair(dataset: Dataset) -> tuple[dict, dict]:
    """``(nodes, rels)`` with ``rels[(source label, target label)][(source, target)] = props`` (order_by)."""
    nodes = {label: {record[key]: dict(record) for record in dataset.nodes[label]}
             for label, (_, key) in MP_NODE_TOPICS.items()}
    rels: dict = {pair: {} for pair in MP_PAIRS}
    for topic, rows in dataset.edges["LIKES"].items():
        pair = ("Customer", MP_EDGE_TOPICS[topic][0])
        for source, target, properties in rows:
            current = rels[pair].get((source, target))
            if current is None or properties["liked_at"] > current["liked_at"]:
                rels[pair][(source, target)] = {name: _canonical(value) for name, value in properties.items()}
    return nodes, rels


def read_multi_pair(driver, database: str | None) -> tuple[dict, dict, dict]:
    """Return ``(nodes, rels, duplicates)`` in the :func:`expected_multi_pair` shape. ``duplicates`` also counts
    ``LIKES`` relationships between any other labels (cross-pair edges) under ``"cross_pair"``."""
    session_args = {"database": database} if database else {}
    nodes: dict = {label: {} for label in MP_NODE_TOPICS}
    rels: dict = {pair: {} for pair in MP_PAIRS}
    duplicates: dict[str, int] = {}
    keys = {label: key for label, (_, key) in MP_NODE_TOPICS.items()}
    with driver.session(**session_args) as session:
        for label, key in keys.items():
            for record in session.run(f"MATCH (n:`{label}`) RETURN properties(n) AS p"):
                props = {name: _native(value) for name, value in record["p"].items()}
                if props[key] in nodes[label]:
                    duplicates[label] = duplicates.get(label, 0) + 1
                nodes[label][props[key]] = props
        for source_label, target_label in MP_PAIRS:
            query = (f"MATCH (s:`{source_label}`)-[r:LIKES]->(t:`{target_label}`) "
                     f"RETURN s.customer_id AS s, t.`{keys[target_label]}` AS t, properties(r) AS p")
            for record in session.run(query):
                pair = (_native(record["s"]), _native(record["t"]))
                if pair in rels[(source_label, target_label)]:
                    duplicates[f"LIKES {source_label}->{target_label}"] = duplicates.get(
                        f"LIKES {source_label}->{target_label}", 0) + 1
                rels[(source_label, target_label)][pair] = {
                    name: _native(value) for name, value in record["p"].items()}
        cross = session.run(
            "MATCH (s)-[r:LIKES]->(t) WHERE NOT (s:Customer AND (t:Post OR t:Comment)) RETURN count(r) AS c"
        ).single()["c"]
    if cross:
        duplicates["cross_pair"] = cross
    return nodes, rels, duplicates


def compare_multi_pair(expected: tuple[dict, dict], actual: tuple[dict, dict, dict]) -> list[str]:
    """Differences per labelled pair (empty = exact match, no cross-pair edges)."""
    (want_nodes, want_rels), (got_nodes, got_rels, duplicates) = expected, actual
    named_want = {f"LIKES {s}->{t}": rows for (s, t), rows in want_rels.items()}
    named_got = {f"LIKES {s}->{t}": rows for (s, t), rows in got_rels.items()}
    return compare((want_nodes, named_want), (got_nodes, named_got, duplicates))


def multi_pair_summary(expected: tuple[dict, dict], dataset: Dataset) -> dict:
    nodes, rels = expected
    posts, comments = (set(rels[pair]) for pair in MP_PAIRS)
    return {"nodes": {label: len(items) for label, items in nodes.items()},
            "relationships": {f"LIKES {s}->{t}": len(rows) for (s, t), rows in rels.items()},
            "records": {topic: len(rows) for topic, rows in dataset.edges["LIKES"].items()},
            "colliding_keys": len(posts & comments)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("publish", "verify", "expect"):
        command = sub.add_parser(name)
        command.add_argument("--customers", type=int, default=2_000)
        command.add_argument("--seed", type=int, default=7)
    reset = sub.add_parser("reset", help="delete the example's topics")
    reset.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    for command in sub.choices.values():
        command.add_argument(
            "--variant", choices=("multi-topic", "multi-pair"), default="multi-topic",
            help="multi-pair: graph_schema_multi_pair.yaml (LIKES over Customer->Post and Customer->Comment)",
        )
    publish_cmd = sub.choices["publish"]
    publish_cmd.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    publish_cmd.add_argument("--partitions", type=int, default=3)
    publish_cmd.add_argument(
        "--only", choices=("all", "nodes", "edges"), default="all",
        help="publish only the nodes or only the relationships (edges first: the dead_letter variant)",
    )
    verify = sub.choices["verify"]
    verify.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    verify.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USERNAME", "neo4j"))
    verify.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD", "changeme"))
    verify.add_argument("--database", default=os.environ.get("NEO4J_DATABASE") or None)
    verify.add_argument("--types", default=None, help="comma-separated relationship types to compare (default: all)")
    args = parser.parse_args(argv)
    if args.variant == "multi-pair":
        return _main_multi_pair(args)
    if args.command == "reset":
        delete_topics(args.bootstrap)
        return 0
    dataset = generate(args.customers, args.seed)
    if args.command == "publish":
        started = time.monotonic()
        counts = publish(dataset, args.bootstrap, partitions=args.partitions, only=args.only)
        print(json.dumps({"published": counts, "edges": dataset.edge_count,
                          "seconds": round(time.monotonic() - started, 3)}))
        return 0
    expected = expected_graph(dataset)
    if args.command == "expect":
        print(json.dumps(summary(expected, dataset), indent=1))
        return 0
    from neo4j import GraphDatabase

    with GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)) as driver:
        actual = read_graph(driver, args.database)
    types = set(args.types.split(",")) if args.types else None
    problems = compare(expected, actual, types)
    print(json.dumps({"exact_match": not problems, "problems": problems, **summary(expected, dataset)}, indent=1))
    return 0 if not problems else 1


def _main_multi_pair(args) -> int:
    if args.command == "reset":
        delete_topics(args.bootstrap, multi_pair_topics())
        return 0
    if getattr(args, "types", None):
        raise SystemExit("--types is not used by the multi-pair variant")
    dataset = generate_multi_pair(args.customers, args.seed)
    if args.command == "publish":
        started = time.monotonic()
        counts = publish_multi_pair(dataset, args.bootstrap, partitions=args.partitions, only=args.only)
        print(json.dumps({"published": counts, "edges": dataset.edge_count,
                          "seconds": round(time.monotonic() - started, 3)}))
        return 0
    expected = expected_multi_pair(dataset)
    if args.command == "expect":
        print(json.dumps(multi_pair_summary(expected, dataset), indent=1))
        return 0
    from neo4j import GraphDatabase

    with GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)) as driver:
        actual = read_multi_pair(driver, args.database)
    problems = compare_multi_pair(expected, actual)
    print(json.dumps({"exact_match": not problems, "problems": problems,
                      **multi_pair_summary(expected, dataset)}, indent=1))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    raise SystemExit(main())
