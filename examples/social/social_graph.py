#!/usr/bin/env python3
"""Synthetic social/commerce graph: generate, publish per the library contract, verify.

The library contract is only ``graph_schema.yaml`` plus JSON records on the
schema-declared topics:

* node topic of ``Label``: ``{"<key>": value, "<prop>": value, ...}``
  (Kafka key = the node key as text);
* edge topic of ``TYPE``: ``{"source": {"<source key>": value},
  "target": {"<target key>": value}, "properties": {...}}``
  (Kafka key = the source key as text, so every record of one
  ``(source, target)`` pair lands in one partition and keeps its order).

Dates travel as ``YYYY-MM-DD`` strings and datetimes as ISO-8601 strings with
an offset.  The expected graph is computed here directly from the generated
Python values (MERGE one relationship per pair; ``SET r += props`` so a later
record overwrites the properties it carries and keeps the others), without
any library code, and compared exactly with what Neo4j holds.

Usage::

    python examples/social/social_graph.py publish --bootstrap localhost:9092 --persons 20000
    python -m src.cli start --mode bulk --config examples/social/graph_schema.yaml --edge-plan
    python examples/social/social_graph.py verify --persons 20000 --neo4j-uri bolt://localhost:7687
"""
from __future__ import annotations

import argparse
import bisect
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import sys
import time

SCHEMA_PATH = Path(__file__).with_name("graph_schema.yaml")

NODE_TOPICS = {
    "Person": ("social-person", "person_id"), "Company": ("social-company", "company_id"),
    "Product": ("social-product", "sku"), "City": ("social-city", "name"), "Day": ("social-day", "day"),
    "Country": ("social-country", "code"),
}
# edge type -> (topic, source label, target label, source key, target key)
EDGE_TOPICS = {
    "WORKS_AT": ("social-works-at", "Person", "Company", "person_id", "company_id"),
    "BOUGHT": ("social-bought", "Person", "Product", "person_id", "sku"),
    "FRIEND_OF": ("social-friend-of", "Person", "Person", "person_id", "person_id"),
    "LIVES_IN": ("social-lives-in", "Person", "City", "person_id", "name"),
    "VISITED": ("social-visited", "Person", "City", "person_id", "name"),
    "ACTIVE_ON": ("social-active-on", "Person", "Day", "person_id", "day"),
}
_ZONES = [timezone.utc, timezone(timedelta(hours=2)), timezone(timedelta(hours=-5, minutes=-30))]


@dataclass
class Dataset:
    """Typed Python values; ``edges[type]`` is in publication order."""

    nodes: dict[str, list[dict]]
    edges: dict[str, list[tuple[object, object, dict]]]

    @property
    def edge_count(self) -> int:
        return sum(len(rows) for rows in self.edges.values())


_CUMULATIVE: dict[tuple[int, float], list[float]] = {}


def _zipf_choice(rng: random.Random, items: list, skew: float = 1.1):
    cumulative = _CUMULATIVE.get((len(items), skew))
    if cumulative is None:
        total, cumulative = 0.0, []
        for rank in range(len(items)):
            total += 1.0 / (rank + 1) ** skew
            cumulative.append(total)
        _CUMULATIVE[(len(items), skew)] = cumulative
    return items[min(bisect.bisect_left(cumulative, rng.random() * cumulative[-1]), len(items) - 1)]


def generate(persons: int = 20_000, seed: int = 7) -> Dataset:
    """Deterministic dataset for ``persons`` people (~17 relationships each)."""
    rng = random.Random(seed)
    person_ids = [i * 7919 - 50_000 for i in range(persons)]  # negative, small and large integers
    person_ids[-1] = 2 ** 53 + 11
    rng.shuffle(person_ids)
    companies = [f"C{i:05d}" for i in range(max(2, persons // 50))]
    products = [f"sku-{i:06d}" for i in range(max(3, persons // 5))]
    cities = [f"City {chr(65 + i)}" for i in range(25)]
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(60)]
    nodes = {
        "Person": [
            {"person_id": pid, "name": f"person {pid}", "age": 18 + pid % 60,
             **({"score": round(rng.uniform(0, 100), 3)} if pid % 3 else {}),
             **({"joined": date(2015, 1, 1) + timedelta(days=pid % 3000)} if pid % 4 else {})}
            for pid in person_ids
        ],
        "Company": [{"company_id": key, "name": f"company {key}", "founded": 1950 + i % 70}
                    for i, key in enumerate(companies)],
        "Product": [{"sku": key, "title": f"product {key}", "price": round(1 + (i % 997) * 0.37, 2)}
                    for i, key in enumerate(products)],
        "City": [{"name": key, "population": 10_000 * (i + 1)} for i, key in enumerate(cities)],
        "Day": [{"day": day, "weekday": day.strftime("%A")} for day in days],
        "Country": [{"code": code, "name": f"country {code}"} for code in ("DE", "FR", "US", "JP", "BR")],
    }
    edges: dict[str, list] = {name: [] for name in EDGE_TOPICS}
    for pid in person_ids:
        company = rng.choice(companies)
        edges["WORKS_AT"].append((pid, company, {"since": date(2010, 1, 1) + timedelta(days=pid % 4000),
                                                   "role": rng.choice(["engineer", "sales", "ops"])}))
        if rng.random() < 0.2:  # role change: same pair, role overwritten, since overwritten
            edges["WORKS_AT"].append((pid, company, {"since": date(2023, 6, 1), "role": "manager"}))
        for _ in range(rng.randint(2, 8)):
            product = _zipf_choice(rng, products)
            props = {"quantity": rng.randint(1, 5)}
            if rng.random() < 0.7:
                props["amount"] = round(rng.uniform(1, 500), 2)
            if rng.random() < 0.5:
                props["at"] = datetime(2024, 1, 1, tzinfo=rng.choice(_ZONES)) + timedelta(
                    seconds=rng.randint(0, 10_000_000))
            edges["BOUGHT"].append((pid, product, props))
        for _ in range(rng.randint(1, 7)):
            friend = pid if rng.random() < 0.01 else rng.choice(person_ids)  # rare self loops
            props = {"since": rng.randint(2000, 2024)}
            if rng.random() < 0.6:
                props["weight"] = rng.choice([0.5, 1.0, -0.0, 2.25, rng.random()])
            edges["FRIEND_OF"].append((pid, friend, props))
        city = _zipf_choice(rng, cities, 1.3)  # a few very large hub cities
        edges["LIVES_IN"].append((pid, city, {"since": date(2000, 1, 1) + timedelta(days=pid % 8000)}
                                  if pid % 5 else {}))
        for _ in range(rng.randint(0, 4)):
            edges["VISITED"].append((pid, _zipf_choice(rng, cities, 0.8), {"times": rng.randint(1, 30)}))
        for _ in range(rng.randint(1, 5)):
            edges["ACTIVE_ON"].append((pid, rng.choice(days), {"sessions": rng.randint(1, 12)}))
    # Interleave people randomly, but keep each pair's records in generation
    # order: a pair's rows are put back, in order, into the slots they were
    # shuffled to.
    for name, rows in edges.items():
        slots = list(range(len(rows)))
        rng.shuffle(slots)
        by_pair: dict[tuple, list[int]] = {}
        for index, (source, target, _) in enumerate(rows):
            by_pair.setdefault((source, target), []).append(index)
        reordered = [None] * len(rows)
        for indexes in by_pair.values():
            for slot, index in zip(sorted(slots[index] for index in indexes), indexes):
                reordered[slot] = rows[index]
        edges[name] = reordered
    return Dataset(nodes, edges)


# -- wire format ---------------------------------------------------------------


def wire_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def node_record(record: dict) -> dict:
    return {name: wire_value(value) for name, value in record.items()}


def edge_record(edge_type: str, source: object, target: object, properties: dict) -> dict:
    _, _, _, source_key, target_key = EDGE_TOPICS[edge_type]
    return {"source": {source_key: wire_value(source)}, "target": {target_key: wire_value(target)},
            "properties": {name: wire_value(value) for name, value in properties.items()}}


def all_topics() -> list[str]:
    return [topic for topic, _ in NODE_TOPICS.values()] + [item[0] for item in EDGE_TOPICS.values()]


def delete_topics(bootstrap: str) -> None:
    """Delete the example's topics (and wait until they are gone)."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = [topic for topic in all_topics() if topic in admin.list_topics(timeout=20).topics]
    for future in (admin.delete_topics(existing, operation_timeout=30).values() if existing else ()):
        future.result(60)
    deadline = time.monotonic() + 60
    while any(topic in admin.list_topics(timeout=20).topics for topic in existing):
        if time.monotonic() > deadline:
            raise RuntimeError("topics were not deleted")
        time.sleep(0.5)


def publish(dataset: Dataset, bootstrap: str, *, partitions: int = 4, create: bool = True) -> dict[str, int]:
    """Create the topics (if asked) and publish every record; return records per topic."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    topics = {topic: (1 if label in ("City", "Day", "Country") else partitions)
              for label, (topic, _) in NODE_TOPICS.items()}
    topics.update({topic: partitions for topic, *_ in EDGE_TOPICS.values()})
    if create:
        admin = AdminClient({"bootstrap.servers": bootstrap})
        existing = admin.list_topics(timeout=20).topics
        new = [NewTopic(topic, num_partitions=count, replication_factor=1)
               for topic, count in topics.items() if topic not in existing]
        for future in (admin.create_topics(new).values() if new else ()):
            future.result(30)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 20,
                         "compression.type": "lz4", "queue.buffering.max.messages": 500_000})
    counts: dict[str, int] = {}

    def produce(topic: str, key: object, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=str(wire_value(key)).encode("utf-8"), value=payload)
                break
            except BufferError:
                producer.poll(0.05)
        counts[topic] = counts.get(topic, 0) + 1

    for label, records in dataset.nodes.items():
        topic, key = NODE_TOPICS[label]
        for record in records:
            produce(topic, record[key], node_record(record))
    for edge_type, rows in dataset.edges.items():
        topic = EDGE_TOPICS[edge_type][0]
        for source, target, properties in rows:
            produce(topic, source, edge_record(edge_type, source, target, properties))
        producer.poll(0)
    if producer.flush(120):
        raise RuntimeError("Kafka did not deliver every record")
    return counts


# -- expected graph and Neo4j comparison ----------------------------------------


def _canonical(value: object) -> object:
    if isinstance(value, float) and value == 0.0:
        return 0.0
    if isinstance(value, datetime):
        return ("datetime", value.astimezone(timezone.utc).isoformat())
    return value


def expected_graph(dataset: Dataset) -> tuple[dict, dict]:
    """``(nodes, rels)`` with ``nodes[label][key] = props`` and ``rels[type][(s, t)] = props``."""
    nodes = {label: {} for label in NODE_TOPICS}
    for label, records in dataset.nodes.items():
        key = NODE_TOPICS[label][1]
        for record in records:
            merged = nodes[label].setdefault(_canonical(record[key]), {})
            merged.update({name: _canonical(value) for name, value in record.items()})
    rels: dict[str, dict] = {name: {} for name in EDGE_TOPICS}
    for edge_type, rows in dataset.edges.items():
        for source, target, properties in rows:
            merged = rels[edge_type].setdefault((_canonical(source), _canonical(target)), {})
            merged.update({name: _canonical(value) for name, value in properties.items()})
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
                identity = props[key]
                if identity in nodes[label]:
                    duplicates[label] = duplicates.get(label, 0) + 1
                nodes[label][identity] = props
        for edge_type, (_, source_label, target_label, source_key, target_key) in EDGE_TOPICS.items():
            query = (f"MATCH (s:`{source_label}`)-[r:`{edge_type}`]->(t:`{target_label}`) "
                     f"RETURN s.`{source_key}` AS s, t.`{target_key}` AS t, properties(r) AS p")
            for record in session.run(query):
                pair = (_native(record["s"]), _native(record["t"]))
                if pair in rels[edge_type]:
                    duplicates[edge_type] = duplicates.get(edge_type, 0) + 1
                rels[edge_type][pair] = {name: _native(value) for name, value in record["p"].items()}
        total = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        extra = total - sum(len(items) for items in rels.values()) - sum(
            count for name, count in duplicates.items() if name in EDGE_TOPICS)
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
                sample = next(iter(wrong), None)
                problems.append(
                    f"{kind} {name}: expected {len(left)} got {len(right)}; missing {len(missing)}, "
                    f"unexpected {len(extra)}, wrong properties {len(wrong)}"
                    + (f" e.g. {sample}: expected {left[sample]} got {right[sample]}" if sample is not None else "")
                )
    return problems


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


def summary(expected: tuple[dict, dict], dataset: Dataset | None = None) -> dict:
    nodes, rels = expected
    return {
        "nodes": {label: len(items) for label, items in nodes.items()},
        "relationships": {name: len(items) for name, items in rels.items()},
        **({"last_write_wins": last_write_evidence(dataset)} if dataset is not None else {}),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("publish", "verify", "expect"):
        command = sub.add_parser(name)
        command.add_argument("--persons", type=int, default=20_000)
        command.add_argument("--seed", type=int, default=7)
    reset = sub.add_parser("reset", help="delete the example's topics")
    reset.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    sub.choices["publish"].add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                                                            "localhost:9092"))
    sub.choices["publish"].add_argument("--partitions", type=int, default=4)
    verify = sub.choices["verify"]
    verify.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    verify.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USERNAME", "neo4j"))
    verify.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD", "changeme"))
    verify.add_argument("--database", default=os.environ.get("NEO4J_DATABASE") or None)
    verify.add_argument("--against-database", default=None,
                        help="also compare the graph with this database's graph (e.g. a classic-mode load)")
    args = parser.parse_args(argv)
    started = time.monotonic()
    if args.command == "reset":
        delete_topics(args.bootstrap)
        return 0
    dataset = generate(args.persons, args.seed)
    if args.command == "publish":
        counts = publish(dataset, args.bootstrap, partitions=args.partitions)
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
        other = read_graph(driver, args.against_database) if args.against_database else None
    problems = compare(expected, actual)
    result = {"exact_match": not problems, "problems": problems}
    if other is not None:
        between = compare(actual[:2], other)
        result.update({"identical_to": args.against_database, "graphs_identical": not between,
                       "graph_differences": between})
        problems = problems + between
    print(json.dumps({**result, **summary(expected, dataset)}, indent=1))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    raise SystemExit(main())
