"""v7-S3 story 3.4: planned ``order_by`` across chunks, against a real Neo4j (testcontainers, skipped without Docker).

Two topics feed one relationship type with ``order_by: since``.  The planner (fake Kafka) cuts one row per chunk
(``chunk_rows=1``), so a pair published in both topics always spans two chunks of one unit.  The chunks are then
written in chain order by the same ``ApocLockedEdgeWriter`` configuration ``planned_writer.main`` uses.  The
later-listed topic carries the OLDER value for half of the pairs: the stored value must stay the newer one.
"""
from __future__ import annotations

import json

import pytest

from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.edge_loader import MissingRelationshipEndpointError
from src.loader.edge_plan import decode_chunk_message
from src.loader.planned_writer import compile_record_normalizer
from test_edge_planner import FakeCluster
from test_planned_multi_topic import friend, multi_edge, plan_kafka, publish_topic

pytestmark = pytest.mark.integration

PAIRS = 20


def _chunk_chain(plan, cluster):
    successors = {chunk.after: chunk for chunk in plan.chunks if chunk.after != -1}
    for chunk in plan.chunks:
        if chunk.after != -1:
            continue
        current = chunk
        while current is not None:
            yield current, decode_chunk_message(cluster.logs[plan.topic][current.partition][current.offset])[1]
            current = successors.get(current.id)


def test_later_listed_topic_with_an_older_value_never_overwrites_across_chunks(tmp_path, neo4j_driver, clean_neo4j):
    edge = multi_edge(["topic-a", "topic-b"], rule="order_by", order_by="since")
    cluster = FakeCluster()
    publish_topic(cluster, "topic-a", [friend(i, 100 + i, since=10, weight=1.5) for i in range(PAIRS)])
    # i < 10: the later-listed topic is OLDER (5); i >= 10 it is newer (20).
    publish_topic(cluster, "topic-b", [friend(i, 100 + i, since=5 if i < 10 else 20) for i in range(PAIRS)])
    plan = plan_kafka(cluster, edge, tmp_path, buckets=1, chunk_rows=1).plan
    assert len(plan.chunks) == 2 * PAIRS  # one row per chunk: every pair spans two chunks

    with neo4j_driver.session() as session:
        session.run("UNWIND range(0, $n) AS i MERGE (:Person {person_id: i}) MERGE (:Person {person_id: 100 + i})",
                    n=PAIRS).consume()
    writer = ApocLockedEdgeWriter(neo4j_driver, edge, explicit_locks=False, sort_rows=False,
                                  missing_endpoint_error_factory=MissingRelationshipEndpointError)
    normalize = compile_record_normalizer(edge)
    for _chunk, records in _chunk_chain(plan, cluster):
        rows = []
        for raw in records:
            source_key, target_key, properties = normalize(json.loads(raw) if isinstance(raw, (str, bytes)) else raw)
            rows.append({"source_key": source_key, "target_key": target_key, "properties": properties})
        writer.write_rows(rows)

    with neo4j_driver.session() as session:
        stored = {
            record["s"]: (record["since"], record["weight"])
            for record in session.run("MATCH (s:Person)-[r:FRIEND_OF]->(t:Person) "
                                      "RETURN s.person_id AS s, r.since AS since, r.weight AS weight")
        }
    assert len(stored) == PAIRS
    for i in range(PAIRS):
        assert stored[i] == (10 if i < 10 else 20, 1.5)
