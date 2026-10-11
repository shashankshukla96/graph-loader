"""Edge-before-node DLQ proof: P5-S3's committed row-level missing-endpoint
DLQ interface, exercised end to end against a real (ephemeral,
testcontainers) Kafka and the shared Neo4j integration fixtures.

Written during P6-S4's authorship as an auto-activating scaffold, before
Phase 5 Slice 3 existed; updated here, as part of Slice 3's own Story 7,
to drive the real committed interface instead of guessing at it. The
writer is now constructed with a real ``missing_endpoint_sink`` and
driven through ``write_provenanced_batch`` -- P5-S3 Story 3's row-level
write path -- instead of the old all-or-nothing ``write_batch``, which
can never DLQ a single row (it was always going to report "not merged
yet" regardless of whether Slice 3 had landed).

The writer is built with ``explicit_locks=False, sort_rows=False``
(planned mode's exact query branch), which never hits the
``CALL (vars) { ... }`` Cypher syntax P5-S1 Story 6 found
``neo4j:5.21-enterprise`` (the former shared pin; now 5.26) cannot parse -- this test runs against the
plain, shared ``neo4j_driver``/``clean_neo4j`` fixtures every other
pre-existing integration test in this repo already uses.

The retry half drives the real retry consumer path (BUG-1 regression).
"""
from __future__ import annotations

from datetime import date
import importlib.util
import json
import uuid

import pytest

from src.loader.dead_letter import (
    DeadLetterProducer,
    DeadLetterRecordSink,
    dead_letter_topic_name,
    decode_envelope,
    ensure_dead_letter_topic,
)
from src.loader.edge_execution import ApocLockedEdgeWriter
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError, PendingEdgeRecord
from src.models.schema import DeadLetterConfig
from src.utils.schema_loader import load_schema


def _retry_consumer_available() -> bool:
    """Capability probe for Phase 5 Slice 4's retry consumer; must never itself raise.

    Uses `find_spec` (never an actual `import` statement of a name that
    might not exist yet), wrapped in a broad `except`, so a legitimate
    skip can never turn into a collection/test error.
    """
    try:
        return importlib.util.find_spec("src.loader.retry_consumer") is not None
    except Exception:
        return False


def _works_at_edge_config_with_row_level_dlq(dlq_topic: str):
    """WORKS_AT, opted into P5-S3's row-level missing-endpoint DLQ.

    ``dead_letter.enabled=True`` plus ``route_missing_endpoints_in_bulk=True``
    -- the bulk opt-in (Story 1/5). This test drives ``ApocLockedEdgeWriter``
    directly rather than through ``build_edge_execution``, mirroring the
    construction the lease-bulk/classic-bulk branches would do once opted in.
    """
    base = next(edge for edge in load_schema("config/graph_schema.yaml").edges if edge.type == "WORKS_AT")
    return base.model_copy(update={
        "dead_letter": DeadLetterConfig(enabled=True, topic=dlq_topic, route_missing_endpoints_in_bulk=True),
    })


@pytest.mark.integration
def test_edge_before_node_is_dlqd_row_level_and_retried_once_the_node_arrives(
    neo4j_driver, clean_neo4j, kafka_bootstrap,
) -> None:
    from confluent_kafka import Consumer, Producer

    dlq_topic = f"dlq-scaffold-works-at-{uuid.uuid4().hex[:8]}"
    ensure_dead_letter_topic(kafka_bootstrap, dlq_topic, source_topic="works-at-events")
    edge_config = _works_at_edge_config_with_row_level_dlq(dlq_topic)
    assert dead_letter_topic_name(edge_config.topic, edge_config.dead_letter) == dlq_topic

    # A real Company node, so only the SOURCE (Person) endpoint is missing.
    company_key = f"dlq-scaffold-company-{uuid.uuid4().hex[:8]}"
    with neo4j_driver.session() as session:
        session.run("MERGE (c:Company {companyId: $key})", key=company_key).consume()
    missing_person_key = f"dlq-scaffold-missing-person-{uuid.uuid4().hex[:8]}"
    # A real date object, matching what normalize_edge_record would have
    # produced -- also exercises Story 2's date-safe DLQ value encoding
    # (_dlq_json_safe) rather than a plain string that sidesteps it.
    bad_record = EdgeRecord(missing_person_key, company_key, {"since": date(2024, 1, 1), "role": "scaffold"})
    pending = PendingEdgeRecord(bad_record, "works-at-events", 0, 0)

    kafka_producer = Producer({"bootstrap.servers": kafka_bootstrap})
    try:
        sink = DeadLetterRecordSink(
            producer=DeadLetterProducer(kafka_producer), topic=dlq_topic,
            run_id="scaffold-run", replica_id="0", kind="edge",
        )
        writer = ApocLockedEdgeWriter(
            neo4j_driver, edge_config, explicit_locks=False, sort_rows=False,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
            missing_endpoint_sink=sink,
        )
        assert writer.row_level_dlq_enabled is True

        # Must not raise: the bad row is routed to the DLQ, not fatal.
        writer.write_provenanced_batch([pending])
    finally:
        kafka_producer.flush(10)

    with neo4j_driver.session() as session:
        count = session.run(
            "MATCH (:Person {personId: $pid})-[r:WORKS_AT]->(:Company {companyId: $key}) RETURN count(r) AS c",
            pid=missing_person_key, key=company_key,
        ).single()["c"]
    assert count == 0

    consumer = Consumer({
        "bootstrap.servers": kafka_bootstrap, "group.id": f"scaffold-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    try:
        consumer.subscribe([dlq_topic])
        message = None
        for _ in range(50):
            message = consumer.poll(0.2)
            if message is not None and message.error() is None:
                break
        assert message is not None and message.error() is None, "no envelope landed on the DLQ topic"
        envelope = decode_envelope(message.value())
    finally:
        consumer.close()

    assert envelope.failure_class == "missing_endpoint"
    assert envelope.entity_kind == "edge" and envelope.entity_name == "WORKS_AT"
    assert envelope.reason_code == "missing_source"
    assert envelope.retry_after_ms >= envelope.first_failure_at_ms
    decoded_value = json.loads(envelope.value_bytes.decode("utf-8"))
    assert decoded_value["source"] == {"personId": missing_person_key}
    assert decoded_value["target"] == {"companyId": company_key}
    assert decoded_value["properties"] == {"since": "2024-01-01", "role": "scaffold"}
    # And the ISO date re-parses through the loader's own normalizer exactly.
    assert date.fromisoformat(decoded_value["properties"]["since"]) == date(2024, 1, 1)

    if not _retry_consumer_available():
        pytest.skip("src.loader.retry_consumer is not importable in this checkout")

    # The node arrives late. Drive the REAL retry path -- the exact writer
    # main() builds (adapter over the real ApocLockedEdgeWriter), a real Kafka
    # consumer on the DLQ topic and the real DeadLetterRecordSink -- until
    # the edge is written. (BUG-1: a fake writer here would have hidden that
    # the wired writer had no ``retry_batch``.) Single writer, serial: the
    # shared Neo4j fixture has no query log, so lock-wait accounting does not
    # apply here (see tests/integration/test_relationship_correctness_regressions.py
    # for the query-logged suite).
    from confluent_kafka import Consumer as KafkaConsumer

    from src.loader.retry_consumer import RetryConsumer, build_retry_writer

    with neo4j_driver.session() as session:
        session.run("MERGE (p:Person {personId: $key})", key=missing_person_key).consume()

    retry_producer = Producer({"bootstrap.servers": kafka_bootstrap})
    retry_kafka_consumer = KafkaConsumer({
        "bootstrap.servers": kafka_bootstrap, "group.id": f"retry-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    try:
        retry_kafka_consumer.subscribe([dlq_topic])
        retry_sink = DeadLetterRecordSink(
            producer=DeadLetterProducer(retry_producer), topic=dlq_topic,
            run_id="scaffold-retry", replica_id="retry", kind="edge",
        )
        retry_writer = build_retry_writer(writer)
        retry_consumer = RetryConsumer(
            consumer=retry_kafka_consumer,
            writer_by_edge_type={"WORKS_AT": retry_writer},
            sink_by_edge_type={"WORKS_AT": retry_sink},
            max_retry_attempts_by_edge_type={"WORKS_AT": 3},
            parked_topic_by_edge_type={"WORKS_AT": f"{dlq_topic}.parked"},
            base_delay_seconds_by_edge_type={"WORKS_AT": 0.1},
            max_delay_seconds_by_edge_type={"WORKS_AT": 1.0},
            poll_timeout_seconds=0.2, due_batch_coalesce_seconds=0.0,
            wall_clock=lambda: envelope.retry_after_ms + 10_000,  # the envelope is due
        )
        written = 0
        for _ in range(100):
            retry_consumer.tick()
            with neo4j_driver.session() as session:
                written = session.run(
                    "MATCH (:Person {personId: $pid})-[r:WORKS_AT]->(:Company {companyId: $key}) RETURN count(r) AS c",
                    pid=missing_person_key, key=company_key,
                ).single()["c"]
            if written:
                break
    finally:
        retry_kafka_consumer.close()
        retry_producer.flush(10)
    assert written == 1, "retry consumer never wrote the DLQ'd edge once its source node existed"
