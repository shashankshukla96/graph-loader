"""Unit tests for the pure schema-driven relationship record contract."""
from __future__ import annotations

from datetime import date
import itertools
import threading
import time
from queue import Queue
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry
from threading import Event

from src.loader.edge_loader import (
    EdgeRecord,
    EdgeRecordValidationError,
    EdgeLoader,
    EdgeWriter,
    MissingRelationshipEndpointError,
    build_edge_upsert_query,
    main as edge_loader_main,
    normalize_edge_record,
    WorkerBatch,
    WorkerResult,
    PendingEdgeRecord,
)
from src.loader.edge_execution import LaneExecutionResult
from src.loader.mix_and_batch import endpoint_buckets
from src.loader.node_loader import RejectionSink
from src.loader.slot_admission import LeaseStateError, SlotAdmission, SlotAwareAdmissionBuffer
from src.models.schema import CoordinationConfig, EdgeConfig, LoadingConfig
from src.orchestrator.coordination import ClockLease, encode_clock_lease
from src.utils.metrics import GraphLoaderMetrics
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def works_at(schema):
    return next(edge for edge in schema.edges if edge.type == "WORKS_AT")


@pytest.fixture(scope="module")
def knows(schema):
    return next(edge for edge in schema.edges if edge.type == "KNOWS")


def test_normalizes_works_at_event(works_at) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"companyId": "c-001"},
            "properties": {"since": "2020-01-02", "role": "Engineer"},
        },
        works_at,
    )
    assert record.source_key == "p-001"
    assert record.target_key == "c-001"
    assert record.properties == {"since": date(2020, 1, 2), "role": "Engineer"}


def test_normalizes_self_referencing_event(knows) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"personId": "p-002"},
            "properties": {"since": "2020-01-02", "strength": 1},
        },
        knows,
    )
    assert (record.source_key, record.target_key) == ("p-001", "p-002")
    assert record.properties["strength"] == 1.0


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"source": {}, "target": {}, "properties": {}, "extra": {}},
        {"source": {}, "target": {}, "properties": None},
        {"source": {"personId": "p-001", "extra": "no"}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": None}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": "c-001"}, "properties": {"unknown": "no", "since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": "c-001"}, "properties": {"since": None}},
        {"source": {"personId": 1}, "target": {"companyId": "c-001"}, "properties": {"since": "2020-01-02"}},
        {"source": {"personId": "p-001"}, "target": {"companyId": 1}, "properties": {"since": "2020-01-02"}},
    ],
)
def test_invalid_envelopes_are_rejected(works_at, payload: object) -> None:
    with pytest.raises(EdgeRecordValidationError):
        normalize_edge_record(payload, works_at)


def test_optional_null_property_is_omitted(works_at) -> None:
    record = normalize_edge_record(
        {
            "source": {"personId": "p-001"},
            "target": {"companyId": "c-001"},
            "properties": {"since": "2020-01-02", "role": None},
        },
        works_at,
    )
    assert record.properties == {"since": date(2020, 1, 2)}


def test_validation_error_never_echoes_raw_value(works_at) -> None:
    with pytest.raises(EdgeRecordValidationError) as exc_info:
        normalize_edge_record(
            {
                "source": {"personId": "secret-payload"},
                "target": {"companyId": "c-001"},
                "properties": {"since": "not-a-date"},
            },
            works_at,
        )
    assert "secret-payload" not in str(exc_info.value)


def test_direct_edge_config_fails_without_graph_schema_binding() -> None:
    edge = EdgeConfig(
        type="WORKS_AT",
        topic="works-at-events",
        nodes={"source": "Person", "target": "Company"},
        source_key_property="personId",
        target_key_property="companyId",
        properties={"since": {"type": "date", "required": True}},
    )
    with pytest.raises(EdgeRecordValidationError, match="unresolved endpoint schema bindings"):
        normalize_edge_record(
            {
                "source": {"personId": "p-001"},
                "target": {"companyId": "c-001"},
                "properties": {"since": "2020-01-02"},
            },
            edge,
        )


def _record() -> EdgeRecord:
    return EdgeRecord(
        source_key="p-001",
        target_key="c-001",
        properties={"since": date(2020, 1, 2)},
    )


def _writer_with_preflight(works_at, preflight_rows: list[dict[str, int]]):
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    preflight = MagicMock()
    preflight.__iter__.return_value = iter(preflight_rows)
    upsert = MagicMock()
    transaction.run.side_effect = [preflight, upsert]
    return EdgeWriter(driver, works_at), driver, transaction, preflight, upsert


def test_build_edge_upsert_query_uses_declared_identifiers(works_at, knows) -> None:
    works_query = build_edge_upsert_query(works_at)
    knows_query = build_edge_upsert_query(knows)
    assert "MATCH (s:`Person` {`personId`: row.source_key})" in works_query
    assert "MATCH (t:`Company` {`companyId`: row.target_key})" in works_query
    assert "MERGE (s)-[r:`WORKS_AT`]->(t)" in works_query
    assert "MERGE (s)-[r:`KNOWS`]->(t)" in knows_query
    assert "CREATE" not in works_query


def test_writer_preflights_then_merges_and_commits(works_at) -> None:
    writer, driver, transaction, preflight, upsert = _writer_with_preflight(
        works_at, [{"source_matches": 1, "target_matches": 1}]
    )
    writer.write(_record())
    rows = [{"source_key": "p-001", "target_key": "c-001", "properties": {"since": date(2020, 1, 2)}}]
    assert transaction.run.call_count == 2
    assert transaction.run.call_args_list[0].kwargs["rows"] == rows
    assert transaction.run.call_args_list[1].kwargs["rows"] == rows
    preflight.consume.assert_called_once_with()
    upsert.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()
    driver.close.assert_not_called()


def test_writer_accepts_repeated_input_events_as_distinct_rows(works_at) -> None:
    writer, _driver, transaction, preflight, upsert = _writer_with_preflight(
        works_at,
        [
            {"row_index": 0, "source_matches": 1, "target_matches": 1},
            {"row_index": 1, "source_matches": 1, "target_matches": 1},
        ],
    )
    writer.write_batch([_record(), _record()])
    assert "row_index" in transaction.run.call_args_list[0].args[0]
    assert len(transaction.run.call_args_list[1].kwargs["rows"]) == 2
    preflight.consume.assert_called_once_with()
    upsert.consume.assert_called_once_with()
    transaction.commit.assert_called_once_with()


@pytest.mark.parametrize(
    "counts",
    [
        {"source_matches": 0, "target_matches": 1},
        {"source_matches": 1, "target_matches": 0},
        {"source_matches": 2, "target_matches": 1},
    ],
)
def test_missing_or_duplicate_endpoints_never_merge_or_commit(works_at, counts) -> None:
    writer, _driver, transaction, preflight, _upsert = _writer_with_preflight(works_at, [counts])
    with pytest.raises(MissingRelationshipEndpointError, match="WORKS_AT"):
        writer.write(_record())
    assert transaction.run.call_count == 1
    preflight.consume.assert_called_once_with()
    transaction.commit.assert_not_called()


def test_writer_propagates_query_failure_without_commit(works_at) -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    transaction.run.side_effect = RuntimeError("driver failed")
    with pytest.raises(RuntimeError, match="driver failed"):
        EdgeWriter(driver, works_at).write(_record())
    transaction.commit.assert_not_called()


def test_empty_batch_does_not_open_session(works_at) -> None:
    driver = MagicMock()
    EdgeWriter(driver, works_at).write_batch([])
    driver.session.assert_not_called()


def _message(
    payload: bytes,
    error=None,
    *,
    topic: str = "works-at-events",
    partition: int = 0,
    offset: int = 3,
):
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = error
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _payload() -> bytes:
    return (
        b'{"source":{"personId":"p-001"},"target":{"companyId":"c-001"},'
        b'"properties":{"since":"2020-01-02"}}'
    )


def _clock_lease_payload(epoch: int, owner: str, owners=None) -> bytes:
    return encode_clock_lease(ClockLease(
        run_id="run-slot", epoch=epoch, slot_id=epoch % 64, issued_at_ms=10,
        expires_at_ms=100, active_edge_types=("BOUGHT", "WORKS_AT"),
        bucket_owners=owners or {bucket: owner for bucket in range(64)},
    ))


def _gated_loader(consumer, writer, works_at, coordination_values, metrics=None):
    values = iter(coordination_values)
    kwargs = {}
    if metrics is not None:
        kwargs["metrics"] = metrics
    return EdgeLoader(
        consumer, writer, works_at, run_id="run-slot",
        loading_config=LoadingConfig(
            unwind_batch_size=1,
            coordination=CoordinationConfig(bucket_count=64, slot_duration_ms=10, lease_timeout_ms=20),
        ),
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=SlotAwareAdmissionBuffer(max_records=2),
        coordination_poll=lambda _timeout: next(values, None),
        wall_clock_ms=lambda: 11,
        **kwargs,
    )


def test_edge_loader_rejects_partial_slot_gating_dependencies(works_at) -> None:
    dependencies = (
        SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        SlotAwareAdmissionBuffer(max_records=2),
        lambda _timeout: None,
        lambda: 11,
    )
    for selected in itertools.chain.from_iterable(
        itertools.combinations(range(4), count) for count in range(1, 4)
    ):
        consumer = MagicMock()
        values = [None] * 4
        for index in selected:
            values[index] = dependencies[index]
        with pytest.raises(LeaseStateError, match="all present or all absent"):
            EdgeLoader(consumer, MagicMock(), works_at,
                       slot_admission=values[0], slot_buffer=values[1],
                       coordination_poll=values[2], wall_clock_ms=values[3])
        consumer.subscribe.assert_not_called()


def test_edge_loader_rejects_slot_admission_bucket_count_mismatch_before_subscription(works_at) -> None:
    consumer = MagicMock()
    with pytest.raises(LeaseStateError, match="bucket count does not match"):
        EdgeLoader(
            consumer, MagicMock(), works_at, run_id="run-slot",
            slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=4),
            slot_buffer=SlotAwareAdmissionBuffer(max_records=2),
            coordination_poll=lambda _timeout: None,
            wall_clock_ms=lambda: 11,
        )
    consumer.subscribe.assert_not_called()


def test_unleased_edge_record_is_buffered_without_write_or_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "BOUGHT"), None])
    assert loader.run(max_messages=1) == 1
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_later_owned_lease_releases_buffered_record_then_commits(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "BOUGHT"), None, _clock_lease_payload(2, "WORKS_AT"), None,
    ])
    assert loader.run(max_messages=1) == 0
    writer.write_batch.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 4


def test_new_lease_requeues_already_batched_record_before_write(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "WORKS_AT"), None, _clock_lease_payload(2, "BOUGHT"), None,
    ])
    assert loader.run(max_messages=1) == 1
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_coordination_drain_observes_queued_grant_then_revocation_before_write(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "WORKS_AT"), _clock_lease_payload(2, "BOUGHT"),
        None,
    ])
    assert loader.run(max_messages=1) == 1
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_revocation_arriving_during_write_blocks_offset_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    writer.write_batch.side_effect = lambda _records: None
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "WORKS_AT"), None, None, None,
        _clock_lease_payload(2, "BOUGHT"), None,
    ])
    assert loader.run(max_messages=1) == 1
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_post_write_grant_does_not_resolve_or_commit_unwritten_buffered_record(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    first_record = EdgeRecord("p-001", "c-001", {})
    first_buckets = endpoint_buckets(first_record, 64)
    second_keys = next(
        (f"p-{index}", f"c-{index}")
        for index in range(2, 1_000)
        if {
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).source,
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).target,
        }.isdisjoint({first_buckets.source, first_buckets.target})
    )
    owners = {bucket: "BOUGHT" for bucket in range(64)}
    owners[first_buckets.source] = "WORKS_AT"
    owners[first_buckets.target] = "WORKS_AT"
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "BOUGHT", owners), None, None,
        _clock_lease_payload(2, "WORKS_AT"), None,
    ])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    second_payload = (
        '{"source":{"personId":"%s"},"target":{"companyId":"%s"},'
        '"properties":{"since":"2020-01-02"}}' % second_keys
    ).encode("utf-8")
    assert loader._buffer_message(_message(second_payload, offset=4))
    assert len(loader._batch) == 1 and len(loader._slot_buffer) == 1
    assert loader._flush_batch() is True
    assert [record.source_key for record in writer.write_batch.call_args.args[0]] == ["p-001"]
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 4
    assert [item.source_key for item in (pending.record for pending in loader._batch)] == [second_keys[0]]


def test_slot_wall_clock_failure_is_contextual_and_fails_closed(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot",
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=SlotAwareAdmissionBuffer(max_records=2),
        coordination_poll=lambda _timeout: _clock_lease_payload(1, "WORKS_AT"),
        wall_clock_ms=lambda: (_ for _ in ()).throw(RuntimeError("clock gone")),
    )
    assert loader.run(max_messages=1) == 1
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_slot_buffer_backpressure_resumes_after_owned_work_is_written(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = None
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "BOUGHT"), None,
        _clock_lease_payload(2, "WORKS_AT"), None,
    ])
    loader._slot_buffer = SlotAwareAdmissionBuffer(max_records=1)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    consumer.pause.assert_called_once()
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()

    loader._poll_coordination()
    assert len(loader._slot_buffer) == 0
    assert loader._buffer_paused  # the unstarted batch can still be requeued
    assert loader._flush_batch()
    writer.write_batch.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 4
    consumer.resume.assert_called_once()
    assert not loader._buffer_paused


def test_slot_backpressure_reserves_space_for_queued_worker_batch(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._slot_buffer = SlotAwareAdmissionBuffer(max_records=1)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    assert loader._enqueue_worker_batch()
    assert loader._buffer_paused
    assert len(loader._slot_buffer) == 0
    consumer.resume.assert_not_called()


def test_gated_revoke_of_buffered_record_is_attributed_and_fails_closed(works_at, caplog) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "BOUGHT"), None])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])
    loader._on_revoke(consumer, [assigned])
    assert loader._rebalance_failure is True
    assert "stage=rebalance edge=WORKS_AT replica=0 run_id=run-slot epoch=1 slot=1" in caplog.text
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_worker_batch_and_result_invariants_reject_unsafe_state() -> None:
    pending = PendingEdgeRecord(EdgeRecord("p", "c", {}), "topic", 0, 1)
    with pytest.raises(ValueError, match="lease context"):
        WorkerBatch(1, (pending,), (), 1, None, None)
    batch = WorkerBatch(1, (pending,), (), None, None, None)
    with pytest.raises(ValueError, match="write failure"):
        WorkerResult(batch, "WRITE_FAILURE")
    with pytest.raises(ValueError, match="cannot carry"):
        WorkerResult(batch, "SUCCESS", RuntimeError("no"))


def test_gated_writer_executes_off_the_kafka_polling_thread(works_at, caplog) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    seen_threads: list[int] = []
    writer.write_batch.side_effect = lambda _records: seen_threads.append(threading.get_ident())
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    with caplog.at_level("INFO", logger="src.loader.edge_loader"):
        assert loader.run(max_messages=1) == 0
    assert seen_threads and seen_threads[0] != threading.get_ident()
    assert consumer.commit.call_count == 1
    assert "stage=edge-write edge=WORKS_AT" in caplog.text
    assert "records=1 write_ms=" in caplog.text


def test_gated_loader_with_no_control_message_keeps_work_uncommitted(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = _gated_loader(consumer, writer, works_at, [None])
    assert loader.run(max_messages=1) == 1
    assert loader._slot_admission.current is None
    writer.write_batch.assert_not_called()
    consumer.commit.assert_not_called()


def test_worker_write_failure_keeps_offset_uncommitted_and_stops_worker(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    writer.write_batch.side_effect = RuntimeError("database down")
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    assert loader.run(max_messages=1) == 1
    consumer.commit.assert_not_called()
    assert loader._worker is None


def test_failed_worker_never_writes_a_later_queued_batch(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    first_buckets = endpoint_buckets(EdgeRecord("p-001", "c-001", {}), 64)
    second_source, second_target = next(
        (f"p-{index}", f"c-{index}")
        for index in range(2, 1_000)
        if {
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).source,
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).target,
        }.isdisjoint({first_buckets.source, first_buckets.target})
    )
    second_payload = (
        '{"source":{"personId":"%s"},"target":{"companyId":"%s"},'
        '"properties":{"since":"2020-01-02"}}' % (second_source, second_target)
    ).encode()
    messages = iter([_message(_payload(), offset=3), _message(second_payload, offset=4)])
    consumer.poll.side_effect = lambda _timeout: next(messages, None)
    entered, release = threading.Event(), threading.Event()

    def fail_first(_records):
        entered.set()
        release.wait(1)
        raise RuntimeError("neo4j down")

    writer.write_batch.side_effect = fail_first
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    result: list[int] = []
    runner = threading.Thread(target=lambda: result.append(loader.run(max_messages=2)))
    runner.start()
    assert entered.wait(1)
    release.set()
    runner.join(2)
    assert result == [1]
    assert writer.write_batch.call_count == 1
    consumer.commit.assert_not_called()
    assert loader._worker is None


def test_blocked_worker_keeps_poll_owner_heartbeating_before_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    started, release = threading.Event(), threading.Event()
    writer.write_batch.side_effect = lambda _records: (started.set(), release.wait(1))
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    result: list[int] = []
    runner = threading.Thread(target=lambda: result.append(loader.run(max_messages=1)))
    runner.start()
    assert started.wait(1)
    assert consumer.poll.call_count >= 2
    consumer.commit.assert_not_called()
    release.set()
    runner.join(2)
    assert result == [0]
    assert consumer.commit.call_count == 1


def test_blocked_worker_leaves_all_kafka_calls_in_poll_owner_thread(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    started, release = threading.Event(), threading.Event()
    calls: list[tuple[str, int]] = []
    consumer.poll.side_effect = lambda timeout: (calls.append(("poll", threading.get_ident())), _message(_payload(), offset=3))[1]
    consumer.commit.side_effect = lambda **kwargs: (
        calls.append(("commit", threading.get_ident())), kwargs["offsets"]
    )[1]
    writer.write_batch.side_effect = lambda _records: (started.set(), release.wait(1))
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    result: list[int] = []
    runner = threading.Thread(target=lambda: result.append(loader.run(max_messages=1)))
    runner.start()
    assert started.wait(1)
    release.set()
    runner.join(2)
    assert result == [0]
    assert calls and {thread_id for _operation, thread_id in calls} == {runner.ident}


def test_worker_result_mismatch_fails_closed_without_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    pending = PendingEdgeRecord(_record(), "works-at-events", 0, 3)
    expected = WorkerBatch(1, (pending,), (), None, None, None)
    unexpected = WorkerBatch(2, (pending,), (), None, None, None)
    loader._outstanding_batches[1] = expected
    loader._permit_states[1] = "STARTED"
    with pytest.raises(RuntimeError, match="stage=worker.*unexpected or duplicate"):
        loader._resolve_worker_result(WorkerResult(unexpected, "SUCCESS"))
    consumer.commit.assert_not_called()


def test_worker_queue_full_fails_closed_and_removes_unaccepted_snapshot(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    loader._worker_queue = Queue(maxsize=1)
    loader._worker_queue.put_nowait(object())
    with pytest.raises(RuntimeError, match="stage=worker.*queue is full"):
        loader._enqueue_worker_batch()
    assert not loader._outstanding_batches
    consumer.commit.assert_not_called()


def test_revoke_detects_queued_and_started_worker_batches(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    pending = PendingEdgeRecord(_record(), "works-at-events", 0, 3)
    for batch_id, state in ((1, "QUEUED"), (2, "STARTED")):
        batch = WorkerBatch(batch_id, (pending,), (), None, None, None)
        loader._outstanding_batches[batch_id] = batch
        loader._permit_states[batch_id] = state
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])
    loader._on_revoke(consumer, [assigned])
    assert loader._rebalance_failure is True
    consumer.commit.assert_not_called()


def test_new_lease_cancels_queued_tombstone_and_requeues_it(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [
        _clock_lease_payload(1, "WORKS_AT"), None, _clock_lease_payload(2, "BOUGHT"), None,
    ])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    batch = WorkerBatch(1, tuple(loader._batch), tuple(loader._gated_batch), 1, 1, 100)
    loader._batch.clear()
    loader._gated_batch.clear()
    loader._outstanding_batches[1] = batch
    loader._permit_states[1] = "QUEUED"
    loader._worker_queue.put_nowait(batch)
    loader._poll_coordination()
    assert loader._permit_states[1] == "CANCELLED_PRESTART"
    assert len(loader._slot_buffer) == 1
    loader._worker_queue.put_nowait(loader._worker_sentinel)
    worker = threading.Thread(target=loader._worker_loop)
    worker.start()
    worker.join(1)
    assert not worker.is_alive()
    assert loader._resolve_worker_result(loader._worker_results.get_nowait()) is True
    assert not loader._outstanding_batches
    writer.write_batch.assert_not_called()


def test_inflight_hold_blocks_locally_overlapping_release(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    first = WorkerBatch(1, tuple(loader._batch), tuple(loader._gated_batch), 1, 1, 100)
    loader._batch.clear()
    loader._gated_batch.clear()
    loader._outstanding_batches[1] = first
    loader._permit_states[1] = "STARTED"
    assert loader._buffer_message(_message(_payload(), offset=4))
    assert not loader._batch
    assert len(loader._slot_buffer) == 1
    assert loader.inflight_resource_holds[0].batch_id == 1


def test_real_inflight_hold_delays_overlapping_batch_until_result_resolution(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    started, release = threading.Event(), threading.Event()
    writes: list[int] = []

    def write(records):
        writes.append(records[0].source_key == "p-001")
        if len(writes) == 1:
            started.set()
            release.wait(1)

    writer.write_batch.side_effect = write
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    loader._start_worker()
    assert loader._enqueue_worker_batch()
    assert started.wait(1)
    assert loader._buffer_message(_message(_payload(), offset=4))
    assert len(loader._slot_buffer) == 1
    assert len(writes) == 1
    release.set()
    result = loader._worker_results.get(timeout=1)
    assert loader._resolve_worker_result(result)
    assert loader._enqueue_worker_batch()
    result = loader._worker_results.get(timeout=1)
    assert loader._resolve_worker_result(result)
    assert len(writes) == 2
    assert loader._stop_worker()
    assert loader._worker is None


def test_lease_expiry_without_a_control_message_fails_closed_after_worker_write(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    started, release = threading.Event(), threading.Event()
    writer.write_batch.side_effect = lambda _records: (started.set(), release.wait(1))
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._wall_clock_ms = lambda: 100 if started.is_set() else 11
    result: list[int] = []
    runner = threading.Thread(target=lambda: result.append(loader.run(max_messages=1)))
    runner.start()
    assert started.wait(1)
    release.set()
    runner.join(2)
    assert result == [1]
    consumer.commit.assert_not_called()


def test_worker_factory_failure_and_frozen_shutdown_clock_fail_without_live_thread(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT")])
    loader._thread_factory = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("cannot start"))
    assert loader.run(max_messages=0) == 1

    class FakeLiveWorker:
        def join(self, timeout):
            raise AssertionError("full queue must prevent a join attempt")

        def is_alive(self):
            return True

    loader._worker = FakeLiveWorker()
    loader._worker_queue = Queue(maxsize=1)
    loader._worker_queue.put_nowait(object())
    loader._worker_deadline_clock = lambda: 0.0
    assert loader._stop_worker() is False


def test_sentinel_full_join_timeout_polls_and_real_worker_is_cleaned_up(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    started, release = threading.Event(), threading.Event()
    writer.write_batch.side_effect = lambda _records: (started.set(), release.wait(1))
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    loader._worker_queue = Queue(maxsize=1)
    loader._worker_join_timeout_seconds = 0.02
    loader._worker_wait_seconds = 0.001
    loader._worker_deadline_clock = time.monotonic
    loader._poll_coordination()
    assert loader._buffer_message(_message(_payload(), offset=3))
    loader._start_worker()
    assert loader._enqueue_worker_batch()
    assert started.wait(1)
    # Same record shape means B waits in the slot buffer while A is held; make
    # B disjoint so it occupies the bounded work queue behind blocked A.
    first = endpoint_buckets(EdgeRecord("p-001", "c-001", {}), 64)
    source, target = next(
        (f"p-{index}", f"c-{index}") for index in range(2, 1_000)
        if {
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).source,
            endpoint_buckets(EdgeRecord(f"p-{index}", f"c-{index}", {}), 64).target,
        }.isdisjoint({first.source, first.target})
    )
    payload = ('{"source":{"personId":"%s"},"target":{"companyId":"%s"},'
               '"properties":{"since":"2020-01-02"}}' % (source, target)).encode()
    assert loader._buffer_message(_message(payload, offset=4))
    assert loader._enqueue_worker_batch()
    assert loader._stop_worker() is False
    assert consumer.poll.call_count > 0
    assert "stage=worker" in str(loader._worker_failure("shutdown join timeout"))
    release.set()
    first_result = loader._worker_results.get(timeout=1)
    cancelled_result = loader._worker_results.get(timeout=1)
    assert {first_result.outcome, cancelled_result.outcome} == {"SUCCESS", "CANCELLED_PRESTART"}
    loader._worker_queue.put_nowait(loader._worker_sentinel)
    loader._worker.join(1)
    assert not loader._worker.is_alive()
    loader._worker = None


def test_worker_queue_factory_and_timeout_validation_are_explicit(works_at) -> None:
    consumer = MagicMock()
    queues: list[Queue] = []

    def queue_factory(*, maxsize):
        queue = Queue(maxsize=maxsize)
        queues.append(queue)
        return queue

    EdgeLoader(consumer, MagicMock(), works_at, queue_factory=queue_factory)
    assert len(queues) == 2
    for invalid in (0, -1, True, "one"):
        with pytest.raises(ValueError, match="worker timeouts"):
            EdgeLoader(consumer, MagicMock(), works_at, worker_join_timeout_seconds=invalid)


def test_edge_loader_writes_before_committing_next_offset(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=9)
    call_order: list[str] = []
    writer.write_batch.side_effect = lambda _records: call_order.append("write")
    consumer.commit.side_effect = lambda **kwargs: (
        call_order.append("commit"), kwargs["offsets"]
    )[1]
    loader = EdgeLoader(consumer, writer, works_at)

    assert loader.run(max_messages=1) == 0
    consumer.subscribe.assert_called_once()
    writer.write_batch.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.topic, committed.partition, committed.offset) == ("works-at-events", 0, 10)
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    assert call_order == ["write", "commit"]


# ── Metrics instrumentation (P6-S2 Story 5) ──────────────────────────────────


def test_classic_flush_batch_increments_records_consumed_and_write_duration_once(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=9)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = EdgeLoader(consumer, writer, works_at, metrics=metrics)

    assert loader.run(max_messages=1) == 0

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_count",
        {"kind": "edge", "name": "WORKS_AT", "stage": "edge-write"},
    ) == 1.0


def test_classic_flush_write_failure_does_not_increment_records_consumed(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=9)
    writer.write_batch.side_effect = RuntimeError("boom")
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = EdgeLoader(consumer, writer, works_at, metrics=metrics)

    assert loader.run(max_messages=1) == 1

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
    ) is None


def test_classic_flush_metrics_failure_never_blocks_a_successful_write(works_at, monkeypatch) -> None:
    """P6-S2 review finding S4-1's pattern, applied to the edge worker
    loop: a metrics-library exception must never be mistaken for a write
    failure this far after the write already succeeded."""
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=9)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    def boom(**_kwargs):
        raise ValueError("simulated metrics failure")

    monkeypatch.setattr(metrics.records_consumed, "labels", boom)
    loader = EdgeLoader(consumer, writer, works_at, metrics=metrics)

    assert loader.run(max_messages=1) == 0
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.topic, committed.partition, committed.offset) == ("works-at-events", 0, 10)


def test_worker_loop_batch_increments_records_consumed_and_write_duration_once(works_at) -> None:
    """The slot-gated streaming path (stories.md's own '_worker_loop, line
    464' reference) - distinct from the classic/bulk _flush_batch path
    covered above. Both converge on the same instrumentation shape, but
    each needs its own test since they are two different code paths
    (P6-S2 review finding T1)."""
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = _gated_loader(
        consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None], metrics=metrics
    )

    assert loader.run(max_messages=1) == 0

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_count",
        {"kind": "edge", "name": "WORKS_AT", "stage": "edge-write"},
    ) == 1.0


def test_worker_loop_write_failure_does_not_increment_records_consumed(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    writer.write_batch.side_effect = RuntimeError("database down")
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = _gated_loader(
        consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None], metrics=metrics
    )

    assert loader.run(max_messages=1) == 1

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
    ) is None


def test_worker_loop_metrics_failure_never_blocks_a_successful_write(works_at, monkeypatch) -> None:
    """P6-S2 review finding S4-1's pattern, applied to the slot-gated
    _worker_loop path specifically (T1): a metrics-library exception must
    never be mistaken for a write failure this far after the write already
    succeeded."""
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    def boom(**_kwargs):
        raise ValueError("simulated metrics failure")

    monkeypatch.setattr(metrics.records_consumed, "labels", boom)
    loader = _gated_loader(
        consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None], metrics=metrics
    )

    assert loader.run(max_messages=1) == 0


def test_edge_loader_rejects_malformed_before_resolving_offset(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 0
    sink.route.assert_called_once()
    assert sink.route.call_args.kwargs["edge_type"] == "WORKS_AT"
    assert sink.route.call_args.kwargs["offset"] == 17
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)
    writer.write_batch.assert_not_called()


def test_legacy_file_sink_rejection_does_not_claim_dlq_routing(works_at, caplog) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)
    with caplog.at_level("WARNING"):
        loader.run(max_messages=1)
    text = " ".join(record.getMessage() for record in caplog.records)
    assert "phase5_action=reject_to_file" in text and "route_to_dlq" not in text


# ── DLQ routing (Story 5) ─────────────────────────────────────────────────────


def test_edge_dead_letter_sink_receives_exact_raw_bytes_and_commits_only_after_publish(works_at) -> None:
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1_700_000_000_000,
    )
    consumer, writer = MagicMock(), MagicMock()
    message = _message(b"{bad", partition=2, offset=17)
    message.key.return_value = b"p-17"
    consumer.poll.return_value = message
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 0

    dlq_producer.publish.assert_called_once()
    (published_topic, envelope), _ = dlq_producer.publish.call_args
    assert published_topic == "works-at-events.dlq"
    assert envelope.key_bytes == b"p-17"
    assert envelope.value_bytes == b"{bad"
    assert envelope.entity_kind == "edge"
    assert envelope.entity_name == "WORKS_AT"
    assert envelope.failure_class == "malformed"
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)


def test_edge_dlq_routed_record_increments_dlq_counter_not_records_consumed(works_at) -> None:
    """P6-S2 Story 5's only DLQ deliverable: confirm the edge path also
    increments dlq_routed_total{kind="edge",...} (Story 4 already wired
    the on_dlq_routed listener itself; both loaders share
    build_malformed_record_sink/DeadLetterProducer). Uses a real
    DeadLetterProducer (matching tests/test_dead_letter.py's own
    _delivering_producer pattern), not a mocked one, so on_dlq_routed
    fires exactly the way it does in production - after a real ack."""
    from src.loader.dead_letter import DeadLetterProducer, DeadLetterRecordSink
    from src.utils import metrics as metrics_module

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    try:
        metrics_module.install_dlq_routed_listener(metrics)

        kafka_producer = MagicMock()
        delivery_state = {"on_delivery": None}

        def produce(topic, key, value, on_delivery):
            delivery_state["on_delivery"] = on_delivery

        def flush():
            delivery_state["on_delivery"](None, MagicMock())
            return 0

        kafka_producer.produce.side_effect = produce
        kafka_producer.flush.side_effect = flush
        dlq_producer = DeadLetterProducer(kafka_producer)
        sink = DeadLetterRecordSink(
            producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
            replica_id="0", kind="edge", clock=lambda: 1_700_000_000_000,
        )
        consumer, writer = MagicMock(), MagicMock()
        message = _message(b"{bad", partition=2, offset=17)
        message.key.return_value = b"p-17"
        consumer.poll.return_value = message
        loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink, metrics=metrics)

        assert loader.run(max_messages=1) == 0

        assert metrics.registry.get_sample_value(
            "graph_loader_dlq_routed_total",
            {"kind": "edge", "name": "WORKS_AT", "failure_class": "malformed"},
        ) == 1.0
        assert metrics.registry.get_sample_value(
            "graph_loader_records_consumed_total",
            {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
        ) is None
    finally:
        metrics_module._reset_dlq_routed_listener_for_tests()


def test_edge_dead_letter_publish_failure_leaves_offset_uncommitted(works_at) -> None:
    from src.loader.dead_letter import DeadLetterPublishError, DeadLetterRecordSink

    dlq_producer = MagicMock()
    dlq_producer.publish.side_effect = DeadLetterPublishError("broker unreachable")
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1,
    )
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", offset=4)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 1

    consumer.commit.assert_not_called()
    assert loader._partition_ledgers[("works-at-events", 0)].resolved_offsets == set()


def test_edge_mixed_batch_dlq_offset_resolves_independent_of_batch_flush_timing(works_at) -> None:
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1,
    )
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.side_effect = [
        _message(_payload(), offset=3),
        _message(b"{bad", offset=4),
        _message(_payload(), offset=5),
    ]
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=sink,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )

    assert loader.run(max_messages=3) == 0

    dlq_producer.publish.assert_called_once()
    writer.write_batch.assert_called_once()
    final_offset = consumer.commit.call_args_list[-1].kwargs["offsets"][0].offset
    assert final_offset == 6


def test_edge_dead_letter_disabled_is_byte_identical_to_file_fallback(works_at, tmp_path) -> None:
    path = tmp_path / "edge-rejections.jsonl"
    sink = RejectionSink(path)
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 0

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert lines[0] == (
        '{"edge_type":"WORKS_AT","offset":17,"partition":2,'
        '"reason":"Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",'
        '"topic":"works-at-events"}'
    )


def test_edge_dead_letter_redelivery_produces_a_second_envelope_at_least_once(works_at) -> None:
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    clock_values = iter([1_700_000_000_000, 1_700_000_000_999])
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: next(clock_values),
    )
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    for _ in range(2):
        message = _message(b"{bad", offset=4)
        assert loader._buffer_message(message) is True

    assert dlq_producer.publish.call_count == 2
    first_envelope = dlq_producer.publish.call_args_list[0][0][1]
    second_envelope = dlq_producer.publish.call_args_list[1][0][1]
    assert first_envelope.dlq_routed_at_ms != second_envelope.dlq_routed_at_ms


def test_edge_malformed_record_under_slot_gating_never_touches_admission_buffer(works_at) -> None:
    """A malformed record must be DLQ'd at buffer time, before slot admission.

    It must never consume lease buffer capacity or block on a lease grant
    it will never need.
    """
    sink = MagicMock()
    consumer, writer = MagicMock(), MagicMock()
    slot_buffer = SlotAwareAdmissionBuffer(max_records=2)
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot", rejection_sink=sink,
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=slot_buffer,
        coordination_poll=lambda _timeout: None,
        wall_clock_ms=lambda: 0,
    )

    assert loader._buffer_message(_message(b"{bad", offset=3)) is True

    sink.route.assert_called_once()
    assert len(slot_buffer) == 0


def test_edge_malformed_record_under_slot_gating_commits_only_after_route_succeeds(works_at) -> None:
    sink = MagicMock()
    consumer, writer = MagicMock(), MagicMock()
    slot_buffer = SlotAwareAdmissionBuffer(max_records=2)
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot", rejection_sink=sink,
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=slot_buffer,
        coordination_poll=lambda _timeout: None,
        wall_clock_ms=lambda: 0,
    )

    assert loader._buffer_message(_message(b"{bad", offset=3)) is True

    sink.route.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (0, 4)
    assert len(slot_buffer) == 0


def test_edge_malformed_record_under_slot_gating_route_failure_leaves_offset_uncommitted(works_at) -> None:
    sink = MagicMock()
    sink.route.side_effect = OSError("disk full")
    consumer, writer = MagicMock(), MagicMock()
    slot_buffer = SlotAwareAdmissionBuffer(max_records=2)
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot", rejection_sink=sink,
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=slot_buffer,
        coordination_poll=lambda _timeout: None,
        wall_clock_ms=lambda: 0,
    )

    assert loader._buffer_message(_message(b"{bad", offset=3)) is False

    consumer.commit.assert_not_called()
    assert len(slot_buffer) == 0


def test_edge_slot_gating_value_error_from_admission_machinery_propagates_not_dlqd(works_at, monkeypatch) -> None:
    """A ValueError from the admission/lease machinery (not the record) must

    propagate and fail the loader -- never be silently misclassified as a
    malformed record, DLQ'd, and committed as if the data itself were bad.
    """
    import src.loader.edge_loader as edge_loader_module

    sink = MagicMock()
    consumer, writer = MagicMock(), MagicMock()
    slot_buffer = SlotAwareAdmissionBuffer(max_records=2)
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot", rejection_sink=sink,
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=slot_buffer,
        coordination_poll=lambda _timeout: None,
        wall_clock_ms=lambda: 0,
    )
    monkeypatch.setattr(
        edge_loader_module, "endpoint_buckets",
        MagicMock(side_effect=ValueError("bucket_count must be an integer from 1 through 4096")),
    )

    with pytest.raises(ValueError):
        loader._buffer_message(_message(_payload(), offset=3))

    sink.route.assert_not_called()
    assert len(slot_buffer) == 0
    consumer.commit.assert_not_called()


def test_edge_revoke_settles_buffered_valid_record_and_already_dlqd_malformed_record(works_at) -> None:
    """Classic mode: a revoke settles a buffered valid record alongside an
    already-DLQ'd malformed one, writing and committing through both.
    """
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1,
    )
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=sink,
        loading_config=LoadingConfig(unwind_batch_size=2),
    )
    loader._running = True  # as inside run()
    p0 = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3)) is True
    assert loader._buffer_message(_message(b"{bad", offset=4)) is True
    dlq_producer.publish.assert_called_once()

    loader._on_revoke(consumer, [p0])

    writer.write_batch.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (0, 5)
    assert loader._rebalance_failure is False
    consumer.unassign.assert_called_once()


def test_edge_revoke_during_slot_gating_fails_closed_and_redelivers(works_at) -> None:
    """Slot-gated mode: _settle_before_revoke always returns early (the
    worker/lease buffer cannot be settled synchronously inside a Kafka
    callback), so a revoke with an already-DLQ'd-but-uncommitted malformed
    record and a slot-buffered valid record fails closed -- nothing
    commits, and both records replay (the malformed one DLQs a second
    time: documented at-least-once, matching P5-Q5's precedent).
    """
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1,
    )
    consumer, writer = MagicMock(), MagicMock()
    slot_buffer = SlotAwareAdmissionBuffer(max_records=2)
    loader = EdgeLoader(
        consumer, writer, works_at, run_id="run-slot", rejection_sink=sink,
        slot_admission=SlotAdmission(edge_type="WORKS_AT", run_id="run-slot", bucket_count=64),
        slot_buffer=slot_buffer,
        coordination_poll=lambda _timeout: None,
        wall_clock_ms=lambda: 0,
    )
    loader._running = True
    p0 = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [p0])
    assert loader._buffer_message(_message(_payload(), offset=3)) is True
    assert loader._buffer_message(_message(b"{bad", offset=4)) is True
    dlq_producer.publish.assert_called_once()

    loader._on_revoke(consumer, [p0])

    consumer.commit.assert_not_called()
    assert loader._rebalance_failure is True


def test_edge_bucket_lease_coordinator_mode_dlqs_malformed_and_commits_both(works_at) -> None:
    """Bucket-lease execution (--edge-leases, coordinator.routes_records=True):

    a malformed record is DLQ'd at buffer time (never reaching the
    coordinator), the coordinator's execute_pending receives only the
    valid record, and the final commit covers both offsets.
    """
    from src.loader.dead_letter import DeadLetterRecordSink

    dlq_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=dlq_producer, topic="works-at-events.dlq", run_id="run-1",
        replica_id="0", kind="edge", clock=lambda: 1,
    )
    consumer, writer = MagicMock(), MagicMock()
    coordinator = MagicMock()
    coordinator.routes_records = True
    coordinator.execute_pending.return_value = [SimpleNamespace(success=True, exception=None)]
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=sink, coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=1),
    )
    consumer.poll.side_effect = [
        _message(b"{bad", offset=3),
        _message(_payload(), offset=4),
    ]

    assert loader.run(max_messages=2) == 0

    dlq_producer.publish.assert_called_once()
    coordinator.execute_pending.assert_called_once()
    (records_arg,), _ = coordinator.execute_pending.call_args
    assert [pending.offset for pending in records_arg] == [4]
    final_offset = consumer.commit.call_args_list[-1].kwargs["offsets"][0].offset
    assert final_offset == 5


def test_edge_rejection_sink_uses_edge_discriminator_and_fsync(tmp_path) -> None:
    path = tmp_path / "edge-rejections.jsonl"
    sink = RejectionSink(path)
    with patch("src.loader.dead_letter.os.fsync") as fsync:
        sink.append(
            topic="works-at-events", partition=1, offset=3,
            edge_type="WORKS_AT", reason="invalid envelope",
        )
    entry = __import__("json").loads(path.read_text())
    assert entry["edge_type"] == "WORKS_AT"
    assert "node_label" not in entry
    fsync.assert_called_once()


def test_rejection_sink_preserves_node_json_shape_after_edge_extension(tmp_path) -> None:
    path = tmp_path / "node-rejections.jsonl"
    RejectionSink(path).append(
        topic="person-events", partition=1, offset=3,
        node_label="Person", reason="invalid record",
    )
    entry = __import__("json").loads(path.read_text())
    assert entry == {
        "topic": "person-events", "partition": 1, "offset": 3,
        "node_label": "Person", "reason": "invalid record",
    }


@pytest.mark.parametrize("failure", [MissingRelationshipEndpointError("missing"), RuntimeError("neo4j failed")])
def test_missing_endpoint_or_write_failure_leaves_offset_uncommitted(works_at, failure) -> None:
    consumer, writer = MagicMock(), MagicMock()
    writer.write_batch.side_effect = failure
    consumer.poll.side_effect = [_message(_payload(), offset=3), _message(_payload(), offset=4)]
    loader = EdgeLoader(consumer, writer, works_at)

    assert loader.run(max_messages=2) == 1
    assert consumer.poll.call_count == 1
    consumer.commit.assert_not_called()


def test_edge_loader_commits_independent_partitions(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.side_effect = [
        _message(_payload(), partition=1, offset=7),
        _message(_payload(), partition=2, offset=11),
    ]
    loader = EdgeLoader(
        consumer, writer, works_at, loading_config=LoadingConfig(unwind_batch_size=2)
    )

    assert loader.run(max_messages=2) == 0
    consumer.commit.assert_called_once()
    committed = consumer.commit.call_args.kwargs["offsets"]
    assert [(item.partition, item.offset) for item in committed] == [(1, 8), (2, 12)]
    assert consumer.commit.call_args.kwargs["asynchronous"] is False


def test_bulk_edge_flush_keeps_kafka_fetching(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])
    assert loader._buffer_message(_message(_payload(), offset=3))

    assert loader._flush_batch()
    writer.write_batch.assert_called_once()
    consumer.commit.assert_called_once()
    consumer.pause.assert_not_called()
    consumer.resume.assert_not_called()


def test_bulk_edge_batch_log_reports_write_duration(works_at) -> None:
    consumer, writer, event_logger = MagicMock(), MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, event_logger=event_logger)
    assert loader._buffer_message(_message(_payload(), offset=3))

    with patch("src.loader.edge_loader.time.perf_counter", side_effect=[2.0, 2.027]):
        assert loader._flush_batch()

    message, *values = event_logger.info.call_args.args
    rendered = message % tuple(values)
    assert "stage=edge-write edge=WORKS_AT" in rendered
    assert "records=1 write_ms=27.000" in rendered


def test_partial_edge_offset_commit_keeps_all_ledgers_unresolved(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 1), TopicPartition("works-at-events", 2)])
    assert loader._buffer_message(_message(_payload(), partition=1, offset=7))
    assert loader._buffer_message(_message(_payload(), partition=2, offset=11))
    consumer.commit.return_value = [
        SimpleNamespace(topic="works-at-events", partition=1, offset=8, error=None),
        SimpleNamespace(topic="works-at-events", partition=2, offset=12, error=RuntimeError("rejected")),
    ]

    assert loader._flush_batch() is False
    consumer.commit.assert_called_once()
    assert loader._partition_ledgers[("works-at-events", 1)].next_offset == 7
    assert loader._partition_ledgers[("works-at-events", 2)].next_offset == 11


def test_missing_synchronous_commit_result_keeps_offset_unresolved(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.commit.return_value = None
    loader = EdgeLoader(consumer, writer, works_at)
    assert loader._buffer_message(_message(_payload(), offset=3))

    assert loader._flush_batch() is False
    assert loader._partition_ledgers[("works-at-events", 0)].next_offset == 3


def test_bulk_drain_batches_prefetched_records(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = None
    loader = EdgeLoader(
        consumer, writer, works_at,
        loading_config=LoadingConfig(unwind_batch_size=500),
    )
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])
    loader._deferred_messages.extend(
        _message(_payload(), offset=offset) for offset in range(1001)
    )

    assert loader._finish_run()
    assert [len(call.args[0]) for call in writer.write_batch.call_args_list] == [500, 500, 1]
    assert consumer.commit.call_count == 3
    consumer.pause.assert_called_once()
    consumer.resume.assert_not_called()


def test_edge_outer_batch_override_does_not_change_node_batch_size(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = None
    loading = LoadingConfig(unwind_batch_size=500, edge_unwind_batch_size=2000)
    loader = EdgeLoader(consumer, writer, works_at, loading_config=loading)
    loader._deferred_messages.extend(
        _message(_payload(), offset=offset) for offset in range(2001)
    )

    assert loading.unwind_batch_size == 500
    assert loader._finish_run()
    assert [len(call.args[0]) for call in writer.write_batch.call_args_list] == [2000, 1]


def test_later_malformed_offset_waits_behind_earlier_valid_gap(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)
    assert loader._buffer_message(_message(_payload(), offset=3))
    assert loader._buffer_message(_message(b"{bad", offset=4))
    consumer.commit.assert_not_called()

    assert loader._flush_batch() is True
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (0, 5)


def test_post_write_revoke_prevents_edge_offset_commit(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at)
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])
    assert loader._buffer_message(_message(_payload(), offset=3))

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll
    assert loader._flush_batch() is False
    consumer.commit.assert_not_called()
    consumer.unassign.assert_called_once()


def test_malformed_resolution_revoke_prevents_edge_offset_commit(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)
    assigned = TopicPartition("works-at-events", 0)
    loader._on_assign(consumer, [assigned])

    def revoke_on_poll(_timeout):
        loader._on_revoke(consumer, [assigned])
        return None

    consumer.poll.side_effect = revoke_on_poll
    assert loader._buffer_message(_message(b"{bad", offset=3)) is False
    sink.route.assert_called_once()
    consumer.commit.assert_not_called()


def test_edge_loader_idle_flushes_partial_batch(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    shutdown_requested = Event()
    message = _message(_payload(), offset=7)
    calls: list[str] = []

    def poll(_timeout):
        if consumer.poll.call_count == 1:
            return message
        shutdown_requested.set()
        return None

    consumer.poll.side_effect = poll
    writer.write_batch.side_effect = lambda _records: calls.append("write")
    clock_values = iter([0.0, 1.1])
    loader = EdgeLoader(
        consumer, writer, works_at, shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=5, flush_interval_ms=1000),
        clock=lambda: next(clock_values),
    )
    assert loader.run() == 0
    writer.write_batch.assert_called_once()
    assert calls == ["write"]
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 8


def test_self_referencing_edge_uses_same_loader_path(knows) -> None:
    consumer, writer = MagicMock(), MagicMock()
    payload = (
        b'{"source":{"personId":"p-001"},"target":{"personId":"p-002"},'
        b'"properties":{"since":"2020-01-02"}}'
    )
    consumer.poll.return_value = _message(payload, topic="knows-events", offset=6)
    assert EdgeLoader(consumer, writer, knows).run(max_messages=1) == 0
    record = writer.write_batch.call_args.args[0][0]
    assert (record.source_key, record.target_key) == ("p-001", "p-002")


def test_edge_loader_fails_when_rejection_sink_is_not_durable(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    sink.route.side_effect = OSError("disk full")
    consumer.poll.return_value = _message(b"{bad", offset=4)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 1
    consumer.commit.assert_not_called()


def test_edge_loader_main_uses_manual_commit_and_closes_resources(schema) -> None:
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer) as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "1", "--replica-id", "2"]) == 0
    config = consumer_cls.call_args.args[0]
    assert config["enable.auto.commit"] is False
    assert config["group.id"] == f"{schema.loading.consumer_group_id}-WORKS_AT"
    assert loader.run.call_args.kwargs == {"max_messages": 1}
    assert loader_cls.call_args.kwargs["run_id"] == "default_run"
    consumer.close.assert_called_once()
    driver.close.assert_called_once()


def test_edge_loader_sigterm_mid_run_leaves_snapshot_with_accumulated_counters(schema, tmp_path, monkeypatch) -> None:
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM arriving mid-run,
    after at least one counted write, still leaves a metrics-*.prom
    snapshot with that write's counters. Drives the real main()/EdgeLoader
    (classic, non-slot-gated _flush_batch path) - not mocked out."""
    import signal

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    # P6-S2 review finding S10-2: pin to a free ephemeral port instead of
    # binding the real 0.0.0.0:9464 during the unit suite.
    import socket as _socket_module
    with _socket_module.socket(_socket_module.AF_INET, _socket_module.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(_s.getsockname()[1]))
    snapshot_path = tmp_path / "metrics-edge-WORKS_AT-0.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    consumer, driver = MagicMock(), MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value
    result = MagicMock()
    result.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1}])
    transaction.run.return_value = result
    handlers: dict[str, object] = {}
    real_signal = signal.signal

    def capturing_signal(sig, handler):
        previous = real_signal(sig, handler)
        if sig == signal.SIGTERM:
            handlers["current"] = handler
        return previous

    poll_state = {"n": 0}

    def poll_side_effect(timeout):
        poll_state["n"] += 1
        if poll_state["n"] == 1:
            return _message(_payload(), offset=3)
        handlers["current"](signal.SIGTERM, None)
        return None

    consumer.poll.side_effect = poll_side_effect

    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.signal.signal", side_effect=capturing_signal):
        assert edge_loader_main(["--edge-type", "WORKS_AT"]) == 0

    assert snapshot_path.exists()
    from prometheus_client.parser import text_string_to_metric_families

    families = {family.name: family for family in text_string_to_metric_families(snapshot_path.read_text())}
    samples = families["graph_loader_records_consumed"].samples
    assert any(sample.value > 0 for sample in samples)


def test_edge_loader_main_passes_mode_and_dlq_sink_as_missing_endpoint_sink(schema) -> None:
    """P5-S3 Story 5: main() must actually pass mode=args.mode and reuse
    the real DeadLetterRecordSink as build_edge_execution's
    missing_endpoint_sink -- without this, streaming row-level DLQ stays
    silently off no matter what Stories 3-4 built (reviewer's Story 4
    nice-to-have #1)."""
    from src.loader.dead_letter import DeadLetterRecordSink
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    edge_index = next(i for i, edge in enumerate(dlq_schema.edges) if edge.type == "WORKS_AT")
    dlq_schema.edges[edge_index] = dlq_schema.edges[edge_index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="dlq-works-at")}
    )
    real_sink = MagicMock(spec=DeadLetterRecordSink)

    consumer, driver, loader, dlq_producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    fake_index = MagicMock()
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader), \
         patch("src.loader.edge_loader.build_malformed_record_sink", return_value=(real_sink, dlq_producer)), \
         patch("src.loader.edge_loader.build_edge_execution") as build_exec, \
         patch("src.loader.edge_loader.ParkedPairIndex", return_value=fake_index) as index_cls:
        build_exec.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "stream", "--max-messages", "1",
        ]) == 0

    assert build_exec.call_args.kwargs["mode"] == "stream"
    assert build_exec.call_args.kwargs["missing_endpoint_sink"] is real_sink
    # Review finding M16: a live stream-mode replica of a DLQ-enabled,
    # non-order_by edge type must construct its own writable
    # ParkedPairIndex, start() it before build_edge_execution is even
    # called, pass that exact instance through, and stop() it on shutdown
    # -- without this, IndexedMissingEndpointSink is never active and the
    # retry consumer's own index sees nothing to retry or block on.
    index_cls.assert_called_once()
    assert index_cls.call_args.kwargs["topic"] == "dlq-works-at.parked-pairs"
    assert index_cls.call_args.kwargs["writable"] is True
    fake_index.start.assert_called_once()
    assert build_exec.call_args.kwargs["parked_pair_index"] is fake_index
    fake_index.stop.assert_called_once()


def test_edge_loader_main_skips_parked_pair_index_for_order_by_edge_type(schema) -> None:
    """M16 corollary: an order_by-configured edge type must never get a
    ParkedPairIndex at all (Decisions §10 -- it uses a conditional SET
    instead), even though it is otherwise DLQ-enabled and streaming."""
    from src.loader.dead_letter import DeadLetterRecordSink
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    edge_index = next(i for i, edge in enumerate(dlq_schema.edges) if edge.type == "WORKS_AT")
    dlq_schema.edges[edge_index] = dlq_schema.edges[edge_index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="dlq-works-at"), "order_by": "since"}
    )
    real_sink = MagicMock(spec=DeadLetterRecordSink)

    consumer, driver, loader, dlq_producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader), \
         patch("src.loader.edge_loader.build_malformed_record_sink", return_value=(real_sink, dlq_producer)), \
         patch("src.loader.edge_loader.build_edge_execution") as build_exec, \
         patch("src.loader.edge_loader.ParkedPairIndex") as index_cls:
        build_exec.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "stream", "--max-messages", "1",
        ]) == 0

    index_cls.assert_not_called()
    assert build_exec.call_args.kwargs["parked_pair_index"] is None


def test_edge_loader_main_skips_parked_pair_index_in_bulk_mode(schema) -> None:
    """M16 corollary: bulk mode never constructs a ParkedPairIndex, even

    for an otherwise DLQ-enabled, non-order_by edge type -- bulk has no
    live retry consumer to protect against (Decisions §1)."""
    from src.loader.dead_letter import DeadLetterRecordSink
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    edge_index = next(i for i, edge in enumerate(dlq_schema.edges) if edge.type == "WORKS_AT")
    dlq_schema.edges[edge_index] = dlq_schema.edges[edge_index].model_copy(
        update={"dead_letter": DeadLetterConfig(
            enabled=True, topic="dlq-works-at", route_missing_endpoints_in_bulk=True,
        )}
    )
    real_sink = MagicMock(spec=DeadLetterRecordSink)

    consumer, driver, loader, dlq_producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader), \
         patch("src.loader.edge_loader.build_malformed_record_sink", return_value=(real_sink, dlq_producer)), \
         patch("src.loader.edge_loader.build_edge_execution") as build_exec, \
         patch("src.loader.edge_loader.ParkedPairIndex") as index_cls:
        build_exec.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "bulk", "--max-messages", "1",
        ]) == 0

    index_cls.assert_not_called()
    assert build_exec.call_args.kwargs["parked_pair_index"] is None


def test_edge_loader_main_rejection_sink_fallback_passes_no_missing_endpoint_sink(schema) -> None:
    """dead_letter absent (RejectionSink fallback): missing_endpoint_sink
    must stay None -- never the file-sink object, which doesn't satisfy
    MissingEndpointSink at all."""
    consumer, driver, loader = MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader), \
         patch("src.loader.edge_loader.build_edge_execution") as build_exec:
        build_exec.return_value = (MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "1"]) == 0

    assert build_exec.call_args.kwargs["mode"] == "stream"  # --mode default
    assert build_exec.call_args.kwargs["missing_endpoint_sink"] is None


def test_edge_loader_main_wires_dead_letter_sink_and_flushes_its_producer(schema) -> None:
    """main() routes to build_malformed_record_sink and flushes its producer.

    Uses a schema copy with WORKS_AT's dead_letter enabled, so the factory
    picks the DeadLetterRecordSink branch instead of the RejectionSink
    fallback -- proving edge_config.dead_letter, run_id, entity_kind, and
    max_poll_interval_ms all actually reach main().
    """
    from src.models.schema import DeadLetterConfig

    dlq_schema = schema.model_copy(deep=True)
    edge_index = next(i for i, edge in enumerate(dlq_schema.edges) if edge.type == "WORKS_AT")
    dlq_schema.edges[edge_index] = dlq_schema.edges[edge_index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="dlq-works-at")}
    )

    consumer, driver, loader, dlq_producer = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=dlq_schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls, \
         patch(
             "src.loader.edge_loader.build_malformed_record_sink",
             return_value=(MagicMock(), dlq_producer),
         ) as build_sink:
        assert edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "1"]) == 0

    build_sink_kwargs = build_sink.call_args.kwargs
    assert build_sink_kwargs["dead_letter"] is dlq_schema.edges[edge_index].dead_letter
    assert build_sink_kwargs["entity_topic"] == "works-at-events"
    assert build_sink_kwargs["entity_kind"] == "edge"
    assert build_sink_kwargs["run_id"] == "default_run"
    assert build_sink_kwargs["max_poll_interval_ms"] == dlq_schema.loading.max_poll_interval_ms
    assert loader_cls.call_args.kwargs["rejection_sink"] is build_sink.return_value[0]
    dlq_producer.flush.assert_called_once()


def test_edge_loader_main_slot_gating_uses_distinct_coordination_consumer(schema) -> None:
    work_consumer, clock_consumer, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", side_effect=[work_consumer, clock_consumer]) as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT",
            "--run-id", "run-42", "--replica-id", "2", "--max-messages", "1",
        ]) == 0
    work_config, clock_config = (call.args[0] for call in consumer_cls.call_args_list)
    assert work_config["enable.auto.commit"] is False
    assert clock_config["enable.auto.commit"] is False
    assert clock_config["auto.offset.reset"] == "earliest"
    assert work_config["group.id"] != clock_config["group.id"]
    assert clock_config["group.id"].endswith("WORKS_AT-run-42-2")
    clock_consumer.subscribe.assert_called_once_with(["graph.loader.coordination"])
    assert loader_cls.call_args.kwargs["slot_admission"].bucket_count == schema.loading.coordination.bucket_count
    assert loader_cls.call_args.kwargs["coordination_poll"] == clock_consumer.poll
    work_consumer.close.assert_called_once()
    clock_consumer.close.assert_called_once()
    driver.close.assert_called_once()


def test_edge_loader_main_rejects_self_reference_slot_fleet_before_kafka(schema) -> None:
    with patch("src.loader.edge_loader.Consumer") as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver") as driver_cls, \
         patch("src.loader.edge_loader.load_schema", return_value=schema):
        assert edge_loader_main([
            "--edge-type", "KNOWS", "--slot-gating", "--coordination-topic", "graph.loader.coordination",
            "--fleet-edge-types", "KNOWS", "--run-id", "run-isolation",
        ]) == 1
    consumer_cls.assert_not_called()
    driver_cls.assert_not_called()


@pytest.mark.parametrize("argv", [
    ["--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "clock-topic"],
    ["--edge-type", "WORKS_AT", "--slot-gating", "--run-id", "run-1"],
    ["--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", " ", "--run-id", "run-1"],
    ["--edge-type", "WORKS_AT", "--coordination-topic", "clock-topic"],
])
def test_edge_loader_main_rejects_incomplete_slot_contract_before_resources(argv) -> None:
    with patch("src.loader.edge_loader.Consumer") as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver") as driver_cls:
        with pytest.raises(SystemExit):
            edge_loader_main(argv)
    consumer_cls.assert_not_called()
    driver_cls.assert_not_called()


def test_edge_loader_main_rejects_equal_groups_before_resources(schema) -> None:
    with patch.dict("src.loader.edge_loader.os.environ", {
        "KAFKA_GROUP_ID": "same-group", "KAFKA_COORDINATION_GROUP_ID": "same-group",
    }, clear=False), \
         patch("src.loader.edge_loader.Consumer") as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver") as driver_cls, \
         patch("src.loader.edge_loader.load_schema", return_value=schema):
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1",
        ]) == 1
    consumer_cls.assert_not_called()
    driver_cls.assert_not_called()


def test_edge_loader_main_accepts_distinct_coordination_group_override(schema) -> None:
    work, clock, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch.dict("src.loader.edge_loader.os.environ", {
        "KAFKA_GROUP_ID": "work-override", "KAFKA_COORDINATION_GROUP_ID": "clock-override",
    }, clear=False), \
         patch("src.loader.edge_loader.Consumer", side_effect=[work, clock]) as consumer_cls, \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1",
        ]) == 0
    assert [call.args[0]["group.id"] for call in consumer_cls.call_args_list] == ["work-override", "clock-override"]


def test_edge_loader_main_coordination_construction_failure_closes_work_consumer(schema) -> None:
    work = MagicMock()
    with patch("src.loader.edge_loader.Consumer", side_effect=[work, RuntimeError("clock unavailable")]), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.get_neo4j_driver") as driver_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1",
        ]) == 1
    work.close.assert_called_once()
    driver_cls.assert_not_called()
    work.commit.assert_not_called()


def test_edge_loader_main_coordination_subscription_failure_closes_both_consumers(schema) -> None:
    work, clock = MagicMock(), MagicMock()
    clock.subscribe.side_effect = RuntimeError("cannot subscribe")
    with patch("src.loader.edge_loader.Consumer", side_effect=[work, clock]), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.get_neo4j_driver") as driver_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1",
        ]) == 1
    clock.close.assert_called_once()
    work.close.assert_called_once()
    driver_cls.assert_not_called()
    work.commit.assert_not_called()


def test_edge_loader_main_close_failures_are_attributed_and_finish_cleanup(schema, caplog) -> None:
    work, clock, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    clock.close.side_effect = RuntimeError("clock close")
    work.close.side_effect = RuntimeError("work close")
    driver.close.side_effect = RuntimeError("driver close")
    with patch("src.loader.edge_loader.Consumer", side_effect=[work, clock]), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1", "--replica-id", "7",
        ]) == 1
    assert "stage=coordination edge=WORKS_AT replica=7 run_id=run-1 reason=close failed" in caplog.text
    assert "stage=shutdown edge=WORKS_AT replica=7 run_id=run-1 component=work-consumer" in caplog.text
    assert "stage=shutdown edge=WORKS_AT replica=7 run_id=run-1 component=driver" in caplog.text
    clock.close.assert_called_once()
    work.close.assert_called_once()
    driver.close.assert_called_once()


def test_edge_loader_main_keeps_sigterm_handler_even_when_close_fails(schema) -> None:
    """P6-S5 Story 4, Decision 10 (confirmation-only): node_loader.py and
    planned_writer.py already have a getsignal-based test proving the
    previous SIGTERM handler is restored on main() exit, including when a
    close/flush step raises; edge_loader.py had none, even though the
    production code (main()'s finally at the bottom of the function)
    already does this correctly. Reuses the exact fixture shape of
    test_edge_loader_main_close_failures_are_attributed_and_finish_cleanup
    above - no production code change expected here."""
    import signal

    work, clock, driver, loader = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    work.close.side_effect = RuntimeError("work close")

    with patch("src.loader.edge_loader.Consumer", side_effect=[work, clock]), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader):
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--slot-gating", "--coordination-topic", "graph.loader.coordination", "--fleet-edge-types", "WORKS_AT", "--run-id", "run-1", "--replica-id", "7",
        ]) == 1

    work.close.assert_called_once()
    # F2: SIGTERM is ignored as the last shutdown step (CPython resets Python handlers
    # to SIG_DFL at interpreter exit; only SIG_IGN survives a repeated stop signal).
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN


def test_edge_loader_main_rejects_unknown_type_and_invalid_cap() -> None:
    assert edge_loader_main(["--edge-type", "UNKNOWN", "--max-messages", "1"]) == 1
    with pytest.raises(SystemExit, match="positive"):
        edge_loader_main(["--edge-type", "WORKS_AT", "--max-messages", "0"])


def _control_producer() -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0
    return producer


def _control_payload(producer: MagicMock, call_index: int = 0) -> dict:
    return __import__("json").loads(producer.produce.call_args_list[call_index].kwargs["value"])


def test_edge_bulk_assignment_ack_is_run_scoped_and_topic_qualified(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(
        consumer, writer, works_at, producer=producer, run_id="run-7", replica_id="3"
    )

    loader._on_assign(consumer, [TopicPartition("works-at-events", 2)])
    loader._publish_pending_assignments()

    payload = _control_payload(producer)
    assert payload == {
        "run_id": "run-7",
        "edge_type": "WORKS_AT",
        "replica_id": "3",
        "type": "ASSIGNMENT",
        "assignment_epoch": 1,
        "assigned_partitions": [{"topic": "works-at-events", "partition": 2}],
    }
    assert producer.produce.call_args.args[0] == "__graph_loader_control"
    assert producer.produce.call_args.kwargs["key"] == "run-7"


def test_edge_bulk_idle_surplus_and_revoke_advance_control_epoch(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    assignment = TopicPartition("works-at-events", 0)

    loader._on_assign(consumer, [assignment])
    loader._publish_pending_assignments()
    loader._on_revoke(consumer, [assignment])
    loader._publish_pending_assignments()

    assert _control_payload(producer, 0)["type"] == "ASSIGNMENT"
    revoke_payload = _control_payload(producer, 1)
    assert revoke_payload["type"] == "IDLE_SURPLUS"
    assert revoke_payload["assignment_epoch"] == 2
    consumer.unassign.assert_called_once()


def test_edge_partial_revoke_with_unassign_never_claims_remaining_partitions(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    first = TopicPartition("works-at-events", 0)
    second = TopicPartition("works-at-events", 1)

    loader._on_assign(consumer, [first, second])
    loader._publish_pending_assignments()
    loader._on_revoke(consumer, [first])
    loader._publish_pending_assignments()

    payload = _control_payload(producer, 1)
    assert payload["type"] == "IDLE_SURPLUS"
    assert "assigned_partitions" not in payload


def test_edge_assignment_control_delivery_failure_returns_nonzero(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    producer.flush.return_value = 1
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])

    assert loader.run(max_messages=1) == 1
    assert _control_payload(producer)["type"] == "ASSIGNMENT"
    assert producer.produce.call_count == 1


@pytest.mark.parametrize("failure_method", ["produce", "flush"])
def test_edge_control_producer_exception_returns_nonzero_without_drain(works_at, failure_method) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    setattr(producer, failure_method, MagicMock(side_effect=BufferError("broker unavailable")))
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])

    assert loader.run(max_messages=1) == 1
    if failure_method == "produce":
        producer.flush.assert_not_called()
    assert producer.produce.call_count == 1


def test_edge_sigterm_flushes_then_acknowledges_drain(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    shutdown_requested = Event()
    poll_count = 0

    def poll(_timeout):
        nonlocal poll_count
        poll_count += 1
        if poll_count == 1:
            return _message(_payload(), offset=7)
        shutdown_requested.set()
        return None

    consumer.poll.side_effect = poll
    loader = EdgeLoader(
        consumer,
        writer,
        works_at,
        producer=producer,
        shutdown_requested=shutdown_requested,
        loading_config=LoadingConfig(unwind_batch_size=5),
    )
    assert loader.run() == 0
    writer.write_batch.assert_called_once()
    assert consumer.commit.call_args.kwargs["offsets"][0].offset == 8
    assert _control_payload(producer)["type"] == "DRAIN_COMPLETE"


@pytest.mark.parametrize("failure", [RuntimeError("write failed"), RuntimeError("commit failed")])
def test_edge_bulk_failure_never_acknowledges_drain(works_at, failure) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    consumer.poll.return_value = _message(_payload(), offset=3)
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)
    if str(failure) == "write failed":
        writer.write_batch.side_effect = failure
    else:
        consumer.commit.side_effect = failure

    assert loader.run(max_messages=1) == 1
    assert all(_control_payload(producer, index)["type"] != "DRAIN_COMPLETE"
               for index in range(producer.produce.call_count))


def test_edge_final_drain_delivery_failure_returns_nonzero(works_at) -> None:
    consumer, writer, producer = MagicMock(), MagicMock(), _control_producer()
    producer.flush.return_value = 1
    loader = EdgeLoader(consumer, writer, works_at, producer=producer)

    assert loader.run(max_messages=0) == 1
    assert _control_payload(producer)["type"] == "DRAIN_COMPLETE"


def test_edge_stream_mode_has_no_control_producer_side_effects(works_at) -> None:
    consumer, writer = MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload())

    assert EdgeLoader(consumer, writer, works_at).run(max_messages=1) == 0


def test_edge_loader_main_bulk_passes_and_flushes_control_producer(schema) -> None:
    consumer, producer, driver, loader = MagicMock(), _control_producer(), MagicMock(), MagicMock()
    loader.run.return_value = 0
    with patch("src.loader.edge_loader.Consumer", return_value=consumer), \
         patch("src.loader.edge_loader.Producer", return_value=producer), \
         patch("src.loader.edge_loader.get_neo4j_driver", return_value=driver), \
         patch("src.loader.edge_loader.load_schema", return_value=schema), \
         patch("src.loader.edge_loader.EdgeLoader", return_value=loader) as loader_cls:
        assert edge_loader_main([
            "--edge-type", "WORKS_AT", "--mode", "bulk", "--run-id", "run-9", "--replica-id", "2"
        ]) == 0
    assert loader_cls.call_args.kwargs["producer"] is producer
    assert loader_cls.call_args.kwargs["run_id"] == "run-9"
    assert loader_cls.call_args.kwargs["replica_id"] == "2"
    producer.flush.assert_called_once()


def test_edge_loader_coordinator_success_commits_after_execution(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    routed = MagicMock(); routed.lane_id = 1; routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), True),)
    assert EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator).run(max_messages=1) == 0
    coordinator.execute.assert_called_once(); consumer.commit.assert_called_once()


def test_edge_loader_failed_coordinator_keeps_offsets_uncommitted(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    routed = MagicMock(); routed.lane_id = 1; routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), False, RuntimeError("boom")),)
    assert EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator).run(max_messages=1) == 1
    consumer.commit.assert_not_called()


def test_worker_coordinator_failure_retains_first_lane_cause(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    pending = PendingEdgeRecord(_record(), "works-at-events", 0, 3)
    routed = MagicMock(); routed.record = pending.record; routed.topic = pending.topic; routed.partition = pending.partition; routed.offset = pending.offset
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    root_cause, later_cause = RuntimeError("neo4j diagnostic"), RuntimeError("later lane diagnostic")
    coordinator.execute.return_value = (
        LaneExecutionResult(1, (routed,), False, root_cause),
        LaneExecutionResult(2, (routed,), False, later_cause),
    )
    loader = EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator)
    batch = WorkerBatch(1, (pending,), (), None, None, None)
    loader._outstanding_batches[batch.batch_id] = batch
    loader._permit_states[batch.batch_id] = "QUEUED"
    loader._worker_queue.put(batch)
    loader._worker_queue.put(loader._worker_sentinel)
    loader._worker_loop()
    result = loader._worker_results.get_nowait()
    assert result.outcome == "WRITE_FAILURE"
    assert isinstance(result.error, RuntimeError)
    assert str(result.error) == "lane execution failed"
    assert result.error.__cause__ is root_cause
    consumer.commit.assert_not_called()


def test_gated_worker_failure_logs_chained_cause_without_record_payload(works_at) -> None:
    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    secret_key = "must-not-appear-in-worker-log"
    consumer.poll.return_value = _message(_payload().replace(b"p-001", secret_key.encode()), offset=3)
    routed = MagicMock(); routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), False, RuntimeError("database failure")),)
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    event_logger = MagicMock(); loader._logger = event_logger
    loader._coordinator, loader._partitioner, loader._lane_batcher = coordinator, partitioner, batcher
    assert loader.run(max_messages=1) == 1
    consumer.commit.assert_not_called()
    error_call = event_logger.error.call_args
    assert error_call.kwargs["exc_info"] is True
    assert "stage=worker edge=WORKS_AT replica=0" in str(error_call.args[3])
    assert secret_key not in str(error_call)


# ── P5-S3 Story 6: offset-ledger, fail-closed, and ordering correctness ────
#
# Test-only: no production code change is expected here. These prove
# Decisions points 5-7's "falls out of the existing WorkerResult/
# PartitionLedger contract for free" claim through the *real*
# EdgeLoader/LaneExecutionCoordinator/ApocLockedEdgeWriter stack (a fake
# Neo4j driver only), not just the writer-level unit tests Stories 3-5
# already specify.


def _works_at_payload(person_id: str, company_id: str = "c-1") -> bytes:
    return (
        b'{"source":{"personId":"' + person_id.encode() + b'"},'
        b'"target":{"companyId":"' + company_id.encode() + b'"},'
        b'"properties":{"since":"2020-01-02"}}'
    )


def _agg(checked_rows: int, invalid_rows: int, merged_rows: int):
    result = MagicMock()
    result.__iter__.return_value = iter(
        [{"checked_rows": checked_rows, "invalid_rows": invalid_rows, "merged_rows": merged_rows}]
    )
    return result


def _class_rows(triples):
    result = MagicMock()
    result.__iter__.return_value = iter(
        [{"row_index": i, "source_matches": s, "target_matches": t} for i, s, t in triples]
    )
    return result


def _ok(rows: int = 1):
    result = MagicMock()
    result.__iter__.return_value = iter([{"checked_rows": rows, "invalid_rows": 0, "merged_rows": rows}])
    return result


def _row_level_loader(works_at, tx_run_side_effect, *, sink=None):
    """A real EdgeLoader wired through a real LaneExecutionCoordinator and a
    real ApocLockedEdgeWriter with row-level DLQ enabled, against a fake
    Neo4j driver whose transaction.run() replays tx_run_side_effect in order."""
    from src.loader.edge_execution import ApocLockedEdgeWriter, LaneExecutionCoordinator
    from src.loader.mix_and_batch import LaneBatcher, MixAndBatchPartitioner

    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = tx_run_side_effect
    sink = sink if sink is not None else MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    coordinator = LaneExecutionCoordinator(writer, mode="python_apoc", worker_count=1)
    consumer = MagicMock()
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(),
        partitioner=MixAndBatchPartitioner(works_at),
        lane_batcher=LaneBatcher(works_at, clock=lambda: 0.0),
        coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=10),
    )
    return loader, tx, sink


def test_story6_mixed_batch_commit_resolves_all_offsets_and_dlqs_the_unresolved_row(works_at) -> None:
    """Mixed-batch commit through the real ledger: offsets 10,11,12, 11's
    endpoint never resolves. All three offsets resolve (_resolve_worker_
    result's contract has no concept of *why* a record resolved), the
    committed offset advances past 12, and exactly one envelope was
    published for offset 11."""
    loader, tx, sink = _row_level_loader(works_at, [
        _agg(3, 1, 0),
        _class_rows([(0, 1, 1), (1, 0, 1), (2, 1, 1)]),
        _agg(2, 0, 2),
    ])
    for offset, person in ((10, "p-10"), (11, "missing-src"), (12, "p-12")):
        assert loader._buffer_message(_message(_works_at_payload(person), offset=offset)) is True
    assert loader._flush_batch() is True
    ledger = loader._partition_ledgers[("works-at-events", 0)]
    assert ledger.next_offset == 13
    assert ledger.resolved_offsets == set()
    sink.route_unresolved_endpoint.assert_called_once()
    assert sink.route_unresolved_endpoint.call_args.kwargs["offset"] == 11
    assert sink.route_unresolved_endpoint.call_args.kwargs["reason_code"] == "missing_source"


def test_story6_dlq_produce_failure_resolves_no_offsets_then_succeeds_on_replay(works_at) -> None:
    """DLQ produce failure fails the whole batch closed: zero offsets
    resolve, including the rows Neo4j already durably committed inside
    the same write_provenanced_batch call moments earlier. A second,
    identical attempt (simulating Kafka replay) with a non-failing
    producer succeeds and resolves all three -- idempotent MERGE makes
    replaying the already-written rows safe."""
    from src.loader.dead_letter import DeadLetterPublishError

    failing_sink = MagicMock()
    failing_sink.route_unresolved_endpoint.side_effect = DeadLetterPublishError("broker unreachable")
    loader, _tx, _sink = _row_level_loader(works_at, [
        _agg(3, 1, 0), _class_rows([(0, 1, 1), (1, 0, 1), (2, 1, 1)]), _agg(2, 0, 2),
    ], sink=failing_sink)
    for offset, person in ((10, "p-10"), (11, "missing-src"), (12, "p-12")):
        assert loader._buffer_message(_message(_works_at_payload(person), offset=offset)) is True
    assert loader._flush_batch() is False
    ledger = loader._partition_ledgers[("works-at-events", 0)]
    assert ledger.resolved_offsets == set() and ledger.next_offset == 10

    replay_loader, _tx2, _sink2 = _row_level_loader(works_at, [
        _agg(3, 1, 0), _class_rows([(0, 1, 1), (1, 0, 1), (2, 1, 1)]), _agg(2, 0, 2),
    ])
    for offset, person in ((10, "p-10"), (11, "missing-src"), (12, "p-12")):
        assert replay_loader._buffer_message(_message(_works_at_payload(person), offset=offset)) is True
    assert replay_loader._flush_batch() is True
    assert replay_loader._partition_ledgers[("works-at-events", 0)].next_offset == 13


def test_story6_rebalance_mid_batch_settles_row_level_dlq_batch_before_revoke(works_at) -> None:
    """Rebalance mid-batch: the existing 'settle a buffered batch before
    revoke' contract (P5-S1 Story 7's confirmation that _on_revoke's
    settlement logic is coordinator-agnostic) is unaffected by a batch
    that happens to contain a row-level DLQ split internally."""
    loader, _tx, sink = _row_level_loader(works_at, [
        _agg(2, 1, 0), _class_rows([(0, 1, 1), (1, 0, 1)]), _agg(1, 0, 1),
    ])
    loader._running = True
    p0 = TopicPartition("works-at-events", 0)
    loader._on_assign(loader._consumer, [p0])
    assert loader._buffer_message(_message(_works_at_payload("p-10"), offset=10)) is True
    assert loader._buffer_message(_message(_works_at_payload("missing-src"), offset=11)) is True

    loader._on_revoke(loader._consumer, [p0])

    committed = loader._consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (0, 12)
    assert loader._rebalance_failure is False
    sink.route_unresolved_endpoint.assert_called_once()


class _FixedGrantLeaseClient:
    """Minimal BucketLeaseClient double: one cell, one immediate grant,
    never denies, never merges. ``ping_result`` controls the pre-commit
    liveness check (P5-S3 Story 6 must-fix: proving a lease-loss abort
    through the *real* BucketLeaseExecutionCoordinator, not a mocked
    coordinator that already carries the exception)."""

    broken = False

    def __init__(self, ping_result: bool = True) -> None:
        self._ping_result = ping_result
        self._next_lease_id = 0

    def connect(self) -> None:
        pass

    def acquire_any(self, resource_sets, *, wait_ms):
        self._next_lease_id += 1
        return (f"lease-{self._next_lease_id}", 0)

    def extend(self, lease_id, weighted_sets, *, budget):
        return []

    def release(self, lease_id) -> None:
        pass

    def close(self) -> None:
        pass

    def ping(self) -> bool:
        return self._ping_result


def _lease_backed_loader(works_at, tx_run_side_effect, *, sink=None, ping_result: bool = True,
                         client_factory=None, bucket_count=1, **loader_kwargs):
    """A real EdgeLoader wired through a real BucketLeaseExecutionCoordinator
    (streaming's default path, routes_records=True -- bypasses partitioner/
    lane_batcher entirely) and a real row-level ApocLockedEdgeWriter, against
    a fake Neo4j driver and a fake (always-granting) lease client."""
    from src.loader.edge_execution import ApocLockedEdgeWriter, BucketLeaseExecutionCoordinator
    from src.loader.lease_client import BucketCellRouter

    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = tx_run_side_effect
    sink = sink if sink is not None else MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    coordinator = BucketLeaseExecutionCoordinator(
        writer, router=BucketCellRouter(works_at.nodes.source, works_at.nodes.target, bucket_count),
        worker_count=1, batch_size=10,
        client_factory=client_factory or (lambda: _FixedGrantLeaseClient(ping_result=ping_result)),
        **loader_kwargs.pop("coordinator_kwargs", {}),
    )
    consumer = MagicMock()
    loader = EdgeLoader(
        consumer, writer, works_at, rejection_sink=MagicMock(), coordinator=coordinator,
        loading_config=LoadingConfig(unwind_batch_size=10), **loader_kwargs,
    )
    return loader, tx, sink


@pytest.mark.parametrize("scenario", ["attempt_1", "fallback"])
def test_story6_lease_loss_mid_transaction_resolves_no_offsets_and_never_touches_dlq(works_at, scenario) -> None:
    """Decisions point 7, through the full EdgeLoader stack and the REAL
    BucketLeaseExecutionCoordinator (not a mocked coordinator that already
    carries the exception -- that only re-proves the pre-existing 'a
    failed lane result resolves no offsets' behavior). A dead lease
    (ping() -> False) aborts before commit, in either Attempt 1 (an
    all-resolved batch) or the fallback (a mixed batch) -- zero offsets
    resolve, nothing commits, the sink is never invoked, and (since
    on_dlq_routed only ever fires from inside DeadLetterProducer.publish)
    the underlying Kafka producer's produce() is never called either."""
    from src.loader.dead_letter import DeadLetterProducer, DeadLetterRecordSink

    kafka_producer = MagicMock()
    sink = DeadLetterRecordSink(
        producer=DeadLetterProducer(kafka_producer), topic="works-at-events.dlq",
        run_id="run-1", replica_id="0", kind="edge",
    )
    if scenario == "attempt_1":
        tx_run_side_effect = [_ok(1)]  # all-resolved: dies at Attempt 1's pre-commit check
        offsets_and_persons = [(10, "p-10")]
    else:
        tx_run_side_effect = [_agg(2, 1, 0), _class_rows([(0, 1, 1), (1, 0, 1)]), _agg(1, 0, 1)]
        offsets_and_persons = [(10, "p-10"), (11, "missing-src")]
    loader, tx, _sink = _lease_backed_loader(works_at, tx_run_side_effect, sink=sink, ping_result=False)
    for offset, person in offsets_and_persons:
        assert loader._buffer_message(_message(_works_at_payload(person), offset=offset)) is True

    assert loader._flush_batch() is False

    # Pins the cause without catching the exception type: every statement
    # ran (the side_effect list was exhausted, not a MagicMock mismatch or
    # an unrelated routing error) and the aggregate/matched-write checks
    # passed, yet commit was never called and nothing was produced -- the
    # only path left is the liveness check returning False and raising
    # LeaseCoordinatorError.
    assert tx.run.call_count == len(tx_run_side_effect)
    loader._consumer.commit.assert_not_called()
    tx.commit.assert_not_called()
    ledger = loader._partition_ledgers[("works-at-events", 0)]
    assert ledger.resolved_offsets == set()
    sink_calls = kafka_producer.produce.call_args_list  # via the real DeadLetterRecordSink/DeadLetterProducer
    assert sink_calls == []


def test_story6_lease_backed_mixed_batch_commits_and_dlqs_through_the_default_streaming_path(works_at) -> None:
    """SHOULD-FIX (reviewer): the spec's mixed-batch scenario named
    BucketLeaseExecutionCoordinator, streaming's default path -- exercised
    at the coordinator level in Story 4, but never before through the full
    EdgeLoader into the offset ledger. Same shape as the classic-path
    mixed-batch test above, through the lease-backed loader instead."""
    loader, _tx, sink = _lease_backed_loader(works_at, [
        _agg(3, 1, 0), _class_rows([(0, 1, 1), (1, 0, 1), (2, 1, 1)]), _agg(2, 0, 2),
    ])
    for offset, person in ((10, "p-10"), (11, "missing-src"), (12, "p-12")):
        assert loader._buffer_message(_message(_works_at_payload(person), offset=offset)) is True
    assert loader._flush_batch() is True
    ledger = loader._partition_ledgers[("works-at-events", 0)]
    assert ledger.next_offset == 13
    sink.route_unresolved_endpoint.assert_called_once()
    assert sink.route_unresolved_endpoint.call_args.kwargs["offset"] == 11


def test_story6_same_pair_ordering_end_to_end_through_edge_loader(works_at) -> None:
    """Same-pair ordering, end to end: two records for the same pair in
    one poll batch, the earlier missing, the later individually
    resolvable. The final state has no relationship for that pair (no
    matched-subset write call at all), and both envelopes exist with the
    expected reason_codes."""
    loader, tx, sink = _row_level_loader(works_at, [
        _agg(2, 1, 0),
        _class_rows([(0, 0, 1), (1, 1, 1)]),
    ])
    for offset, person in ((20, "missing-src"), (21, "missing-src")):
        assert loader._buffer_message(_message(_works_at_payload(person, "c-1"), offset=offset)) is True
    assert loader._flush_batch() is True
    assert tx.run.call_count == 2  # never a 3rd call: matched_rows was empty
    ledger = loader._partition_ledgers[("works-at-events", 0)]
    assert ledger.next_offset == 22
    assert sink.route_unresolved_endpoint.call_count == 2
    by_offset = {c.kwargs["offset"]: c.kwargs["reason_code"] for c in sink.route_unresolved_endpoint.call_args_list}
    assert by_offset[20] == "missing_source"
    assert by_offset[21] == "blocked_by_earlier_unresolved_row"


class _DeniedLeaseClient(_FixedGrantLeaseClient):
    """Every lease is held by a dead writer still inside its fence."""

    def acquire_any(self, resource_sets, *, wait_ms):
        time.sleep(0.01)
        return None


def test_f1_stopping_loader_with_unobtainable_lease_abandons_batch_exits_zero_commits_nothing(works_at, caplog) -> None:
    import logging

    stop = Event()
    loader, tx, _sink = _lease_backed_loader(
        works_at, [], client_factory=_DeniedLeaseClient, shutdown_requested=stop,
        coordinator_kwargs={"stop_event": stop, "drain_budget_seconds": 0.2},
    )
    assert loader._buffer_message(_message(_works_at_payload("p-10"), offset=10)) is True
    stop.set()
    started = time.monotonic()
    with caplog.at_level(logging.WARNING):
        assert loader.run() == 0
    assert time.monotonic() - started < 3.0
    loader._consumer.commit.assert_not_called()
    tx.run.assert_not_called()  # never wrote without a lease
    tx.commit.assert_not_called()
    assert loader._partition_ledgers[("works-at-events", 0)].resolved_offsets == set()
    assert any(
        "stage=edge-drain-abandoned" in record.getMessage() and record.levelno == logging.WARNING
        for record in caplog.records
    )


def test_f1_gated_worker_path_abandons_drain_exits_zero_commits_nothing(works_at) -> None:
    """Slot-gated _worker_loop -> DRAIN_ABANDONED -> _resolve_worker_result -> exit 0."""
    from src.loader.edge_execution import LeaseDrainAbandoned

    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(_payload(), offset=3)
    routed = MagicMock(); routed.record = _record(); routed.topic = "works-at-events"; routed.partition = 0; routed.offset = 3
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), False, LeaseDrainAbandoned("budget spent")),)
    loader = _gated_loader(consumer, writer, works_at, [_clock_lease_payload(1, "WORKS_AT"), None])
    event_logger = MagicMock(); loader._logger = event_logger
    loader._coordinator, loader._partitioner, loader._lane_batcher = coordinator, partitioner, batcher
    assert loader.run(max_messages=1) == 0
    consumer.commit.assert_not_called()
    assert "stage=edge-drain-abandoned" in str(event_logger.warning.call_args.args[0])
    event_logger.error.assert_not_called()


def test_f1_worker_loop_reports_drain_abandoned_outcome(works_at) -> None:
    from src.loader.edge_execution import LeaseDrainAbandoned

    consumer, writer, coordinator, partitioner, batcher = MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    pending = PendingEdgeRecord(_record(), "works-at-events", 0, 3)
    routed = MagicMock(); routed.record = pending.record; routed.topic = pending.topic; routed.partition = pending.partition; routed.offset = pending.offset
    partitioner.route.return_value = routed; batcher.drain_all.return_value = [(routed,)]
    cause = LeaseDrainAbandoned("budget spent")
    coordinator.execute.return_value = (LaneExecutionResult(1, (routed,), False, cause),)
    loader = EdgeLoader(consumer, writer, works_at, partitioner=partitioner, lane_batcher=batcher, coordinator=coordinator)
    batch = WorkerBatch(1, (pending,), (), None, None, None)
    loader._outstanding_batches[batch.batch_id] = batch
    loader._permit_states[batch.batch_id] = "QUEUED"
    loader._worker_queue.put(batch)
    loader._worker_loop()
    result = loader._worker_results.get_nowait()
    assert result.outcome == "DRAIN_ABANDONED" and result.error is cause
    consumer.commit.assert_not_called()


def test_f1_partial_flush_with_one_abandoned_cell_commits_no_offsets(works_at, caplog) -> None:
    """Some cells written, one cell's lease unobtainable: the whole batch stays
    uncommitted (replayed later), the loader still exits 0."""
    import logging
    from src.loader.lease_client import BucketCellRouter

    router = BucketCellRouter(works_at.nodes.source, works_at.nodes.target, 8)
    cells = {}
    for index in range(200):
        cells.setdefault(router.cell(f"p-{index}", "c-1"), f"p-{index}")
        if len(cells) == 2:
            break
    (cell_ok, person_ok), (cell_blocked, person_blocked) = list(cells.items())
    blocked = set(router.resources(cell_blocked)) - set(router.resources(cell_ok))

    class _PartiallyBlockedClient(_FixedGrantLeaseClient):
        def acquire_any(self, resource_sets, *, wait_ms):
            for index, resources in enumerate(resource_sets):
                if not blocked & set(resources):
                    self._next_lease_id += 1
                    return (f"lease-{self._next_lease_id}", index)
            time.sleep(0.01)
            return None

    stop = Event()
    loader, tx, _sink = _lease_backed_loader(
        works_at, [_ok(1)], client_factory=_PartiallyBlockedClient, bucket_count=8, shutdown_requested=stop,
        coordinator_kwargs={"stop_event": stop, "drain_budget_seconds": 0.2},
    )
    assert loader._buffer_message(_message(_works_at_payload(person_ok), offset=10)) is True
    assert loader._buffer_message(_message(_works_at_payload(person_blocked), offset=11)) is True
    stop.set()
    with caplog.at_level(logging.WARNING):
        assert loader.run() == 0
    assert tx.run.called  # the unblocked cell really was written
    loader._consumer.commit.assert_not_called()
    assert loader._partition_ledgers[("works-at-events", 0)].resolved_offsets == set()
    assert any("stage=edge-drain-abandoned" in r.getMessage() for r in caplog.records)
