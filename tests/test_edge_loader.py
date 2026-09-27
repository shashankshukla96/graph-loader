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
from src.loader.node_loader import ControlDeliveryError, RejectionSink
from src.loader.slot_admission import LeaseStateError, SlotAdmission, SlotAwareAdmissionBuffer
from src.models.schema import CoordinationConfig, EdgeConfig, LoadingConfig
from src.orchestrator.coordination import ClockLease, encode_clock_lease
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


def _gated_loader(consumer, writer, works_at, coordination_values):
    values = iter(coordination_values)
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


def test_edge_loader_rejects_malformed_before_resolving_offset(works_at) -> None:
    consumer, writer, sink = MagicMock(), MagicMock(), MagicMock()
    consumer.poll.return_value = _message(b"{bad", partition=2, offset=17)
    loader = EdgeLoader(consumer, writer, works_at, rejection_sink=sink)

    assert loader.run(max_messages=1) == 0
    sink.append.assert_called_once()
    assert sink.append.call_args.kwargs["edge_type"] == "WORKS_AT"
    assert sink.append.call_args.kwargs["offset"] == 17
    committed = consumer.commit.call_args.kwargs["offsets"][0]
    assert (committed.partition, committed.offset) == (2, 18)
    writer.write_batch.assert_not_called()


def test_edge_rejection_sink_uses_edge_discriminator_and_fsync(tmp_path) -> None:
    path = tmp_path / "edge-rejections.jsonl"
    sink = RejectionSink(path)
    with patch("src.loader.node_loader.os.fsync") as fsync:
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
    sink.append.assert_called_once()
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
    sink.append.side_effect = OSError("disk full")
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
