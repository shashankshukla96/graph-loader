"""v7-S4: one DLQ, parking lot and parked-pair index per relationship type with several topics.

The retry consumer runs here against a REAL ``ParkedPairIndex`` (in-memory Kafka doubles), so the
cross-topic order is the one production computes, not a fake's answer.
"""
from __future__ import annotations

from contextlib import ExitStack
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.loader.dead_letter import DeadLetterRecordSink
from src.loader.edge_execution import IndexedMissingEndpointSink
from src.loader.edge_loader import EdgeLoader
from src.loader.edge_records import EdgeRecord
from src.loader.parked_pair_index import ParkedPairIndex
from src.loader.retry_consumer import RETRY_OUTCOME_WRITTEN, main as retry_consumer_main
from src.models.schema import GraphSchema
from tests.test_parked_pair_index import _FakeConsumer as _IndexConsumer
from tests.test_parked_pair_index import _FakeProducer as _IndexProducer
from tests.test_parked_pair_index import _FakeTopicLog
from tests.test_retry_consumer import _envelope, _FakeConsumer, _FakeDlqMessage, _make_consumer

ROOT = Path(__file__).resolve().parents[1]
A, B = "works-at-a", "works-at-b"


def _schema(**edge_overrides) -> GraphSchema:
    data = yaml.safe_load((ROOT / "config" / "graph_schema.yaml").read_text())
    for edge in data["edges"]:
        if edge["type"] == "WORKS_AT":
            edge.pop("topic")
            edge["topics"] = [A, B]
            edge["topic_conflict"] = "arrival"
            edge.pop("order_by", None)
            edge["dead_letter"] = {"enabled": True, "max_retry_attempts": 5}
            for key, value in edge_overrides.items():
                if value is None:
                    edge.pop(key, None)
                else:
                    edge[key] = value
    return GraphSchema.model_validate(data)


class _AckingIndexProducer(_IndexProducer):
    def produce(self, topic, key=None, value=None, on_delivery=None, **kwargs) -> None:
        super().produce(topic, key=key, value=value)
        if on_delivery is not None:
            on_delivery(None, MagicMock())


def _index(log: _FakeTopicLog, *, sequenced: bool) -> ParkedPairIndex:
    return ParkedPairIndex(
        topic="WORKS_AT.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True, sequenced=sequenced,
        consumer_factory=lambda config: _IndexConsumer(config, log, "WORKS_AT.dlq.parked-pairs"),
        producer_factory=lambda config: _AckingIndexProducer(config, log),
        poll_timeout_seconds=0.01,
    )


class _WriteEverything:
    """Every retry write succeeds; records the order rows are written in."""

    def __init__(self) -> None:
        self.written: list[tuple[str, int]] = []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.written.extend((e.original_topic, e.original_offset) for e in envelopes)
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


class _RecordingSink:
    def __init__(self) -> None:
        self.routed: list[tuple[str, int]] = []

    def route_unresolved_endpoint(self, *, topic, partition, offset, **_kwargs) -> None:
        self.routed.append((topic, offset))


def _live_dlq(index: ParkedPairIndex, rows) -> None:
    """The live writer's DLQ hook for each (topic, offset) row of pair (p1, c1), in arrival order."""
    sink = IndexedMissingEndpointSink(_RecordingSink(), index)
    for topic, offset in rows:
        sink.route_unresolved_endpoint(
            topic=topic, partition=0, offset=offset,
            record=EdgeRecord(source_key="p1", target_key="c1", properties={}),
            edge_type="WORKS_AT", source_key_property="personId", target_key_property="companyId",
            failure_class="missing_endpoint", reason_code="missing_source", error_text="x", retry_after_seconds=0.0,
        )


def _drain(index: ParkedPairIndex, dlq_order, expect: int | None = None) -> list[tuple[str, int]]:
    """Run the retry consumer over the type's ONE DLQ (envelopes in ``dlq_order``)."""
    consumer = _FakeConsumer()
    for dlq_offset, (topic, offset) in enumerate(dlq_order):
        envelope = _envelope(
            entity_name="WORKS_AT", source_key="p1", target_key="c1",
            original_topic=topic, original_partition=0, original_offset=offset, retry_after_ms=1_000,
        )
        consumer.push(_FakeDlqMessage("WORKS_AT.dlq", 0, dlq_offset, envelope))
    writer = _WriteEverything()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, index=index, entity_name="WORKS_AT", parked_topic="WORKS_AT.dlq.parked",
    )
    deadline = time.monotonic() + 5  # the real index applies tombstones on its own poller thread
    while len(writer.written) < (expect or len(dlq_order)) and time.monotonic() < deadline:
        rc.tick()
        time.sleep(0.005)
    return writer.written


@pytest.mark.parametrize("dlq_order", [[(B, 40), (A, 2)], [(A, 2), (B, 40)]], ids=["dlq-in-order", "root-re-dlqd"])
def test_pair_parked_from_topic_b_then_blocked_on_topic_a_retries_in_arrival_order(dlq_order) -> None:
    """Topic names sort a < b, but b's record arrived first. Both DLQ orders (the root may have
    been re-DLQ'd behind the follower) retry b's record before a's."""
    log = _FakeTopicLog()
    live = _index(log, sequenced=True)
    live.start()
    try:
        _live_dlq(live, [(B, 40)])
        assert live.is_blocked("p1", "c1")  # a later record of the pair from topic a is parked behind it
        _live_dlq(live, [(A, 2)])
    finally:
        live.stop()
    retry_index = _index(log, sequenced=True)
    retry_index.start()
    try:
        assert _drain(retry_index, dlq_order) == [(B, 40), (A, 2)]
        assert not retry_index.is_blocked("p1", "c1")
    finally:
        retry_index.stop()


def test_without_the_sequence_the_topic_name_order_would_invert_the_pair() -> None:
    """The bug the sequence closes: an unsequenced index orders by topic name."""
    log = _FakeTopicLog()
    live = _index(log, sequenced=False)
    live.start()
    try:
        _live_dlq(live, [(B, 40), (A, 2)])
    finally:
        live.stop()
    retry_index = _index(log, sequenced=False)
    retry_index.start()
    try:
        assert _drain(retry_index, [(B, 40), (A, 2)]) == [(A, 2), (B, 40)]
    finally:
        retry_index.stop()


def test_a_pairs_records_within_one_topic_keep_offset_order_among_other_topic_records() -> None:
    log = _FakeTopicLog()
    live = _index(log, sequenced=True)
    live.start()
    try:
        _live_dlq(live, [(B, 40), (A, 2), (B, 41), (A, 3)])
    finally:
        live.stop()
    retry_index = _index(log, sequenced=True)
    retry_index.start()
    try:
        assert _drain(retry_index, [(A, 3), (B, 41), (A, 2), (B, 40)]) == [(B, 40), (A, 2), (B, 41), (A, 3)]
    finally:
        retry_index.stop()


# ── wiring: names, retry consumer main, edge loader ─────────────────────────


def test_type_wide_names_and_partition_sources() -> None:
    from src.models.schema import (
        edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name,
    )

    edge = next(e for e in _schema().edges if e.type == "WORKS_AT")
    assert edge_dead_letter_topic_name(edge) == "WORKS_AT.dlq"
    assert edge_parked_topic_name(edge) == "WORKS_AT.dlq.parked"
    assert edge_parked_pair_index_topic_name(edge) == "WORKS_AT.dlq.parked-pairs"
    renamed = next(e for e in _schema(dead_letter={"enabled": True, "topic": f"{A}.dlq"}).edges if e.type == "WORKS_AT")
    assert edge_parked_pair_index_topic_name(renamed) == f"{A}.dlq.parked-pairs"


def _main_patches(stack, schema):
    stack.enter_context(patch("src.loader.retry_consumer.load_schema", return_value=schema))
    stack.enter_context(patch("confluent_kafka.Consumer", return_value=MagicMock()))
    stack.enter_context(patch("neo4j.GraphDatabase.driver", return_value=MagicMock()))
    build_sink = stack.enter_context(patch(
        "src.loader.retry_consumer.build_malformed_record_sink",
        return_value=(MagicMock(spec=DeadLetterRecordSink), None),
    ))
    rc_cls = stack.enter_context(patch("src.loader.retry_consumer.RetryConsumer"))
    rc_cls.return_value.run_forever.return_value = None
    return rc_cls, build_sink


def test_retry_consumer_main_subscribes_to_the_type_dlq_with_a_sequenced_index() -> None:
    """Before v7-S4 main() raised MultiTopicEdgeError on edge.topic for ANY multi-topic edge."""
    schema = _schema()
    fake_index = MagicMock()
    with ExitStack() as stack:
        rc_cls, build_sink = _main_patches(stack, schema)
        index_cls = stack.enter_context(patch("src.loader.retry_consumer.ParkedPairIndex", return_value=fake_index))
        stack.enter_context(patch("src.loader.lease_client.BucketLeaseClient"))
        stack.enter_context(patch("src.loader.lease_client.BucketCellRouter"))
        stack.enter_context(patch.dict("os.environ", {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700", "GRAPH_LOADER_LEASE_BUCKETS": "16"}))
        assert retry_consumer_main(["--dlq-topics", "WORKS_AT.dlq", "--lease-address", "lease:7700"]) == 0
    kwargs = rc_cls.call_args.kwargs
    assert kwargs["dlq_topic_by_edge_type"] == {"WORKS_AT": "WORKS_AT.dlq"}
    assert kwargs["parked_topic_by_edge_type"] == {"WORKS_AT": "WORKS_AT.dlq.parked"}
    assert kwargs["parked_pair_index_by_edge_type"] == {"WORKS_AT": fake_index}
    assert index_cls.call_args.kwargs["topic"] == "WORKS_AT.dlq.parked-pairs"
    assert index_cls.call_args.kwargs["sequenced"] is True
    assert build_sink.call_args.kwargs["entity_topic"] == "WORKS_AT"
    rc_cls.return_value.run_forever.assert_called_once()


def test_retry_consumer_main_runs_a_single_topic_type_next_to_a_multi_topic_one() -> None:
    schema = _schema(dead_letter=None, topic_conflict=None, order_by="since")
    knows = next(e for e in schema.edges if e.type != "WORKS_AT")
    from src.models.schema import DeadLetterConfig

    idx = schema.edges.index(knows)
    schema.edges[idx] = knows.model_copy(update={"dead_letter": DeadLetterConfig(enabled=True)})
    with ExitStack() as stack:
        rc_cls, _ = _main_patches(stack, schema)
        assert retry_consumer_main(["--dlq-topics", f"{knows.topics[0]}.dlq"]) == 0
    assert rc_cls.call_args.kwargs["dlq_topic_by_edge_type"] == {knows.type: f"{knows.topics[0]}.dlq"}


class _BarrierIndex:
    def __init__(self) -> None:
        self.snapshots = 0
        self.ready = False

    def watermark_snapshot(self):
        self.snapshots += 1
        return {0: 5}

    def caught_up(self, snapshot) -> bool:
        return self.ready


def _barrier_loader(index, clock):
    from src.utils.metrics import GraphLoaderMetrics
    from prometheus_client import CollectorRegistry

    edge = next(e for e in _schema().edges if e.type == "WORKS_AT")
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    consumer = MagicMock()
    loader = EdgeLoader(consumer, MagicMock(), edge, admission_index=index, clock=clock, metrics=metrics)
    return loader, consumer, metrics


def _wait_gauge(metrics) -> float:
    return metrics.parked_pair_index_wait.labels(kind="edge", name="WORKS_AT")._value.get()


def test_admission_barrier_holds_new_partitions_until_the_index_caught_up(caplog) -> None:
    """v7-S4 review M2/S1: no record of a new assignment is read, written or DLQ'd (so no
    `is_blocked` and no sequenced `add`) before the index view includes the previous owner's
    entries; it never falls back to a stale view, however long it waits."""
    now = [100.0]
    index = _BarrierIndex()
    loader, consumer, metrics = _barrier_loader(index, lambda: now[0])
    loader._on_assign(consumer, [MagicMock(topic=B, partition=1), MagicMock(topic=A, partition=0)])
    paused = consumer.pause.call_args.args[0]
    assert sorted((tp.topic, tp.partition) for tp in paused) == [(A, 0), (B, 1)]
    assert loader._admission_ready() is False and index.snapshots == 1
    now[0] += 3600  # an hour later: still waiting, never admits on a stale view
    assert loader._admission_ready() is False and index.snapshots == 1
    assert "reason=parked_pair_index_lagging" in caplog.text
    assert _wait_gauge(metrics) == pytest.approx(3600)
    consumer.resume.assert_not_called()
    index.ready = True
    assert loader._admission_ready() is True
    assert sorted((tp.topic, tp.partition) for tp in consumer.resume.call_args.args[0]) == [(A, 0), (B, 1)]
    assert _wait_gauge(metrics) == 0
    assert loader._admission_ready() is True  # no further snapshots once admitted
    assert index.snapshots == 1


def test_admission_barrier_retries_a_failed_snapshot_and_resets_on_revoke() -> None:
    now = [0.0]
    index = _BarrierIndex()
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("broker timeout")
        return {0: 1}

    index.watermark_snapshot = flaky
    loader, consumer, _metrics = _barrier_loader(index, lambda: now[0])
    loader._on_assign(consumer, [MagicMock(topic=A, partition=0)])
    assert loader._admission_ready() is False  # snapshot failed: still held
    index.ready = True
    assert loader._admission_ready() is True and calls["n"] == 2
    loader._on_assign(consumer, [MagicMock(topic=A, partition=0)])  # every assignment re-arms it
    assert loader._admission_barrier is not None


def test_run_loop_defers_records_fetched_before_the_barrier_cleared() -> None:
    from collections import deque

    index = _BarrierIndex()
    loader, consumer, _metrics = _barrier_loader(index, time.monotonic)
    loader._on_assign(consumer, [MagicMock(topic=A, partition=0)])
    message = MagicMock()
    consumer.poll.return_value = message
    # one gated iteration: poll, defer, never buffer
    assert loader._admission_ready() is False
    loader._deferred_messages = deque()
    with patch.object(loader, "_buffer_message") as buffer_message:
        polls = {"n": 0}

        def poll(timeout=None):
            polls["n"] += 1
            if polls["n"] >= 3:
                loader._shutdown_requested.set()
            return message

        consumer.poll.side_effect = poll
        with patch.object(loader, "_finish_run", return_value=True):
            assert loader.run() == 0
        buffer_message.assert_not_called()
    assert list(loader._deferred_messages) == [message] * polls["n"]  # every fetched record kept for later


@pytest.mark.parametrize("caught_up", [False, True])
def test_a_stop_behind_the_admission_barrier_drops_deferred_records_with_the_real_finish_run(caught_up) -> None:
    """Review M-a (pU_stop_bypass): SIGTERM while the barrier is up must not buffer and write the
    deferred records through `_finish_run` (the M2 inversion on the stop path); they stay
    uncommitted for the next owner. Once admitted, a stop flushes them as before."""
    from collections import deque

    index = _BarrierIndex()
    loader, consumer, _metrics = _barrier_loader(index, time.monotonic)
    loader._on_assign(consumer, [MagicMock(topic=A, partition=0)])
    if caught_up:
        index.ready = True
        assert loader._admission_ready()
    loader._deferred_messages = deque([MagicMock(), MagicMock(), MagicMock()])
    loader._shutdown_requested.set()
    with patch.object(loader, "_buffer_message", return_value=True) as buffer_message:
        assert loader.run() == 0  # the REAL _finish_run
    assert buffer_message.call_count == (3 if caught_up else 0)
    assert len(loader._deferred_messages) == 0
    if not caught_up:
        consumer.commit.assert_not_called()


def test_edge_loader_without_an_index_has_no_admission_barrier() -> None:
    edge = next(e for e in _schema().edges if e.type == "WORKS_AT")
    consumer = MagicMock()
    loader = EdgeLoader(consumer, MagicMock(), edge)
    loader._on_assign(consumer, [MagicMock(topic=A, partition=0)])
    consumer.pause.assert_not_called()
    assert loader._admission_ready() is True


def test_a_re_read_record_keeps_its_seq_once_the_view_has_caught_up() -> None:
    """pS_barrier_timeout as a test: the new owner of works-at-a/0 re-reads an uncommitted A1
    (already indexed, seq 1, ahead of B1 seq 2). Behind the admission barrier its view has A1, so
    the idempotent re-add keeps seq 1 and the retry order stays A1 then B1."""
    log = _FakeTopicLog()
    old = _index(log, sequenced=True)
    old.start()
    _live_dlq(old, [(A, 10)])
    old.stop()
    other = _index(log, sequenced=True)
    other.start()
    _live_dlq(other, [(B, 7)])
    other.stop()
    new_owner = _index(log, sequenced=True)
    new_owner.start()  # rebuilt to EOF: caught up with every add made before the assignment
    try:
        assert new_owner.caught_up(new_owner.watermark_snapshot())
        _live_dlq(new_owner, [(A, 10)])  # the re-read
        assert new_owner.order_key("p1", "c1", original_topic=A, original_partition=0, original_offset=10)[0] == 1
    finally:
        new_owner.stop()
    retry_index = _index(log, sequenced=True)
    retry_index.start()
    try:
        assert _drain(retry_index, [(A, 10), (B, 7), (A, 10)], expect=2) == [(A, 10), (B, 7)]
    finally:
        retry_index.stop()


def test_order_by_multi_topic_type_uses_the_conditional_set_on_retry() -> None:
    """A parked/retried older record of topic a can never overwrite b's newer committed value."""
    from src.loader.retry_consumer import build_retry_base_writer

    edge = next(e for e in _schema(topic_conflict=None, order_by="since").edges if e.type == "WORKS_AT")
    writer = build_retry_base_writer(MagicMock(), edge, tx_timeout_seconds=30, leased=True)
    assert "row.properties[$order_by] > r[$order_by]" in writer._query


def test_bulk_classic_two_topic_opt_in_routes_every_topics_rows_to_the_type_dlq() -> None:
    """Story 4.5: in bulk (classic) both topics' unresolved rows go to ``<type>.dlq`` and keep their topic."""
    from src.loader.dead_letter import build_malformed_record_sink
    from src.loader.edge_execution import build_edge_execution

    edge = next(
        e for e in _schema(dead_letter={"enabled": True, "route_missing_endpoints_in_bulk": True}).edges
        if e.type == "WORKS_AT"
    )
    published: list = []

    class _Producer:
        def __init__(self, config):
            pass

        def produce(self, topic, key=None, value=None, on_delivery=None, **_kwargs):
            published.append(topic)
            on_delivery(None, MagicMock())

        def poll(self, *_a):
            return 0

        def flush(self, *_a):
            return 0

    sink, _producer = build_malformed_record_sink(
        dead_letter=edge.dead_letter, entity_topic=edge.dead_letter_entity, entity_kind="edge",
        rejection_log_path="unused.jsonl", bootstrap_servers="kafka:9092", run_id="r", replica_id="0",
        producer_factory=_Producer,
    )
    assert isinstance(sink, DeadLetterRecordSink)
    writer, _p, _b, _c = build_edge_execution(edge, MagicMock(), mode="bulk", missing_endpoint_sink=sink)
    assert writer.row_level_dlq_enabled is True
    for topic in (A, B):
        sink.route_unresolved_endpoint(
            topic=topic, partition=0, offset=1, record=EdgeRecord(source_key="p1", target_key="c1", properties={}),
            edge_type="WORKS_AT", source_key_property="personId", target_key_property="companyId",
            failure_class="missing_endpoint", reason_code="missing_source", error_text="x", retry_after_seconds=0.0,
        )
    assert published == ["WORKS_AT.dlq", "WORKS_AT.dlq"]


# ── v7-S4 review S4: refuse to orphan a former default DLQ ──────────────────

from argparse import Namespace  # noqa: E402

from src import cli  # noqa: E402

_REAL_REQUIRE_NO_STALE = cli._require_no_stale_dead_letter_topics


class _PreflightAdmin:
    def __init__(self, records: dict[str, int]) -> None:
        self.records = records

    def list_topics(self, timeout=None):
        return MagicMock(topics={name: MagicMock(error=None, partitions={0: None, 1: None}) for name in self.records})


class _PreflightConsumer:
    def __init__(self, records: dict[str, int]) -> None:
        self.records = records
        self.closed = False

    def get_watermark_offsets(self, tp, timeout=None):
        return (0, self.records[tp.topic] if tp.partition == 0 else 0)

    def close(self):
        self.closed = True


def _stale(schema, records):
    return cli._stale_dead_letter_topics(
        schema, "kafka:9092", admin_factory=lambda config: _PreflightAdmin(records),
        consumer_factory=lambda config: _PreflightConsumer(records),
    )


def test_adding_a_second_topic_is_refused_while_the_old_topic_dlq_holds_records() -> None:
    schema = _schema()
    assert _stale(schema, {f"{A}.dlq": 3}) == [("WORKS_AT", f"{A}.dlq", 3, "WORKS_AT.dlq")]
    with patch.object(cli, "_stale_dead_letter_topics", return_value=_stale(schema, {f"{A}.dlq": 3})):
        with pytest.raises(ValueError, match=rf"set `dead_letter.topic: {A}\.dlq` on WORKS_AT"):
            _REAL_REQUIRE_NO_STALE(schema, "kafka:9092")


def test_going_back_to_one_topic_is_refused_while_the_type_dlq_holds_records() -> None:
    schema = _schema(topics=[A], topic_conflict=None, order_by="since")
    assert _stale(schema, {"WORKS_AT.dlq": 7}) == [("WORKS_AT", "WORKS_AT.dlq", 7, f"{A}.dlq")]


def test_an_explicit_dead_letter_topic_or_an_empty_former_dlq_passes() -> None:
    explicit = _schema(dead_letter={"enabled": True, "topic": "my-dlq"})
    assert _stale(explicit, {f"{A}.dlq": 3, "WORKS_AT.dlq": 3}) == []
    assert _stale(_schema(), {f"{A}.dlq": 0}) == []
    assert _stale(_schema(), {}) == []


def test_handle_start_refuses_before_any_topic_is_created(caplog) -> None:
    schema = _schema()
    args = Namespace(mode="stream", config="c.yaml", network="n", skip_image_build=True)
    stale = [("WORKS_AT", f"{A}.dlq", 3, "WORKS_AT.dlq")]
    with patch.object(cli, "load_schema", return_value=schema), \
            patch.object(cli, "_require_no_stale_dead_letter_topics", _REAL_REQUIRE_NO_STALE), \
            patch.object(cli, "_stale_dead_letter_topics", return_value=stale), \
            patch.object(cli, "ensure_dead_letter_topic") as ensure, \
            patch.object(cli, "get_neo4j_driver") as driver, \
            patch.object(cli, "get_neo4j_credentials", return_value=("bolt://x", "u", "p")):
        assert cli.handle_start(args) == 1
    ensure.assert_not_called()
    driver.assert_not_called()  # before DDL, images and any container
    assert "dead_letter.topic" in caplog.text


def test_accept_former_dlq_lets_a_kept_or_foreign_former_dlq_through() -> None:
    """Review S-c: a drained DLQ kept for audit (records stay until deleted) or a `<type>.dlq`
    another deployment owns on a shared cluster must not block every start."""
    schema = _schema()
    records = {f"{A}.dlq": 3, f"{B}.dlq": 1}
    stale = cli._stale_dead_letter_topics(
        schema, "kafka:9092", admin_factory=lambda config: _PreflightAdmin(records),
        consumer_factory=lambda config: _PreflightConsumer(records), accepted=(f"{A}.dlq",),
    )
    assert [name for _t, name, _n, _r in stale] == [f"{B}.dlq"]
    with patch.object(cli, "_stale_dead_letter_topics", return_value=[]) as check:
        _REAL_REQUIRE_NO_STALE(schema, "kafka:9092", accepted=(f"{A}.dlq", f"{B}.dlq"))
    assert check.call_args.kwargs["accepted"] == (f"{A}.dlq", f"{B}.dlq")


def test_start_passes_every_accept_former_dlq_flag_to_the_preflight() -> None:
    args = cli.build_parser().parse_args([
        "start", "--mode", "stream", "--accept-former-dlq", f"{A}.dlq", "--accept-former-dlq", "WORKS_AT.dlq",
    ])
    assert args.accept_former_dlq == [f"{A}.dlq", "WORKS_AT.dlq"]
    schema = _schema()
    seen = {}

    def fake(schema_arg, bootstrap, *, accepted=()):
        seen["accepted"] = accepted
        raise ValueError("stop here")

    with patch.object(cli, "load_schema", return_value=schema), \
            patch.object(cli, "_require_no_stale_dead_letter_topics", fake), \
            patch.object(cli, "get_neo4j_credentials", return_value=("bolt://x", "u", "p")):
        args.skip_image_build, args.network = True, "n"
        assert cli.handle_start(args) == 1
    assert seen["accepted"] == (f"{A}.dlq", "WORKS_AT.dlq")
