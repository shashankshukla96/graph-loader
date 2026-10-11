"""Unit tests for src/loader/parked_pair_index.py, against a fake Kafka
client double (closest precedent: tests/test_dead_letter_replay.py's
_FakeConsumer style) -- no real broker, no edge_execution.py hook (that
integration is P5-S4 Story 3's second half, landed in Phase B once P5-S3's
row-level DLQ writer has merged).

Per-record design (not per-pair counter): see the module docstring in
src/loader/parked_pair_index.py for the review finding (M2) that drove this
shape -- a plain counter cannot give the required "retry path blocks only
on an *earlier* outstanding record, live path blocks on *any*" semantics
without either self-blocking (livelock) or losing idempotency across a
crash/re-read.
"""
from __future__ import annotations

import json
import threading

import pytest

from src.loader.parked_pair_index import ParkedPairIndex, ParkedPairIndexError, _pair_key, _record_wire_key


class _FakeTopicLog:
    """Shared in-memory single-partition log every fake consumer/producer
    below reads from/writes to, so multiple ParkedPairIndex instances in
    one test can observe each other's (or a prior instance's) writes."""

    def __init__(self) -> None:
        self.records: list[tuple[bytes, bytes | None]] = []
        self.lock = threading.Lock()

    def append(self, key: bytes, value: bytes | None) -> None:
        with self.lock:
            self.records.append((key, value))

    def high_watermark(self) -> int:
        with self.lock:
            return len(self.records)


class _FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int, key, value, error=None) -> None:
        self._topic, self._partition, self._offset, self._key, self._value, self._error = (
            topic, partition, offset, key, value, error,
        )

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error


def _eof_error():
    from confluent_kafka import KafkaError

    return KafkaError(KafkaError._PARTITION_EOF)


class _FakeConsumer:
    """Single-partition fake consumer. ``poll()`` delivers every record in
    the shared log in order, then repeatedly emits a ``_PARTITION_EOF``
    marker (real confluent_kafka broker behavior with
    ``enable.partition.eof=True``) instead of ``None`` once caught up --
    exercising the same completion signal ``start()`` relies on (review
    finding M4), not a stale captured high-watermark offset."""

    def __init__(self, config: dict, log: _FakeTopicLog, topic_name: str = "t.dlq.parked-pairs") -> None:
        self._log = log
        self._topic_name = topic_name
        self._next_offset = 0
        self.closed = False

    def list_topics(self, topic=None, timeout=None):
        class _Item:
            error = None
            partitions = {0: object()}

        class _Metadata:
            topics = {self._topic_name: _Item()}

        return _Metadata()

    def assign(self, partitions) -> None:
        self._next_offset = 0

    def get_watermark_offsets(self, topic_partition, timeout=None):
        return (0, self._log.high_watermark())

    def poll(self, timeout=None):
        with self._log.lock:
            if self._next_offset < len(self._log.records):
                key, value = self._log.records[self._next_offset]
                message = _FakeMessage(self._topic_name, 0, self._next_offset, key, value)
                self._next_offset += 1
                return message
            return _FakeMessage(self._topic_name, 0, len(self._log.records), None, None, error=_eof_error())

    def close(self) -> None:
        self.closed = True


class _FakeProducer:
    def __init__(self, config: dict, log: _FakeTopicLog) -> None:
        self._log = log

    def produce(self, topic, key=None, value=None, **kwargs) -> None:
        self._log.append(key, value)

    def flush(self, *args, **kwargs) -> int:
        return 0


def _make_index(log: _FakeTopicLog, *, writable: bool = False, clock=lambda: 1_700_000_000_000) -> ParkedPairIndex:
    return ParkedPairIndex(
        topic="t.dlq.parked-pairs",
        bootstrap_servers="kafka:9092",
        writable=writable,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=lambda config: _FakeProducer(config, log),
        clock=clock,
        poll_timeout_seconds=0.01,
    )


def _seed(log: _FakeTopicLog, source_key, target_key, *, original_topic, original_partition, original_offset,
          first_failure_at_ms=1_700_000_000_000) -> None:
    key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset).encode("utf-8")
    value = json.dumps({"first_failure_at_ms": first_failure_at_ms}).encode("utf-8")
    log.append(key, value)


# ── is_blocked / rebuild-on-start ────────────────────────────────────────


def test_is_blocked_true_immediately_after_start_with_existing_outstanding_record() -> None:
    log = _FakeTopicLog()
    _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=5)
    index = _make_index(log, writable=False)
    index.start()
    try:
        assert index.is_blocked("a", "b") is True
        assert index.is_blocked("c", "d") is False
    finally:
        index.stop()


def test_is_blocked_false_with_no_records_at_all() -> None:
    log = _FakeTopicLog()
    index = _make_index(log, writable=False)
    index.start()
    try:
        assert index.is_blocked("a", "b") is False
    finally:
        index.stop()


# ── add/remove idempotency and tombstone-on-remove ─────────────────────────


def test_add_then_remove_publishes_tombstone_and_a_fresh_index_sees_unblocked() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        assert writer.is_blocked("a", "b") is True
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)
        assert writer.is_blocked("a", "b") is False
    finally:
        writer.stop()

    assert log.records[-1][1] is None  # the final record on the shared log is a tombstone

    fresh = _make_index(log, writable=False)
    fresh.start()
    try:
        assert fresh.is_blocked("a", "b") is False
    finally:
        fresh.stop()


def test_add_is_idempotent_for_the_same_record_identity() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        # Two adds of the identical record id -- still exactly one outstanding
        # record for the pair, not two (no counter to double-increment).
        assert writer.is_blocked("a", "b") is True
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)
        assert writer.is_blocked("a", "b") is False  # one remove clears it -- proves there was only ever one entry
    finally:
        writer.stop()


def test_remove_of_never_added_record_is_a_harmless_no_op() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)
        assert writer.is_blocked("a", "b") is False
    finally:
        writer.stop()


def test_remove_then_a_duplicate_remove_after_a_simulated_crash_stays_idempotent() -> None:
    """Review finding M2(b): a re-read/duplicate remove (e.g. after a crash
    and restart before the retry consumer's own offset committed) must
    never re-block or corrupt the pair's state."""
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)  # duplicate
        assert writer.is_blocked("a", "b") is False
    finally:
        writer.stop()


# ── is_blocked (live path) vs. is_blocked_by_earlier (retry path) ──────────


def test_live_path_blocks_on_any_outstanding_record_including_a_root_cause() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        # A later, individually-resolvable record for the same pair must be
        # diverted by the live path -- this is the exact cross-flush gap
        # P5-S3 handed to this slice.
        assert writer.is_blocked("a", "b") is True
    finally:
        writer.stop()


# ── contains(): duplicate-copy detection (review finding M12) ─────────────


def test_contains_true_while_the_specific_record_is_outstanding() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        assert writer.contains("a", "b", original_topic="knows", original_partition=0, original_offset=1) is True
    finally:
        writer.stop()


def test_contains_false_after_remove_the_duplicate_copy_signal() -> None:
    """The exact scenario M12 fixes: a duplicate copy of an already-
    resolved record must be recognized as no longer outstanding."""
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=1)
        # The (now-resolved) record's own identity is gone -- a duplicate
        # DLQ copy of it must be recognized as already handled.
        assert writer.contains("a", "b", original_topic="knows", original_partition=0, original_offset=1) is False
    finally:
        writer.stop()


def test_contains_false_for_a_record_never_added_at_all() -> None:
    """Covers the bulk-opt-in case (Decisions §1): a record that was never
    indexed at all reads the same as an already-resolved duplicate."""
    log = _FakeTopicLog()
    index = _make_index(log, writable=False)
    index.start()
    try:
        assert index.contains("x", "y", original_topic="knows", original_partition=0, original_offset=9) is False
    finally:
        index.stop()


def test_contains_distinguishes_between_two_records_of_the_same_pair() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=1_000)
        # A second, different record of the SAME pair was never added.
        assert writer.contains("a", "b", original_topic="knows", original_partition=0, original_offset=2) is False
        assert writer.contains("a", "b", original_topic="knows", original_partition=0, original_offset=1) is True
    finally:
        writer.stop()


def test_retry_path_never_blocks_a_record_on_its_own_presence() -> None:
    """Review finding M2: excluding a retried record's own count naively
    (a plain counter) reintroduces the ordering bug via re-DLQ-to-the-back.
    This index avoids that differently: is_blocked_by_earlier always
    excludes the record's own identity, and a re-DLQ never re-adds it (same
    identity, no new index entry) -- so the *only* record instance is
    always excluded from its own check, with no counter to under/overshoot."""
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        blocked = writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000,
        )
        assert blocked is False  # the only outstanding record for this pair IS itself
    finally:
        writer.stop()


def test_retry_path_blocks_only_on_a_strictly_earlier_record_same_partition() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        # R1 (offset 0, the root cause) and R2 (offset 1, blocked behind it)
        # are both outstanding.
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=2_000)

        # R2 (offset 1) IS blocked by R1 (offset 0, earlier in the same partition).
        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=2_000,
        ) is True

        # R1 (offset 0) is NOT blocked by R2 (offset 1, later) -- the root
        # cause itself must always be free to retry.
        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000,
        ) is False
    finally:
        writer.stop()


def test_retry_path_falls_back_to_first_failure_at_ms_across_different_source_partitions() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        # Same pair, but the two outstanding rows arrived on two different
        # source-topic partitions -- offsets aren't comparable across them,
        # so first_failure_at_ms is the documented best-effort fallback.
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=99, first_failure_at_ms=1_000)
        writer.add("a", "b", original_topic="knows", original_partition=1, original_offset=0, first_failure_at_ms=5_000)

        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=1, original_offset=0, first_failure_at_ms=5_000,
        ) is True  # the partition-0 record (first_failure_at_ms=1000) is earlier
        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=99, first_failure_at_ms=1_000,
        ) is False
    finally:
        writer.stop()


def test_root_re_dlq_does_not_unblock_a_later_record_of_the_same_pair() -> None:
    """The scenario the review flagged: R1 (root) is re-DLQ'd to the back
    of its partition (same identity, no index write) while R2 (behind it)
    is still outstanding. R2 must remain blocked throughout."""
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=2_000)

        # R1 is retried, still fails, and is re-DLQ'd -- no index write at all
        # (same record identity: original_topic/partition/offset unchanged).

        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=2_000,
        ) is True  # R2 is still correctly blocked behind R1

        # Only once R1 is durably written (or parked) does its entry clear.
        writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=0)
        assert writer.is_blocked_by_earlier(
            "a", "b", original_topic="knows", original_partition=0, original_offset=1, first_failure_at_ms=2_000,
        ) is False
    finally:
        writer.stop()


# ── writable gate ─────────────────────────────────────────────────────────


def test_read_only_instance_rejects_add_and_remove() -> None:
    log = _FakeTopicLog()
    reader = _make_index(log, writable=False)
    reader.start()
    try:
        with pytest.raises(ParkedPairIndexError, match="writable=True"):
            reader.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        with pytest.raises(ParkedPairIndexError, match="writable=True"):
            reader.remove("a", "b", original_topic="knows", original_partition=0, original_offset=0)
    finally:
        reader.stop()
    assert log.records == []  # neither call touched the topic


# ── Confirmed publish, fail-closed (review finding M3) ──────────────────────


class _FailingProducer:
    """Simulates a broker rejecting the publish via the delivery callback --
    the same failure shape ``DeadLetterProducer.publish`` guards against."""

    def __init__(self, config: dict, log: _FakeTopicLog) -> None:
        self._log = log

    def produce(self, topic, key=None, value=None, on_delivery=None, **kwargs) -> None:
        if on_delivery is not None:
            on_delivery(RuntimeError("simulated broker rejection"), None)

    def flush(self, *args, **kwargs) -> int:
        return 0


def test_add_raises_and_never_updates_the_local_view_when_publish_fails() -> None:
    log = _FakeTopicLog()
    writer = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=lambda config: _FailingProducer(config, log),
        poll_timeout_seconds=0.01,
    )
    writer.start()
    try:
        with pytest.raises(ParkedPairIndexError, match="failed"):
            writer.add("a", "b", original_topic="knows", original_partition=0, original_offset=0, first_failure_at_ms=1_000)
        # An add() the broker never confirmed must never make this replica
        # believe the pair is indexed -- every other replica still sees it
        # as unblocked, and this one must agree.
        assert writer.is_blocked("a", "b") is False
    finally:
        writer.stop()


def test_remove_raises_and_leaves_the_pair_blocked_locally_when_publish_fails() -> None:
    log = _FakeTopicLog()
    _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=0)
    writer = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=lambda config: _FailingProducer(config, log),
        poll_timeout_seconds=0.01,
    )
    writer.start()
    try:
        assert writer.is_blocked("a", "b") is True
        with pytest.raises(ParkedPairIndexError, match="failed"):
            writer.remove("a", "b", original_topic="knows", original_partition=0, original_offset=0)
        # A remove() the broker never confirmed must leave the pair blocked
        # locally too -- the safe failure direction (stuck blocked a little
        # longer, self-correcting once the retry consumer tries again) is
        # never "silently unblocked while every other replica disagrees".
        assert writer.is_blocked("a", "b") is True
    finally:
        writer.stop()


# ── EOF-based rebuild completion on a compacted topic (review finding M4) ───


class _StaleWatermarkConsumer(_FakeConsumer):
    """Reports a high watermark the log's actual records never reach --
    simulating a compacted-away tail (a tombstone past its
    delete.retention.ms, or a superseded key) so a rebuild that waited for
    "a message whose offset matches this watermark" would stall forever.
    start() must instead complete from the broker's own EOF marker."""

    def get_watermark_offsets(self, topic_partition, timeout=None):
        return (0, 5)  # far past the 2 real records the log actually holds


def test_start_completes_via_eof_even_when_the_watermark_is_stale_past_a_compacted_gap() -> None:
    log = _FakeTopicLog()
    _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=1)
    _seed(log, "c", "d", original_topic="knows", original_partition=0, original_offset=2)
    index = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=False,
        consumer_factory=lambda config: _StaleWatermarkConsumer(config, log),
        poll_timeout_seconds=0.01,
    )
    index.start()  # must not stall/raise despite a captured high (5) no offset ever reaches
    try:
        assert index.is_blocked("a", "b") is True
        assert index.is_blocked("c", "d") is True
    finally:
        index.stop()


# ── Non-blocking read barrier: watermark_snapshot/caught_up (M5) ────────────


def test_start_gives_both_consumers_a_group_id() -> None:
    """Review finding M6: a real confluent_kafka Consumer refuses to
    construct without group.id ("group.id must be set"), which the fake
    client in every other test here can't catch. This test inspects the
    config dict passed to consumer_factory directly, and each consumer
    must get its own unique id (never subscribed, never committed --
    assign() only -- but librdkafka still requires the field)."""
    log = _FakeTopicLog()
    configs: list[dict] = []

    def factory(config: dict):
        configs.append(config)
        return _FakeConsumer(config, log)

    index = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=False,
        consumer_factory=factory, poll_timeout_seconds=0.01,
    )
    index.start()
    try:
        assert len(configs) == 2  # self._consumer and self._watermark_consumer
        group_ids = [config.get("group.id") for config in configs]
        assert all(group_id for group_id in group_ids), group_ids
        assert len(set(group_ids)) == 2  # each consumer gets its own unique id
        for config in configs:
            assert config.get("enable.auto.commit") is False
    finally:
        index.stop()


def test_watermark_snapshot_requires_start_first() -> None:
    index = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=False,
    )
    with pytest.raises(ParkedPairIndexError, match="requires start"):
        index.watermark_snapshot()


def test_watermark_snapshot_raises_instead_of_hanging_on_a_broker_timeout() -> None:
    """Review finding M7: confluent_kafka's get_watermark_offsets blocks
    forever with no timeout. watermark_snapshot() is called from the retry
    consumer's own poll-loop thread (Story 5) -- a hang there would break
    the "never block poll()" rule. A bounded timeout must raise, not hang,
    so the caller can treat it as "not caught up this tick"."""
    log = _FakeTopicLog()

    class _TimingOutConsumer(_FakeConsumer):
        def get_watermark_offsets(self, topic_partition, timeout=None):
            raise Exception(f"simulated broker timeout after {timeout}s")

    # Only the *second* constructed consumer (the dedicated
    # watermark_snapshot() connection) times out -- the first (the
    # rebuild/poller consumer) must behave normally, so start() itself
    # succeeds and only the later watermark_snapshot() call fails.
    factory_calls = {"count": 0}

    def factory(config: dict):
        factory_calls["count"] += 1
        if factory_calls["count"] == 1:
            return _FakeConsumer(config, log)
        return _TimingOutConsumer(config, log)

    index = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=False,
        consumer_factory=factory,
        poll_timeout_seconds=0.01, watermark_timeout_seconds=2.5,
    )
    index.start()
    try:
        with pytest.raises(ParkedPairIndexError, match="timed out after 2.5s"):
            index.watermark_snapshot()
    finally:
        index.stop()


def test_caught_up_false_while_the_poller_lags_a_fresh_snapshot_then_true_once_applied() -> None:
    log = _FakeTopicLog()
    index = _make_index(log, writable=False)
    index.start()
    # Stop the background poller deterministically so this test controls
    # exactly when the local view advances, instead of depending on real
    # thread scheduling.
    index._stop_event.set()
    index._thread.join(timeout=2.0)
    try:
        # Another replica's add() lands on the topic; this instance's
        # poller has not observed it yet (it's stopped).
        _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=0)
        snapshot = index.watermark_snapshot()
        assert index.caught_up(snapshot) is False

        # Once applied (simulating the poller's next iteration), the
        # snapshot is caught up.
        message = index._consumer.poll(0.01)
        index._apply(message)
        assert index.caught_up(snapshot) is True
    finally:
        index.stop()


# ── Topic metadata errors ───────────────────────────────────────────────────


def test_start_raises_when_topic_has_no_partition_metadata() -> None:
    log = _FakeTopicLog()

    class _NoMetadataConsumer(_FakeConsumer):
        def list_topics(self, topic=None, timeout=None):
            class _Metadata:
                topics: dict = {}

            return _Metadata()

    index = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=False,
        consumer_factory=lambda config: _NoMetadataConsumer(config, log),
    )
    with pytest.raises(ParkedPairIndexError, match="no partition metadata"):
        index.start()


# ── Construction guards ─────────────────────────────────────────────────────


@pytest.mark.parametrize("field, value", [("topic", ""), ("bootstrap_servers", "  ")])
def test_construction_rejects_blank_required_fields(field: str, value: str) -> None:
    kwargs = {"topic": "t.dlq.parked-pairs", "bootstrap_servers": "kafka:9092"}
    kwargs[field] = value
    with pytest.raises(ParkedPairIndexError):
        ParkedPairIndex(**kwargs)


def test_pair_key_and_record_wire_key_are_order_sensitive_and_stable() -> None:
    assert _pair_key("a", "b") == _pair_key("a", "b")
    assert _pair_key("a", "b") != _pair_key("b", "a")
    assert _record_wire_key("a", "b", "knows", 0, 1) == _record_wire_key("a", "b", "knows", 0, 1)
    assert _record_wire_key("a", "b", "knows", 0, 1) != _record_wire_key("a", "b", "knows", 0, 2)


# ── remove_many: one flush, per-record results (retry drain throughput) ──────


class _DeliveringProducer(_FakeProducer):
    def __init__(self, config: dict, log: _FakeTopicLog) -> None:
        super().__init__(config, log)
        self.flushes = 0

    def produce(self, topic, key=None, value=None, on_delivery=None, **kwargs) -> None:
        super().produce(topic, key=key, value=value)
        on_delivery(None, None)

    def flush(self, *args, **kwargs) -> int:
        self.flushes += 1
        return 0


def test_remove_many_tombstones_every_record_with_one_flush_and_unblocks_the_pairs() -> None:
    log = _FakeTopicLog()
    for offset in range(3):
        _seed(log, f"a{offset}", "b", original_topic="knows", original_partition=0, original_offset=offset)
    producers = []

    def producer_factory(config):
        producers.append(_DeliveringProducer(config, log))
        return producers[-1]

    writer = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=producer_factory, poll_timeout_seconds=0.01,
    )
    writer.start()
    try:
        assert all(writer.is_blocked(f"a{i}", "b") for i in range(3))
        results = writer.remove_many([(f"a{i}", "b", "knows", 0, i) for i in range(3)])
        assert results == [None, None, None]
        assert producers[0].flushes == 1
        assert not any(writer.is_blocked(f"a{i}", "b") for i in range(3))
    finally:
        writer.stop()


def test_remove_many_leaves_unconfirmed_records_blocked_locally() -> None:
    log = _FakeTopicLog()
    _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=0)
    writer = ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=lambda config: _FailingProducer(config, log), poll_timeout_seconds=0.01,
    )
    writer.start()
    try:
        (result,) = writer.remove_many([("a", "b", "knows", 0, 0)])
        assert isinstance(result, ParkedPairIndexError)
        assert writer.is_blocked("a", "b") is True  # the safe failure direction
    finally:
        writer.stop()


def test_blocking_order_is_a_strict_total_order_even_under_clock_skew() -> None:
    """Offset order within a partition, wall-clock across partitions was not
    transitive: A<C (offset), C<B (time), B<A (time) cycled, so every record
    was blocked by another and re-DLQ'd forever. Now exactly one is unblocked."""
    log = _FakeTopicLog()
    records = {
        "A": (0, 1, 300), "B": (1, 1, 200), "C": (0, 2, 100),  # (partition, offset, first_failure_at_ms)
    }
    for partition, offset, failed_at in records.values():
        _seed(log, "a", "b", original_topic="knows", original_partition=partition, original_offset=offset,
              first_failure_at_ms=failed_at)
    index = _make_index(log, writable=False)
    index.start()
    try:
        blocked = {
            name: index.is_blocked_by_earlier(
                "a", "b", original_topic="knows", original_partition=partition, original_offset=offset,
                first_failure_at_ms=failed_at,
            )
            for name, (partition, offset, failed_at) in records.items()
        }
    finally:
        index.stop()
    assert blocked == {"A": False, "B": True, "C": True}  # one head of line; the others wait behind it


# ── Poll-loop error backoff (P5-S4 review N9) ───────────────────────────────


class _ScriptedPollConsumer:
    """Delivers ``script`` one item per ``poll()``: an exception is raised, anything else returned."""

    def __init__(self, script, stop) -> None:
        self._script, self._stop = list(script), stop

    def poll(self, timeout=None):
        if not self._script:
            self._stop.set()
            return None
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _RecordingStop:
    def __init__(self) -> None:
        self.waits: list[float] = []
        self._set = False

    def set(self) -> None:
        self._set = True

    def is_set(self) -> bool:
        return self._set

    def wait(self, timeout=None) -> bool:
        self.waits.append(timeout)
        return self._set


def test_poll_loop_backs_off_exponentially_capped_on_errors_and_resets_after_a_good_poll() -> None:
    class _BrokerError:
        def code(self):
            return -195  # anything but _PARTITION_EOF

    class _ErrorMessage:
        def error(self):
            return _BrokerError()

    stop = _RecordingStop()
    boom = RuntimeError("poll failed")
    script = [boom, _ErrorMessage(), boom, None] + [boom] * 8
    index = ParkedPairIndex(topic="knows.idx", bootstrap_servers="unused")
    index._stop_event = stop
    index._consumer = _ScriptedPollConsumer(script, stop)
    index._poll_forever()
    # 0.5, 1, 2 -- a clean poll resets -- 0.5, then doubling again up to the 30 s cap
    assert stop.waits == [0.5, 1.0, 2.0, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


# ── v7-S4: one index per multi-topic type, causal order across topics ─────


def _make_sequenced(log: _FakeTopicLog, *, writable: bool = True) -> ParkedPairIndex:
    return ParkedPairIndex(
        topic="t.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=writable, sequenced=True,
        consumer_factory=lambda config: _FakeConsumer(config, log),
        producer_factory=lambda config: _FakeProducer(config, log),
        clock=lambda: 1_700_000_000_000, poll_timeout_seconds=0.01,
    )


def _wait_caught_up(index: ParkedPairIndex) -> None:
    import time as _time

    snapshot = index.watermark_snapshot()
    deadline = _time.monotonic() + 5
    while not index.caught_up(snapshot):
        assert _time.monotonic() < deadline, "index poller did not catch up"
        _time.sleep(0.01)


def _ident(topic, offset, partition=0):
    return dict(original_topic=topic, original_partition=partition, original_offset=offset)


def test_unsequenced_index_wire_value_is_unchanged() -> None:
    log = _FakeTopicLog()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("a", "b", **_ident("knows", 3), first_failure_at_ms=1_000)
    finally:
        writer.stop()
    assert log.records[-1][1] == b'{"first_failure_at_ms":1000}'


def test_pair_parked_from_topic_b_blocks_its_later_record_from_topic_a_in_arrival_order() -> None:
    """Topic names sort a < b, but b's record arrived (was indexed) first: it must stay first."""
    log = _FakeTopicLog()
    writer = _make_sequenced(log)
    writer.start()
    try:
        writer.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1_000)
        # live path: the later record of the pair from topic a is blocked (pair key has no topic)
        assert writer.is_blocked("p", "c") is True
        writer.add("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=1_000)
        assert writer.is_blocked_by_earlier("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=0) is True
        assert writer.is_blocked_by_earlier("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=0) is False
        assert writer.earlier_outstanding("p", "c", **_ident("works-at-a", 2)) == [("works-at-b", 0, 40)]
        assert writer.earlier_outstanding("p", "c", **_ident("works-at-b", 40)) == []
        assert writer.order_key("p", "c", **_ident("works-at-b", 40)) == (1, "works-at-b", 0, 40)
        assert writer.order_key("p", "c", **_ident("works-at-a", 2)) == (2, "works-at-a", 0, 2)
        # a record that is not indexed orders like its identity (seq 0)
        assert writer.order_key("p", "c", **_ident("works-at-a", 99)) == (0, "works-at-a", 0, 99)
    finally:
        writer.stop()
    assert json.loads(log.records[0][1]) == {"first_failure_at_ms": 1_000, "seq": 1}


def test_sequence_survives_a_rebuild_and_a_re_add_keeps_its_seq() -> None:
    log = _FakeTopicLog()
    writer = _make_sequenced(log)
    writer.start()
    try:
        writer.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1)
        writer.add("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=2)
        writer.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1)  # duplicate add after a replay
        assert writer.order_key("p", "c", **_ident("works-at-b", 40))[0] == 1
    finally:
        writer.stop()
    reader = _make_sequenced(log, writable=False)
    reader.start()
    try:
        assert reader.earlier_outstanding("p", "c", **_ident("works-at-a", 2)) == [("works-at-b", 0, 40)]
        assert reader.is_blocked_by_earlier("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=0) is False
    finally:
        reader.stop()


def test_a_later_add_sorts_after_every_outstanding_record_of_the_pair() -> None:
    log = _FakeTopicLog()
    writer = _make_sequenced(log)
    writer.start()
    try:
        writer.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1)
        writer.add("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=2)
        writer.remove("p", "c", **_ident("works-at-b", 40))
        writer.add("p", "c", **_ident("works-at-b", 41), first_failure_at_ms=3)
        assert writer.earlier_outstanding("p", "c", **_ident("works-at-b", 41)) == [("works-at-a", 0, 2)]
        # other pairs have their own sequence
        writer.add("q", "c", **_ident("works-at-a", 1), first_failure_at_ms=4)
        assert writer.order_key("q", "c", **_ident("works-at-a", 1))[0] == 1
    finally:
        writer.stop()


def test_a_second_replica_sequences_after_records_it_has_applied() -> None:
    """Two live replicas own different topics of the type and share the index topic."""
    log = _FakeTopicLog()
    replica_a, replica_b = _make_sequenced(log), _make_sequenced(log)
    replica_a.start()
    replica_b.start()
    try:
        replica_b.add("p", "c", **_ident("works-at-b", 7), first_failure_at_ms=1)
        _wait_caught_up(replica_a)
        assert replica_a.is_blocked("p", "c") is True
        replica_a.add("p", "c", **_ident("works-at-a", 0), first_failure_at_ms=2)
        _wait_caught_up(replica_b)
        for index in (replica_a, replica_b):
            assert index.earlier_outstanding("p", "c", **_ident("works-at-a", 0)) == [("works-at-b", 0, 7)]
            assert index.is_blocked_by_earlier("p", "c", **_ident("works-at-b", 7), first_failure_at_ms=0) is False
    finally:
        replica_a.stop()
        replica_b.stop()


def test_sequenced_index_reads_entries_written_without_a_seq() -> None:
    log = _FakeTopicLog()
    _seed(log, "p", "c", original_topic="works-at-b", original_partition=0, original_offset=5)
    index = _make_sequenced(log)
    index.start()
    try:
        assert index.order_key("p", "c", **_ident("works-at-b", 5)) == (0, "works-at-b", 0, 5)
        index.add("p", "c", **_ident("works-at-a", 1), first_failure_at_ms=1)
        assert index.earlier_outstanding("p", "c", **_ident("works-at-a", 1)) == [("works-at-b", 0, 5)]
    finally:
        index.stop()


def test_unsequenced_add_ties_with_sequenced_entries_left_from_a_multi_topic_past() -> None:
    """The type went from two topics back to one (dead_letter.topic kept): a later
    record must not sort before the outstanding sequenced ones."""
    log = _FakeTopicLog()
    old = _make_sequenced(log)
    old.start()
    try:
        old.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1)
    finally:
        old.stop()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("p", "c", **_ident("works-at-b", 41), first_failure_at_ms=2)
        assert writer.order_key("p", "c", **_ident("works-at-b", 41))[0] == 2
        assert writer.earlier_outstanding("p", "c", **_ident("works-at-b", 41)) == [("works-at-b", 0, 40)]
    finally:
        writer.stop()
    assert json.loads(log.records[-1][1]) == {"first_failure_at_ms": 2, "seq": 2}


def test_after_topic_removal_a_later_record_sorts_after_leftovers_whatever_its_topic_name() -> None:
    """v7-S4 review S2 (pT_tie): type [a, b] -> [a]; b's leftover entry has seq 1. A later
    a-record DLQ'd behind it by an unsequenced writer must not tie and sort first by name."""
    log = _FakeTopicLog()
    old = _make_sequenced(log)
    old.start()
    try:
        old.add("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1)
    finally:
        old.stop()
    writer = _make_index(log, writable=True)
    writer.start()
    try:
        writer.add("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=2)
        assert writer.is_blocked_by_earlier("p", "c", **_ident("works-at-a", 2), first_failure_at_ms=2) is True
        assert writer.is_blocked_by_earlier("p", "c", **_ident("works-at-b", 40), first_failure_at_ms=1) is False
        assert writer.earlier_outstanding("p", "c", **_ident("works-at-b", 40)) == []
        # a pair without sequenced leftovers keeps seq 0 (single-topic wire bytes unchanged)
        writer.add("q", "c", **_ident("works-at-a", 3), first_failure_at_ms=3)
    finally:
        writer.stop()
    assert log.records[-1][1] == b'{"first_failure_at_ms":3}'
