"""Story 7: reading a DLQ topic and republishing matching envelopes."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition

from src.cli import build_parser, handle_replay_dlq
from src.loader.dead_letter import (
    SCHEMA_VERSION,
    DeadLetterEnvelope,
    LoaderIdentity,
    encode_envelope,
)
from src.loader.dead_letter_replay import (
    DeadLetterReplayError,
    ReplayFilter,
    ReplaySummary,
    run_replay,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


_NEXT_OFFSET = iter(range(10**9))


def _envelope(**overrides) -> DeadLetterEnvelope:
    """Each call is a distinct record (distinct ``original_offset``) unless overridden: replay
    collapses copies of one record identity to its latest DLQ copy (v7-S4 review S3)."""
    fields = dict(
        schema_version=SCHEMA_VERSION,
        failure_class="malformed",
        entity_kind="node",
        entity_name="Person",
        original_topic="person-events",
        original_partition=0,
        original_offset=next(_NEXT_OFFSET),
        key_bytes=b"key",
        value_bytes=b"value",
        reason_code="invalid_json",
        error_text="bad record",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="node"),
        first_failure_at_ms=1_000,
        attempt_count=0,
        dlq_routed_at_ms=1_000,
        retry_after_ms=None,
    )
    fields.update(overrides)
    return DeadLetterEnvelope(**fields)


class _FakeMessage:
    def __init__(self, topic, partition, offset, value, key=None):
        self._topic, self._partition, self._offset, self._value, self._key = (
            topic, partition, offset, value, key,
        )

    def error(self):
        return None

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key


class _ErrorMessage:
    def __init__(self, error):
        self._error = error

    def error(self):
        return self._error


class _FakeConsumer:
    """A minimal fake covering list_topics/watermarks/offsets_for_times/assign/poll.

    ``logs`` maps partition -> ordered list of (offset, key_bytes, envelope).
    ``timestamps`` maps (partition, offset) -> first_failure_at_ms, used by
    offsets_for_times to resolve --since.
    """

    def __init__(self, config, *, logs: dict[int, list[tuple[int, bytes | None, object]]]):
        self.config = config
        self.logs = logs
        self.positions: dict[int, int] = {}
        self.topic_name: str | None = None
        self.closed = False
        self.committed_offsets: list = []

    def list_topics(self, topic=None, timeout=None):
        self.topic_name = topic
        return SimpleNamespace(
            topics={topic: SimpleNamespace(partitions={p: None for p in self.logs}, error=None)}
        )

    def get_watermark_offsets(self, tp, timeout=None):
        entries = self.logs.get(tp.partition, [])
        low = entries[0][0] if entries else 0
        high = (entries[-1][0] + 1) if entries else 0
        return low, high

    def offsets_for_times(self, tps, timeout=None):
        results = []
        for tp in tps:
            since_ms = tp.offset
            found = None
            for offset, _key, envelope in self.logs.get(tp.partition, []):
                if envelope.first_failure_at_ms >= since_ms:
                    found = offset
                    break
            results.append(
                TopicPartition(tp.topic, tp.partition, found if found is not None else -1)
            )
        return results

    def assign(self, tps):
        for tp in tps:
            self.positions[tp.partition] = tp.offset

    def poll(self, timeout=None):
        for partition in sorted(self.positions):
            pos = self.positions[partition]
            entry = next((e for e in self.logs.get(partition, []) if e[0] == pos), None)
            if entry is not None:
                offset, key, envelope = entry
                self.positions[partition] = offset + 1
                return _FakeMessage(self.topic_name, partition, offset, encode_envelope(envelope), key)
        return None

    def commit(self, *args, **kwargs):  # pragma: no cover - must never be called
        self.committed_offsets.append((args, kwargs))

    def close(self):
        self.closed = True


def _logs_from_envelopes(envelopes: list[DeadLetterEnvelope]) -> dict[int, list]:
    by_partition: dict[int, list] = {}
    for envelope in envelopes:
        by_partition.setdefault(envelope.original_partition, []).append(envelope)
    logs = {}
    for partition, items in by_partition.items():
        logs[partition] = [(offset, item.key_bytes, item) for offset, item in enumerate(items)]
    return logs


def _consumer_factory_for(logs):
    def factory(config):
        return _FakeConsumer(config, logs=logs)
    return factory


def test_reason_code_filter_selects_exact_subset() -> None:
    envelopes = [
        _envelope(reason_code="invalid_json", original_partition=0),
        _envelope(reason_code="missing_required", original_partition=0),
        _envelope(reason_code="invalid_json", original_partition=0),
    ]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="person-events.dlq", target_topic="person-events", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(reason_code="invalid_json"), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary == ReplaySummary(matched=2, replayed=0, dry_run=True)


def test_time_range_filter_selects_exact_subset() -> None:
    envelopes = [
        _envelope(first_failure_at_ms=100, original_partition=0),
        _envelope(first_failure_at_ms=200, original_partition=0),
        _envelope(first_failure_at_ms=300, original_partition=0),
    ]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(since_ms=150, until_ms=250), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary.matched == 1


def test_limit_caps_matching_not_scanned() -> None:
    envelopes = [_envelope(original_partition=0) for _ in range(5)]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(limit=2), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary.matched == 2


def test_filters_across_multiple_partitions() -> None:
    envelopes = [
        _envelope(reason_code="invalid_json", original_partition=0),
        _envelope(reason_code="missing_required", original_partition=1),
        _envelope(reason_code="invalid_json", original_partition=1),
    ]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(reason_code="invalid_json"), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary.matched == 2


def test_dry_run_makes_zero_produce_calls_under_every_filter() -> None:
    envelopes = [_envelope(reason_code="invalid_json", original_partition=0) for _ in range(3)]
    logs = _logs_from_envelopes(envelopes)
    fake_producer = MagicMock()

    def producer_factory(config):
        return fake_producer

    for replay_filter in (
        ReplayFilter(), ReplayFilter(reason_code="invalid_json"), ReplayFilter(limit=1),
        ReplayFilter(since_ms=0, until_ms=10_000_000),
    ):
        run_replay(
            dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
            replay_filter=replay_filter, dry_run=True,
            consumer_factory=_consumer_factory_for(logs), producer_factory=producer_factory,
        )
    fake_producer.produce.assert_not_called()


def test_live_replay_reproduces_exact_key_and_value_bytes() -> None:
    envelope = _envelope(key_bytes=b"the-key", value_bytes=b"the-original-value", original_partition=0)
    logs = _logs_from_envelopes([envelope])
    fake_producer = MagicMock()

    def produce(topic, key, value, on_delivery):
        on_delivery(None, MagicMock())

    fake_producer.produce.side_effect = produce
    fake_producer.flush.return_value = 0

    summary = run_replay(
        dlq_topic="t.dlq", target_topic="target-topic", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(logs),
        producer_factory=lambda config: fake_producer,
    )

    assert summary == ReplaySummary(matched=1, replayed=1, dry_run=False)
    fake_producer.produce.assert_called_once()
    args, kwargs = fake_producer.produce.call_args
    assert args[0] == "target-topic"
    assert kwargs["key"] == b"the-key"
    assert kwargs["value"] == b"the-original-value"


def test_live_replay_only_reports_success_after_every_record_acknowledged() -> None:
    envelopes = [_envelope(original_partition=0) for _ in range(2)]
    logs = _logs_from_envelopes(envelopes)
    fake_producer = MagicMock()

    def produce(topic, key, value, on_delivery):
        on_delivery(None, MagicMock())

    fake_producer.produce.side_effect = produce
    fake_producer.flush.return_value = 0

    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(logs),
        producer_factory=lambda config: fake_producer,
    )
    assert summary.replayed == 2


def test_mid_batch_publish_failure_stops_and_reports_partial_progress(caplog) -> None:
    envelopes = [_envelope(original_partition=0) for _ in range(3)]
    logs = _logs_from_envelopes(envelopes)
    fake_producer = MagicMock()
    call_count = {"n": 0}

    def produce(topic, key, value, on_delivery):
        call_count["n"] += 1
        if call_count["n"] == 2:
            on_delivery(RuntimeError("broker down"), MagicMock())
        else:
            on_delivery(None, MagicMock())

    fake_producer.produce.side_effect = produce
    fake_producer.flush.return_value = 0

    with pytest.raises(DeadLetterReplayError):
        run_replay(
            dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=False,
            consumer_factory=_consumer_factory_for(logs),
            producer_factory=lambda config: fake_producer,
        )

    # Exactly one record replayed successfully before the failure; the third
    # is never attempted.
    assert fake_producer.produce.call_count == 2


def test_dlq_topic_offsets_never_committed_dry_run_or_live() -> None:
    envelope = _envelope(original_partition=0)
    logs = _logs_from_envelopes([envelope])
    captured_consumers = []

    def consumer_factory(config):
        assert config.get("enable.auto.commit") is False
        consumer = _FakeConsumer(config, logs=logs)
        captured_consumers.append(consumer)
        return consumer

    fake_producer = MagicMock()

    def produce(topic, key, value, on_delivery):
        on_delivery(None, MagicMock())

    fake_producer.produce.side_effect = produce
    fake_producer.flush.return_value = 0

    run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=True, consumer_factory=consumer_factory,
    )
    run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False, consumer_factory=consumer_factory,
        producer_factory=lambda config: fake_producer,
    )

    assert len(captured_consumers) == 2
    for consumer in captured_consumers:
        assert consumer.committed_offsets == []
        assert consumer.closed is True


def test_since_resolves_via_offsets_for_times() -> None:
    envelopes = [
        _envelope(first_failure_at_ms=100, original_partition=0),
        _envelope(first_failure_at_ms=200, original_partition=0),
        _envelope(first_failure_at_ms=300, original_partition=0),
    ]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(since_ms=200), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    # Only offsets 1 and 2 (first_failure_at_ms 200, 300) are read at all.
    assert summary.matched == 2


def test_since_with_no_message_at_or_after_timestamp_reads_nothing() -> None:
    envelopes = [_envelope(first_failure_at_ms=100, original_partition=0)]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(since_ms=999_999), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary.matched == 0


def test_offset_gap_in_dlq_topic_still_completes_the_scan() -> None:
    """Progress is tracked by offset, not a per-partition message count.

    Offsets 0 and 2 exist; offset 1 is a gap (e.g. a deleted record or a
    transactional control marker from an unrelated producer). The scan
    must still terminate once the position reaches the high watermark.
    """
    envelope0 = _envelope(original_partition=0, first_failure_at_ms=1)
    envelope2 = _envelope(original_partition=0, first_failure_at_ms=2)
    entries = [(0, None, envelope0), (2, None, envelope2)]

    def consumer_factory(config):
        fake = _FakeConsumer(config, logs={0: entries})
        fake.get_watermark_offsets = lambda tp, timeout=None: (0, 3)  # high=3; offset 1 is missing
        cursor = {"index": 0}

        def poll(timeout=None):
            if cursor["index"] >= len(entries):
                return None
            offset, key, env = entries[cursor["index"]]
            cursor["index"] += 1
            return _FakeMessage(fake.topic_name, 0, offset, encode_envelope(env), key)

        fake.poll = poll
        return fake

    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=True,
        consumer_factory=consumer_factory,
    )
    assert summary.matched == 2


def test_offset_gap_right_before_boundary_never_reads_past_the_end_watermark() -> None:
    """A gap right before the captured high watermark must not let a

    message that arrived after the boundary (at or past ``ends[p]``) leak
    into the scan.
    """
    envelope0 = _envelope(original_partition=0, first_failure_at_ms=1)
    envelope_past_boundary = _envelope(original_partition=0, first_failure_at_ms=999)
    # High watermark is 2 (only offset 0 was in range at scan start); offset
    # 1 is a gap, and offset 2 arrived after the boundary was captured.
    entries = [(0, None, envelope0), (2, None, envelope_past_boundary)]

    def consumer_factory(config):
        fake = _FakeConsumer(config, logs={0: entries})
        fake.get_watermark_offsets = lambda tp, timeout=None: (0, 2)
        cursor = {"index": 0}

        def poll(timeout=None):
            if cursor["index"] >= len(entries):
                return None
            offset, key, env = entries[cursor["index"]]
            cursor["index"] += 1
            return _FakeMessage(fake.topic_name, 0, offset, encode_envelope(env), key)

        fake.poll = poll
        return fake

    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=True,
        consumer_factory=consumer_factory,
    )
    assert summary.matched == 1


def test_stall_with_no_progress_raises_instead_of_hanging_forever() -> None:
    """A scan that never receives a message (broker unreachable, or a gap

    that can never be filled) must raise, not spin forever.
    """
    def consumer_factory(config):
        fake = _FakeConsumer(config, logs={0: []})
        fake.get_watermark_offsets = lambda tp, timeout=None: (0, 1)  # expects a message that never arrives
        fake.poll = lambda timeout=None: None
        return fake

    with pytest.raises(DeadLetterReplayError, match="no progress"):
        run_replay(
            dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=True,
            consumer_factory=consumer_factory,
            timeout=0.001, stall_timeout=0.02,
        )


def test_entity_filter_excludes_envelopes_from_a_different_entity() -> None:
    envelopes = [
        _envelope(entity_kind="node", entity_name="Person", original_partition=0),
        _envelope(entity_kind="node", entity_name="Company", original_partition=0),
    ]
    logs = _logs_from_envelopes(envelopes)
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(entity_kind="node", entity_name="Person"), dry_run=True,
        consumer_factory=_consumer_factory_for(logs),
    )
    assert summary.matched == 1


def test_live_replay_skips_envelope_with_no_payload() -> None:
    envelope = _envelope(value_bytes=None, reason_code="empty_payload", original_partition=0)
    logs = _logs_from_envelopes([envelope])
    fake_producer = MagicMock()

    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(logs),
        producer_factory=lambda config: fake_producer,
    )

    fake_producer.produce.assert_not_called()
    assert summary == ReplaySummary(matched=1, replayed=0, dry_run=False)


def test_live_replay_producer_config_raises_message_max_bytes() -> None:
    envelope = _envelope(original_partition=0)
    logs = _logs_from_envelopes([envelope])
    captured_configs = []

    def produce(topic, key, value, on_delivery):
        on_delivery(None, MagicMock())

    fake_producer = MagicMock()
    fake_producer.produce.side_effect = produce
    fake_producer.flush.return_value = 0

    def producer_factory(config):
        captured_configs.append(config)
        return fake_producer

    run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(logs), producer_factory=producer_factory,
    )

    assert captured_configs[0]["message.max.bytes"] >= 1_000_000
    assert captured_configs[0]["allow.auto.create.topics"] is False


def test_empty_dlq_topic_matches_nothing() -> None:
    summary = run_replay(
        dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=True,
        consumer_factory=_consumer_factory_for({0: []}),
    )
    assert summary == ReplaySummary(matched=0, replayed=0, dry_run=True)


def test_unreadable_envelope_raises_replay_error() -> None:
    def consumer_factory(config):
        fake = _FakeConsumer(config, logs={0: []})
        fake.topic_name = "t.dlq"
        fake.positions = {0: 0}
        fake.poll = lambda timeout=None: _FakeMessage("t.dlq", 0, 0, b"not a valid envelope")
        fake.get_watermark_offsets = lambda tp, timeout=None: (0, 1)
        return fake

    with pytest.raises(DeadLetterReplayError):
        run_replay(
            dlq_topic="t.dlq", target_topic="t", bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=True, consumer_factory=consumer_factory,
        )


# ── CLI wiring (handle_replay_dlq) ───────────────────────────────────────────


def _cli_schema(node_dead_letter=None, edge_dead_letter=None):

    edge = SimpleNamespace(
        type="WORKS_AT", topic="works-at-events", topics=["works-at-events"], dead_letter_entity="works-at-events",
        is_multi_topic=False, conflict_rule="single", dead_letter=edge_dead_letter,
    )
    return SimpleNamespace(
        nodes=[SimpleNamespace(label="Person", topic="person-events", dead_letter=node_dead_letter)],
        edges=[edge],
        edge_for_topic=lambda topic: edge if topic in edge.topics else None,
    )


def test_replay_dlq_parser_requires_exactly_one_entity_selector() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["replay-dlq"])
    with pytest.raises(SystemExit):
        parser.parse_args(["replay-dlq", "--node-label", "Person", "--edge-type", "WORKS_AT"])


def test_handle_replay_dlq_resolves_node_topic_and_defaults(monkeypatch) -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=3, replayed=3, dry_run=False))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person"])
        assert handle_replay_dlq(args) == 0

    call_kwargs = mock_run_replay.call_args.kwargs
    assert call_kwargs["dlq_topic"] == "person-events.dlq"
    assert call_kwargs["target_topic"] == "person-events"
    assert call_kwargs["dry_run"] is False


def test_handle_replay_dlq_resolves_edge_topic_and_target_topic_override() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=True, topic="dlq-works-at"))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args([
            "replay-dlq", "--edge-type", "WORKS_AT", "--target-topic", "staging-works-at", "--dry-run",
        ])
        assert handle_replay_dlq(args) == 0

    call_kwargs = mock_run_replay.call_args.kwargs
    assert call_kwargs["dlq_topic"] == "dlq-works-at"
    assert call_kwargs["target_topic"] == "staging-works-at"
    assert call_kwargs["dry_run"] is True


def test_handle_replay_dlq_rejects_entity_without_dead_letter_enabled() -> None:
    schema = _cli_schema(node_dead_letter=None)
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person"])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_resolves_topic_even_when_dead_letter_disabled() -> None:
    """Reversibility: an entity flipped back to enabled=false must still be

    able to replay a backlog left in its DLQ topic from before the flip
    (docs/USAGE.md section 10 promises the switch is reversible).
    """
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=False, topic="person-events.dlq"))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person", "--dry-run"])
        assert handle_replay_dlq(args) == 0

    assert mock_run_replay.call_args.kwargs["dlq_topic"] == "person-events.dlq"


def test_handle_replay_dlq_accepts_dlq_topic_override_with_no_schema_block() -> None:
    schema = _cli_schema(node_dead_letter=None)
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args([
            "replay-dlq", "--node-label", "Person", "--dlq-topic", "orphaned.dlq", "--dry-run",
        ])
        assert handle_replay_dlq(args) == 0

    assert mock_run_replay.call_args.kwargs["dlq_topic"] == "orphaned.dlq"


def test_handle_replay_dlq_parked_resolves_parking_lot_topic() -> None:
    """P5-S4 Story 8: --parked resolves parked_topic_name(...) instead of
    the ordinary DLQ topic, and a replayed parked record still republishes
    onto the original entity topic (fresh attempt_count=0 re-entry)."""
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=True))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=1, replayed=1, dry_run=False))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args(["replay-dlq", "--edge-type", "WORKS_AT", "--parked"])
        assert handle_replay_dlq(args) == 0

    call_kwargs = mock_run_replay.call_args.kwargs
    assert call_kwargs["dlq_topic"] == "works-at-events.dlq.parked"
    assert call_kwargs["target_topic"] == "works-at-events"


def test_handle_replay_dlq_parked_respects_explicit_parked_topic_override() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=True, parked_topic="custom.parked"))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args(["replay-dlq", "--edge-type", "WORKS_AT", "--parked", "--dry-run"])
        assert handle_replay_dlq(args) == 0

    assert mock_run_replay.call_args.kwargs["dlq_topic"] == "custom.parked"


def test_handle_replay_dlq_parked_is_ignored_when_dlq_topic_override_given() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=True))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args([
            "replay-dlq", "--edge-type", "WORKS_AT", "--parked", "--dlq-topic", "orphaned.dlq", "--dry-run",
        ])
        assert handle_replay_dlq(args) == 0

    assert mock_run_replay.call_args.kwargs["dlq_topic"] == "orphaned.dlq"


def test_handle_replay_dlq_parked_rejects_dead_letter_disabled() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=False, topic="works-at-events.dlq"))
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args(["replay-dlq", "--edge-type", "WORKS_AT", "--parked"])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_rejects_until_earlier_than_since() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args([
            "replay-dlq", "--node-label", "Person",
            "--since", "2024-06-01T00:00:00Z", "--until", "2024-01-01T00:00:00Z",
        ])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_rejects_unknown_entity() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "NoSuchLabel"])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_passes_reason_code_and_time_filters() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        args = build_parser().parse_args([
            "replay-dlq", "--node-label", "Person", "--reason-code", "invalid_json",
            "--since", "2024-01-01T00:00:00Z", "--until", "2024-06-01T00:00:00Z",
            "--limit", "10", "--dry-run",
        ])
        assert handle_replay_dlq(args) == 0

    replay_filter = mock_run_replay.call_args.kwargs["replay_filter"]
    assert replay_filter.reason_code == "invalid_json"
    assert replay_filter.limit == 10
    assert replay_filter.since_ms is not None and replay_filter.until_ms is not None
    assert replay_filter.since_ms < replay_filter.until_ms


def test_handle_replay_dlq_rejects_invalid_since_timestamp() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person", "--since", "not-a-date"])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_rejects_nonpositive_limit() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    with patch("src.cli.load_schema", return_value=schema):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person", "--limit", "0"])
        assert handle_replay_dlq(args) == 1


def test_handle_replay_dlq_propagates_replay_error_as_exit_1() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(node_dead_letter=DeadLetterConfig(enabled=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", side_effect=DeadLetterReplayError("boom")):
        args = build_parser().parse_args(["replay-dlq", "--node-label", "Person"])
        assert handle_replay_dlq(args) == 1


# ── v7-S4: one DLQ for a relationship type with several topics ──────────────


def _edge_envelope(topic: str, offset: int, value: bytes) -> DeadLetterEnvelope:
    return _envelope(
        entity_kind="edge", entity_name="WORKS_AT", original_topic=topic, original_partition=0,
        original_offset=offset, failure_class="missing_endpoint", reason_code="missing_source",
        retry_after_ms=2_000, key_bytes=b'["p1","c1"]', value_bytes=value,
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
    )


def _type_dlq_logs(envelopes):
    """All envelopes in ONE DLQ partition (the pair key), in DLQ order."""
    return {0: [(offset, e.key_bytes, e) for offset, e in enumerate(envelopes)]}


def _acking_producer():
    producer = MagicMock()
    producer.produce.side_effect = lambda topic, key, value, on_delivery: on_delivery(None, MagicMock())
    producer.flush.return_value = 0
    return producer


def test_multi_topic_replay_republishes_each_envelope_to_its_original_topic() -> None:
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("works-at-b", 9, b"b9"),
                 _edge_envelope("works-at-a", 6, b"a6")]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert summary == ReplaySummary(matched=3, replayed=3, dry_run=False)
    assert [(c.args[0], c.kwargs["value"]) for c in producer.produce.call_args_list] == [
        ("works-at-a", b"a5"), ("works-at-b", b"b9"), ("works-at-a", b"a6"),
    ]


def test_source_topic_filter_replays_only_that_topics_envelopes() -> None:
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("works-at-b", 9, b"b9")]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(original_topics=frozenset({"works-at-b"})), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert summary.matched == 1 and summary.replayed == 1
    assert [c.args[0] for c in producer.produce.call_args_list] == ["works-at-b"]


@pytest.mark.parametrize("dry_run", [True, False])
def test_unknown_original_topic_aborts_with_a_clear_error(dry_run) -> None:
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("retired-topic", 7, b"x")]
    producer = _acking_producer()
    with pytest.raises(DeadLetterReplayError, match="original_topic='retired-topic'.*0 envelope.*--target-topic"):
        run_replay(
            dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=dry_run,
            consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
            producer_factory=lambda config: producer,
            original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
        )
    producer.produce.assert_not_called()  # validated before anything is published


def test_source_topic_can_leave_out_a_topic_the_type_no_longer_reads() -> None:
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("retired-topic", 7, b"x")]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(original_topics=frozenset({"works-at-a"})), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert summary.replayed == 1
    assert [c.args[0] for c in producer.produce.call_args_list] == ["works-at-a"]


def test_missing_target_topic_fails_closed_before_its_first_publish() -> None:
    class _NoTopicB(_FakeConsumer):
        def list_topics(self, topic=None, timeout=None):
            if topic == "works-at-b":
                return SimpleNamespace(topics={topic: SimpleNamespace(partitions={}, error="UNKNOWN_TOPIC")})
            return super().list_topics(topic=topic, timeout=timeout)

    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("works-at-b", 9, b"b9")]
    producer = _acking_producer()
    with pytest.raises(DeadLetterReplayError, match="works-at-b does not exist"):
        run_replay(
            dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=False,
            consumer_factory=lambda config: _NoTopicB(config, logs=_type_dlq_logs(envelopes)),
            producer_factory=lambda config: producer,
            original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
        )
    producer.produce.assert_not_called()


def test_run_replay_needs_a_target() -> None:
    with pytest.raises(DeadLetterReplayError, match="no target topic"):
        run_replay(
            dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
            replay_filter=ReplayFilter(), dry_run=True, consumer_factory=_consumer_factory_for({}),
        )


def _multi_topic_schema(**edge_overrides):
    import yaml

    from src.models.schema import GraphSchema

    data = yaml.safe_load((PROJECT_ROOT / "config" / "graph_schema.yaml").read_text())
    for edge in data["edges"]:
        if edge["type"] == "WORKS_AT":
            edge.pop("topic")
            edge["topics"] = ["works-at-a", "works-at-b"]
            edge["order_by"] = "since"
            edge["dead_letter"] = {"enabled": True}
            for key, value in edge_overrides.items():
                if value is None:
                    edge.pop(key, None)
                else:
                    edge[key] = value
    return GraphSchema.model_validate(data)


def _replay_cli(schema, *argv):
    mock_run_replay = MagicMock(return_value=ReplaySummary(matched=0, replayed=0, dry_run=True))
    with patch("src.cli.load_schema", return_value=schema), \
         patch("src.loader.dead_letter_replay.run_replay", mock_run_replay):
        rc = handle_replay_dlq(build_parser().parse_args(["replay-dlq", "--edge-type", "WORKS_AT", *argv]))
    return rc, mock_run_replay


def test_cli_multi_topic_replay_reads_the_type_dlq_and_targets_each_original_topic() -> None:
    rc, run = _replay_cli(_multi_topic_schema())
    assert rc == 0
    kwargs = run.call_args.kwargs
    assert kwargs["dlq_topic"] == "WORKS_AT.dlq"
    assert kwargs["target_topic"] is None
    assert kwargs["original_topic_targets"] == frozenset({"works-at-a", "works-at-b"})
    assert kwargs["replay_filter"].original_topics is None


def test_cli_source_topic_filter_is_repeatable_and_validated_against_the_type() -> None:
    rc, run = _replay_cli(_multi_topic_schema(), "--source-topic", "works-at-b", "--source-topic", "works-at-a")
    assert rc == 0
    assert run.call_args.kwargs["replay_filter"].original_topics == frozenset({"works-at-a", "works-at-b"})
    rc, run = _replay_cli(_multi_topic_schema(), "--source-topic", "not-a-topic")
    assert rc == 1 and not run.called
    # a topic of ANOTHER type (KNOWS) is refused too
    schema = _multi_topic_schema()
    knows_topic = next(e for e in schema.edges if e.type != "WORKS_AT").topics[0]
    rc, run = _replay_cli(schema, "--source-topic", knows_topic)
    assert rc == 1 and not run.called


def test_cli_parked_and_target_override_for_a_multi_topic_type() -> None:
    rc, run = _replay_cli(_multi_topic_schema(), "--parked", "--source-topic", "works-at-a")
    assert rc == 0
    kwargs = run.call_args.kwargs
    assert kwargs["dlq_topic"] == "WORKS_AT.dlq.parked"
    assert kwargs["target_topic"] is None
    assert kwargs["replay_filter"].original_topics == frozenset({"works-at-a"})
    rc, run = _replay_cli(_multi_topic_schema(), "--target-topic", "staging")
    assert run.call_args.kwargs["target_topic"] == "staging"
    assert "original_topic_targets" not in run.call_args.kwargs


def test_cli_explicit_dead_letter_topic_keeps_its_name_for_a_multi_topic_type() -> None:
    schema = _multi_topic_schema(dead_letter={"enabled": True, "topic": "works-at-a.dlq"})
    rc, run = _replay_cli(schema, "--parked")
    assert rc == 0 and run.call_args.kwargs["dlq_topic"] == "works-at-a.dlq.parked"
    rc, run = _replay_cli(schema)
    assert run.call_args.kwargs["dlq_topic"] == "works-at-a.dlq"


def test_cli_warns_when_replaying_into_an_arrival_type(caplog) -> None:
    schema = _multi_topic_schema(order_by=None, topic_conflict="arrival")
    rc, _run = _replay_cli(schema, "--dry-run")
    assert rc == 0
    assert "topic_conflict=arrival: a replayed record" in caplog.text


def test_cli_single_topic_source_topic_filter() -> None:
    from src.models.schema import DeadLetterConfig

    schema = _cli_schema(edge_dead_letter=DeadLetterConfig(enabled=True))
    rc, run = _replay_cli(schema, "--source-topic", "works-at-events")
    assert rc == 0
    assert run.call_args.kwargs["target_topic"] == "works-at-events"
    assert run.call_args.kwargs["replay_filter"].original_topics == frozenset({"works-at-events"})


def test_an_empty_envelope_from_an_unknown_topic_is_skipped_not_fatal() -> None:
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _envelope(
        entity_kind="edge", entity_name="WORKS_AT", original_topic="retired-topic", original_partition=0,
        original_offset=1, value_bytes=None, reason_code="empty_payload",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
    )]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert summary == ReplaySummary(matched=2, replayed=1, dry_run=False)


# ── v7-S4 review S3: one captured range for both passes, one copy per record ──


def _copy(topic: str, offset: int, value: bytes, routed_at: int) -> DeadLetterEnvelope:
    envelope = _edge_envelope(topic, offset, value)
    import dataclasses

    return dataclasses.replace(envelope, dlq_routed_at_ms=routed_at)


def test_replay_publishes_one_envelope_per_record_in_first_copy_order() -> None:
    """The retry consumer re-publishes a record many times (copies carry the same key and value);
    replaying every copy would re-apply an old value after newer ones. One per (original_topic,
    partition, offset), in the order the records FIRST reached the DLQ (review S-b)."""
    envelopes = [
        _copy("works-at-a", 5, b"a5", 1_000), _copy("works-at-b", 9, b"b9", 1_100),
        _copy("works-at-a", 5, b"a5", 2_000), _copy("works-at-a", 6, b"a6", 2_100),
        _copy("works-at-a", 5, b"a5", 3_000),
    ]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert summary == ReplaySummary(matched=3, replayed=3, dry_run=False)
    assert [c.kwargs["value"] for c in producer.produce.call_args_list] == [b"a5", b"b9", b"a6"]


def test_dry_run_counts_distinct_records() -> None:
    envelopes = [_copy("works-at-a", 5, b"x", 1_000), _copy("works-at-a", 5, b"x", 2_000)]
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=True,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        original_topic_targets=frozenset({"works-at-a"}),
    )
    assert summary.matched == 1


class _GrowingConsumer(_FakeConsumer):
    """The DLQ grows between the selection pass and the live pass (the retry consumer keeps
    publishing), including a record from a topic the type does not read."""

    grown = False

    def assign(self, tps):
        if self.positions and not _GrowingConsumer.grown:  # the second (live) assign
            _GrowingConsumer.grown = True
            log = self.logs[0]
            for extra in (_edge_envelope("retired-topic", 1, b"late-bad"), _edge_envelope("works-at-a", 77, b"late")):
                log.append((log[-1][0] + 1, extra.key_bytes, extra))
        super().assign(tps)


def test_records_published_between_the_passes_are_never_read_or_validated() -> None:
    _GrowingConsumer.grown = False
    envelopes = [_edge_envelope("works-at-a", 5, b"a5"), _edge_envelope("works-at-b", 9, b"b9")]
    logs = _type_dlq_logs(envelopes)
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=lambda config: _GrowingConsumer(config, logs=logs),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert _GrowingConsumer.grown
    assert summary == ReplaySummary(matched=2, replayed=2, dry_run=False)
    assert [c.kwargs["value"] for c in producer.produce.call_args_list] == [b"a5", b"b9"]


def test_limit_selects_distinct_records_and_the_live_pass_stops_at_the_same_positions() -> None:
    envelopes = [
        _copy("works-at-a", 1, b"a1", 1_000), _copy("works-at-a", 1, b"a1", 1_500),
        _copy("works-at-a", 2, b"a2", 2_000), _copy("works-at-a", 3, b"a3", 3_000),
        _copy("works-at-a", 4, b"a4", 4_000),
    ]
    producer = _acking_producer()
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic="works-at-a", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(limit=2), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
    )
    assert summary == ReplaySummary(matched=2, replayed=2, dry_run=False)
    assert [c.kwargs["value"] for c in producer.produce.call_args_list] == [b"a1", b"a2"]


def test_a_root_re_dlqd_after_its_followers_is_still_replayed_first() -> None:
    """Review S-b: a pair's root is re-DLQ'd every attempt, so its LATEST copy sits after its
    followers' copies. Ordering by the latest copy replayed the follower first and the root's older
    value last (it won). Replay follows the first copies: root, then follower."""
    import dataclasses

    root = _copy("works-at-a", 5, b"root-older", 1_000)
    follower = dataclasses.replace(_copy("works-at-b", 9, b"follower-newer", 1_100), reason_code="blocked_by_earlier_unresolved_row")
    envelopes = [root, follower, _copy("works-at-a", 5, b"root-older", 2_000), _copy("works-at-a", 5, b"root-older", 3_000)]
    producer = _acking_producer()
    run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic=None, bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(), dry_run=False,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
        producer_factory=lambda config: producer,
        original_topic_targets=frozenset({"works-at-a", "works-at-b"}),
    )
    assert [c.kwargs["value"] for c in producer.produce.call_args_list] == [b"root-older", b"follower-newer"]


def test_the_reason_code_filter_sees_each_records_latest_copy() -> None:
    import dataclasses

    envelopes = [
        _copy("works-at-a", 5, b"a5", 1_000),  # first copy: missing_source
        dataclasses.replace(_copy("works-at-a", 5, b"a5", 2_000), reason_code="blocked_by_earlier_unresolved_row"),
        _copy("works-at-a", 6, b"a6", 2_100),
    ]
    summary = run_replay(
        dlq_topic="WORKS_AT.dlq", target_topic="works-at-a", bootstrap_servers="kafka:9092",
        replay_filter=ReplayFilter(reason_code="missing_source"), dry_run=True,
        consumer_factory=_consumer_factory_for(_type_dlq_logs(envelopes)),
    )
    assert summary.matched == 1  # a5's current reason is blocked; only a6 matches
