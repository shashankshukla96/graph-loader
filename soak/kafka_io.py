"""Kafka access for the soak: the publisher, offset and read-back helpers, and the pure drain predicates.

The checks that decide "drained" are plain functions over numbers so they can be tested without a broker. ``Kafka`` is the real
adapter; tests use any object with the same four methods (``partitions``, ``watermarks``, ``committed``, ``read``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import threading
import time

SEND_HEADER = 'e13-send'


@dataclass
class Msg:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: dict = field(default_factory=dict)

    def json(self):
        return None if self.value is None else json.loads(self.value)


# ---- pure predicates -----------------------------------------------------------------------------------------------------

def source_lagging(snapshot: dict, committed: dict) -> list:
    """Partitions of a source snapshot that a group has not caught up with.

    ``snapshot`` is ``{(topic, partition): (low, high)}`` taken while the generator is paused; ``committed`` is
    ``{(topic, partition): offset or None}``. A partition empty at the snapshot (low == high) needs no commit; a non-empty
    one without a commit, or with a commit below the snapshot's end, is lagging.
    """
    lagging = []
    for tp, (low, high) in sorted(snapshot.items()):
        if low == high:
            continue
        commit = committed.get(tp)
        if commit is None or commit < high:
            lagging.append((tp, commit, high))
    return lagging


def dlq_bracket_drained(lows: dict, ends_before: dict, commits: dict, ends_after: dict) -> list:
    """The DLQ frontier moves (a retry can re-publish an unresolved row past any snapshot and commit its old copy), so one
    read is bracketed: end offsets, then the retry consumer's commits, then the end offsets again. Returns the partitions that
    are NOT drained: the two end reads must agree, and the commit must equal the end (an empty partition, low == high with no
    commit, matches the product's own low-watermark rule)."""
    bad = []
    for tp in sorted(ends_before):
        before, after, low = ends_before[tp], ends_after.get(tp), lows.get(tp, 0)
        commit = commits.get(tp)
        if before != after:
            bad.append((tp, 'end moved', before, after))
        elif commit is None:
            if not (low == after):
                bad.append((tp, 'no commit on a non-empty partition', commit, after))
        elif commit != after:
            bad.append((tp, 'commit differs from end', commit, after))
    return bad


class DlqFrontier:
    """Drained only after two clean bracketed reads at least ``gap`` seconds apart with the parked topics' ends unchanged."""

    def __init__(self, gap: float = 30.0):
        self.gap = gap
        self.first = None  # (time, parked_ends) of the first clean read
        self.last_bad = []

    def observe(self, now: float, lows, ends_before, commits, ends_after, parked_ends) -> bool:
        bad = dlq_bracket_drained(lows, ends_before, commits, ends_after)
        self.last_bad = bad
        if bad:
            self.first = None
            return False
        if self.first is None or self.first[1] != parked_ends:
            self.first = (now, dict(parked_ends))
            return False
        return now - self.first[0] >= self.gap


# ---- read-back of uncertain sends ------------------------------------------------------------------------------------------

def resolve_uncertain(journal, kafka, *, advance_floor: bool) -> dict:
    """Match uncertain sends to Kafka by their ``e13-send`` header and nothing else.

    A message carrying the header resolves that send to its offset. No match proves nothing (an append can land after the read),
    so the send stays uncertain and is searched again at every later call, from the floor. The floor only moves up to the end
    offsets read *before* the scan, and only when the caller says no send was in flight at that moment (``advance_floor``)."""
    wanted = {str(row['send_id']): row for row in journal.uncertain()}
    floor = journal.meta('scan_floor', {})
    topics = sorted({row['topic'] for row in wanted.values()}) if wanted else []
    found = 0
    new_floor = dict(floor)
    scanned_all = True
    for topic in topics:
        for partition in kafka.partitions(topic):
            low, high = kafka.watermarks(topic, partition)
            start = max(low, floor.get(f'{topic}/{partition}', 0))
            for msg in kafka.read(topic, partition, start, high):
                sid = msg.headers.get(SEND_HEADER)
                if sid in wanted:
                    journal.mark_ack(int(sid), topic, partition, msg.offset)
                    wanted.pop(sid)
                    found += 1
            new_floor[f'{topic}/{partition}'] = high
    if advance_floor and scanned_all:
        journal.set_meta('scan_floor', new_floor)
    return {'resolved': found, 'still_uncertain': len(wanted)}


# ---- real adapters --------------------------------------------------------------------------------------------------------

class Kafka:
    def __init__(self, bootstrap: str, *, probe_group: str):
        from confluent_kafka import Consumer
        self.bootstrap = bootstrap
        self._consumer_cls = Consumer
        self._probe_group = probe_group
        self._consumer = None
        self._group_consumers = {}
        self.retry_sleep = 1.0
        self._lock = threading.Lock()

    def _probe(self):
        if self._consumer is None:
            self._consumer = self._consumer_cls({'bootstrap.servers': self.bootstrap, 'group.id': self._probe_group,
                                                 'enable.auto.commit': False, 'socket.timeout.ms': 5000})
        return self._consumer

    def close(self):
        with self._lock:
            for consumer in [self._consumer, *self._group_consumers.values()]:
                if consumer is not None:
                    consumer.close()
            self._consumer = None
            self._group_consumers = {}

    def partitions(self, topic, timeout=10):
        with self._lock:
            meta = self._probe().list_topics(topic=topic, timeout=timeout)
        item = meta.topics.get(topic)
        if item is None or item.error is not None:
            raise RuntimeError(f'topic {topic} has no metadata')
        return sorted(item.partitions)

    def watermarks(self, topic, partition, timeout=10):
        from confluent_kafka import KafkaError, KafkaException, TopicPartition
        with self._lock:
            consumer = self._probe()
            try:
                return consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
            except KafkaException as error:
                if error.args[0].code() != KafkaError._UNKNOWN_PARTITION:
                    raise
            # A topic created after this consumer's metadata was cached (the CLI creates the DLQ topics): refresh it, then retry once.
            consumer.list_topics(topic=topic, timeout=timeout)
            return consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)

    def committed(self, group, tps, timeout=15):
        """{(topic, partition): committed offset or None} for ``group`` without joining it.

        A consumer that has the group's id and never subscribes reads the group's committed offsets (OffsetFetch) and is not a member.
        ``AdminClient.list_consumer_group_offsets`` is deliberately not used: with confluent-kafka 2.16.1 (librdkafka) it crashed the
        harness with SIGSEGV in ``rd_kafka_handle_OffsetFetch`` (see evidence/E13.F1.S1.md)."""
        from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, TopicPartition
        retriable = {KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError.COORDINATOR_NOT_AVAILABLE,
                     KafkaError._TRANSPORT, KafkaError._TIMED_OUT}
        with self._lock:
            consumer = self._group_consumers.get(group)
            if consumer is None:
                consumer = self._group_consumers[group] = self._consumer_cls({
                    'bootstrap.servers': self.bootstrap, 'group.id': group, 'enable.auto.commit': False,
                    'enable.auto.offset.store': False, 'socket.timeout.ms': 5000})
            for attempt in range(12):  # the group coordinator moves after a rebalance or a restart: that is not a lag reading
                try:
                    rows = consumer.committed([TopicPartition(t, p) for t, p in tps], timeout=timeout)
                    break
                except KafkaException as error:
                    if error.args[0].code() not in retriable or attempt == 11:
                        raise
                    time.sleep(self.retry_sleep)
        out = {(t, p): None for t, p in tps}
        for row in rows:
            if row.error is not None:
                raise RuntimeError(f'committed offset unavailable for {row.topic}/{row.partition}: {row.error}')
            out[(row.topic, row.partition)] = None if row.offset in (OFFSET_INVALID, None) or row.offset < 0 else row.offset
        return out

    def read(self, topic, partition, start, end, *, timeout=120):
        """Messages ``[start, end)`` of one partition with their headers (a dedicated consumer, never committing)."""
        from confluent_kafka import Consumer, TopicPartition
        if end <= start:
            return
        consumer = Consumer({'bootstrap.servers': self.bootstrap, 'group.id': self._probe_group + '-read', 'enable.auto.commit': False,
                             'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest', 'fetch.min.bytes': 1,
                             'socket.timeout.ms': 5000})
        deadline = time.monotonic() + timeout
        try:
            consumer.assign([TopicPartition(topic, partition, start)])
            position = start
            while position < end:
                if time.monotonic() > deadline:
                    raise TimeoutError(f'read of {topic}/{partition} [{start},{end}) timed out at {position}')
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == -191:  # partition EOF
                        break
                    raise RuntimeError(str(msg.error()))
                if msg.offset() >= end:
                    break
                position = msg.offset() + 1
                headers = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (msg.headers() or [])}
                yield Msg(topic, partition, msg.offset(), msg.key(), msg.value(), headers)
        finally:
            consumer.close()


class Publisher:
    """Producer with the soak's contract: ``acks=all``, idempotence, records keyed per the producer contract, journalled per send."""

    def __init__(self, bootstrap: str, journal, *, linger_ms=20, message_timeout_ms=60000):
        from confluent_kafka import Producer
        self.journal = journal
        self.producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True, 'acks': 'all',
                                  'linger.ms': linger_ms, 'compression.type': 'lz4', 'message.timeout.ms': message_timeout_ms,
                                  'socket.timeout.ms': 10000})

    def send(self, rec, *, window=0, origin='gen') -> int:
        send_id = self.journal.new_send(rec.rid, origin=origin, window=window)
        journal = self.journal

        def delivered(error, message, send_id=send_id):
            if error is not None:
                journal.mark_uncertain(send_id)
            else:
                journal.mark_ack(send_id, message.topic(), message.partition(), message.offset())
        try:
            self.producer.produce(rec.topic, key=rec.msg_key.encode(), value=json.dumps(rec.payload, separators=(',', ':')).encode(),
                                  headers=[(SEND_HEADER, str(send_id).encode())], on_delivery=delivered)
        except BufferError:
            self.producer.poll(1.0)
            try:
                self.producer.produce(rec.topic, key=rec.msg_key.encode(), value=json.dumps(rec.payload, separators=(',', ':')).encode(),
                                      headers=[(SEND_HEADER, str(send_id).encode())], on_delivery=delivered)
            except Exception:
                journal.mark_never(send_id)
                raise
        except Exception:
            journal.mark_never(send_id)
            raise
        return send_id

    def poll(self, timeout=0):
        return self.producer.poll(timeout)

    def flush(self, timeout=60) -> bool:
        """True when every send has an outcome. Sends still without one become uncertain and stay searchable."""
        remaining = self.producer.flush(timeout)
        if remaining:
            self.journal.settle_inflight()
        return remaining == 0
