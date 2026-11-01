"""Controls for the inspection helpers the live ordering and DLQ tests rely on (fakes only, no services).

An inspection failure, a partial read or two physical relationships for one endpoint pair must never read as the expected state or
as evidence of absence.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from confluent_kafka import KafkaError
import pytest

from tests.installed_service import dlq_fixture as dq
from tests.installed_service import inspect_strict as strict
from tests.installed_service import order_fixture as of
from tests.installed_service import test_service_ingest_dlq as dlq_test

pytestmark = pytest.mark.integration


class Session:
    def __init__(self, rows):
        self.rows = rows

    def run(self, *_args, **_kwargs):
        return list(self.rows)


class Driver:
    def __init__(self, rows):
        self.rows = rows

    @contextmanager
    def session(self, **_kwargs):
        yield Session(self.rows)


def graph_stack(rows):
    return SimpleNamespace(driver=Driver(rows), database='scratch')


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(strict.time, 'sleep', lambda _seconds: None)


def test_two_conflicting_relationships_for_one_pair_fail_the_ordering_inspector():
    rows = [{'p': 'p0', 'i': 'i0', 'v': 9, 'n': 'right'}, {'p': 'p0', 'i': 'i0', 'v': 3, 'n': 'wrong'}]
    with pytest.raises(AssertionError, match='duplicate relationship'):
        of.relationships(graph_stack(rows))
    assert of.relationships(graph_stack(rows[:1])) == {('p0', 'i0'): (9, 'right')}


def test_two_conflicting_relationships_for_one_pair_fail_the_dlq_inspector():
    rows = [{'p': 1, 'c': 'c1', 'since': '2020-01-01', 'role': 'right'}, {'p': 1, 'c': 'c1', 'since': '2019-01-01', 'role': 'wrong'}]
    with pytest.raises(AssertionError, match='duplicate relationship'):
        dlq_test.likes(graph_stack(rows))
    assert dlq_test.likes(graph_stack(rows[:1])) == {(1, 'c1'): ('2020-01-01', 'right')}


class Admin:
    def __init__(self, topics=None, error=None):
        self.topics, self.error = topics or {}, error

    def list_topics(self, topic=None, timeout=None):
        assert topic is None, 'a metadata request that names a topic auto-creates it'
        if self.error:
            raise self.error
        return SimpleNamespace(topics=self.topics)


def topic_stack(admin, ends=None):
    return SimpleNamespace(admin=lambda: admin, end_offsets=lambda topic, partitions: ends or {0: 3})


def meta(partitions=1, error=None):
    return SimpleNamespace(partitions={i: None for i in range(partitions)}, error=error)


def test_a_topic_the_broker_confirms_missing_is_zero_only_when_the_caller_allows_it():
    missing = topic_stack(Admin({}))
    assert strict.topic_total(missing, 't', missing_ok=True) == 0
    with pytest.raises(RuntimeError, match='does not exist'):
        strict.topic_total(missing, 't')
    unknown = SimpleNamespace(code=lambda: KafkaError.UNKNOWN_TOPIC_OR_PART)
    assert strict.topic_total(topic_stack(Admin({'t': meta(error=unknown)})), 't', missing_ok=True) == 0


def test_an_inspection_failure_is_never_zero():
    broken = topic_stack(Admin(error=RuntimeError('broker unreachable')))
    for missing_ok in (True, False):
        with pytest.raises(RuntimeError, match='inspection failed after 3 attempts'):
            strict.topic_total(broken, 't', missing_ok=missing_ok)
    other = SimpleNamespace(code=lambda: KafkaError.BROKER_NOT_AVAILABLE, __str__=lambda self: 'broker')
    with pytest.raises(RuntimeError, match='inspection failed'):
        strict.topic_total(topic_stack(Admin({'t': meta(error=other)})), 't', missing_ok=True)
    assert strict.topic_total(topic_stack(Admin({'t': meta()}), ends={0: 3}), 't') == 3


def test_group_offsets_distinguish_uncommitted_from_a_failed_read(monkeypatch):
    stack = SimpleNamespace(owner='a' * 32, bootstrap='x', kafka=SimpleNamespace(reload=lambda: None, status='running'))
    monkeypatch.setattr(strict, 'committed_offsets', lambda **_k: [{'partition': 0, 'offset': None}])
    assert strict.group_offsets(stack, 'g', 't', 1) == {0: None}

    def fail(**_k):
        raise RuntimeError('coordinator unavailable')
    monkeypatch.setattr(strict, 'committed_offsets', fail)
    with pytest.raises(RuntimeError, match='inspection failed after 3 attempts'):
        strict.group_offsets(stack, 'g', 't', 1)
    monkeypatch.setattr(strict, 'committed_offsets', lambda **_k: [{'partition': 0, 'offset': 4}])
    with pytest.raises(RuntimeError, match='every partition'):
        strict.group_offsets(stack, 'g', 't', 2)


class Message:
    def __init__(self, partition, offset, value=b'x'):
        self._p, self._o, self._v = partition, offset, value

    def error(self):
        return None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return b'k'

    def value(self):
        return self._v


class Consumer:
    messages = []

    def __init__(self, _config):
        self.queue = list(self.messages)

    def get_watermark_offsets(self, _tp, timeout):
        return 0, 2

    def assign(self, _tps):
        pass

    def poll(self, _timeout):
        return self.queue.pop(0) if self.queue else None

    def close(self):
        pass


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        self.now += 20.0
        return self.now


def test_a_topic_read_that_stops_short_of_its_watermark_fails_instead_of_returning_a_partial_list(monkeypatch):
    stack = SimpleNamespace(owner='a' * 32, bootstrap='x', admin=lambda: Admin({'t': meta(1)}), end_offsets=lambda t, partitions: {0: 2})
    monkeypatch.setattr(dq, 'Consumer', Consumer)
    monkeypatch.setattr(dq, 'time', Clock())
    Consumer.messages = [Message(0, 0)]
    with pytest.raises(RuntimeError, match='not read up to its watermarks'):
        dq.read_topic(stack, 't', 1)
    Consumer.messages = [Message(0, 0), Message(0, 1)]
    assert [row[1] for row in dq.read_topic(stack, 't', 1)] == [0, 1]
    with pytest.raises(RuntimeError, match='partitions, expected'):
        dq.read_topic(stack, 't', 2)


def test_a_group_listing_that_reports_errors_is_never_an_empty_list():
    class Result:
        def __init__(self, valid, errors):
            self.valid, self.errors = valid, errors

    class Future:
        def __init__(self, result):
            self._result = result

        def result(self):
            return self._result

    class GroupAdmin:
        def __init__(self, result):
            self.result = result

        def list_consumer_groups(self):
            return Future(self.result)
    bad = SimpleNamespace(admin=lambda: GroupAdmin(Result([], [RuntimeError('coordinator loading')])))
    with pytest.raises(RuntimeError, match='inspection failed after 3 attempts'):
        strict.list_groups(bad)
    good = SimpleNamespace(admin=lambda: GroupAdmin(Result([SimpleNamespace(group_id='b'), SimpleNamespace(group_id='a')], [])))
    assert strict.list_groups(good) == ['a', 'b']
    empty = SimpleNamespace(admin=lambda: GroupAdmin(Result([], [])))
    assert strict.list_groups(empty) == []


def test_bulk_groups_come_from_the_edge_container_names_one_per_type():
    def row(kind, stage):
        return {'inspected': {'labels': {'component': 'edge-loader'}, 'name': f'graph-loader-edge-{kind}-0-' + stage.encode().hex()}}
    snapshot = {'containers': {'1': row('A', 'r1.bulk.0.h1'), '2': row('B_C', 'r1.isolated.0.h2'),
                               '3': {'inspected': {'labels': {'component': 'node-loader'}, 'name': 'graph-loader-node-X-0-ab'}}}}
    assert strict.bulk_groups(snapshot, 'g') == {'A': 'g-A-r1.bulk.0.h1', 'B_C': 'g-B_C-r1.isolated.0.h2'}
    snapshot['containers']['4'] = row('A', 'r2.bulk.0.h3')
    with pytest.raises(AssertionError, match='started twice'):
        strict.bulk_groups(snapshot, 'g')
