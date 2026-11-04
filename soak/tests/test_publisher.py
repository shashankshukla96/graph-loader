"""The publisher's journal states and the supervisor's accounting of fleet exits."""
import threading
import time

import pytest

from soak import kafka_io
from soak.fleet import Supervisor
from soak.generator import Generator
from soak.kafka_io import Publisher
from soak.tests import helpers as h


class FakeProducer:
    """Delivers on poll/flush: ``outcome(topic, key)`` returns ('ack', partition, offset), ('error',), ('raise',) or ('buffer',)."""
    outcome = staticmethod(lambda topic, key: ('ack', 1, 7))
    instances = []

    def __init__(self, conf):
        self.conf, self.pending, self.calls = conf, [], []
        FakeProducer.instances.append(self)

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        verdict = FakeProducer.outcome(topic, key)
        self.calls.append((topic, key, value, headers))
        if verdict[0] == 'raise':
            raise RuntimeError('produce failed')
        if verdict[0] == 'buffer' and len(self.calls) == 1:
            raise BufferError('queue full')
        self.pending.append((on_delivery, verdict, topic))

    def poll(self, timeout=0):
        return self._deliver()

    def flush(self, timeout=0):
        self._deliver()
        return 0 if not getattr(self, 'stuck', False) else 1

    def _deliver(self):
        pending, self.pending = self.pending, []
        for callback, verdict, topic in pending:
            if verdict[0] == 'timeout':
                self.pending.append((callback, verdict, topic))
                continue
            error = None if verdict[0] in ('ack', 'buffer') else RuntimeError('delivery failed')
            message = type('M', (), {'topic': lambda s: topic, 'partition': lambda s: verdict[1] if len(verdict) > 1 else 0,
                                     'offset': lambda s: verdict[2] if len(verdict) > 2 else 0})()
            callback(error, message)
        return len(pending)


@pytest.fixture
def publisher(tmp_path, monkeypatch):
    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, 'Producer', FakeProducer)
    journal, _ = h.new_journal(tmp_path)
    gen = Generator(journal, h.TOKEN)
    return Publisher('127.0.0.1:1', journal), journal, gen


def states(journal):
    return {r['send_id']: (r['state'], r['part'], r['off']) for r in journal.query('SELECT * FROM sends')}


def test_an_acknowledged_send_records_topic_partition_and_offset_and_carries_the_send_header(publisher):
    pub, journal, gen = publisher
    rec = gen.make_node('Store', 's01')
    send = pub.send(rec, window=2)
    assert states(journal)[send][0] == 'inflight'
    pub.flush()
    assert states(journal)[send] == ('ack', 1, 7)
    topic, key, value, headers = pub.producer.calls[0]
    assert topic == rec.topic and key == b's01' and headers == [(kafka_io.SEND_HEADER, str(send).encode())]
    assert pub.producer.conf['enable.idempotence'] is True and pub.producer.conf['acks'] == 'all'


def test_a_delivery_error_makes_the_send_uncertain_not_absent(publisher):
    pub, journal, gen = publisher
    FakeProducer.outcome = staticmethod(lambda t, k: ('error',))
    send = pub.send(gen.make_node('Store', 's01'))
    pub.flush()
    assert states(journal)[send][0] == 'uncertain'
    FakeProducer.outcome = staticmethod(lambda t, k: ('ack', 0, 0))


def test_a_produce_call_that_raises_is_never_enqueued(publisher):
    pub, journal, gen = publisher
    FakeProducer.outcome = staticmethod(lambda t, k: ('raise',))
    with pytest.raises(RuntimeError):
        pub.send(gen.make_node('Store', 's01'))
    assert [v[0] for v in states(journal).values()] == ['never']
    FakeProducer.outcome = staticmethod(lambda t, k: ('ack', 0, 0))


def test_a_full_queue_is_retried_once_and_a_flush_timeout_leaves_uncertain_sends(publisher):
    pub, journal, gen = publisher
    FakeProducer.outcome = staticmethod(lambda t, k: ('buffer',))
    first = pub.send(gen.make_node('Store', 's01'))
    FakeProducer.outcome = staticmethod(lambda t, k: ('timeout',))
    second = pub.send(gen.make_node('Store', 's02'))
    pub.producer.stuck = True
    assert pub.flush(0.01) is False
    pub.producer.flush = lambda timeout=0: 1
    got = states(journal)
    assert got[second][0] == 'uncertain'
    FakeProducer.outcome = staticmethod(lambda t, k: ('ack', 0, 0))


# ---- the supervisor ----------------------------------------------------------------------------------------------------------------

class FakeCli:
    def __init__(self, scripted):
        self.returncode = None
        self.scripted = scripted
        self.process = self
        self.deadline_at = None

    def poll(self):
        return self.returncode

    def running(self):
        return self.returncode is None

    def text(self):
        return 'stage=lease-relationship-fleet run_id=x failed: relationship edge=PURCHASED exited before drain acknowledgement\n'


class FakeFleet:
    def __init__(self, incarnation):
        self.incarnation = incarnation
        self.started_at = time.time()
        self.cli = FakeCli(None)
        self.observer = None

    def wait_ready(self, timeout):
        return 0

    def reason_lines(self):
        return ['stage=... exited before drain acknowledgement']

    def finish(self):
        return {'leftovers': {'removed': [], 'failed': []}}

    def sigterm(self, timeout=90):
        self.cli.returncode = 0
        return 0


class Ctx:
    ready_timeout = 5


def test_an_exit_nobody_injected_is_an_unexplained_restart_and_an_injected_one_is_not(tmp_path):
    fleets = []

    def make(i):
        fleets.append(FakeFleet(i))
        return fleets[-1]
    supervisor = Supervisor(Ctx, make, backoff=0.05)
    supervisor.start()
    deadline = time.time() + 5
    while not fleets and time.time() < deadline:
        time.sleep(0.01)
    supervisor.expect('kill edge loader', exit_codes=(1,))
    fleets[0].cli.returncode = 1
    while len(fleets) < 2 and time.time() < deadline:
        time.sleep(0.01)
    assert supervisor.events[0]['cause'] == 'kill edge loader' and supervisor.events[0]['injected'] and not supervisor.unexplained
    fleets[1].cli.returncode = 1                              # nobody asked for this one
    while len(fleets) < 3 and time.time() < deadline:
        time.sleep(0.01)
    assert len(supervisor.unexplained) == 1 and supervisor.unexplained[0]['incarnation'] == 2
    supervisor.explain_until('kafka restart', time.time() + 60)
    fleets[2].cli.returncode = 1
    while len(fleets) < 4 and time.time() < deadline:
        time.sleep(0.01)
    assert supervisor.events[2]['cause'] == 'kafka restart' and len(supervisor.unexplained) == 1
    supervisor.stop_requested.set()
    fleets[3].cli.returncode = 0
    supervisor.join(timeout=5)
    assert not supervisor.failed


def test_foreign_snapshot_differences_are_reported():
    from soak.runtime import diff_foreign
    before = {'containers': {'a': {'state': 'running'}}, 'networks': {'n': 'x'}, 'volumes': {}, 'images': {'i': ['t']}}
    assert diff_foreign(before, before) == {}
    after = {'containers': {'a': {'state': 'exited'}, 'b': {}}, 'networks': {}, 'volumes': {}, 'images': {'i': ['t']}}
    changes = diff_foreign(before, after)
    assert changes['containers'] == {'added': ['b'], 'removed': [], 'changed': ['a']} and changes['networks']['removed'] == ['n']


# ---- the real Kafka adapter against a fake admin client -------------------------------------------------------------------------------

def test_committed_offsets_are_read_by_an_unsubscribed_consumer_never_by_the_admin_api(monkeypatch):
    """AdminClient.list_consumer_group_offsets crashed the harness (SIGSEGV in librdkafka 2.16.1): the soak must not call it."""
    from confluent_kafka import TopicPartition
    import confluent_kafka
    made = []

    class FakeConsumer:
        def __init__(self, conf):
            self.conf = conf
            made.append(self)

        def committed(self, tps, timeout=0):
            return [TopicPartition(tp.topic, tp.partition, {0: 41, 1: -1001}.get(tp.partition, -1001)) for tp in tps]

        def subscribe(self, *a, **k):
            raise AssertionError('the offset reader must never join the group')

    monkeypatch.setattr(confluent_kafka, 'Consumer', FakeConsumer)
    kafka = kafka_io.Kafka('127.0.0.1:1', probe_group='probe')
    assert not hasattr(kafka, 'admin')
    got = kafka.committed('g', [('t', 0), ('t', 1), ('t', 2)])
    assert got == {('t', 0): 41, ('t', 1): None, ('t', 2): None}
    kafka.committed('g', [('t', 0)])
    assert len(made) == 1 and made[0].conf['group.id'] == 'g' and made[0].conf['enable.auto.commit'] is False   # cached per group


def test_watermarks_refresh_the_metadata_of_a_topic_created_after_the_probe_consumer_started(monkeypatch):
    from confluent_kafka import KafkaError, KafkaException

    class FakeConsumer:
        def __init__(self, conf):
            self.refreshed, self.calls = [], 0

        def get_watermark_offsets(self, tp, timeout=0):
            self.calls += 1
            if not self.refreshed:
                raise KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION))
            return (0, 9)

        def list_topics(self, topic=None, timeout=0):
            self.refreshed.append(topic)

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, 'Consumer', FakeConsumer)
    kafka = kafka_io.Kafka('127.0.0.1:1', probe_group='probe')
    assert kafka.watermarks('late-topic', 3) == (0, 9)
    assert kafka._consumer.refreshed == ['late-topic'] and kafka._consumer.calls == 2


def test_a_moved_group_coordinator_is_retried_not_read_as_a_lag(monkeypatch):
    from confluent_kafka import KafkaError, KafkaException, TopicPartition
    import confluent_kafka
    attempts = []

    class FakeConsumer:
        def __init__(self, conf):
            pass

        def committed(self, tps, timeout=0):
            attempts.append(1)
            if len(attempts) < 3:
                raise KafkaException(KafkaError(KafkaError.NOT_COORDINATOR))
            return [TopicPartition(tp.topic, tp.partition, 5) for tp in tps]
    monkeypatch.setattr(confluent_kafka, 'Consumer', FakeConsumer)
    kafka = kafka_io.Kafka('127.0.0.1:1', probe_group='probe')
    kafka.retry_sleep = 0
    assert kafka.committed('g', [('t', 0)]) == {('t', 0): 5} and len(attempts) == 3
    attempts.clear()

    class Failing(FakeConsumer):
        def committed(self, tps, timeout=0):
            raise KafkaException(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))
    monkeypatch.setattr(confluent_kafka, 'Consumer', Failing)
    other = kafka_io.Kafka('127.0.0.1:1', probe_group='probe')
    with pytest.raises(KafkaException):
        other.committed('g2', [('t', 0)])
