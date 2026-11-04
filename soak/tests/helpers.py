"""In-memory stand-ins for Kafka and Neo4j, and journal builders, for the soak's own tests."""
from __future__ import annotations

import base64
import json

from soak import spec
from soak.journal import Journal, node_record, rel_record
from soak.kafka_io import SEND_HEADER, Msg
from soak.oracle import Oracle

TOKEN = 'a' * 32
GROUP = f'e13-{TOKEN}'


class FakeKafka:
    """Topics with partitions of ``Msg``; committed offsets default to the end (drained) unless a test sets a lag."""

    def __init__(self, autocommit=True):
        self.logs = {}
        self.commits = {}
        self.autocommit = autocommit
        self.on_committed = None  # a test hook: something that happens while the checker is draining
        for topic, (partitions, _, _) in spec.source_topics(TOKEN).items():
            self.create(topic, partitions)
        for dlq, parked, partitions in spec.dlq_topics(TOKEN).values():
            self.create(dlq, partitions)
            self.create(parked, partitions)

    def create(self, topic, partitions):
        self.logs[topic] = {p: [] for p in range(partitions)}

    def append(self, topic, partition, value, key=b'k', headers=None):
        log = self.logs[topic][partition]
        log.append(Msg(topic, partition, len(log), key, value if isinstance(value, bytes) else json.dumps(value).encode(), headers or {}))
        return len(log) - 1

    def partitions(self, topic):
        return sorted(self.logs[topic])

    def watermarks(self, topic, partition):
        return (0, len(self.logs[topic][partition]))

    def committed(self, group, tps):
        if self.on_committed is not None:
            hook, self.on_committed = self.on_committed, None
            hook()
        out = {}
        for topic, p in tps:
            default = len(self.logs[topic][p]) if self.autocommit else None
            out[(topic, p)] = self.commits.get((group, topic, p), default)
        return out

    def read(self, topic, partition, start, end):
        yield from self.logs[topic][partition][start:end]


class FakeGraph:
    """The graph as a test plants it: nodes[label][key] = props, rels[(type, a_label, b_label)] = [(a, b, props), ...]."""

    def __init__(self):
        self.nodes_ = {label: {} for label in spec.NODES}
        self.rels = {}

    def nodes(self, label):
        for key in sorted(self.nodes_[label]):
            yield key, self.nodes_[label][key]

    def relationships(self, rel, a_label, b_label):
        yield from sorted(self.rels.get((rel, a_label, b_label), []), key=lambda r: (r[0], r[1]))


def mirror(journal, oracle, graph, *, skip=()):
    """Plant in ``graph`` exactly what the model expects (the harness's own healthy product), except ``skip`` identities."""
    oracle.refresh()
    for label in spec.NODES:
        for entry in oracle.stream('N', label, label):
            if entry.identity not in skip:
                graph.nodes_[label][entry.identity[3]] = dict(entry.props)
    for rel, rspec in spec.RELS.items():
        for a_label, b_label in rspec.pairs:
            for entry in oracle.stream('R', rel, a_label, b_label):
                if entry.identity not in skip:
                    graph.rels.setdefault((rel, a_label, b_label), []).append((entry.identity[3], entry.identity[5], dict(entry.props)))


class Counter:
    n = 0


def rid():
    Counter.n += 1
    return f'r{Counter.n:08d}'


def put(journal, rec, *, partition=0, offset=None, state='ack', origin='gen', window=0):
    """Journal one logical record and one physical send in the given state."""
    journal.add_record(rec)
    send = journal.new_send(rec.rid, origin=origin, window=window)
    if state == 'ack':
        journal.mark_ack(send, rec.topic, partition, offset if offset is not None else next_offset(journal, rec.topic, partition))
    elif state == 'uncertain':
        journal.mark_uncertain(send)
    elif state == 'never':
        journal.mark_never(send)
    return send


_OFFSETS = {}


def next_offset(journal, topic, partition):
    key = (journal.path, topic, partition)
    _OFFSETS[key] = _OFFSETS.get(key, -1) + 1
    return _OFFSETS[key]


def customer(journal, key, *, name='N', tier=None, city=None, partition=0, offset=None, state='ack', rid_=None, **extra):
    r = rid_ or rid()
    payload = {'customer_id': key, 'rid': r, 'name': name, **({'tier': tier} if tier is not None else {}),
               **({'city': city} if city is not None else {}), **extra}
    return put(journal, node_record(r, 'Customer', key, spec.topic_name(TOKEN, 'customers'), payload),
               partition=partition, offset=offset, state=state), r


def product(journal, key, *, title='P', price=1.5, partition=0, offset=None, state='ack', **extra):
    r = rid()
    payload = {'product_id': key, 'rid': r, 'title': title, 'price': price, **extra}
    put(journal, node_record(r, 'Product', key, spec.topic_name(TOKEN, 'products'), payload), partition=partition, offset=offset, state=state)
    return r


def store(journal, key, *, name='S', partition=0, offset=None):
    r = rid()
    put(journal, node_record(r, 'Store', key, spec.topic_name(TOKEN, 'stores'), {'store_id': key, 'rid': r, 'name': name}),
        partition=partition, offset=offset)
    return r


def rel(journal, type_, source_label, source, target_label, target, topic_suffix, props, *, partition=0, offset=None, state='ack',
        klass=None, valid=True, window=0, rid_=None, payload_override=None, origin='gen'):
    r = rid_ or rid()
    body = {'rid': r, **props}
    payload = payload_override or {'source': {spec.key_of(source_label): source}, 'target': {spec.key_of(target_label): target},
                                   'properties': body}
    rec = rel_record(r, type_, source_label, source, target_label, target, spec.topic_name(TOKEN, topic_suffix), payload,
                     valid=valid, klass=klass, window=window)
    return put(journal, rec, partition=partition, offset=offset, state=state, origin=origin, window=window), r


def purchased(journal, c, p, version, *, topic='purchases-web', **kw):
    return rel(journal, 'PURCHASED', 'Customer', c, 'Product', p, topic,
               {'qty': 1, 'amount': 2.5, 'version': version, 'at': '2026-01-01T00:00:00+00:00'}, **kw)


def stocks(journal, s, p, updated_at, *, qty=5, **kw):
    return rel(journal, 'STOCKS', 'Store', s, 'Product', p, 'stock', {'quantity': qty, 'updated_at': updated_at}, **kw)


def follows(journal, a, b, props, **kw):
    return rel(journal, 'FOLLOWS', 'Customer', a, 'Customer', b, 'follows', props, **kw)


def envelope(rel_type, rec_rid, *, failure_class='missing_endpoint', attempt=0, reason='missing_source'):
    """A DLQ/parked envelope as the product writes it, with the record's value inside ``value_bytes``."""
    value = json.dumps({'source': {}, 'target': {}, 'properties': {'rid': rec_rid}}).encode()
    return json.dumps({'schema_version': 2, 'failure_class': failure_class, 'entity_kind': 'edge', 'entity_name': rel_type,
                       'reason_code': reason, 'attempt_count': attempt, 'original_topic': 't', 'original_partition': 0,
                       'original_offset': 0, 'value_bytes': base64.b64encode(value).decode(), 'key_bytes': None}).encode()


def new_journal(tmp_path):
    journal = Journal(tmp_path / 'journal.db')
    return journal, Oracle(journal)


class Clock:
    """Virtual time: ``sleep`` only advances it."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def checker(tmp_path, *, replay=None, autocommit=True, prepare=None, rejected=None, **kw):
    from soak.checker import Checker
    journal, oracle = new_journal(tmp_path)
    kafka, graph, clock = FakeKafka(autocommit), FakeGraph(), Clock()
    chk = Checker(token=TOKEN, group_id=GROUP, journal=journal, oracle=oracle, graph=graph, kafka=kafka,
                  prepare=prepare or (lambda: True), replay=replay, rejected_locations=rejected, clock=clock, sleep=clock.sleep, **kw)
    return chk, journal, oracle, kafka, graph


def park(kafka, rel_type, rec_rid, partition=0):
    dlq, parked, _ = spec.dlq_topics(TOKEN)[rel_type]
    return kafka.append(parked, partition, envelope(rel_type, rec_rid, attempt=11, reason='missing_source'))


def dead_letter(kafka, rel_type, rec_rid, partition=0, failure_class='missing_endpoint'):
    dlq, _, _ = spec.dlq_topics(TOKEN)[rel_type]
    return kafka.append(dlq, partition, envelope(rel_type, rec_rid, failure_class=failure_class))


def republish(kafka, journal, rec_rid):
    """What ``replay-dlq`` does: the stored payload goes back to its source topic, without headers."""
    rec = journal.record(rec_rid)
    topic = rec.topic
    return kafka.append(topic, 0, rec.payload, key=rec.msg_key.encode(), headers={})
