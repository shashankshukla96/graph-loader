"""Dead-letter fixtures for E09.F1.S4 (native shared Docker Engine substrate).

* the maintained arrival dataset (``tests/fixtures/multi_topic_arrival.py``): one multi-topic ``LIKES`` type with
  ``topic_conflict: arrival``, relationships published before their nodes, drained by the retry consumer;
* a small single-topic case with a tight retry budget whose missing endpoint never arrives (parking), then arrives (replay).
"""
from __future__ import annotations

import json
import time

from confluent_kafka import Consumer, TopicPartition

from neo4j_graph_loader.loader.dead_letter import decode_envelope
from tests.fixtures.multi_topic_arrival import arrival_dataset

from .inspect_strict import topic_partitions

BUCKET = '''  bucket_lease:
    tx_timeout_ms: 6000
    restart_quiet_margin_ms: 2000
    expected_chunk_commit_ms: 100
'''


def arrival_schema(prefix, group):
    t = lambda name: f'{prefix}{name}'  # noqa: E731
    return f'''loading:
  mode: stream
  consumer_group_id: {group}
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  flush_interval_ms: 50
{BUCKET}nodes:
  - label: DlqPerson
    topic: {t('person')}
    key_property: person_id
    properties:
      person_id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
  - label: DlqCompany
    topic: {t('company')}
    key_property: company_id
    properties:
      company_id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
edges:
  - type: LIKES
    topics: [{t('likes-a')}, {t('likes-b')}]
    topic_conflict: arrival
    nodes: {{source: DlqPerson, target: DlqCompany}}
    source_key_property: person_id
    target_key_property: company_id
    replicas: 3
    retry: {{base_delay_seconds: 1.0, max_delay_seconds: 5.0}}
    dead_letter: {{enabled: true, max_retry_attempts: 200, topic: {t('likes.dlq')}, parked_topic: {t('likes.dlq.parked')}}}
    properties:
      since: {{type: date, required: true}}
      role: {{type: string}}
    execution: {{mode: python_apoc, worker_count: 4}}
'''


def limits_schema(prefix, group, *, attempts=3):
    t = lambda name: f'{prefix}{name}'  # noqa: E731
    return f'''loading:
  mode: stream
  consumer_group_id: {group}
  unwind_batch_size: 500
  flush_interval_ms: 50
{BUCKET}nodes:
  - label: DlqPerson
    topic: {t('person')}
    key_property: person_id
    properties:
      person_id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
  - label: DlqCompany
    topic: {t('company')}
    key_property: company_id
    properties:
      company_id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
edges:
  - type: LIKES
    topic: {t('likes')}
    nodes: {{source: DlqPerson, target: DlqCompany}}
    source_key_property: person_id
    target_key_property: company_id
    retry: {{base_delay_seconds: 1.0, max_delay_seconds: 2.0}}
    dead_letter: {{enabled: true, max_retry_attempts: {attempts}}}
    properties:
      since: {{type: date, required: true}}
      role: {{type: string}}
'''


def publish_likes(stack, topic, rows):
    stack.produce(topic, [(person, {'source': {'person_id': person}, 'target': {'company_id': company},
                                    'properties': {'since': since.isoformat(), 'role': role}}) for _t, person, company, since, role in rows])


def arrival():
    """(b rows, a rows, {(person, company): (date, role)}) exactly as the maintained dataset builds them."""
    return arrival_dataset()


def read_topic(stack, topic, partitions, *, limit=50000):
    """Every record ever written to ``topic`` as (partition, offset, key, value), read with a throwaway non-committing consumer.
    Raises unless every partition is read up to the watermark captured at the start."""
    actual = topic_partitions(stack, topic)
    if actual != partitions:
        raise RuntimeError(f'{topic} has {actual} partitions, expected {partitions}')
    consumer = Consumer({'bootstrap.servers': stack.bootstrap, 'group.id': f'e09-{stack.owner}-dlq-probe-{abs(hash(topic)) % 10**6}',
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    rows = []
    try:
        wanted = {}
        for partition in range(partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high > low:
                wanted[partition] = (low, high)
        consumer.assign([TopicPartition(topic, p, low) for p, (low, _high) in wanted.items()])
        seen = {p: low for p, (low, _high) in wanted.items()}
        end = time.monotonic() + 60
        while any(seen[p] < wanted[p][1] for p in wanted) and time.monotonic() < end:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f'reading {topic} failed: {message.error()}')
            if message.offset() < wanted[message.partition()][1]:
                seen[message.partition()] = message.offset() + 1
                rows.append((message.partition(), message.offset(), message.key(), message.value()))
            assert len(rows) <= limit, 'topic read exceeds its bound'
        short = {p: (seen[p], wanted[p][1]) for p in wanted if seen[p] < wanted[p][1]}
        if short:
            raise RuntimeError(f'{topic} was not read up to its watermarks: {short}')
    finally:
        consumer.close()
    return rows


def envelopes(stack, topic, partitions):
    rows = read_topic(stack, topic, partitions)
    if any(value is None for _p, _o, _k, value in rows):
        raise RuntimeError(f'{topic} holds a record without a value (a tombstone), not a dead-letter envelope')
    return [decode_envelope(value) for _p, _o, _k, value in rows]


def summary(envelope_list):
    return json.dumps(sorted({(e.entity_kind, e.entity_name, e.original_topic.rsplit('-', 1)[-1], e.failure_class, e.reason_code)
                              for e in envelope_list}))
