"""Ordering probes for E09.F2.S1 (native shared Docker Engine substrate): a ``RATED`` relationship with an integer ``version``.

``OrdPerson`` and ``OrdItem`` nodes (string ids). The type is either multi-topic with ``order_by: version`` (newest version wins),
or a single-topic type without ``order_by`` (the record applied last wins), optionally with a dead-letter queue.
"""
from __future__ import annotations

import json

from .inspect_strict import unique

BUCKET = '''  bucket_lease:
    tx_timeout_ms: 6000
    restart_quiet_margin_ms: 2000
    expected_chunk_commit_ms: 100
'''
PERSONS = [f'p{i}' for i in range(4)]
ITEMS = [f'i{i}' for i in range(10)]


def schema(prefix, group, *, topics, order_by, dead_letter=False, attempts=200, replicas=1, mode='stream'):
    t = lambda name: f'{prefix}{name}'  # noqa: E731
    topic_line = f'    topics: [{", ".join(topics)}]\n' if len(topics) > 1 else f'    topic: {topics[0]}\n'
    order = '    order_by: version\n' if order_by else ''
    dlq = ''
    if dead_letter:
        dlq = (f'    retry: {{base_delay_seconds: 1.0, max_delay_seconds: 2.0}}\n'
               f'    dead_letter: {{enabled: true, max_retry_attempts: {attempts}, topic: {t("rated.dlq")}, parked_topic: {t("rated.dlq.parked")}}}\n')
    return f'''loading:
  mode: {mode}
  consumer_group_id: {group}
  flush_interval_ms: 25
  unwind_batch_size: 50
{BUCKET}nodes:
  - label: OrdPerson
    topic: {t('person')}
    key_property: id
    properties:
      id: {{type: string, required: true}}
  - label: OrdItem
    topic: {t('item')}
    key_property: id
    properties:
      id: {{type: string, required: true}}
edges:
  - type: RATED
{topic_line}{order}    nodes: {{source: OrdPerson, target: OrdItem}}
    source_key_property: id
    target_key_property: id
    replicas: {replicas}
{dlq}    properties:
      version: {{type: integer}}
      note: {{type: string}}
'''


def record(person, item, version, note):
    properties = {'note': note}
    if version is not None:
        properties['version'] = version
    return (f'{person}-{item}', {'source': {'id': person}, 'target': {'id': item}, 'properties': properties})


def publish(stack, topic, rows, *, partition=None):
    stack.produce(topic, [record(*row) for row in rows], partition=partition)


def publish_nodes(stack, prefix, *, items=ITEMS):
    stack.produce(prefix + 'person', [(p, {'id': p}) for p in PERSONS])
    stack.produce(prefix + 'item', [(i, {'id': i}) for i in items])


def relationships(stack):
    """{(person, item): (version, note)} of every RATED relationship; two physical relationships for one pair fail the inspection."""
    with stack.driver.session(database=stack.database) as session:
        rows = [dict(r) for r in session.run(
            'MATCH (p:OrdPerson)-[r:RATED]->(i:OrdItem) RETURN p.id AS p, i.id AS i, r.version AS v, r.note AS n')]
    unique(rows, lambda r: (r['p'], r['i']), 'RATED')
    return {(r['p'], r['i']): (r['v'], r['n']) for r in rows}


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:OrdPerson|OrdItem) DETACH DELETE n').consume()


def dumps(value):
    return json.dumps(value, sort_keys=True, default=str)


def publish_conflicts(stack, topics, *, phase=None):
    """Four versions of each of 12 pairs over two topics x two partitions, newest at the lowest offsets of topic a / partition 0
    and the older ones afterwards (behind 12 noise records that push their offsets up). ``phase`` 'newest' publishes only the newest
    versions, 'older' only the rest (so they arrive after a run that already stored the newest); None publishes everything.
    Returns (expected final state, record count of everything)."""
    main = [(p, i) for p in PERSONS[:3] for i in ITEMS[:4]]
    noise = [(p, i) for p in PERSONS[:3] for i in ITEMS[4:8]]
    if phase in (None, 'newest'):
        publish(stack, topics[0], [(p, i, 400 + k, f'new-{k}') for k, (p, i) in enumerate(main)], partition=0)
    if phase in (None, 'older'):
        publish(stack, topics[0], [(p, i, 300 + k, f'old3-{k}') for k, (p, i) in enumerate(main)], partition=1)
        publish(stack, topics[1], [(p, i, 200 + k, f'old2-{k}') for k, (p, i) in enumerate(main)], partition=0)
        publish(stack, topics[1], [(p, i, 50 + k, f'noise-{k}') for k, (p, i) in enumerate(noise)], partition=1)
        publish(stack, topics[1], [(p, i, 100 + k, f'old1-{k}') for k, (p, i) in enumerate(main)], partition=1)
    want = {pair: (400 + k, f'new-{k}') for k, pair in enumerate(main)}
    want.update({pair: (50 + k, f'noise-{k}') for k, pair in enumerate(noise)})
    return want, 4 * len(main) + len(noise)
