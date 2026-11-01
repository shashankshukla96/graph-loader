"""Kafka and graph inspections that never turn an inspection failure into evidence of absence.

A resource that is confirmed missing (unknown topic, no committed offset) is reported as such; any other failure is retried a
few times and then raised, so a broken probe cannot read as "nothing there".
"""
from __future__ import annotations

import time

from confluent_kafka import KafkaError, KafkaException

from .observers import committed_offsets

ATTEMPTS = 3


def retried(call, *, attempts=ATTEMPTS, pause=1.0):
    last = None
    for attempt in range(attempts):
        try:
            return call()
        except Exception as error:  # noqa: BLE001 - re-raised below after the bounded retries
            last = error
            if attempt + 1 < attempts:
                time.sleep(pause)
    raise RuntimeError(f'inspection failed after {attempts} attempts: {type(last).__name__}: {last}') from last


def group_offsets(stack, group, topic, partitions):
    """{partition: committed offset, or None when the group has no commit there}. Errors raise; None means confirmed uncommitted."""
    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'

    def read():
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=group,
                                 partitions=[(topic, p) for p in range(partitions)], broker_guard=running)
        return {r['partition']: r['offset'] for r in rows}
    result = retried(read)
    if sorted(result) != list(range(partitions)):
        raise RuntimeError(f'offsets were not returned for every partition of {topic}: {sorted(result)}')
    return result


def topic_partitions(stack, topic):
    """Partition count of ``topic``, or None when the broker confirms it does not exist. Any other metadata error raises.

    Reads the metadata of ALL topics: asking for one topic by name makes the broker auto-create it when it is missing, which would
    both change the state under test and break a later launch that expects to create the topic itself."""
    def read():
        meta = stack.admin().list_topics(timeout=10).topics.get(topic)
        if meta is None:
            return None
        if meta.error is not None:
            if meta.error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return None
            raise KafkaException(meta.error)
        return len(meta.partitions)
    return retried(read)


def topic_total(stack, topic, *, missing_ok=False):
    """Records ever written to ``topic`` (all its partitions). A confirmed-missing topic counts 0 only when ``missing_ok``."""
    count = topic_partitions(stack, topic)
    if count is None:
        if missing_ok:
            return 0
        raise RuntimeError(f'topic {topic} does not exist')
    return sum(retried(lambda: stack.end_offsets(topic, partitions=count)).values())


def unique(rows, key, what):
    """Rows as a list; fails if two rows share a key (a physical duplicate must never be projected away)."""
    rows = list(rows)
    keys = [key(row) for row in rows]
    if len(keys) != len(set(keys)):
        raise AssertionError(f'{what}: {len(keys) - len(set(keys))} duplicate relationship(s) for one endpoint pair')
    return rows


def list_groups(stack):
    """Names of every consumer group on the broker. A listing that reports errors raises: its ``valid`` part may be empty because the
    listing failed (for example while the group coordinator is loading), which is not evidence that no group exists."""
    def read():
        admin = stack.admin()  # keep the client alive until the result is read
        result = admin.list_consumer_groups().result()
        if result.errors:
            raise KafkaException(result.errors[0])
        return sorted(g.group_id for g in result.valid)
    return retried(read)


def bulk_groups(snapshot, group):
    """Run-scoped relationship groups a bulk run used: {type: group name}, one per type. The edge containers are named
    ``graph-loader-edge-<type>-<replica>-<hex of the stage run id>`` and the group is ``<group>-<type>-<stage run id>``."""
    import re
    found = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        if not info or info['labels'].get('component') != 'edge-loader':
            continue
        match = re.fullmatch(r'graph-loader-edge-(\w+?)-(\d+)-([0-9a-f]+)', info['name'])
        if match is None:
            raise AssertionError(f"unexpected edge container name {info['name']}")
        kind, replica, encoded = match.groups()
        key = (kind, replica)
        if key in found:
            raise AssertionError(f'{key} started twice in one run')
        found[key] = f'{group}-{kind}-{bytes.fromhex(encoded).decode()}'
    return {kind: name for (kind, _replica), name in found.items()}
