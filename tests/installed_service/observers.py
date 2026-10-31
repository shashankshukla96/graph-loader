"""Bounded read-only observations for uniquely owned service scenarios.

The caller must independently authenticate the driver and provision the exact
scratch database; database naming alone does not prove server ownership.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from itertools import islice

from confluent_kafka import Consumer, OFFSET_INVALID, TopicPartition
from neo4j import Query


def _owner_database(owner, database):
    if (not isinstance(owner, str) or re.fullmatch(r'[0-9a-f]{32}', owner) is None
            or database != 'e09'+owner[:16]):
        raise ValueError('scratch database identity differs')


def graph_observations(driver, *, owner, database, maximum=10000, key='id'):
    """Observe nodes and relationships separately, not as one atomic snapshot.

    The caller must prove all graph writers are fenced for both queries before
    combining these observations into a single graph-state claim. Only
    controlled synthetic properties may be persisted as evidence. The driver
    can materialize one record before the post-fetch byte limit is applied;
    the caller must separately bound synthetic record sizes.
    """
    _owner_database(owner, database)
    if (type(maximum) is not int or not 1 <= maximum <= 10000
            or not isinstance(key, str) or re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', key) is None):
        raise ValueError('graph observation bound differs')
    queries = {
        'nodes': 'MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties LIMIT $limit',
        'relationships': ('MATCH (a)-[r]->(b) RETURN labels(a) AS source_labels, properties(a) AS source, '
                          'type(r) AS type, properties(r) AS properties, labels(b) AS target_labels, '
                          'properties(b) AS target LIMIT $limit'),
    }
    result = {}
    total_bytes = 0
    with driver.session(database=database, fetch_size=1) as session:
        for name, query in queries.items():
            encoded = []
            for row in islice(session.run(Query(query, timeout=10), limit=maximum+1), maximum+1):
                if len(encoded) >= maximum:
                    raise RuntimeError('graph observation exceeds row bound')
                try:
                    item = dict(row)
                    fields = ('labels',) if name == 'nodes' else ('source_labels', 'target_labels')
                    for field in fields:
                        labels = item[field]
                        if (not isinstance(labels, list) or not labels
                                or any(not isinstance(label, str) or not label for label in labels)
                                or len(labels) != len(set(labels))):
                            raise ValueError('graph labels differ')
                        item[field] = sorted(labels)
                    value = json.dumps(item, sort_keys=True, separators=(',', ':'), allow_nan=False)
                except (TypeError, ValueError) as error:
                    raise RuntimeError('graph properties are not bounded JSON values') from error
                total_bytes += len(value.encode())
                if total_bytes > 2*1024*1024:
                    raise RuntimeError('graph observation exceeds byte bound')
                encoded.append(value)
            result[name] = [json.loads(value) for value in sorted(encoded)]
    def identity(labels, properties):
        if (not isinstance(properties, dict) or key not in properties or type(properties[key]) not in (str, int)
                or (isinstance(properties[key], str) and not properties[key])):
            raise RuntimeError('graph node key is missing or unsupported')
        return tuple(labels), type(properties[key]).__name__, properties[key]
    nodes = {}
    for node in result['nodes']:
        item = identity(node['labels'], node['properties'])
        if item in nodes:
            raise RuntimeError('graph node keys are not unique')
        nodes[item] = node['properties']
    for relationship in result['relationships']:
        for labels, properties in (('source_labels', 'source'), ('target_labels', 'target')):
            item = identity(relationship[labels], relationship[properties])
            if item not in nodes or nodes[item] != relationship[properties]:
                raise RuntimeError('graph relationship endpoint differs from node observation')
    return result


def _regular_under(attempt_dir, name, maximum=8*1024*1024):
    """Open the canonical attempt file through no-follow directory handles."""
    root = Path(os.path.abspath(attempt_dir))
    if not root.name.startswith('graph-loader-') or name not in {'edge-plan.json', 'edge-plan.json.ledger.json'}:
        raise ValueError('owned attempt artifact identity differs')
    if not hasattr(os, 'O_DIRECTORY') or not hasattr(os, 'O_NOFOLLOW') or os.open not in os.supports_dir_fd:
        raise RuntimeError('safe attempt directory traversal is unavailable')
    flags = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0)
    directory = os.open('/', flags)
    try:
        for part in (*root.parts[1:], 'plans'):
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(name, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0), dir_fd=directory)
    finally:
        os.close(directory)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError('observation requires a bounded regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(maximum+1)
        if len(data) > maximum:
            raise ValueError('observation file exceeds bound')
        return data
    finally:
        os.close(fd)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _constant(value):
    raise ValueError('non-finite JSON value')


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('non-finite JSON value')
    return result


def committed_offsets(*, owner, bootstrap, group, partitions, broker_guard):
    """Read offsets without committing; caller must authenticate broker ownership.

    Loopback and token-prefixed names alone do not establish broker ownership.
    """
    prefix = 'e09-'+owner+'-' if isinstance(owner, str) and re.fullmatch(r'[0-9a-f]{32}', owner) else None
    if (prefix is None or not isinstance(bootstrap, str)
            or re.fullmatch(r'127\.0\.0\.1:[0-9]{1,5}', bootstrap) is None
            or not 1 <= int(bootstrap.rsplit(':', 1)[1]) <= 65535
            or not isinstance(group, str) or not group.startswith(prefix)
            or re.fullmatch(r'[A-Za-z0-9._-]{1,249}', group) is None
            or not isinstance(partitions, list) or not partitions or len(partitions) > 1000
            or not callable(broker_guard)):
        raise ValueError('scratch offset observation identity differs')
    expected = []
    for item in partitions:
        if (not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[0], str)
                or not item[0].startswith(prefix) or re.fullmatch(r'[A-Za-z0-9._-]{1,249}', item[0]) is None
                or type(item[1]) is not int or not 0 <= item[1] <= 2147483647 or item in expected):
            raise ValueError('scratch topic partition identity differs')
        expected.append(item)
    if broker_guard() is not True:
        raise RuntimeError('scratch broker ownership is unverified')
    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': group,
                         'enable.auto.commit': False, 'enable.auto.offset.store': False,
                         'socket.timeout.ms': 1000})
    failure = None
    try:
        rows = consumer.committed([TopicPartition(topic, partition) for topic, partition in expected], timeout=5)
        if len(rows) != len(expected) or [(row.topic, row.partition) for row in rows] != expected:
            raise RuntimeError('committed offset partition set differs')
        if any(row.error is not None or type(row.offset) is not int
               or (row.offset < 0 and row.offset != OFFSET_INVALID) for row in rows):
            raise RuntimeError('committed offset is unavailable')
        if broker_guard() is not True:
            raise RuntimeError('scratch broker ownership is unverified')
        return [{'topic': row.topic, 'partition': row.partition,
                 'offset': None if row.offset == OFFSET_INVALID else row.offset} for row in rows]
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            consumer.close()
        except BaseException as close_error:
            if failure is not None:
                failure.add_note('offset observer close also failed: '+type(close_error).__name__)
            else:
                raise


def plan_ledger(attempt_dir, *, expected_rows, expected_digest, expected_plan_id):
    """Check canonical artifacts against independently established run identity.

    The caller must independently prove that the attempt directory belongs to
    this run; a matching digest or directory name alone does not prove ownership.
    """
    if (type(expected_rows) is not int or expected_rows < 0 or not isinstance(expected_digest, str)
            or re.fullmatch(r'[0-9a-f]{64}', expected_digest) is None
            or not isinstance(expected_plan_id, str) or not expected_plan_id):
        raise ValueError('expected row count differs')
    raw = _regular_under(attempt_dir, 'edge-plan.json')
    try:
        plan = json.loads(raw, object_pairs_hook=_unique, parse_constant=_constant, parse_float=_finite_float)
        ledger = json.loads(_regular_under(attempt_dir, 'edge-plan.json.ledger.json'), object_pairs_hook=_unique, parse_constant=_constant, parse_float=_finite_float)
        chunks, rows = plan['chunks'], ledger['ledger']
        if (not isinstance(chunks, list) or not isinstance(rows, list)
                or len(chunks) > 10000 or len(rows) > 10000):
            raise ValueError('chunk collection differs')
        plan_counts = {}
        for chunk in chunks:
            if (not isinstance(chunk, list) or len(chunk) < 4 or type(chunk[0]) is not int
                    or type(chunk[3]) is not int or chunk[0] < 0 or chunk[3] < 0 or chunk[0] in plan_counts):
                raise ValueError('plan chunk differs')
            plan_counts[chunk[0]] = chunk[3]
        ledger_counts = {}
        for row in rows:
            if (not isinstance(row, list) or len(row) < 3 or type(row[0]) is not int
                    or type(row[2]) is not int or row[0] < 0 or row[2] < 0 or row[0] in ledger_counts):
                raise ValueError('ledger chunk differs')
            ledger_counts[row[0]] = row[2]
        digest = hashlib.sha256(raw).hexdigest()
        if (plan['plan_id'] != expected_plan_id or ledger['plan_id'] != expected_plan_id
                or digest != expected_digest or ledger['plan_digest'] != digest or ledger['state'] != 'complete'
                or type(ledger['chunks']) is not int or ledger['chunks'] != len(plan_counts)
                or type(ledger['rows']) is not int or ledger['rows'] != expected_rows
                or plan_counts != ledger_counts or sum(plan_counts.values()) != expected_rows):
            raise ValueError('plan and completed ledger differ')
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, RecursionError) as error:
        raise RuntimeError('plan ledger observation failed') from error
    return {'plan_digest': digest, 'chunks': len(plan_counts), 'rows': expected_rows}
