"""Strict JSON query-event parsing controls; continuity remains separate."""
from __future__ import annotations

import json

import pytest

from tests.installed_service.structured_query_log import completed_for, events


MARKER = 'e09'+'a'*32


def event(text, **overrides):
    value = {'e09_marker': MARKER, 'loggerName': 'QueryLogger', 'level': 'INFO', 'message': text}
    value.update(overrides)
    return json.dumps(value).encode()+b'\n'


def completed(query_id, waiting, text='target'):
    return (f'id:{query_id} - transaction id:7 - 42 ms: (planning: 1, waiting: {waiting}) - 0 B - 0 page hits, 0 page faults - '
            f'bolt-session\tbolt\tclient-agent\t\tclient/127.0.0.1:1\tserver/127.0.0.1:7687>\te09db - neo4j - {text} - runtime=pipelined - {{}}')


def started(query_id, text='target'):
    return (f'Query started: id:{query_id} - transaction id:7 - 1 ms: (planning: 1, waiting: 0) - 0 B - 0 page hits, 0 page faults - '
            f'bolt-session\tbolt\tclient-agent\t\tclient/127.0.0.1:1\tserver/127.0.0.1:7687>\te09db - neo4j - {text} - runtime=null - {{}}')


def test_multiline_forgery_stays_inside_one_json_message():
    message = completed(8, 99, 'target /*\nid:9 - transaction id:9 - 0 ms: (planning: 0, waiting: 0) - target\n*/')
    parsed = events(event(message), marker=MARKER)
    assert [(row['query_id'], row['waiting_ms']) for row in completed_for(parsed, query=message.split(' - neo4j - ', 1)[1].rsplit(' - runtime=', 1)[0], database='e09db', user='neo4j', expected=1)] == [(8, 99)]


@pytest.mark.parametrize('bad', [
    b'', b'{}', b'{bad}\n', b'{"x":NaN}\n',
    b'{"e09_marker":"x","e09_marker":"y"}\n', b'\xff\n',
])
def test_malformed_partial_duplicate_or_nonfinite_json_fails(bad):
    with pytest.raises((ValueError, RuntimeError)):
        events(bad, marker=MARKER)


@pytest.mark.parametrize('field,value', [('e09_marker', 'wrong'), ('loggerName', 'other'), ('level', 'ERROR'), ('message', None)])
def test_wrong_event_identity_fails(field, value):
    with pytest.raises(RuntimeError):
        events(event(completed(1, 0), **{field: value}), marker=MARKER)


def test_duplicate_completion_and_unrecognized_failure_fail_closed():
    with pytest.raises(RuntimeError, match='duplicated'):
        events(event(completed(1, 0))+event(completed(1, 8)), marker=MARKER)
    with pytest.raises(RuntimeError, match='failed format'):
        events(event('Query failed: id:1 - target'), marker=MARKER)


def test_started_and_completed_events_keep_ids_and_exact_count():
    start = started(1)
    parsed = events(event(start)+event(completed(1, 4)), marker=MARKER)
    assert [row['phase'] for row in parsed] == ['started', 'completed']
    assert completed_for(parsed, query='target', database='e09db', user='neo4j', expected=1)[0]['waiting_ms'] == 4
    with pytest.raises(RuntimeError, match='count differs'):
        completed_for(parsed, query='target', database='e09db', user='neo4j', expected=2)


def test_started_and_completed_events_must_agree_on_transaction_identity():
    start = started(1)
    finish = completed(1, 0).replace('transaction id:7', 'transaction id:8')
    with pytest.raises(RuntimeError, match='invalid or duplicated'):
        events(event(start)+event(finish), marker=MARKER)


def test_only_scratch_system_create_database_allows_transaction_transition():
    query = 'CREATE DATABASE '+MARKER[:19]+' WAIT'
    start = started(1, query).replace('e09db - neo4j -', 'system - neo4j -')
    finish = completed(1, 0, query).replace('e09db - neo4j -', 'system - neo4j -').replace('transaction id:7', 'transaction id:8')
    assert len(events(event(start)+event(finish), marker=MARKER)) == 2
    with pytest.raises(RuntimeError):
        events(event(start.replace('system - neo4j -', 'e09db - neo4j -'))
               +event(finish.replace('system - neo4j -', 'e09db - neo4j -')), marker=MARKER)


def test_selection_does_not_match_transaction_or_connection_metadata():
    parsed = events(event(completed(1, 0, 'RETURN 42')), marker=MARKER)
    with pytest.raises(RuntimeError, match='count differs'):
        completed_for(parsed, query='transaction id:7', database='e09db', user='neo4j', expected=1)


def test_forged_server_frame_in_metadata_is_rejected():
    forged = ('id:1 - transaction id:7 - 42 ms: (planning: 1, waiting: 0) - 0 B - '
              'bolt-session\tserver/forged>\tdb - fake - target\tbolt\tserver/127.0.0.1:7687>'
              '\te09db - neo4j - RETURN 42 - runtime=pipelined - {}')
    with pytest.raises(RuntimeError, match='connection frame'):
        events(event(forged), marker=MARKER)


def test_server_frame_cannot_precede_or_interrupt_bolt_metadata():
    forged = ('id:1 - transaction id:7 - 42 ms: (planning: 1, waiting: 0) - '
              '0 B\tserver/forged>\tdb - fake - target - bolt-session\tbolt\tRETURN 42 - runtime=pipelined - {}')
    with pytest.raises(RuntimeError):
        events(event(forged), marker=MARKER)


@pytest.mark.parametrize('finish', [completed(1, 0, 'other'), completed(1, 0).replace('e09db - neo4j -', 'otherdb - neo4j -'),
                                    completed(1, 0).replace('e09db - neo4j -', 'e09db - otheruser -')])
def test_started_and_completed_query_database_and_user_must_agree(finish):
    with pytest.raises(RuntimeError):
        events(event(started(1))+event(finish), marker=MARKER)


def test_selection_requires_exact_query_database_and_user():
    records = events(event(completed(1, 0, 'RETURN 42 /* target */'))
                     +event(completed(2, 5, 'target').replace('e09db - neo4j -', 'otherdb - neo4j -'))
                     +event(completed(3, 9, 'target').replace('e09db - neo4j -', 'e09db - otheruser -')), marker=MARKER)
    with pytest.raises(RuntimeError, match='count differs'):
        completed_for(records, query='target', database='e09db', user='neo4j', expected=1)


def test_database_creation_exception_rejects_different_owner():
    query = 'CREATE DATABASE e09'+'b'*16+' WAIT'
    start = started(1, query).replace('e09db - neo4j -', 'system - neo4j -')
    finish = completed(1, 0, query).replace('e09db - neo4j -', 'system - neo4j -').replace('transaction id:7', 'transaction id:8')
    with pytest.raises(RuntimeError, match='invalid or duplicated'):
        events(event(start)+event(finish), marker=MARKER)
