"""Fail-closed controls for the lock-wait query-log window (synthetic logs plus the live owned log)."""
from __future__ import annotations

import json

import pytest

from tests.installed_service import query_window as qw

pytestmark = pytest.mark.integration

MARKER = 'e09' + 'ab' * 16


def event(query_id, phase, *, query='MATCH (n) RETURN n', waiting=0, database='neo4j', user='neo4j', level='INFO',
          marker=MARKER, transaction=7, runtime='pipelined'):
    head = 'Query started: ' if phase == 'started' else ''
    message = (f'{head}id:{query_id} - transaction id:{transaction} - 3 ms: (planning: 1, waiting: {waiting}) - 0 B - '
               f'0 page hits, 0 page faults - bolt-session\tbolt\tpython/1\t\tclient/10.0.0.1:5\tserver/10.0.0.2:7687>\t'
               f'{database} - {user} - {query} - runtime={runtime} - {{}}')
    return json.dumps({'loggerName': 'QueryLogger', 'level': level, 'message': message, 'e09_marker': marker}) + '\n'


def log(*ids, **kwargs):
    return ''.join(event(i, phase, **kwargs) for i in ids for phase in ('started', 'completed')).encode()


def capture(content, inode=11):
    return qw.parse(inode, content, MARKER)


def test_well_formed_window_yields_identified_waits():
    before = capture(log(1, 2))
    wait_query = 'MATCH (n:L {id: 1}) SET n.v = 2'
    content = before.content + log(3, query=wait_query, waiting=1531) + log(4, query=wait_query, waiting=0)
    delta = qw.window(before, capture(content))
    assert qw.waits(delta, query=wait_query, database='neo4j', user='neo4j', expected=2) == [1531, 0]


def test_zero_requires_the_identified_query_to_be_parsed():
    before = capture(log(1))
    delta = qw.window(before, capture(before.content + log(2, query='OTHER')))
    with pytest.raises(RuntimeError, match='count differs'):
        qw.waits(delta, query='MATCH (n:L) RETURN n', database='neo4j', user='neo4j', expected=1)


def test_truncated_log_is_discontinuous():
    before = capture(log(1, 2, 3))
    with pytest.raises(RuntimeError, match='truncated'):
        qw.window(before, capture(log(1, 2)))


def test_same_size_rewrite_is_discontinuous():
    before = capture(log(1, 2))
    rewritten = log(1, 2, query='MATCH (m) RETURN m')
    with pytest.raises(RuntimeError, match='truncated or rewritten'):
        qw.window(before, capture(rewritten + log(3)))


def test_rotated_inode_is_discontinuous():
    before = capture(log(1, 2), inode=11)
    with pytest.raises(RuntimeError, match='rotated'):
        qw.window(before, capture(before.content + log(3), inode=12))


def test_restart_starts_a_new_epoch_and_cannot_be_windowed_across():
    before = capture(log(1, 2, 3))
    restarted = capture(before.content + log(1, 2))
    assert (before.epochs, restarted.epochs, restarted.max_id) == (1, 2, 2)
    with pytest.raises(RuntimeError, match='restarted'):
        qw.window(before, restarted)
    later = capture(restarted.content + log(3))
    assert [row['query_id'] for row in qw.window(restarted, later)] == [3, 3]


def test_a_gap_inside_the_new_epoch_is_rejected():
    with pytest.raises(RuntimeError, match='not contiguous'):
        capture(log(1, 2) + log(1, 3))


def test_missing_id_is_rejected():
    with pytest.raises(RuntimeError, match='not contiguous'):
        capture(log(1, 2, 4))


def test_started_without_completion_is_not_quiescent():
    with pytest.raises(RuntimeError, match='not quiescent'):
        capture(log(1) + event(2, 'started').encode())


@pytest.mark.parametrize('content, message', [
    (b'', 'empty or incomplete'),
    (log(1)[:-1], 'empty or incomplete'),
    (log(1) + b'{not json}\n', 'malformed'),
    (log(1, marker='e09' + 'cd' * 16), 'identity differs'),
    (log(1) + event(2, 'completed', level='ERROR').encode(), 'identity differs'),
    (log(1, 2) + event(2, 'completed').encode(), 'duplicated'),
], ids=['empty', 'partial-line', 'malformed-json', 'foreign-marker', 'failed-query', 'duplicate-completion'])
def test_malformed_missing_foreign_or_failed_records_fail_closed(content, message):
    with pytest.raises(RuntimeError, match=message):
        capture(content)


class FakeContainer:
    def __init__(self, outputs):
        self.outputs, self.calls = list(outputs), 0

    def exec_run(self, command):
        self.calls += 1
        output = self.outputs[min(self.calls - 1, len(self.outputs) - 1)]
        return type('Result', (), {'exit_code': 0, 'output': output})()


def read(content, inode=5, trailer_inode=None):
    return (f'{inode} {len(content)}\n').encode() + content + f'\n{trailer_inode or inode}\n'.encode()


def test_capture_waits_for_two_identical_quiescent_reads():
    partial = log(1) + event(2, 'started').encode()
    container = FakeContainer([read(partial), read(log(1, 2)), read(log(1, 2))])
    assert qw.capture(container, MARKER, timeout=10).max_id == 2
    assert container.calls == 3


def test_capture_times_out_when_never_quiescent():
    container = FakeContainer([read(log(1) + event(2, 'started').encode())])
    with pytest.raises(TimeoutError):
        qw.capture(container, MARKER, timeout=1)


@pytest.mark.parametrize('output', [b'garbage', read(log(1), trailer_inode=9), read(log(1))[:-3]],
                         ids=['garbage', 'trailer-mismatch', 'truncated-trailer'])
def test_read_log_rejects_changed_or_invalid_captures(output):
    with pytest.raises(RuntimeError):
        qw.read_log(FakeContainer([output]))


DDL = 'CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE'


def test_schema_command_repeats_its_completion_once_and_is_flagged_not_counted():
    content = (event(1, 'started', query=DDL, runtime='schema').encode() + event(1, 'completed', query=DDL, runtime='schema').encode()
               + event(1, 'completed', query=DDL, runtime='schema').encode())
    records = capture(content).records
    assert [bool(r.get('duplicate')) for r in records] == [False, False, True]
    window = qw.window(capture(log(1)), capture(log(1) + log(2, query=DDL, runtime='schema') + event(2, 'completed', query=DDL, runtime='schema').encode()))
    assert qw.waits(window, query=DDL, database='neo4j', user='neo4j', expected=1) == [0]


def test_other_repeated_completions_still_fail():
    for runtime, query in (('pipelined', DDL), ('schema', 'MATCH (n) RETURN n')):
        content = (event(1, 'started', query=query, runtime=runtime).encode() + event(1, 'completed', query=query, runtime=runtime).encode()
                   + event(1, 'completed', query=query, runtime=runtime).encode())
        with pytest.raises(RuntimeError, match='duplicated'):
            capture(content)
    triple = event(1, 'started', query=DDL, runtime='schema').encode() + event(1, 'completed', query=DDL, runtime='schema').encode() * 3
    with pytest.raises(RuntimeError, match='duplicated'):
        capture(triple)


@pytest.mark.parametrize('first, second', [
    ({'waiting': 0}, {'waiting': 2500}),
    ({'waiting': 2500}, {'waiting': 0}),
    ({'runtime': 'pipelined'}, {'runtime': 'schema'}),
    ({'runtime': 'schema'}, {'runtime': 'pipelined'}),
])
def test_a_repeated_schema_completion_with_a_conflicting_measurement_fails(first, second):
    base = {'query': DDL, 'runtime': 'schema'}
    content = (event(1, 'started', **base).encode() + event(1, 'completed', **{**base, **first}).encode()
               + event(1, 'completed', **{**base, **second}).encode())
    with pytest.raises(RuntimeError, match='duplicated'):
        capture(content)


def failed_event(query_id, *, failure='The transaction has been terminated. Retry your operation in a new transaction.', **kwargs):
    row = json.loads(event(query_id, 'completed', level='ERROR', **kwargs))
    row['message'] += f' - {failure}'
    return json.dumps(row) + '\n'


def test_a_failed_query_is_one_terminal_record_with_its_wait_and_failure_text():
    wait_query = 'MATCH (n:L {id: 1}) SET n.v = 2'
    before = capture(log(1))
    content = before.content + (event(2, 'started', query=wait_query) + failed_event(2, query=wait_query, waiting=1531)).encode()
    delta = qw.window(before, capture(content))
    failed = [r for r in delta if r['phase'] == 'failed']
    assert [(r['query_id'], r['waiting_ms'], r['query']) for r in failed] == [(2, 1531, wait_query)]
    assert failed[0]['failure'].startswith('The transaction has been terminated')
    with pytest.raises(RuntimeError, match='count differs'):
        qw.waits(delta, query=wait_query, database='neo4j', user='neo4j', expected=1)


@pytest.mark.parametrize('case', ['failed after completed', 'completed after failed', 'failed twice'])
def test_a_query_that_ends_twice_is_rejected(case):
    started, done, failed = event(1, 'started'), event(1, 'completed'), failed_event(1)
    content = {'failed after completed': started + done + failed, 'completed after failed': started + failed + done,
               'failed twice': started + failed + failed}[case]
    with pytest.raises(RuntimeError, match='invalid or duplicated'):
        capture(content.encode())


def test_a_failure_without_a_start_or_a_start_without_an_end_is_not_quiescent():
    with pytest.raises(RuntimeError, match='not quiescent'):
        capture(failed_event(1).encode())
    with pytest.raises(RuntimeError, match='not quiescent'):
        capture(event(1, 'started').encode())


@pytest.mark.parametrize('mutation', ['marker', 'logger', 'no failure text', 'unsupported level', 'failure text too long'])
def test_error_events_that_are_not_well_formed_failures_still_fail_closed(mutation):
    row = json.loads(failed_event(1))
    if mutation == 'marker':
        row['e09_marker'] = 'e09' + 'cd' * 16
    elif mutation == 'logger':
        row['loggerName'] = 'Other'
    elif mutation == 'no failure text':
        row['message'] = json.loads(event(1, 'completed', level='ERROR'))['message']
    elif mutation == 'unsupported level':
        row['level'] = 'WARN'
    else:
        row['message'] += 'x' * 3000
    with pytest.raises(RuntimeError, match='identity differs'):
        capture((event(1, 'started') + json.dumps(row) + '\n').encode())


def open_capture(content, **kwargs):
    return qw.parse(11, content, MARKER, allow_open=True, **kwargs)


def test_a_query_left_open_is_rejected_by_default_and_reported_when_allowed():
    content = log(1) + event(2, 'started').encode()
    with pytest.raises(RuntimeError, match='not quiescent'):
        capture(content)
    allowed = open_capture(content)
    assert (allowed.open_ids, allowed.max_id) == ((2,), 2)
    assert open_capture(log(1, 2)).open_ids == ()


def test_a_window_counts_an_open_query_as_one_record_and_never_hides_missing_ones():
    before = open_capture(log(1))
    after = open_capture(before.content + log(2) + event(3, 'started', query='LEFT OPEN').encode())
    delta = qw.window(before, after)
    assert sorted((r['query_id'], r['phase']) for r in delta) == [(2, 'completed'), (2, 'started'), (3, 'started')]
    # An ID that is missing between the marks stays an error even when open queries are allowed.
    broken = log(1) + event(3, 'started').encode()
    with pytest.raises(RuntimeError, match='contiguous'):
        open_capture(broken)


def test_an_end_without_a_start_is_still_rejected_when_open_queries_are_allowed():
    with pytest.raises(RuntimeError, match='not quiescent'):
        open_capture(event(1, 'completed').encode())


def test_a_query_open_at_the_first_mark_may_end_later_but_never_start_twice():
    first = open_capture(log(1) + event(2, 'started').encode())
    ended = open_capture(first.content + event(2, 'completed').encode() + log(3))
    delta = qw.window(first, ended)
    assert sorted((r['query_id'], r['phase']) for r in delta) == [(2, 'completed'), (3, 'completed'), (3, 'started')]
    failed = open_capture(first.content + failed_event(2).encode())
    assert [r['phase'] for r in qw.window(first, failed)] == ['failed']
    still = open_capture(first.content + log(3))
    assert [r['query_id'] for r in qw.window(first, still)] == [3, 3]
    assert still.open_ids == (2,)
