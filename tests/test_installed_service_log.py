"""Adversarial query-log coverage controls for installed service proof."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from docker.transport.unixconn import UnixHTTPAdapter
from neo4j import BoltDriver, Neo4jDriver
import pytest

from tests.installed_service.query_log import Snapshot, prefix_delta, snapshot, waits, window


OWNER = '1'*32
CONTAINER = '2'*64
IMAGE = 'sha256:'+'3'*64
CREATED = '2026-10-29T00:00:00Z'
SERVER = 'a'*64
SETTINGS = {'db.logs.query.enabled': 'VERBOSE', 'db.logs.query.threshold': '0s',
            'db.logs.query.parameter_logging_enabled': 'false'}


def fake(content, *, inode=42, trailer=None, exit_code=0):
    output = f'{inode} {len(content)}\n'.encode()+content+b'\n'+str(inode if trailer is None else trailer).encode()+b'\n'
    adapter = MagicMock(spec=UnixHTTPAdapter)
    adapter.socket_path = '/tmp/e09.sock'
    def execute(command):
        if 'cypher-shell' in command[-1]:
            return SimpleNamespace(exit_code=0, output=('id\n"'+SERVER+'"\n').encode())
        return SimpleNamespace(exit_code=exit_code, output=output)
    return SimpleNamespace(exec_run=execute,
        reload=lambda: None, client=SimpleNamespace(info=lambda: {'ID': 'daemon'},
            api=SimpleNamespace(base_url='http+docker://localhost', adapters={'http+docker://': adapter})),
        attrs={'Id': CONTAINER, 'Created': CREATED, 'Image': IMAGE,
               'Config': {'Labels': {'io.graph-loader.e09.owner': OWNER, 'io.graph-loader.e09.role': 'neo'},
                          'Env': ['NEO4J_db_logs_query_parameter__logging__enabled=false']}})


def fake_driver(settings=SETTINGS, *, host='127.0.0.1', port=7687):
    driver = MagicMock(spec=BoltDriver)
    driver.get_server_info.return_value = SimpleNamespace(address=SimpleNamespace(host=host, port=port))
    def run(query):
        if query.startswith('CALL dbms.info'):
            return SimpleNamespace(single=lambda: {'id': SERVER})
        return [{'name': name, 'value': value} for name, value in settings.items()]
    driver.session.return_value.__enter__.return_value.run.side_effect = run
    return driver


def capture(container, *, driver=None, **kwargs):
    container.attrs['NetworkSettings'] = {'Ports': {'7687/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '7687'}]}}
    return snapshot(container, owner=OWNER, container_id=CONTAINER, created=CREATED, image_id=IMAGE,
                    daemon_id='daemon', docker_host='unix:///tmp/e09.sock', driver=driver or fake_driver(), **kwargs)


def test_complete_log_window_only_counts_identified_queries():
    before = capture(fake(b'older record\n'))
    after = capture(fake(b'older record\n2026-10-29 13:00:00.123+0000 INFO id:1 - transaction id:1 - 1 ms: (planning: 0, waiting: 0) - target - runtime=pipelined - {}\n'
                         b'2026-10-29 13:00:01.123+0000 INFO id:2 - transaction id:2 - 40 ms: (planning: 1, waiting: 34) - target - runtime=pipelined - {}\n'))
    assert waits(prefix_delta(before, after), needle=b'target', minimum=2) == [0, 34]
    with pytest.raises(RuntimeError, match='coverage'):
        window(before, after)


@pytest.mark.parametrize('after', [Snapshot(43, b'old\nnew\n'), Snapshot(42, b'new\n'), Snapshot(42, b'ol')])
def test_rotation_truncation_and_mutation_never_report_zero(after):
    with pytest.raises(RuntimeError, match='discontinuous'):
        window(Snapshot(42, b'old\n'), after)


@pytest.mark.parametrize('data,needle,minimum', [
    (b'2026-10-29 13:00:00.123+0000 INFO (planning: 0, waiting: 0) unrelated\n', b'target', 1),
    (b'2026-10-29 13:00:00.123+0000 INFO (planning: 0, waiting: broken) target\n', b'target', 1),
    (b'2026-10-29 13:00:00.123+0000 INFO (planning: 0, waiting: 0) (planning: 0, waiting: 0) target\n', b'target', 1),
    (b'2026-10-29 13:00:00.123+0000 INFO (planning: 0, waiting: 0) target', b'target', 1),
])
def test_missing_malformed_or_incomplete_identified_records_fail(data, needle, minimum):
    with pytest.raises(RuntimeError):
        waits(data, needle=needle, minimum=minimum)


def test_snapshot_refuses_rotation_partial_record_and_read_failure():
    with pytest.raises(RuntimeError, match='rotated'):
        capture(fake(b'complete\n', trailer=43))
    with pytest.raises(RuntimeError, match='incomplete'):
        capture(fake(b'partial'))
    with pytest.raises(RuntimeError, match='unavailable'):
        capture(fake(b'', exit_code=1))


def test_snapshot_rejects_oversized_and_malformed_framing():
    with pytest.raises(RuntimeError):
        capture(fake(b'1234\n'), maximum=4)
    broken = fake(b'')
    original = broken.exec_run
    broken.exec_run = lambda command: original(command) if 'cypher-shell' in command[-1] else SimpleNamespace(exit_code=0, output=b'bad\n')
    with pytest.raises(RuntimeError, match='header'):
        capture(broken)


def test_multiline_query_text_cannot_override_the_header_wait():
    data = (b'2026-10-29 13:00:00.123+0000 INFO id:8 - transaction id:6 - 99 ms: (planning: 0, waiting: 99) - query\n'
            b'  MATCH target RETURN "waiting: 0)" - runtime=pipelined - {}\n')
    assert waits(data, needle=b'target', minimum=1) == [99]


def test_lookalike_line_without_real_log_header_is_not_a_query_record():
    with pytest.raises(RuntimeError):
        waits(b'not a log record target waiting: 0)\n', needle=b'target', minimum=1)


def test_empty_mark_cannot_prove_no_same_inode_truncation():
    with pytest.raises(RuntimeError, match='coverage'):
        window(Snapshot(42,b''), Snapshot(42,b'target waiting: 0)\n'))


def test_restored_prefix_still_cannot_prove_no_same_inode_truncation():
    with pytest.raises(RuntimeError, match='coverage'):
        window(Snapshot(42,b'old\n'), Snapshot(42,b'old\ntarget waiting: 0)\n'))


def test_log_read_refuses_unowned_container_or_parameter_logging():
    container = fake(b'')
    container.attrs['Config']['Labels']['io.graph-loader.e09.owner'] = '4'*32
    with pytest.raises(ValueError, match='owned identity'):
        capture(container)
    container = fake(b'')
    unsafe = dict(SETTINGS, **{'db.logs.query.parameter_logging_enabled': 'true'})
    with pytest.raises(ValueError, match='settings'):
        capture(container, driver=fake_driver(unsafe))
    with pytest.raises(ValueError, match='direct Bolt'):
        capture(fake(b''), driver=MagicMock(spec=Neo4jDriver))
    container = fake(b'')
    container.attrs['Config']['Env'] = ['NEO4J_db_logs_query_parameter__logging__enabled=true']
    with pytest.raises(ValueError, match='started with parameter logging disabled'):
        capture(container)


def test_log_read_fails_if_live_settings_change_during_capture():
    container = fake(b'complete\n')
    mutable = dict(SETTINGS)
    driver = fake_driver(mutable)
    original = container.exec_run
    def read_and_change(command):
        if 'cypher-shell' in command[-1]:
            return original(command)
        mutable['db.logs.query.parameter_logging_enabled'] = 'true'
        return SimpleNamespace(exit_code=0, output=b'42 9\ncomplete\n\n42\n')
    container.exec_run = read_and_change
    with pytest.raises(ValueError, match='settings'):
        capture(container, driver=driver)


@pytest.mark.parametrize('identity_change', ['id', 'created', 'image', 'owner', 'role', 'daemon', 'driver'])
def test_log_read_refuses_each_identity_mismatch_before_exec(identity_change):
    container = fake(b'')
    requested = {'id': 'Id', 'created': 'Created', 'image': 'Image'}
    if identity_change in requested:
        container.attrs[requested[identity_change]] = 'different'
    elif identity_change in {'owner', 'role'}:
        container.attrs['Config']['Labels']['io.graph-loader.e09.'+identity_change] = 'different'
    elif identity_change == 'daemon':
        container.client.info = lambda: {'ID': 'different'}
    driver = fake_driver(port=9999 if identity_change == 'driver' else 7687)
    container.exec_run = lambda command: pytest.fail('unverified container must never be read')
    with pytest.raises(ValueError):
        capture(container, driver=driver)


def test_log_read_rejects_remote_or_mismatched_docker_client_before_exec():
    container = fake(b'')
    container.client.api.base_url = 'http://127.0.0.1:2375'
    container.exec_run = lambda command: pytest.fail('unverified daemon must never be read')
    with pytest.raises(ValueError, match='local socket'):
        capture(container)


def test_log_read_rejects_a_forwarded_socket_reaching_a_different_neo4j():
    container = fake(b'')
    driver = fake_driver()
    def wrong_server(command):
        if 'cypher-shell' in command[-1]:
            return SimpleNamespace(exit_code=0, output=b'id\n"'+b'b'*64+b'"\n')
        pytest.fail('mismatched server must not have its log read')
    container.exec_run = wrong_server
    with pytest.raises(ValueError, match='server identities differ'):
        capture(container, driver=driver)
    assert driver.session.return_value.__enter__.return_value.run.called
    container = fake(b'')
    container.client.api.adapters['http+docker://'].socket_path = '/tmp/other.sock'
    with pytest.raises(ValueError, match='local socket'):
        capture(container)


@pytest.mark.parametrize('output,exit_code', [
    (b'private-credential-sentinel', 1),
    (b'\xff', 0),
    (b'id\nnot-a-uuid\n', 0),
    (('id\n'+SERVER+'\n'+SERVER+'\n').encode(), 0),
])
def test_container_identity_probe_errors_do_not_expose_output_or_read_log(output, exit_code):
    container = fake(b'')
    def bad_probe(command):
        if 'cypher-shell' in command[-1]:
            return SimpleNamespace(exit_code=exit_code, output=output)
        pytest.fail('failed identity probe must not read the log')
    container.exec_run = bad_probe
    with pytest.raises(ValueError) as failure:
        capture(container)
    assert 'private-credential-sentinel' not in str(failure.value)


def test_missing_host_server_identity_fails_without_reading_log():
    container = fake(b'')
    driver = fake_driver()
    def run(query):
        if query.startswith('CALL dbms.info'):
            return SimpleNamespace(single=lambda: None)
        return [{'name': name, 'value': value} for name, value in SETTINGS.items()]
    driver.session.return_value.__enter__.return_value.run.side_effect = run
    container.exec_run = lambda command: pytest.fail('missing host identity must not execute container commands')
    with pytest.raises(ValueError, match='server identity is invalid'):
        capture(container, driver=driver)


def test_query_text_cannot_forge_a_completion_timing_field():
    data = b'2026-10-29 13:00:00.123+0000 INFO Query failed - target RETURN "(planning: 0, waiting: 0)"\n'
    with pytest.raises(RuntimeError):
        waits(data, needle=b'target', minimum=1)


def test_query_started_text_inside_completed_query_is_not_a_start_record():
    data = (b'2026-10-29 13:00:00.123+0000 INFO id:8 - transaction id:6 - 9 ms: '
            b'(planning: 0, waiting: 7) - target RETURN "Query started" - runtime=pipelined - {}\n')
    assert waits(data, needle=b'target', minimum=1) == [7]


def test_completed_record_can_contain_nonempty_transaction_metadata():
    data = (b'2026-10-29 13:00:00.123+0000 INFO id:8 - transaction id:6 - 9 ms: '
            b"(planning: 0, waiting: 7) - target - runtime=system - {app: 'cypher-shell', type: 'user-direct'}\n")
    assert waits(data, needle=b'target', minimum=1) == [7]


def test_timestamp_shaped_line_inside_query_cannot_forge_a_zero():
    data = (b'2026-10-29 13:00:00.123+0000 INFO id:8 - transaction id:6 - 99 ms: '
            b'(planning: 0, waiting: 99) - query /*\n'
            b'2026-10-29 13:00:00.123+0000 INFO id:9 - transaction id:9 - 0 ms: '
            b'(planning: 0, waiting: 0) - target\n*/\n')
    with pytest.raises(RuntimeError):
        waits(data, needle=b'target', minimum=1)


def test_forged_header_and_trailer_inside_multiline_comment_cannot_report_zero():
    data = (b'2026-10-29 13:00:00.123+0000 INFO id:8 - transaction id:6 - 99 ms: '
            b'(planning: 0, waiting: 99) - RETURN 1 /* - runtime=pipelined - {}\n'
            b'2026-10-29 13:00:00.123+0000 INFO id:9 - transaction id:9 - 0 ms: '
            b'(planning: 0, waiting: 0) - target\n*/ - runtime=pipelined - {}\n')
    with pytest.raises(RuntimeError):
        waits(data, needle=b'target', minimum=1)
