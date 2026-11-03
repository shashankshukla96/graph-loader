"""The host shapes' own tests (E14.F3.S10): wheel verification with its negative controls, the unit's exit-code reading, the file log
collector's no-double-counting, the registry view, and the fault verdicts for the process shape."""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from soak import host
from soak.host_runtime import FileLogCollector

FIXTURES = Path(__file__).resolve().parents[2] / 'tests' / 'fixtures'


def digest(data: bytes) -> str:
    return 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()


def make_wheel(directory: Path, files: dict) -> Path:
    wheel = directory / 'pkg-1.0-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        rows = []
        for name, data in files.items():
            archive.writestr(name, data)
            rows.append(f'{name},{digest(data)},{len(data)}')
        rows.append('pkg-1.0.dist-info/RECORD,,')
        archive.writestr('pkg-1.0.dist-info/RECORD', '\n'.join(rows) + '\n')
    return wheel


@pytest.fixture
def installed(tmp_path, monkeypatch):
    files = {'pkg/__init__.py': b'VERSION = 1\n', 'pkg/core.py': b'def run():\n    return 1\n'}
    wheel = make_wheel(tmp_path, files)
    site = tmp_path / 'site'
    for name, data in files.items():
        (site / name).parent.mkdir(parents=True, exist_ok=True)
        (site / name).write_bytes(data)
    monkeypatch.setattr(host, 'site_packages', lambda venv_bin: site)
    return wheel, site


def test_the_installed_package_matches_the_wheel(installed):
    wheel, _site = installed
    facts = host.verify_wheel('/v/bin', wheel, expected_sha256=host.sha256_file(wheel))
    assert facts['installed_files_checked'] == 2 and facts['wheel_sha256'] == host.sha256_file(wheel)


def test_a_modified_installed_file_is_refused(installed):
    wheel, site = installed
    (site / 'pkg' / 'core.py').write_bytes(b'def run():\n    return 2\n')
    with pytest.raises(RuntimeError, match='differs from the wheel'):
        host.verify_wheel('/v/bin', wheel)


def test_a_missing_installed_file_is_refused(installed):
    wheel, site = installed
    (site / 'pkg' / 'core.py').unlink()
    with pytest.raises(RuntimeError, match='missing'):
        host.verify_wheel('/v/bin', wheel)


def test_a_wrong_expected_wheel_hash_is_refused(installed):
    wheel, _site = installed
    with pytest.raises(RuntimeError, match='differs from the expected'):
        host.verify_wheel('/v/bin', wheel, expected_sha256='0' * 64)


# ---- the unit's main process --------------------------------------------------------------------------------------------------

class FakeUnit:
    def __init__(self, props, journal=''):
        self.props, self.text = props, journal

    def show(self, *names):
        return {n: self.props[n] for n in names}

    def journal(self, since=None, tail=None):
        return self.text


def main_process(unit):
    process = host._MainProcess.__new__(host._MainProcess)
    process.unit, process.pid, process.returncode, process.started_at, process.identity = unit, 4242, None, 100.0, None
    return process


def test_exit_code_of_a_clean_stop_a_failure_and_a_kill():
    clean = FakeUnit({'ExecMainPID': '4242', 'ExecMainCode': '1', 'ExecMainStatus': '0'})
    failed = FakeUnit({'ExecMainPID': '4242', 'ExecMainCode': '1', 'ExecMainStatus': '1'})
    killed = FakeUnit({'ExecMainPID': '4242', 'ExecMainCode': '2', 'ExecMainStatus': '9'})
    assert [main_process(u)._exit_code() for u in (clean, failed, killed)] == [0, 1, -9]


def test_after_systemd_restarted_the_unit_the_journal_line_of_this_process_decides():
    restarted = FakeUnit({'ExecMainPID': '5555', 'ExecMainCode': '1', 'ExecMainStatus': '0'},
                         journal='x\nunit.service: Main process exited, code=killed, status=9/KILL\ny\n')
    assert main_process(restarted)._exit_code() == -9
    quiet = FakeUnit({'ExecMainPID': '5555', 'ExecMainCode': '1', 'ExecMainStatus': '0'}, journal='Stopped unit.\n')
    assert main_process(quiet)._exit_code() == 0          # a clean stop leaves no "Main process exited" line: never an earlier one


# ---- log counting -------------------------------------------------------------------------------------------------------------

class FakeSoak:
    def __init__(self, tmp_path):
        self.claimed_attempts = {str(tmp_path / 'attempt')}
        self.log_dir = tmp_path / 'logs'
        self.log_dir.mkdir()
        (tmp_path / 'attempt' / 'logs').mkdir(parents=True)
        self.unit = None
        self.supervisor = None


def test_relayed_child_lines_are_not_counted_twice(tmp_path):
    soak = FakeSoak(tmp_path)
    line = 'INFO: stage=write-outage edge=PURCHASED seconds=3'
    (tmp_path / 'attempt' / 'logs' / 'graph-loader-edge-PURCHASED-0.log').write_text(line + '\n')
    (soak.log_dir / 'fleet001.err').write_text(f'[graph-loader-edge-PURCHASED-0] {line}\n' + 'INFO: stage=lease-server-restart attempt=1\n')
    collector = FileLogCollector(soak)
    assert collector.total('stage=write-outage ') == 1                  # the child's file only; the relayed copy in the CLI output is skipped
    assert collector.total('stage=lease-server-restart') == 1           # the CLI's own line is counted


def test_a_partial_last_line_is_read_once_it_is_complete(tmp_path):
    soak = FakeSoak(tmp_path)
    path = tmp_path / 'attempt' / 'logs' / 'child.log'
    path.write_text('INFO: stage=write-outage-recovered a\nINFO: stage=write-out')
    collector = FileLogCollector(soak)
    assert collector.total('stage=write-outage-recovered') == 1
    path.write_text(path.read_text() + 'age-recovered b\n')
    assert collector.total('stage=write-outage-recovered') == 2


# ---- registry view ------------------------------------------------------------------------------------------------------------

ROLE = 'script:' + str(FIXTURES / 'e14_role.py')


@pytest.mark.skipif(sys.platform != 'linux', reason='the identity check of a registry record needs /proc')
def test_a_recorded_child_is_alive_until_it_exits_and_a_pid_reuse_does_not_count(tmp_path):
    from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
    runtime = ProcessRuntime(tmp_path / 'attempt', ready_timeout=30)
    runtime.register_cli()
    handle = runtime.spawn('node-loader', ROLE, ['ready', str(tmp_path / 'm')], {'GRAPH_LOADER_CHILD_ALLOW_SCRIPT': '1'}, name='node-X-0',
                           labels={'component': 'node-loader'})
    try:
        assert [s['name'] for s in host.alive_children(tmp_path / 'attempt')] == ['node-X-0']
        state = host.alive_children(tmp_path / 'attempt')[0]
        stale = {**state, 'start_time': str(int(state['start_time']) + 1)}
        assert not host.is_alive(stale)                                   # same pid, different start time: another process
        assert host.signal_state(state, 15) and handle.wait(20) == 0
        assert host.alive_children(tmp_path / 'attempt') == []
        assert not host.signal_state(state, 9)                            # gone: nothing is signalled
    finally:
        runtime.close()


def test_the_process_environment_names_no_docker_and_the_scratch_credentials():
    class Stack:
        neo_uri, password, database, bootstrap = 'bolt://127.0.0.1:7', 'secret', 'db', '127.0.0.1:9'
    env = host.child_env(Stack(), {'X': '1'})
    assert env['DOCKER_HOST'] == host.NO_DOCKER and env['NEO4J_PASSWORD'] == 'secret' and env['X'] == '1'
    assert 'HOME' in env or 'PATH' in env


# ---- explanation windows never outlive their fault ----------------------------------------------------------------------------

def test_an_ended_explanation_window_no_longer_absorbs_a_fleet_exit():
    import time
    from soak.fleet import Supervisor
    supervisor = Supervisor(None, lambda n: None)
    supervisor.explain_until('kafka broker restart', time.time() + 400)
    assert supervisor._take(1)['cause'] == 'kafka broker restart'          # inside the window an exit is explained by the fault
    supervisor.end_windows('kafka broker restart')                         # the fault is over and the fleet is ready again
    assert supervisor._take(1) is None                                     # an exit now is unexplained, not silently absorbed
