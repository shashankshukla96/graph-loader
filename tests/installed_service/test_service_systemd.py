"""E14.F3.S9 the shipped systemd user unit on a real systemd user manager (Linux host, no sudo, no Docker image).

``examples/systemd/graph-loader.service`` is installed with ``systemctl --user link`` under an owned name
(``e14-<owner8>-<cell>.service``) with only its paths substituted, driven with ``systemctl --user`` against the owned Neo4j/Kafka
stack, and removed again (link, ``daemon-reload``) when the cell ends. No supervisor of our own stands in for systemd.

* load + readiness + ordered stop: ``/readyz`` goes 200, the stream fixture loads to the exact graph, the cgroup holds the main
  process plus the six registered children, child log lines reach the journal with their ``[graph-loader-...]`` prefix,
  ``systemctl --user stop`` ends in an ordered stop inside ``TimeoutStopSec`` (Result=success, status 0, every child exited 0,
  nothing left in the cgroup, no restart);
* control: the same unit with ``KillMode=control-group`` skips the ordered stop;
* ``kill -9`` of the main process during a load: systemd removes the rest, restarts the unit (``Restart=on-failure``), the run resumes
  after the group-member wait with exactly one fleet and the exact graph.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator.process_service import registered_children
from tests.installed_service import stream_fixture as sf
from tests.installed_service.live_stack import free_port
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    TOTAL_BIG, assert_clean, children, memory_guard, mid_load, producing, records, stream_clear, stream_committed, stream_ends,
    stream_own_graph, stream_topics, wait_for,
)

pytestmark = pytest.mark.integration

EXAMPLE = Path(__file__).resolve().parents[2] / 'examples' / 'systemd' / 'graph-loader.service'
USER_UNIT_DIR = Path.home() / '.config' / 'systemd' / 'user'
FLEET = 6                         # lease server + 2 node loaders + 3 edge loaders


def sysenv():
    runtime = f'/run/user/{os.getuid()}'
    if not Path(runtime).is_dir():
        pytest.skip('no systemd user runtime directory')
    return {**os.environ, 'XDG_RUNTIME_DIR': runtime, 'DBUS_SESSION_BUS_ADDRESS': f'unix:path={runtime}/bus'}


def ctl(*args, check=True, timeout=180):
    done = subprocess.run(['systemctl', '--user', *args], env=sysenv(), capture_output=True, text=True, timeout=timeout)
    if check and done.returncode != 0:
        raise AssertionError(f'systemctl --user {" ".join(args)} -> {done.returncode}: {done.stderr or done.stdout}')
    return done


class Unit:
    """The example unit with its paths substituted, linked under an owned name."""

    def __init__(self, stack, config, tag, schema_text, *, kill_mode='mixed'):
        self.stack, self.config = stack, config
        self.name = f'e14-{stack.owner[:8]}-{tag}.service'
        self.dir = Path(config['run_dir']) / f's9-{tag}'
        self.dir.mkdir()
        (self.dir / 'runs').mkdir()
        self.health_port = free_port()
        self.link = USER_UNIT_DIR / self.name
        (self.dir / 'schema.yaml').write_text(schema_text)
        env_file = self.dir / 'graph-loader.env'
        fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as out:
            out.write(f'NEO4J_URI={stack.neo_uri}\nNEO4J_USERNAME=neo4j\nNEO4J_PASSWORD={stack.password}\n'
                      f'NEO4J_DATABASE={stack.database}\nKAFKA_BOOTSTRAP_SERVERS={stack.bootstrap}\nGRAPH_LOADER_METRICS_ENABLED=0\n')
        text = EXAMPLE.read_text()
        for old, new in (('%h/.config/graph-loader/graph-loader.env', str(env_file)),
                         ('%h/graph-loader/venv/bin/graph-loader', str(Path(config['venv_bin']) / 'graph-loader')),
                         ('%h/graph-loader/schema.yaml', str(self.dir / 'schema.yaml')),
                         ('%h/graph-loader/runs', str(self.dir / 'runs')),
                         ('127.0.0.1:8081', f'127.0.0.1:{self.health_port}'),
                         ('KillMode=mixed', f'KillMode={kill_mode}')):
            assert text.count(old) >= 1, old
            text = text.replace(old, new)
        assert '%h' not in text, 'an unsubstituted path is left in the unit'
        (self.dir / self.name).write_text(text)
        self.timeout_stop = 120
        ctl('link', str(self.dir / self.name))
        ctl('daemon-reload')

    def start(self):
        ctl('start', self.name)

    def show(self, *props):
        out = ctl('show', self.name, *[f'-p{p}' for p in props]).stdout
        return dict(line.split('=', 1) for line in out.splitlines() if '=' in line)

    def active(self):
        return ctl('is-active', self.name, check=False).stdout.strip() == 'active'

    def attempts(self):
        found = [p for p in (self.dir / 'runs').iterdir() if (p / 'registry.jsonl').is_file()]
        return sorted(found, key=lambda p: (p / 'registry.jsonl').stat().st_mtime)      # attempt names are random: oldest first by activity

    def attempt(self, *, index=-1, wait=60):
        end = time.monotonic() + wait
        while time.monotonic() < end:
            found = self.attempts()
            if found:
                return found[index]
            time.sleep(0.3)
        raise AssertionError('the unit never created an attempt directory')

    def ready(self, timeout=240):
        def probe():
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{self.health_port}/readyz', timeout=3) as response:
                    return response.status == 200
            except (urllib.error.URLError, OSError):
                return False
        return wait_for('/readyz 200', probe, timeout, step=1)

    def cgroup_pids(self):
        group = self.show('ControlGroup')['ControlGroup']
        if not group:                                   # an inactive unit has no cgroup (the empty path would be the root cgroup)
            return set()
        procs = Path('/sys/fs/cgroup') / group.lstrip('/') / 'cgroup.procs'
        return {int(p) for p in procs.read_text().split()} if procs.is_file() else set()

    def journal(self):
        out = subprocess.run(['journalctl', '--user', '--user-unit', self.name, '--no-pager', '-o', 'cat'], env=sysenv(),
                             capture_output=True, text=True, timeout=60)
        return out.stdout

    def stop(self):
        began = time.monotonic()
        ctl('stop', self.name, timeout=self.timeout_stop + 60)
        return time.monotonic() - began

    def cleanup(self):
        ctl('stop', self.name, check=False, timeout=self.timeout_stop + 60)
        if self.cgroup_pids():
            ctl('kill', '-s', 'KILL', self.name, check=False)
        ctl('reset-failed', self.name, check=False)
        if self.link.is_symlink() and str(self.link.resolve()).startswith(str(self.dir)):
            self.link.unlink()
        ctl('daemon-reload', check=False)


class View:
    """What mid_load/wait_for need from a CLI: a liveness test and the recent journal."""

    def __init__(self, unit):
        self.unit = unit

    def running(self):
        return self.unit.active()

    def text(self):
        return self.unit.journal()[-2500:]


def exit_order(attempt):
    return [r['name'] for r in records(attempt) if r['kind'] == 'exit']


def component_of(attempt, name):
    return next((s.get('labels') or {}).get('component') for s in children(attempt) if s['name'] == name)


def fixture_stream(stack, tag):
    stream_clear(stack)
    topics = stream_topics(stack, tag)
    group = f'{stack.topic_prefix}{tag}-group'
    return topics, group, sf.schema(stack.topic_prefix, topics, group)


def load_batch_a(stack, unit, topics, group):
    nodes, rels = sf.expected(['a'])
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    wait_for('nodes', lambda: stream_own_graph(stack)[0] == nodes, 240, cli=View(unit))
    sf.produce_edges(stack, topics, 'a')
    wait_for('relationships', lambda: stream_own_graph(stack) == (nodes, rels), 300, cli=View(unit))
    end = stream_ends(stack, topics)
    wait_for('offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 120, cli=View(unit))


def test_s9_user_service_loads_reports_ready_and_stops_in_order(stack, service_config):
    topics, group, text = fixture_stream(stack, 'sa')
    unit = Unit(stack, service_config, 'sa', text)
    try:
        unit.start()
        attempt = unit.attempt()
        unit.ready()
        load_batch_a(stack, unit, topics, group)
        main = int(unit.show('MainPID')['MainPID'])
        registered = registered_children(attempt / 'registry.jsonl')
        pids = unit.cgroup_pids()
        assert len(registered) == FLEET and main in pids, (len(registered), main, pids)
        assert {int(s['pid']) for s in registered} | {main} <= pids
        journal = unit.journal()
        prefixes = {name for name in (s['name'] for s in children(attempt)) if f'[{name}]' in journal}
        assert len(prefixes) == FLEET, ('child log lines must reach the journal with their prefix', sorted(prefixes))
        assert stack.password not in journal, 'the password reached the journal'
        seconds = unit.stop()
        state = unit.show('Result', 'ExecMainStatus', 'ActiveState', 'NRestarts')
        left = unit.cgroup_pids()
        journal_after = unit.journal()
    finally:
        unit.cleanup()
    (unit.dir / 'journal.txt').write_text(journal_after)
    (unit.dir / 'observed.json').write_text(json.dumps({'stop_seconds': round(seconds, 2), 'state': state, 'cgroup_processes': len(pids),
                                                       'journal_prefixes': len(prefixes)}, indent=1))
    assert seconds < unit.timeout_stop, seconds
    assert state == {'Result': 'success', 'ExecMainStatus': '0', 'ActiveState': 'inactive', 'NRestarts': '0'}, state
    assert left == set(), left
    assert_clean(attempt)
    assert sorted(s.get('code') for s in children(attempt)) == [0] * FLEET
    order = exit_order(attempt)
    kinds = [component_of(attempt, name) for name in order]
    assert kinds == ['edge-loader'] * 3 + ['node-loader'] * 2 + ['bucket-lease-server'], ('the ordered stop', kinds)
    assert stream_own_graph(stack) == sf.expected(['a'])
    stream_clear(stack)


def test_s9_control_kill_mode_control_group_skips_the_ordered_stop(stack, service_config):
    topics, group, text = fixture_stream(stack, 'sc')
    unit = Unit(stack, service_config, 'sc', text, kill_mode='control-group')
    try:
        unit.start()
        attempt = unit.attempt()
        unit.ready()
        load_batch_a(stack, unit, topics, group)
        seconds = unit.stop()
        state = unit.show('Result', 'ExecMainStatus', 'ActiveState', 'NRestarts')
        left = unit.cgroup_pids()
        journal = unit.journal()
    finally:
        unit.cleanup()
    (unit.dir / 'journal.txt').write_text(journal)
    exits = [(s['name'], s.get('code')) for s in children(attempt)]
    kinds = [component_of(attempt, name) for name in exit_order(attempt)]
    (unit.dir / 'observed.json').write_text(json.dumps({'seconds': seconds, 'state': state, 'exits': exits, 'exit_kinds_in_order': kinds},
                                                       indent=1))
    assert left == set() and state['ActiveState'] == 'inactive'
    assert_clean(attempt)
    # systemd SIGTERMed every process at once: the lease server (nothing to drain) is gone before the loaders that still held leases,
    # which is exactly what the ordered stop (edges, then nodes, then the lease server last) prevents.
    assert kinds.index('bucket-lease-server') < max(i for i, k in enumerate(kinds) if k == 'edge-loader'), kinds
    assert kinds != ['edge-loader'] * 3 + ['node-loader'] * 2 + ['bucket-lease-server']


def test_s9_kill_minus_nine_of_the_main_process_restarts_the_unit_with_one_fleet(stack, service_config):
    topics, group, text = fixture_stream(stack, 'sk')
    unit = Unit(stack, service_config, 'sk', text)
    nodes_big, rels_big = sf.expected(['big'])
    try:
        sf.produce_nodes(stack, topics, 'big', with_firms=True)
        unit.start()
        first = unit.attempt()
        unit.ready()
        wait_for('the nodes', lambda: stream_own_graph(stack)[0] == nodes_big, 240, cli=View(unit))
        with producing(stack, topics, 'big'):
            in_flight = mid_load(stack, View(unit))
            old = {int(s['pid']) for s in registered_children(first / 'registry.jsonl')}
            main = int(unit.show('MainPID')['MainPID'])
            assert len(old) == FLEET
            killed_at = time.monotonic()
            ctl('kill', '-s', 'KILL', '--kill-whom=main', unit.name)
        wait_for('systemd to restart the unit', lambda: int(unit.show('NRestarts')['NRestarts']) >= 1 and unit.active()
                 and int(unit.show('MainPID')['MainPID']) not in (0, main), 180, step=1)
        wait_for('a second attempt directory', lambda: len(unit.attempts()) >= 2, 120, step=1)
        second = unit.attempt()
        assert second != first and len(unit.attempts()) == 2, 'the restart must start exactly one new attempt'
        unit.ready(timeout=300)
        ready_after = time.monotonic() - killed_at
        wait_for('the whole batch after the restart', lambda: stream_own_graph(stack) == (nodes_big, rels_big), 420, cli=View(unit))
        end = stream_ends(stack, topics)
        wait_for('offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 180, cli=View(unit))
        new_main = int(unit.show('MainPID')['MainPID'])
        new = registered_children(second / 'registry.jsonl')
        pids = unit.cgroup_pids()
        assert registered_children(first / 'registry.jsonl') == [], 'a child of the killed run is still alive'
        assert not (old & pids), ('the killed run left a process in the unit', sorted(old & pids))
        assert len(new) == FLEET and pids == {new_main} | {int(s['pid']) for s in new}, ('exactly one fleet', sorted(pids))
        state = unit.show('Result', 'ActiveState', 'NRestarts')
        seconds = unit.stop()
        journal = unit.journal()
    finally:
        unit.cleanup()
    (unit.dir / 'journal.txt').write_text(journal)
    (unit.dir / 'observed.json').write_text(json.dumps({'in_flight': in_flight, 'ready_seconds_after_kill': round(ready_after, 1),
                                                       'state': state, 'old_children': len(old), 'new_children': len(new)}, indent=1))
    assert 0 < in_flight < TOTAL_BIG
    assert state['NRestarts'] == '1' and state['ActiveState'] == 'active', state
    assert seconds < unit.timeout_stop
    assert_clean(second)
    assert stream_own_graph(stack) == (nodes_big, rels_big)
    stream_clear(stack)


# -- the OOM policy of the shipped unit ---------------------------------------------------------------------------------------------
OOM_STANDIN = Path(__file__).resolve().parents[1] / 'fixtures' / 'e14_oom_standin.py'


def oom_unit(stack, config, tag, *, drop_policy):
    """The shipped unit with its ExecStart replaced by the OOM stand-in (a hog role under the real command's supervision rules), a low
    MemoryMax, and - for the control - the ``OOMPolicy=continue`` line removed (systemd's default, ``stop``, applies)."""
    import re
    name = f'e14-{stack.owner[:8]}-{tag}.service'
    directory = Path(config['run_dir']) / f's9-{tag}'
    directory.mkdir()
    text = EXAMPLE.read_text()
    text = re.sub(r'EnvironmentFile=.*\n', '', text)
    text = re.sub(r'ExecStart=.*?\n(?!\s)', f'ExecStart={Path(config["venv_bin"]) / "python"} {OOM_STANDIN} {directory / "root"} hog fail\n', text,
                  count=1, flags=re.S)
    text = text.replace('\nKillMode=mixed\nKillSignal', '\nKillMode=mixed\nMemoryMax=250M\nMemorySwapMax=0\nKillSignal')
    if drop_policy:
        text = text.replace('OOMPolicy=continue\n', '')
    assert ('OOMPolicy=continue' in text) != drop_policy and 'MemoryMax=250M' in text and 'hog fail' in text and '%h' not in text, text
    (directory / name).write_text(text)
    return name, directory


class OomUnit:
    def __init__(self, stack, config, tag, *, drop_policy):
        self.name, self.directory = oom_unit(stack, config, tag, drop_policy=drop_policy)
        self.link = USER_UNIT_DIR / self.name
        ctl('link', str(self.directory / self.name))
        ctl('daemon-reload')

    def journal(self):
        return subprocess.run(['journalctl', '--user', '--user-unit', self.name, '--no-pager', '-o', 'cat'], env=sysenv(), capture_output=True,
                              text=True, timeout=60).stdout

    def show(self, *props):
        out = ctl('show', self.name, *[f'-p{p}' for p in props]).stdout
        return dict(line.split('=', 1) for line in out.splitlines() if '=' in line)

    def cleanup(self):
        ctl('stop', self.name, check=False)
        ctl('reset-failed', self.name, check=False)
        if self.link.is_symlink() and str(self.link.resolve()).startswith(str(self.directory)):
            self.link.unlink()
        ctl('daemon-reload', check=False)


def run_until_restarted(unit, minimum_restarts=1, timeout=240):
    """Start the unit, sample its cgroup, wait for systemd's restart; returns the largest process count seen in the cgroup."""
    biggest = 0
    unit_start = time.time()
    ctl('start', unit.name)
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        group = unit.show('ControlGroup')['ControlGroup']
        procs = Path('/sys/fs/cgroup') / group.lstrip('/') / 'cgroup.procs' if group else None
        if procs is not None and procs.is_file():
            biggest = max(biggest, len(procs.read_text().split()))
        if int(unit.show('NRestarts')['NRestarts']) >= minimum_restarts and 'Main process exited' in unit.journal():
            break
        time.sleep(0.2)
    else:
        raise AssertionError('systemd did not restart the unit: ' + unit.journal()[-1500:])
    return biggest, unit_start


def test_s9_oom_killed_role_is_reported_failed_and_restarted_with_the_shipped_oom_policy(stack, service_config):
    unit = OomUnit(stack, service_config, 'oc', drop_policy=False)
    try:
        biggest, _ = run_until_restarted(unit)
        journal = unit.journal()
        first = journal.split('Scheduled restart job')[0]                      # the first invocation only
        ctl('stop', unit.name)
        left = unit.show('ControlGroup')['ControlGroup']
    finally:
        unit.cleanup()
    reports = [line for line in first.splitlines() if 'stage=oom' in line]
    assert len(reports) == 1 and 'cgroup OOM kill observed; victim unknown' in reports[0], first[-2000:]
    assert 'hog-0' not in reports[0] and 'idle-0' not in reports[0]
    assert 'Main process exited, code=exited, status=1/FAILURE' in journal, journal[-1500:]
    assert 'Scheduled restart job, restart counter is at 1' in journal
    assert biggest <= 3, f'a second fleet: {biggest} processes in the unit (command + idle + hog expected)'
    assert left == '', 'nothing left in the unit after the stop'
    (unit.directory / 'journal.txt').write_text(journal)


def test_s9_control_the_default_oom_policy_stops_the_unit_before_the_command_can_report(stack, service_config):
    unit = OomUnit(stack, service_config, 'od', drop_policy=True)
    try:
        ctl('start', unit.name)
        end = time.monotonic() + 120
        while time.monotonic() < end and 'killed by the OOM killer' not in unit.journal():
            time.sleep(0.5)
        journal = unit.journal()
        time.sleep(2)
    finally:
        unit.cleanup()
    (unit.directory / 'journal.txt').write_text(journal)
    assert 'killed by the OOM killer' in journal, journal[-1500:]
    assert 'stage=oom' not in journal, 'with OOMPolicy=stop the command is stopped before it can report'
