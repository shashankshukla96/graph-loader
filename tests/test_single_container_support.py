"""Offline controls of the E14.F3.S8 live helpers: process-table checks, the OOM-report attribution rule, the container specification.

Nothing here needs Docker. The live cells in ``tests/installed_service/test_service_single_container.py`` rely on these helpers to
mean what they say (a zombie is found, a misattributed OOM line is flagged, the container carries no socket and its owner label).
"""
from __future__ import annotations

import json
import subprocess
import sys

import pytest

from neo4j_graph_loader.orchestrator.run_registry import RunRegistry
from tests.installed_service import container_support as cs
from tests.installed_service.ownership import OWNER_LABEL


def table(*rows):
    return [{'pid': pid, 'comm': comm, 'state': state, 'ppid': ppid, 'cmdline': cmd} for pid, comm, state, ppid, cmd in rows]


PROCS = table((1, 'tini', 'S', 0, 'tini -- python -m neo4j_graph_loader.cli start'),
              (7, 'python', 'S', 1, 'python -m neo4j_graph_loader.cli start'),
              (20, 'python', 'S', 7, 'python -I -u -m neo4j_graph_loader._child neo4j_graph_loader.loader.edge_loader --x'),
              (21, 'python', 'S', 7, 'python -I -u -m neo4j_graph_loader._child neo4j_graph_loader.orchestrator.lease_server'),
              (22, 'python', 'Z', 1, ''),
              (23, 'python', 'Z', 7, 'python -I -u -m neo4j_graph_loader._child neo4j_graph_loader.loader.node_loader'))


def test_process_table_helpers_find_zombies_pid1_and_live_role_children() -> None:
    assert [p['pid'] for p in cs.zombies(PROCS)] == [22, 23]
    assert cs.zombies([p for p in PROCS if p['state'] != 'Z']) == []
    assert cs.pid1(PROCS)['comm'] == 'tini'
    assert [p['pid'] for p in cs.fleet_processes(PROCS)] == [20, 21], 'a zombie is not a live fleet member, the CLI is not a role'


def test_parse_processes_reads_the_last_json_line() -> None:
    assert cs.parse_processes('noise\n' + json.dumps(PROCS) + '\n') == PROCS


def test_oom_report_is_group_level_only() -> None:
    names = ['edge-loader-WORKS_FOR-0', 'node-loader-Citizen-0']
    good = f'ERROR stage=oom {cs.OOM_REPORT} (oom_kill 0 -> 1)\nedge-loader-WORKS_FOR-0 exited with code 137\n'
    assert cs.oom_lines(good) == [f'ERROR stage=oom {cs.OOM_REPORT} (oom_kill 0 -> 1)']
    assert cs.misattributed(good, names, [20, 21]) == []
    by_name = f'{cs.OOM_REPORT}\nOOM killed edge-loader-WORKS_FOR-0\n'
    assert cs.misattributed(by_name, names, []) == ['OOM killed edge-loader-WORKS_FOR-0']
    by_pid = 'oom victim pid 20\n'
    assert cs.misattributed(by_pid, names, [20]) == ['oom victim pid 20']
    assert cs.misattributed('process 2200 killed', names, [20]) == [], 'a pid is matched whole, and only on OOM lines'
    assert cs.oom_lines('room for 3 more\nboom: write failed\nnothing here\n') == [], 'only the word OOM counts'


def test_scripts_compile_and_the_orphan_script_exits_cleanly() -> None:
    for script in (cs.PS_SCRIPT, cs.ORPHAN_SCRIPT, cs.KILL_SCRIPT, cs.OOM_SCRIPT, cs.MEMORY_EVENTS_SCRIPT):
        compile(script, '<script>', 'exec')
    done = subprocess.run([sys.executable, '-I', '-c', cs.ORPHAN_SCRIPT, '3'], capture_output=True, text=True, timeout=30)
    assert done.returncode == 0 and done.stdout.strip() == '3', done


def spec(**options):
    base = dict(image='sha256:abc', name='e14-o-s8-x', owner='o' * 32, role='s8-x', network='e14-net', command=cs.cli_command(),
                env={'A': 'b'}, host_dir='/run/dir', health_port=18081, uid=1000, gid=1001)
    return cs.container_spec(**{**base, **options})


def test_container_spec_mounts_no_docker_socket_and_carries_the_owner_label() -> None:
    made = spec()
    assert list(made['volumes']) == ['/run/dir'] and made['volumes']['/run/dir'] == {'bind': '/work', 'mode': 'rw'}
    assert not any('docker.sock' in key or 'docker.sock' in v['bind'] for key, v in made['volumes'].items())
    assert made['labels'] == {OWNER_LABEL: 'o' * 32, 'io.graph-loader.e09-role': 's8-x'}
    assert made['user'] == '1000:1001' and made['working_dir'] == '/work' and made['network'] == 'e14-net'
    assert made['ports'] == {'8081/tcp': ('127.0.0.1', 18081)}, 'the health port is published on loopback only'
    for forbidden in ('privileged', 'pid_mode', 'cap_add', 'network_mode', 'devices', 'security_opt', 'init'):
        assert forbidden not in made
    assert 'DOCKER_HOST' not in made['environment']
    assert 'mem_limit' not in made and 'restart_policy' not in made and 'entrypoint' not in made


def test_container_spec_limits_memory_without_swap_and_sets_the_restart_policy_and_platform() -> None:
    made = spec(mem_mb=1536, restart={'Name': 'on-failure', 'MaximumRetryCount': 3}, platform='linux/arm64', grace=45)
    assert made['mem_limit'] == made['memswap_limit'] == '1536m'
    assert made['restart_policy'] == {'Name': 'on-failure', 'MaximumRetryCount': 3}
    assert made['platform'] == 'linux/arm64' and made['stop_timeout'] == 45
    assert spec(entrypoint=['sleep', '600'], command=[])['entrypoint'] == ['sleep', '600']


def test_cli_command_is_process_runtime_with_a_pod_reachable_health_address() -> None:
    command = cs.cli_command(extra=('--x',))
    assert command[:2] == ['start', '--mode'] and command[command.index('--runtime') + 1] == 'process'
    assert command[command.index('--health-addr') + 1] == '0.0.0.0:8081' and command[-1] == '--x'
    assert command[command.index('--run-dir') + 1] == '/work/runs'


def test_registry_views_distinguish_live_children_from_receipted_ones(tmp_path) -> None:
    path = tmp_path / 'registry.jsonl'
    registry = RunRegistry(path)
    for index, component in enumerate(('edge-loader', 'node-loader')):
        registry.append('started', id=f'c{index}', name=f'{component}-0', pid=10 + index, labels={'component': component})
    attempt = tmp_path
    assert len(cs.live_children(attempt)) == 2 and len(cs.unreceipted(attempt)) == 2 and not cs.all_receipted(attempt)
    assert [s['pid'] for s in cs.live_children(attempt, component='node-loader')] == [11]
    registry.append('exit', id='c0', name='edge-loader-0', code=0, returncode=0)
    assert [s['name'] for s in cs.live_children(attempt)] == ['node-loader-0'] and not cs.all_receipted(attempt)
    registry.append('exit', id='c1', name='node-loader-0', code=137, returncode=-9)
    assert cs.live_children(attempt) == [] and cs.all_receipted(attempt)
    registry.append('started', id='planner-0-9', name='planner-0', pid=9, external=True)
    assert cs.all_receipted(attempt), 'an external planner process is not a role child'


@pytest.mark.parametrize('text,expected', [('ENTRYPOINT ["a", "b"]\n', ['a', 'b']), ('FROM x\n', None),
                                           ('ENTRYPOINT ["a"]\nENTRYPOINT ["b", "c"]\n', ['b', 'c'])])
def test_dockerfile_entrypoint_reads_the_last_exec_form(tmp_path, text, expected) -> None:
    path = tmp_path / 'Dockerfile'
    path.write_text(text)
    assert cs.dockerfile_entrypoint(path) == expected
