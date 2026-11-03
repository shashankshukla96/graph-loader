"""E14.F3.S8 the single container on the Linux host: the CLI image runs ``start --runtime process`` and every role is a child process.

Written offline against the S5-S9 evidence; it has not been run. ``tini`` is PID 1; no Docker socket is mounted and ``DOCKER_HOST`` is
unset. One owned container per cell (``e14-<owner>-s8-<cell>``, owner label, removed by recorded id) joins the owned Neo4j/Kafka
network; its run directory is a host bind mount so the run's ``registry.jsonl`` is read from the host, while every pid-level
observation and every signal happens inside the container (a registered pid belongs to its pid namespace). The cells:

* image contract: labels, version check, entrypoint, the other three images untouched; image id, size and layers recorded;
* ready without a socket, load, ``docker stop -t`` -> the ordered stop and exit 0 inside the grace;
* a role killed inside fails the run (exit 1), the ``on-failure`` restart policy brings the container back with one fleet;
* ``docker kill`` followed by ``docker start`` redelivers safely (exact graph, one fleet);
* an induced cgroup OOM kill is reported at group level ("victim unknown"); a concurrent OOM kill plus a manual kill is not
  attributed to either child; a manual kill alone is not reported as an OOM (controls);
* no zombies after 20 orphaned child exits (and the same load against a container with no init shows zombies: the check bites);
* arm64: a short stream smoke when an arm64 CLI image is configured (``service_config['arm64_cli_image']``).

Not covered here: the E13 compressed soak in the container (S10).
"""
from __future__ import annotations

import json
import platform as host_platform
import time
from pathlib import Path

import pytest

import neo4j_graph_loader
from tests.installed_service import stream_fixture as sf
from tests.installed_service.container_support import (
    FLEET, GRACE, OOM_REPORT, ORPHAN_SCRIPT, ContainerMemoryWatch, DOCKER_SOCKET_PATHS, SingleContainer, View, all_receipted,
    dockerfile_entrypoint, fleet_processes, live_children, misattributed, oom_lines, pid1, unreceipted, zombies,
)
from tests.installed_service.process_support import (
    START_MIN_AVAILABLE_KB, STOP_BELOW_AVAILABLE_KB, TOTAL_BIG, available_kb, children, mid_load, producing, records, stream_clear,
    stream_committed, stream_ends, stream_own_graph, stream_topics, wait_for,
)
from tests.installed_service.test_service_harness import run_owned_container

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
TINI_ENTRYPOINT = ['tini', '--', 'python', '-m', 'neo4j_graph_loader.cli']
OOM_LIMIT_MB = 1536
RESTART_ON_FAILURE = {'Name': 'on-failure', 'MaximumRetryCount': 3}


@pytest.fixture(autouse=True)
def container_memory_guard(request, service_config):
    """Refuse to start a cell below ~2.4 GB MemAvailable; sample every 5 s; a breach ``docker stop``s the live cell's container."""
    start = available_kb()
    assert start >= START_MIN_AVAILABLE_KB, f'host memory guard: MemAvailable {start} kB < {START_MIN_AVAILABLE_KB} kB'
    watch = ContainerMemoryWatch()
    watch.start()
    yield watch
    watch.done.set()
    watch.join(timeout=10)
    with open(Path(service_config['run_dir']) / 'memory.jsonl', 'a') as out:
        out.write(json.dumps({'test': request.node.name, 'start_kb': start, 'min_available_kb': watch.minimum,
                              'peak_used_kb': start - watch.minimum, 'samples': watch.samples, 'breached': watch.breached}) + '\n')
    assert not watch.breached, f'MemAvailable fell below {STOP_BELOW_AVAILABLE_KB} kB; the cell was stopped (min {watch.minimum} kB)'


# -- shared steps ----------------------------------------------------------------------------------------------------------------
def fixture_stream(stack, tag):
    stream_clear(stack)
    topics = stream_topics(stack, tag)
    group = f'{stack.topic_prefix}{tag}-group'
    return topics, group, sf.schema(stack.topic_prefix, topics, group)


def load_batch_a(stack, box, topics, group):
    nodes, rels = sf.expected(['a'])
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    wait_for('nodes', lambda: stream_own_graph(stack)[0] == nodes, 240, cli=View(box))
    sf.produce_edges(stack, topics, 'a')
    wait_for('relationships', lambda: stream_own_graph(stack) == (nodes, rels), 300, cli=View(box))
    end = stream_ends(stack, topics)
    wait_for('offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 120, cli=View(box))


def start_loaded(stack, config, client, tag, **options):
    """A started single container, ready and past batch A (so the CLI is supervising); returns (box, attempt, topics, group)."""
    topics, group, text = fixture_stream(stack, tag)
    box = SingleContainer(stack, config, client, tag, text, **options).create()
    box.start()
    attempt = box.attempt()
    box.ready()
    load_batch_a(stack, box, topics, group)
    return box, attempt, topics, group


def exit_order(attempt):
    return [r['name'] for r in records(attempt) if r['kind'] == 'exit']


def component_of(attempt, name):
    return next((s.get('labels') or {}).get('component') for s in children(attempt) if s['name'] == name)


def record(box, **values):
    (box.dir / 'observed.json').write_text(json.dumps(values, indent=1, default=str, sort_keys=True))


def require_cgroup_v2():
    if not Path('/sys/fs/cgroup/cgroup.controllers').is_file():
        pytest.skip('the host is not on cgroup v2: container OOM evidence needs memory.events')


def one_fleet(box, attempt):
    """Exactly one fleet is alive: the attempt's registered children and the role processes in the container agree."""
    registered = live_children(attempt)
    inside = fleet_processes(box.processes())
    assert len(registered) == FLEET and len(inside) == FLEET, (len(registered), len(inside))
    assert {int(s['pid']) for s in registered} == {p['pid'] for p in inside}, 'the registry and the container disagree on the fleet'


def no_secret(box, stack):
    assert stack.password not in box.all_text(), 'the scratch password reached the container log'


# -- image contract (item: labels, version check, entrypoint semantics, image bytes and size) ------------------------------------
def test_s8_image_contract_is_unchanged_and_bytes_are_recorded(docker_client, registry, stack, service_config):
    row = service_config['images']['cli']
    image = docker_client.images.get(row['id'])
    version = neo4j_graph_loader.__version__
    labels = image.attrs['Config']['Labels']
    assert labels['org.opencontainers.image.version'] == version
    assert labels['io.graph-loader.component'] == 'cli' and labels['io.graph-loader.image-contract'] == '1'
    assert labels['org.opencontainers.image.source'] == 'https://github.com/shashankshukla96/graph-loader'
    assert image.attrs['Config']['Entrypoint'] == TINI_ENTRYPOINT and not image.attrs['Config'].get('Cmd')
    assert image.attrs['Config'].get('User') in (None, '') and image.attrs['Config']['WorkingDir'] == '/app'

    status, output = run_owned_container(docker_client, registry, stack.owner, 'probe-tini', row['id'], ['tini --version 2>&1'],
                                         entrypoint=['sh', '-c'])
    assert status == 0 and b'tini version 0.19.0' in output, output
    # the build-time version check, repeated in the image as shipped
    status, output = run_owned_container(docker_client, registry, stack.owner, 'probe-version', row['id'],
                                         ['-I', '-c', 'import importlib.metadata as m; print(m.version("neo4j-graph-loader"))'],
                                         entrypoint=['python'])
    assert status == 0 and output.decode().strip() == version == labels['org.opencontainers.image.version']
    # the image's own entrypoint still forwards its arguments to the CLI
    status, output = run_owned_container(docker_client, registry, stack.owner, 'probe-cli', row['id'], ['--version'])
    assert status == 0 and output.decode().strip() == f'graph-loader {version}', output

    for role, dockerfile in (('node', 'Dockerfile.node_loader'), ('edge', 'Dockerfile.edge_loader'), ('clock', 'Dockerfile.clock')):
        other = docker_client.images.get(service_config['images'][role]['id'])
        expected = dockerfile_entrypoint(ROOT / dockerfile)
        assert other.attrs['Config']['Entrypoint'] == expected and 'tini' not in expected, role

    sizes = {r: docker_client.images.get(v['id']).attrs['Size'] for r, v in service_config['images'].items()}
    baseline = (service_config.get('baseline_images') or {}).get('cli')
    old = docker_client.images.get(baseline['id']).attrs['Size'] if baseline else None
    observed = {'id': image.id, 'tag': row['tag'], 'platform': row.get('platform') or f"{image.attrs['Os']}/{image.attrs['Architecture']}", 'size_bytes': image.attrs['Size'],
                'layers': image.attrs['RootFS']['Layers'], 'created': image.attrs['Created'], 'sizes_of_all_images': sizes,
                'baseline_cli_size_bytes': old, 'size_delta_bytes': None if old is None else image.attrs['Size'] - old,
                'entrypoint': image.attrs['Config']['Entrypoint'], 'labels': labels}
    (Path(service_config['run_dir']) / 's8-image.json').write_text(json.dumps(observed, indent=1, sort_keys=True))


# -- ready without a socket, load, ordered stop (items 1 and 2) ----------------------------------------------------------------
def test_s8_container_is_ready_without_a_docker_socket_and_docker_stop_is_the_ordered_stop(stack, service_config, docker_client):
    box, attempt, topics, group = start_loaded(stack, service_config, docker_client, 'sa')
    try:
        facts = box.inspect_summary()
        procs = box.processes()
        top = pid1(procs)
        inside = fleet_processes(procs)
        registered = live_children(attempt)
        pidfd = box.run_python('import os; os.close(os.pidfd_open(1))')[0]
        sockets = [path for path in DOCKER_SOCKET_PATHS if box.exists_in_container(path)]
        before_stop = box.text()
        code, seconds = box.stop(GRACE)
        state = box.state()
        text = box.all_text()
    finally:
        box.cleanup()
    record(box, stop_seconds=round(seconds, 2), exit_code=code, state={k: state[k] for k in ('Status', 'ExitCode', 'OOMKilled')},
           inspect=facts, pid1=top, fleet_pids=[p['pid'] for p in inside], pidfd_open_in_container=pidfd == 0)
    assert not any('docker.sock' in (src or '') or 'docker.sock' in dst for _t, src, dst in facts['mounts']), facts['mounts']
    assert facts['binds'] == [f'{box.dir}:/work:rw'] and not facts['privileged'] and not facts['has_docker_host_env'], facts
    assert sockets == [], f'a Docker socket exists inside the container: {sockets}'
    assert top['comm'] == 'tini' and facts['entrypoint'] == TINI_ENTRYPOINT, (top, facts['entrypoint'])
    assert len(registered) == FLEET and len(inside) == FLEET
    assert zombies(procs) == [], zombies(procs)
    assert code == 0 and state['Status'] == 'exited' and state['ExitCode'] == 0, state
    assert seconds < GRACE, f'docker stop took {seconds:.1f}s of a {GRACE}s grace (it must not reach SIGKILL)'
    assert all_receipted(attempt) and sorted(s.get('code') for s in children(attempt)) == [0] * FLEET
    kinds = [component_of(attempt, name) for name in exit_order(attempt)]
    assert kinds == ['edge-loader'] * 3 + ['node-loader'] * 2 + ['bucket-lease-server'], ('the ordered stop', kinds)
    assert stream_own_graph(stack) == sf.expected(['a'])
    assert stack.password not in text and before_stop
    stream_clear(stack)


# -- a role killed inside: the run fails per policy, the restart policy returns one fleet (item 3) -------------------------------
def test_s8_role_killed_inside_fails_the_run_and_the_restart_policy_returns_one_fleet(stack, service_config, docker_client):
    topics, group, text = fixture_stream(stack, 'sk')
    nodes_big, rels_big = sf.expected(['big'])
    box = SingleContainer(stack, service_config, docker_client, 'sk', text, restart=RESTART_ON_FAILURE).create()
    try:
        sf.produce_nodes(stack, topics, 'big', with_firms=True)
        box.start()
        first = box.attempt()
        box.ready()
        wait_for('the nodes', lambda: stream_own_graph(stack)[0] == nodes_big, 240, cli=View(box))
        with producing(stack, topics, 'big'):
            in_flight = mid_load(stack, View(box))
            victim = live_children(first, component='edge-loader')[0]
            assert len(live_children(first)) == FLEET
            box.kill_inside(victim['pid'])
        wait_for('the failed run to be restarted by the policy', lambda: (box.container.reload() or True)
                 and box.container.attrs['RestartCount'] >= 1 and box.running(), 240, step=1)
        wait_for('a second attempt directory', lambda: len(box.attempts()) >= 2, 120, step=1)
        second = box.attempt()
        assert second != first and len(box.attempts()) == 2, 'the restart must start exactly one new attempt'
        box.ready(timeout=300)
        wait_for('the whole batch after the restart', lambda: stream_own_graph(stack) == (nodes_big, rels_big), 420, cli=View(box))
        end = stream_ends(stack, topics)
        wait_for('offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 180, cli=View(box))
        one_fleet(box, second)
        dies = box.die_codes()
        box.container.reload()
        restarts = box.container.attrs['RestartCount']
        code, seconds = box.stop(GRACE)
        full = box.all_text()
        no_secret(box, stack)
    finally:
        box.cleanup()
    record(box, in_flight=in_flight, die_exit_codes=dies, restart_count=restarts, stop_exit_code=code, stop_seconds=round(seconds, 2),
           victim=victim['name'])
    assert 0 < in_flight < TOTAL_BIG
    assert dies[0] == 1 and 'exited before drain acknowledgement' in full, (dies, full[-2000:])
    assert next(s for s in children(first) if s['id'] == victim['id'])['code'] == 137
    assert all_receipted(first), 'the failed run left a child without an exit receipt'
    assert restarts == 1, 'the policy restarted the container more than once'
    assert code == 0 and seconds < GRACE
    assert all_receipted(second) and stream_own_graph(stack) == (nodes_big, rels_big)
    stream_clear(stack)


# -- docker kill (abrupt), then start: redelivery is safe (item 4) -------------------------------------------------------------
def test_s8_docker_kill_then_restart_redelivers_safely(stack, service_config, docker_client):
    topics, group, text = fixture_stream(stack, 'sd')
    nodes_big, rels_big = sf.expected(['big'])
    box = SingleContainer(stack, service_config, docker_client, 'sd', text).create()
    try:
        sf.produce_nodes(stack, topics, 'big', with_firms=True)
        box.start()
        first = box.attempt()
        box.ready()
        wait_for('the nodes', lambda: stream_own_graph(stack)[0] == nodes_big, 240, cli=View(box))
        with producing(stack, topics, 'big'):
            in_flight = mid_load(stack, View(box))
            assert len(live_children(first)) == FLEET
            code_killed = box.kill()
        left_by_kill = unreceipted(first)
        after_kill = stream_own_graph(stack)
        box.start()
        wait_for('a second attempt directory', lambda: len(box.attempts()) >= 2, 120, step=1)
        second = box.attempt()
        assert len(box.attempts()) == 2
        box.ready(timeout=300)
        wait_for('the whole batch after the restart', lambda: stream_own_graph(stack) == (nodes_big, rels_big), 420, cli=View(box))
        end = stream_ends(stack, topics)
        wait_for('offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 180, cli=View(box))
        one_fleet(box, second)
        code, seconds = box.stop(GRACE)
        no_secret(box, stack)
    finally:
        box.cleanup()
    record(box, in_flight=in_flight, kill_exit_code=code_killed, children_without_receipt=len(left_by_kill),
           relationships_at_kill=len(after_kill[1]), stop_exit_code=code, stop_seconds=round(seconds, 2))
    assert 0 < in_flight < TOTAL_BIG and len(after_kill[1]) < TOTAL_BIG, 'the kill must land on work in flight'
    assert code_killed == 137 and left_by_kill, 'the kill was not abrupt: every child had an exit receipt'
    assert code == 0 and all_receipted(second)
    assert stream_own_graph(stack) == (nodes_big, rels_big), 'redelivery left the graph different from the oracle'
    stream_clear(stack)


# -- OOM: group-level report, concurrent OOM + manual kill, and the no-OOM control (items 5 and 6) --------------------------------
def failed_run(stack, box, attempt, victims):
    """Wait for the CLI to end the run; returns (container exit code, whole log text, registry exit codes of ``victims``)."""
    code = box.wait_exit(240)
    text = box.all_text()
    codes = {v['name']: next(s for s in children(attempt) if s['id'] == v['id']).get('code') for v in victims}
    no_secret(box, stack)
    return code, text, codes


def oom_cell(stack, service_config, docker_client, tag, *, oom, manual):
    """A memory-limited container past batch A; kills (an OOM victim and/or a hand-killed role); returns what was observed."""
    require_cgroup_v2()
    box, attempt, _topics, _group = start_loaded(stack, service_config, docker_client, tag, mem_mb=OOM_LIMIT_MB)
    try:
        edges = live_children(attempt, component='edge-loader')
        assert len(edges) == 3
        victims = [edges[0]] + ([edges[1]] if oom and manual else [])
        names = [s['name'] for s in children(attempt)]
        pids = [int(s['pid']) for s in children(attempt)]
        events_before = box.memory_events()
        if oom:
            induced = box.induce_oom(edges[0]['pid'], edges[1]['pid'] if manual else 0)
        else:
            box.kill_inside(edges[0]['pid'])
            induced = None
        code, text, codes = failed_run(stack, box, attempt, victims)
        state = box.state()
    finally:
        box.cleanup()
    record(box, tag=tag, oom=oom, manual=manual, induced=induced, events_before=events_before, container_exit_code=code,
           registry_exit_codes=codes, docker_oom_killed=state['OOMKilled'], oom_lines=oom_lines(text))
    return {'attempt': attempt, 'code': code, 'text': text, 'codes': codes, 'induced': induced, 'names': names, 'pids': pids}


def assert_oom_counted(seen, *, hand_killed=False):
    """The observer's counters, or - when the failed run ended the container before it could print - the run's own report line."""
    induced = seen['induced']
    if induced.get('observer_lost'):
        assert any(OOM_REPORT in line and '->' in line for line in oom_lines(seen['text'])), ('observer lost and no counters in the report', oom_lines(seen['text']))
        return
    assert induced['oom_kill_after'] >= induced['oom_kill_before'] + 1, induced      # a second kill may take the allocator
    assert induced['hand_killed'] == hand_killed, induced


def test_s8_induced_oom_is_reported_at_group_level_with_the_victim_unknown(stack, service_config, docker_client):
    seen = oom_cell(stack, service_config, docker_client, 'so', oom=True, manual=False)
    assert_oom_counted(seen)
    assert seen['code'] == 1 and 'exited before drain acknowledgement' in seen['text'], seen['text'][-2500:]
    assert list(seen['codes'].values()) == [137]
    assert any(OOM_REPORT in line for line in oom_lines(seen['text'])), ('no group-level OOM report', oom_lines(seen['text']))
    assert misattributed(seen['text'], seen['names'], seen['pids']) == [], 'the OOM was attributed to a single child'
    assert all_receipted(seen['attempt'])
    stream_clear(stack)


def test_s8_concurrent_oom_and_manual_kill_does_not_misattribute(stack, service_config, docker_client):
    seen = oom_cell(stack, service_config, docker_client, 'sm', oom=True, manual=True)
    assert_oom_counted(seen, hand_killed=True)
    assert seen['code'] == 1
    assert sorted(seen['codes'].values()) == [137, 137], 'both the OOM victim and the hand-killed role die by SIGKILL'
    assert any(OOM_REPORT in line for line in oom_lines(seen['text'])), ('no group-level OOM report', oom_lines(seen['text']))
    assert misattributed(seen['text'], seen['names'], seen['pids']) == [], 'the OOM was attributed to a single child'
    assert all_receipted(seen['attempt'])
    stream_clear(stack)


def test_s8_control_manual_kill_without_oom_is_not_reported_as_an_oom(stack, service_config, docker_client):
    seen = oom_cell(stack, service_config, docker_client, 'sn', oom=False, manual=False)
    assert seen['code'] == 1 and 'exited before drain acknowledgement' in seen['text']
    assert list(seen['codes'].values()) == [137]
    assert oom_lines(seen['text']) == [], ('a manual SIGKILL was reported as an OOM', oom_lines(seen['text']))
    assert all_receipted(seen['attempt'])
    stream_clear(stack)


# -- no zombies after 20 child exits (item 7) -----------------------------------------------------------------------------------
def test_s8_no_zombies_after_twenty_orphaned_child_exits_and_the_control_without_init_shows_them(stack, service_config, docker_client):
    box, attempt, _topics, _group = start_loaded(stack, service_config, docker_client, 'sz')
    control = SingleContainer(stack, service_config, docker_client, 'sz0', 'unused: the control runs sleep, not the CLI\n',
                              entrypoint=['sleep', '600']).create()
    try:
        baseline = box.processes()
        code, out = box.run_python(ORPHAN_SCRIPT, 20)
        assert code == 0, out
        time.sleep(2.0)                                 # every grandchild has exited (0.2 s) and been reparented to PID 1
        after = box.processes()
        fleet_after = fleet_processes(after)
        pid1_after = pid1(after)
        # the control: the same 20 exits under a PID 1 that never reaps (no init)
        control.start()
        wait_for('the control container', control.running, 30, step=0.5)
        code, out = control.run_python(ORPHAN_SCRIPT, 20)
        assert code == 0, out
        time.sleep(2.0)
        stuck = zombies(control.processes())
        code_stop, seconds = box.stop(GRACE)
    finally:
        control.cleanup()
        box.cleanup()
    record(box, zombies_before=len(zombies(baseline)), zombies_after=len(zombies(after)), control_zombies=len(stuck),
           pid1=pid1_after['comm'], fleet_after=len(fleet_after), stop_exit_code=code_stop, stop_seconds=round(seconds, 2))
    assert zombies(baseline) == [] and zombies(after) == [], ('zombies in the single container', zombies(after))
    assert pid1_after['comm'] == 'tini' and len(fleet_after) == FLEET, 'the 20 orphan exits disturbed the fleet'
    assert len(stuck) == 20, f'the control (no init) must show the 20 zombies, showed {len(stuck)}: the check would not have caught a leak'
    assert code_stop == 0 and all_receipted(attempt)
    stream_clear(stack)


# -- arm64: build and a short stream smoke (item 8), when an arm64 image is configured -------------------------------------------
def test_s8_arm64_image_runs_a_short_stream_smoke(stack, service_config, docker_client):
    row = service_config.get('arm64_cli_image')
    if not row:
        pytest.skip("service_config has no 'arm64_cli_image' (a locally built linux/arm64 CLI image: id, tag)")
    image = docker_client.images.get(row['id'])
    assert image.attrs['Architecture'] == 'arm64' and image.attrs['Config']['Entrypoint'] == TINI_ENTRYPOINT
    assert image.attrs['Config']['Labels']['io.graph-loader.image-contract'] == '1'
    box, attempt, _topics, _group = start_loaded(stack, service_config, docker_client, 'sr', image=row['id'], platform='linux/arm64')
    try:
        procs = box.processes()
        tini = box.container.exec_run(['tini', '--version']).output.decode().strip()
        code, seconds = box.stop(GRACE)
    finally:
        box.cleanup()
    emulated = host_platform.machine().lower() not in ('aarch64', 'arm64')
    record(box, image=row, size_bytes=image.attrs['Size'], host_machine=host_platform.machine(), emulated=emulated, tini=tini,
           stop_exit_code=code, stop_seconds=round(seconds, 2))
    assert pid1(procs)['comm'] == 'tini' and len(fleet_processes(procs)) == FLEET
    assert code == 0 and all_receipted(attempt) and stream_own_graph(stack) == sf.expected(['a'])
    stream_clear(stack)
