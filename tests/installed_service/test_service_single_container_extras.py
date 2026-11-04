"""E14.F3.S8 follow-ups for the single-container page: four claims the nine S8 cells do not cover.

* ``docker exec <c> python -m neo4j_graph_loader.cli stop --runtime process --run-dir /work/runs`` is the second-shell stop: the same
  ordered stop, the command exits 0, and ``on-failure`` does **not** restart it;
* metrics enabled with ``--metrics-addr 0.0.0.0``: the role endpoints answer from another container on the network, while the default
  (``127.0.0.1``) refuses it; the ``targets.json`` of the run still names ``127.0.0.1`` (the documented limit);
* a **bulk** load in a container with ``--memory``: the planner is sized from the container's cgroup limit (``stage=sizing``), the load
  completes and is exact.
"""
from __future__ import annotations

import json
import time

import pytest

from tests.installed_service import bulk_fixture as fx
from tests.installed_service import stream_fixture as sf
from tests.installed_service.container_support import FLEET, SingleContainer, View, all_receipted
from tests.installed_service.process_support import children, stream_clear, stream_own_graph, wait_for
from tests.installed_service.test_service_harness import run_owned_container
from tests.installed_service.test_service_process_runtime import bulk_clear, bulk_own_graph, bulk_topics
from tests.installed_service.test_service_single_container import (  # noqa: F401  (container_memory_guard is an autouse fixture)
    RESTART_ON_FAILURE, component_of, container_memory_guard, exit_order, load_batch_a, fixture_stream, record, start_loaded,
)

pytestmark = pytest.mark.integration

FETCH = ("import sys,urllib.request; "
         "r=urllib.request.urlopen(sys.argv[1], timeout=5); print(r.status); print(r.read(400).decode(errors='replace'))")


def test_s8x_second_shell_docker_exec_stop_is_the_ordered_stop_and_on_failure_does_not_restart(stack, service_config, docker_client):
    box, attempt, topics, group = start_loaded(stack, service_config, docker_client, 'xs', restart=RESTART_ON_FAILURE)
    try:
        # the page's HEALTHCHECK command (the slim image has no curl): it must exit 0 once the fleet is ready
        health = box.run_python("import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/readyz', timeout=3)")
        began = time.monotonic()
        result = box.container.exec_run(['python', '-m', 'neo4j_graph_loader.cli', 'stop', '--runtime', 'process', '--run-dir', '/work/runs'])
        code = box.wait_exit(90)
        seconds = time.monotonic() - began
        time.sleep(15)                                                       # a restart policy would have acted by now
        box.container.reload()
        state, restarts = box.container.attrs['State'], box.container.attrs['RestartCount']
        dies = box.die_codes()
    finally:
        box.cleanup()
    record(box, healthcheck_exit=health[0], exec_exit=result.exit_code, exec_output=(result.output or b'').decode(errors='replace')[-300:], container_exit=code,
           seconds=round(seconds, 2), restart_count=restarts, die_codes=dies, state=state['Status'])
    assert health[0] == 0, health
    assert result.exit_code == 0, result.output
    assert code == 0 and dies == [0] and restarts == 0 and state['Status'] == 'exited', (code, dies, restarts, state['Status'])
    assert all_receipted(attempt) and sorted(s.get('code') for s in children(attempt)) == [0] * FLEET
    kinds = [component_of(attempt, name) for name in exit_order(attempt)]
    assert kinds == ['edge-loader'] * 3 + ['node-loader'] * 2 + ['bucket-lease-server'], kinds
    assert stream_own_graph(stack) == sf.expected(['a'])
    stream_clear(stack)


COMMAND_METRICS_PORT = 9464      # GRAPH_LOADER_METRICS_PORT's default: the command's own endpoint, bound to GRAPH_LOADER_METRICS_BIND_ADDR


def metrics_cell(stack, config, client, tag, extra):
    topics, group, text = fixture_stream(stack, tag)
    box = SingleContainer(stack, config, client, tag, text, extra=extra)
    box.spec['environment']['GRAPH_LOADER_METRICS_ENABLED'] = '1'
    box.create()
    box.start()
    attempt = box.attempt()
    box.ready()
    wait_for('the run\'s targets file', lambda: (attempt / 'metrics' / 'targets.json').is_file(), 60, cli=View(box))
    targets = json.loads((attempt / 'metrics' / 'targets.json').read_text())
    ports = sorted({int(t.rpartition(':')[2]) for group_ in targets for t in group_.get('targets', [])})
    box.container.reload()
    address = box.container.attrs['NetworkSettings']['Networks'][stack.network.name]['IPAddress']
    fetched = {}
    image = box.image
    for port in ports[:3]:
        status, output = run_owned_container(client, stack.registry, stack.owner, f's8-{tag}-fetch-{port}', image,
                                             [FETCH, f'http://{address}:{port}/metrics'], network=stack.network.name,
                                             entrypoint=['python', '-c'])
        fetched[port] = (status, output.decode(errors='replace')[:200])
    inside = box.run_python(FETCH.replace('sys.argv[1]', repr(f'http://127.0.0.1:{ports[0]}/metrics')))
    return box, targets, ports, fetched, inside


def test_s8x_metrics_addr_0000_makes_role_endpoints_reachable_from_another_container(stack, service_config, docker_client):
    box, targets, ports, fetched, inside = metrics_cell(stack, service_config, docker_client, 'xm', ('--metrics-addr', '0.0.0.0'))
    try:
        host_of_targets = {t.rpartition(':')[0] for group_ in targets for t in group_.get('targets', [])}
    finally:
        box.cleanup()
    record(box, ports=ports, fetched=fetched, targets_hosts=sorted(host_of_targets), inside_exit=inside[0])
    assert ports, targets
    assert inside[0] == 0, inside
    # --metrics-addr is the ROLES' bind address; the command's own endpoint (consumer lag, planner metrics) keeps its loopback default
    roles = {port: result for port, result in fetched.items() if port != COMMAND_METRICS_PORT}
    assert roles, fetched
    assert all(status == 0 and output.startswith('200') for status, output in roles.values()), fetched
    if COMMAND_METRICS_PORT in fetched:
        assert fetched[COMMAND_METRICS_PORT][0] != 0, f'the command\'s own endpoint answered another container: {fetched}'
    assert host_of_targets == {'127.0.0.1'}, 'targets.json names 127.0.0.1 even with --metrics-addr 0.0.0.0 (the documented limit)'


def test_s8x_default_metrics_addr_refuses_other_containers(stack, service_config, docker_client):
    box, targets, ports, fetched, inside = metrics_cell(stack, service_config, docker_client, 'xd', ())
    box.cleanup()
    record(box, ports=ports, fetched=fetched, inside_exit=inside[0])
    assert ports and inside[0] == 0, (ports, inside)
    assert all(status != 0 for status, _ in fetched.values()), f'a loopback-only endpoint answered another container: {fetched}'


def test_s8x_bulk_load_in_a_container_sizes_the_planner_from_the_container_limit(stack, service_config, docker_client):
    bulk_clear(stack)
    scale = (3000, 30)
    topics = bulk_topics(stack, 'xb', scale)
    group = f'{stack.topic_prefix}xb-group'
    box = SingleContainer(stack, service_config, docker_client, 'xb', fx.schema(stack.topic_prefix, topics, group), mem_mb=1400)
    box.spec['command'] = ['start', '--mode', 'bulk', '--config', '/work/schema.yaml', '--runtime', 'process', '--run-dir', '/work/runs',
                           *fx.PLAN_ARGS]
    box.create()
    try:
        box.start()
        code = box.wait_exit(600)
        text = box.all_text()
    finally:
        box.cleanup()
    sizing = [line for line in text.splitlines() if 'stage=sizing' in line]
    record(box, exit_code=code, sizing=sizing[:3], oom_lines=[l for l in text.splitlines() if 'oom' in l.lower()][:3])
    assert code == 0, text[-2500:]
    assert sizing and 'headroom_mb=' in sizing[0], text[-2000:]
    headroom = int(sizing[0].split('headroom_mb=')[1].split()[0])
    assert 0 < headroom <= 1400, f'headroom {headroom} MiB must come from the container limit of 1400 MiB'
    assert bulk_own_graph(stack) == (fx.expected_nodes(*scale), fx.expected_relationships(*scale))
    bulk_clear(stack)
