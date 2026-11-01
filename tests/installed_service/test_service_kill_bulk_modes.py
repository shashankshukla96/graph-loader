"""E09.F1.S5 (review round): classic and lease bulk lose a loader, or the lease server, mid-load (native shared Docker Engine substrate).

Both modes recover only by a full replay, like planned bulk: the run ends (exit non-zero), every container is removed, the partial graph
is a duplicate-free subset of the fixture, and a second run on the same topics converges to the exact graph with its own run-scoped
relationship groups at the log ends. The runner and kill helpers are shared with ``test_service_kill_and_outage.py``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.installed_service import stream_fixture as sf
from tests.installed_service.inspect_strict import bulk_groups, group_offsets, list_groups
from tests.installed_service.test_service_kill_and_outage import (
    bulk_cleanup, bulk_run, clear, declare_open_writes, kill_when_in_flight, own_graph, topics_for)

pytestmark = pytest.mark.integration

BULK_MODES = {'classic': ['--edge-classic'], 'lease': ['--edge-leases']}
BULK_TUNING = {'worker_count': 2, 'batch_size': 100}


@pytest.mark.parametrize('mode,component', [('classic', 'edge-loader'), ('lease', 'edge-loader'), ('lease', 'bucket-lease-server')],
                         ids=['classic-edge-loader', 'lease-edge-loader', 'lease-server'])
def test_s5_bulk_mode_loss_ends_the_run_and_a_full_replay_converges(mode, component, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's5-bulk-{mode}-{component}'
    run.mkdir()
    clear(stack)
    tag = f'bk{mode[0]}{component[0]}'
    topics = topics_for(stack, tag)
    group = f'e09-{stack.owner}-{tag}-group'
    sf.produce_nodes(stack, topics, 'big', with_firms=True)
    sf.produce_edges(stack, topics, 'big')
    ends_edge = {kind: stack.end_offsets(topics[kind])[0] for kind in sf.TYPES}
    text = sf.schema(f'e09-{stack.owner}-', topics, group, bulk=BULK_TUNING)
    watcher = kill_when_in_flight(stack, component)
    first = bulk_run(stack, docker_client, registry, service_config, run, 'start-1', text, BULK_MODES[mode], watcher=watcher)
    facts = {'killed': watcher.state, 'exit_code': first['result'].exit_code}
    (run / 'logs' / 'facts-first.json').write_text(json.dumps(facts, indent=1, default=str) + '\n')
    bulk_cleanup(stack, docker_client, first)
    facts['open_writes_declared'] = declare_open_writes(stack)
    nodes_big, rels_big = sf.expected(['big'])
    nodes, partial = own_graph(stack)
    facts.update(after_loss=len(partial), tail=first['result'].tail[-600:])
    (run / 'logs' / 'facts-first.json').write_text(json.dumps(facts, indent=1, default=str) + '\n')
    assert watcher.state.get('killed'), 'the loss did not hit work in flight'
    assert first['result'].exit_code not in (0, None), 'losing a loader or the lease server mid-load must end a bulk run with a failure'
    assert nodes == nodes_big and set(partial) <= set(rels_big) and len(partial) == len(set(partial))

    second = bulk_run(stack, docker_client, registry, service_config, run, 'start-2', text, BULK_MODES[mode])
    assert second['result'].exit_code == 0, second['result'].tail
    bulk_cleanup(stack, docker_client, second)
    assert own_graph(stack) == (nodes_big, rels_big), 'the full replay must converge to the exact graph'
    groups = bulk_groups(second['snapshot'], group)
    assert sorted(groups) == sorted(sf.TYPES), groups
    listed = list_groups(stack)
    for kind in sf.TYPES:
        assert groups[kind] in listed, (kind, groups[kind])
        assert group_offsets(stack, groups[kind], topics[kind], 1) == {0: ends_edge[kind]}, kind
    assert not set(groups.values()) & set(bulk_groups(first['snapshot'], group).values()), 'the replay must use its own run-scoped groups'
    (run / 'logs' / 'facts-second.json').write_text(json.dumps({'exit_code': 0}) + '\n')
    clear(stack)
