"""E14.F2.S7 supervision guarantees under ``--runtime process`` against the owned Neo4j/Kafka test stack (Linux host).

The process-mode counterpart of the E09 kill/outage/scale cells. No Docker image or daemon is involved (``DOCKER_HOST`` points at
a missing socket); the observer is the run's registry. Each cell compares the whole fixture graph with the closed-form oracle.

* an edge loader killed with SIGKILL mid-load: the run ends with exit 1 (``exited before drain acknowledgement``), every other
  child is stopped and reaped, and the same command in a new run resumes to the exact graph (repeated twice);
* a retry consumer killed: the fleet stops itself with exit 1 and the same command resumes;
* a Neo4j outage of 20 s (twice) and of 6 min: no role exits or is replaced, the writes resume, the graph is exact, an ordered
  second-shell stop ends with exit 0;
* SIGTERM on the CLI during an outage: exit 0 (a loader killed on the stop budget is allowed), the same command converges;
* ``kill -9`` of the CLI: the lifeline stops every child; ``stop --recover`` finds nothing and reports the retained files; the
  same command converges. With a child frozen (SIGSTOP) so that the lifeline cannot act, ``stop --recover`` escalates to SIGKILL
  and reports it; ``stop --recover`` is refused while the CLI is alive;
* node-loader replica scaling (three replicas on a three-partition node topic).

Not covered here: DLQ and ordering (``test_service_process_ordering_dlq.py``), Docker-mode cells (E14 closes them last).
"""
from __future__ import annotations

import json
import os
import signal
import time
from datetime import date

import pytest

from neo4j_graph_loader.orchestrator.process_service import registered_children
from tests.installed_service import stream_fixture as sf
from tests.installed_service import dlq_fixture as dq
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    STREAM_COMPONENTS, TOTAL_BIG, Fleet, assert_clean, attempt_files, child_ids, child_log, children, components_started,
    exited_children, finished, kill_child, kill_cli, lifeline_seconds, live, memory_guard, mid_load, producing, recorded_alive, records,
    run_dir, run_to_completion, second_shell, start_stream, stream_clear, stream_committed, stream_ends, stream_own_graph,
    stream_topics, the_attempt, wait_exit, wait_for,
)
from tests.installed_service import bulk_fixture as fx
from tests.installed_service.test_service_process_runtime import bulk_clear, bulk_own_graph, bulk_topics
from tests.installed_service.test_service_ingest_dlq import clear as dlq_clear
from tests.installed_service.test_service_ingest_dlq import likes as dlq_likes
from tests.installed_service.test_service_ingest_dlq import offsets as dlq_offsets

pytestmark = pytest.mark.integration


def exit_codes(attempt, component):
    return sorted(s.get('code') for s in children(attempt, component=component))


def settle(stack, fleet, topics, group, graph):
    fleet.wait('the exact graph', lambda: stream_own_graph(stack) == graph, 300)
    end = stream_ends(stack, topics)
    fleet.wait('committed offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 120)


# -- a lost role -------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('repeat', range(2))
def test_p7_edge_loader_kill_fails_the_run_and_the_same_command_resumes(repeat, stack, service_config):
    run = run_dir(service_config, f'p7-loader-{repeat}')
    stream_clear(stack)
    topics = stream_topics(stack, f'el{repeat}')
    group = f'{stack.topic_prefix}el{repeat}-group'
    text = sf.schema(stack.topic_prefix, topics, group)
    sf.produce_nodes(stack, topics, 'big', with_firms=True)
    nodes_big, rels_big = sf.expected(['big'])
    first = Fleet(stack, service_config, run, 'start-1', text)
    try:
        first.wait('the nodes', lambda: stream_own_graph(stack)[0] == nodes_big, 180)
        with producing(stack, topics, 'big'):
            in_flight = mid_load(stack, first.cli)
            assert len(live(first.attempt, component='edge-loader')) == 3
            killed = kill_child(first.attempt, component='edge-loader')
        code = wait_exit(first.cli, 120, 'the fleet must stop itself when an edge loader is lost')
    finally:
        first.close()
    text_out = first.cli.text()
    assert code == 1 and 'exited before drain acknowledgement' in text_out, (code, text_out[-2000:])
    finished(stack, first.cli, first.attempt, STREAM_COMPONENTS, code=1)
    assert next(s for s in children(first.attempt) if s['id'] == killed['id'])['code'] == 137
    nodes, partial = stream_own_graph(stack)
    assert nodes == nodes_big and set(partial) <= set(rels_big) and len(partial) == len(set(partial))
    assert 0 < in_flight < TOTAL_BIG and len(partial) < TOTAL_BIG, 'the lost loader must leave work outstanding for the restart'

    second_run = run_dir(service_config, f'p7-loader-{repeat}-2')
    second = Fleet(stack, service_config, second_run, 'start-2', text)
    try:
        settle(stack, second, topics, group, (nodes_big, rels_big))
        code2 = second.stop()
    finally:
        second.close()
    assert code2 == 0, (code2, second.cli.text()[-2000:])
    finished(stack, second.cli, second.attempt, STREAM_COMPONENTS, code=0)
    assert stream_own_graph(stack) == (nodes_big, rels_big)
    (run / 'logs' / 'summary.json').write_text(json.dumps({'in_flight': in_flight, 'after_loss': len(partial)}) + '\n')
    stream_clear(stack)


def test_p7_retry_consumer_kill_stops_the_fleet_and_the_same_command_resumes(stack, service_config):
    run = run_dir(service_config, 'p7-retry-kill')
    dlq_clear(stack)
    prefix = f'{stack.topic_prefix}rk-'
    group = f'{prefix}group'
    topic = prefix + 'likes'
    stack.create_topics([prefix + 'person', prefix + 'company', topic])
    stack.produce(prefix + 'person', [(p, {'person_id': p}) for p in range(1, 11)])
    stack.produce(prefix + 'company', [(c, {'company_id': c}) for c in ('c1', 'c2', 'c3')])
    text = dq.limits_schema(prefix, group, attempts=200)
    components = {'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': 1, 'retry-consumer': 1}
    first_rows = [('t', p, f'c{p % 3 + 1}', date(2020, 1, p), f'r{p}') for p in range(1, 6)]
    dq.publish_likes(stack, topic, first_rows)
    want_first = {(p, c): (d.isoformat(), r) for _t, p, c, d, r in first_rows}
    first = Fleet(stack, service_config, run, 'start-1', text)
    try:
        first.wait('the first relationships', lambda: dlq_likes(stack) == want_first, 180)
        kill_child(first.attempt, component='retry-consumer')
        code = wait_exit(first.cli, 120, 'the fleet must stop itself when the retry consumer is lost')
    finally:
        first.close()
    out = first.cli.text()
    assert code == 1 and 'retry consumer exited during stream supervision' in out, (code, out[-2000:])
    finished(stack, first.cli, first.attempt, components, code=1)

    more = [('t', p, f'c{p % 3 + 1}', date(2020, 2, p), f'm{p}') for p in range(6, 11)]
    dq.publish_likes(stack, topic, more)
    want_all = {**want_first, **{(p, c): (d.isoformat(), r) for _t, p, c, d, r in more}}
    second = Fleet(stack, service_config, run_dir(service_config, 'p7-retry-kill-2'), 'start-2', text)
    try:
        second.wait('the same command converges', lambda: dlq_likes(stack) == want_all, 180)
        second.wait('edge group at the log end', lambda: dlq_offsets(stack, f'{group}-LIKES', topic, 1) == {0: stack.end_offsets(topic)[0]}, 60)
        code2 = second.stop()
    finally:
        second.close()
    assert code2 == 0, (code2, second.cli.text()[-2000:])
    finished(stack, second.cli, second.attempt, components, code=0)
    dlq_clear(stack)


# -- Neo4j outages --------------------------------------------------------------------------------------------------------
def outage_setup(stack, config, run_name, tag):
    run = run_dir(config, run_name)
    stream_clear(stack)
    topics = stream_topics(stack, tag)
    group = f'{stack.topic_prefix}{tag}-group'
    text = sf.schema(stack.topic_prefix, topics, group)
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    nodes, rels_a = sf.expected(['a'])
    fleet = Fleet(stack, config, run, 'start', text)
    fleet.wait('the nodes', lambda: stream_own_graph(stack)[0] == nodes, 180)
    sf.produce_edges(stack, topics, 'a')
    fleet.wait('the first batch', lambda: stream_own_graph(stack) == (nodes, rels_a), 240)
    return run, topics, group, text, nodes, fleet


def hold_outage(stack, fleet, seconds):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        assert fleet.cli.running(), 'the fleet must ride out a Neo4j outage: ' + fleet.cli.text()[-2000:]
        assert exited_children(fleet.attempt) == [], f'a role exited during the outage: {exited_children(fleet.attempt)}'
        time.sleep(1)


def outage_case(stack, config, run_name, tag, seconds):
    _run, topics, group, _text, nodes, fleet = outage_setup(stack, config, run_name, tag)
    try:
        before = child_ids(fleet.attempt)
        assert stack.stop_neo4j(timeout=30) == 'exited'
        sf.produce_edges(stack, topics, 'a2')                 # relationships over existing nodes, published while Neo4j is down
        hold_outage(stack, fleet, seconds)
        stack.start_neo4j()
        _, rels_all = sf.expected(['a', 'a2'])
        settle(stack, fleet, topics, group, (nodes, rels_all))
        assert child_ids(fleet.attempt) == before, 'a role was replaced during the outage'
        assert exited_children(fleet.attempt) == []
        edge_logs = '\n'.join(child_log(fleet.attempt, s) for s in children(fleet.attempt, component='edge-loader'))
        assert 'stage=write-outage' in edge_logs and 'stage=write-outage-recovered' in edge_logs, 'no outage/recovery log in the edge loaders'
        code = fleet.stop()
    finally:
        fleet.close()
    assert code == 0, (code, fleet.cli.text()[-2000:])
    finished(stack, fleet.cli, fleet.attempt, STREAM_COMPONENTS, code=0)
    assert exit_codes(fleet.attempt, 'edge-loader') == [0, 0, 0]
    assert stream_own_graph(stack) == (nodes, rels_all)
    stream_clear(stack)


@pytest.mark.parametrize('repeat', range(2))
def test_p7_short_neo4j_outage_no_role_exits_and_the_graph_is_exact(repeat, stack, service_config):
    outage_case(stack, service_config, f'p7-outage-short-{repeat}', f'os{repeat}', 20)


def test_p7_long_neo4j_outage_no_role_exits_and_the_graph_is_exact(stack, service_config):
    outage_case(stack, service_config, 'p7-outage-long', 'ol', 360)


def test_p7_sigterm_during_an_outage_exits_zero_and_the_same_command_converges(stack, service_config):
    run, topics, group, text, nodes, fleet = outage_setup(stack, service_config, 'p7-outage-sigterm', 'ot')
    try:
        assert stack.stop_neo4j(timeout=30) == 'exited'
        sf.produce_edges(stack, topics, 'a2')
        hold_outage(stack, fleet, 8)
        code = fleet.cli.terminate(timeout=120)          # SIGTERM straight to the CLI, as an operator or systemd would
    finally:
        fleet.close()
    assert code == 0, (code, fleet.cli.text()[-2500:])
    assert fleet.cli.stop_seconds < 120
    finished(stack, fleet.cli, fleet.attempt, STREAM_COMPONENTS, code=0)
    codes = {s['name']: s.get('code') for s in children(fleet.attempt)}
    assert set(codes.values()) <= {0, 137, 143}, codes          # a loader stopped on the stop budget may be killed (137)
    stack.start_neo4j()
    _, rels_all = sf.expected(['a', 'a2'])
    second = Fleet(stack, service_config, run_dir(service_config, 'p7-outage-sigterm-2'), 'start-2', text)
    try:
        settle(stack, second, topics, group, (nodes, rels_all))
        code2 = second.stop()
    finally:
        second.close()
    assert code2 == 0
    finished(stack, second.cli, second.attempt, STREAM_COMPONENTS, code=0)
    stream_clear(stack)


# -- the CLI itself dies ---------------------------------------------------------------------------------------------------
def test_p7_kill_minus_nine_of_the_cli_lifeline_stops_every_child_and_the_same_command_converges(stack, service_config):
    run, topics, group, text, nodes, fleet = outage_setup(stack, service_config, 'p7-kill9', 'k9')
    try:
        alive = [s['name'] for s in registered_children(fleet.attempt / 'registry.jsonl')]
        assert len(alive) == sum(STREAM_COMPONENTS.values())
        refused = fleet.shell('stop', '--recover')
        assert refused.exit_code != 0 and 'still alive' in refused.stderr.read_text() + refused.stdout.read_text()
        killed_at = kill_cli(fleet.cli)
        seconds = lifeline_seconds(fleet.attempt, killed_at)
        recovered = fleet.shell('stop', '--recover')
        recovered_text = recovered.stderr.read_text() + recovered.stdout.read_text()
        assert recovered.exit_code == 0 and 'signalled=[]' in recovered_text, recovered_text
        assert (fleet.attempt / 'registry.jsonl').is_file() and (fleet.attempt / 'logs').is_dir(), 'the attempt files are retained'
    finally:
        fleet.close()
    assert fleet.cli.process.poll() == -signal.SIGKILL
    assert seconds < 30, f'the lifeline took {seconds:.1f}s'
    (fleet.run / 'logs' / 'observed.json').write_text(json.dumps({'children': sum(STREAM_COMPONENTS.values()), 'lifeline_seconds': round(seconds, 3)}) + '\n')
    assert registered_children(fleet.attempt / 'registry.jsonl') == []
    assert any(r['kind'] == 'recovered' for r in records(fleet.attempt))
    sf.produce_nodes(stack, topics, 'b', with_firms=False)
    nodes_ab, _ = sf.expected(['a', 'b'])
    second = Fleet(stack, service_config, run_dir(service_config, 'p7-kill9-2'), 'start-2', text)
    try:
        second.wait('the new nodes', lambda: stream_own_graph(stack)[0] == nodes_ab, 180)
        sf.produce_edges(stack, topics, 'b')
        settle(stack, second, topics, group, sf.expected(['a', 'b']))
        code = second.stop()
    finally:
        second.close()
    assert code == 0
    finished(stack, second.cli, second.attempt, STREAM_COMPONENTS, code=0)
    stream_clear(stack)


def test_p7_stop_recover_escalates_on_a_child_the_lifeline_cannot_reach(stack, service_config):
    run, topics, group, text, nodes, fleet = outage_setup(stack, service_config, 'p7-recover', 'rc')
    frozen = None
    try:
        frozen = live(fleet.attempt, component='node-loader')[0]
        os.kill(int(frozen['pid']), signal.SIGSTOP)            # a stopped process cannot run its lifeline thread
        killed_at = kill_cli(fleet.cli)
        seconds = lifeline_seconds(fleet.attempt, killed_at, besides={frozen['name']})
        left = [s['name'] for s in registered_children(fleet.attempt / 'registry.jsonl')]
        assert left == [frozen['name']], left
        recovered = fleet.shell('stop', '--recover', timeout=120)
        out = recovered.stderr.read_text() + recovered.stdout.read_text()
        assert recovered.exit_code == 0, out
        assert f"signalled=['{frozen['name']}']" in out and f"killed=['{frozen['name']}']" in out and 'remaining=[]' in out, out
        assert registered_children(fleet.attempt / 'registry.jsonl') == []
        (fleet.run / 'logs' / 'observed.json').write_text(json.dumps({'children_besides_frozen': sum(STREAM_COMPONENTS.values()) - 1,
                                                                       'lifeline_seconds': round(seconds, 3)}) + '\n')
    finally:
        if frozen is not None:
            try:
                os.kill(int(frozen['pid']), signal.SIGKILL)    # never leave a frozen process behind if an assertion failed
            except OSError:
                pass
        fleet.close()
    stream_clear(stack)


@pytest.mark.parametrize('moment', ['planner-scanning', 'dispatcher-and-writers-up'])
def test_p7_kill_minus_nine_during_a_planned_bulk_run(moment, stack, service_config):
    """The planner's workers and helper are not registry children; the lifeline and the parent-death socket must still end them."""
    run = run_dir(service_config, f'p7-bulk-kill9-{moment}')
    bulk_clear(stack)
    scale = (3000, 30)                                             # the graph observation is bounded in bytes
    tag = 'bk' + moment[0]
    topics = bulk_topics(stack, tag, scale)
    group = f'{stack.topic_prefix}{tag}-group'
    text = fx.schema(stack.topic_prefix, topics, group)
    chunk_topic = f'{stack.topic_prefix}{tag}-chunks'
    cli = start_stream(stack, service_config, run, 'start-1', text, mode='bulk', extra=(*fx.PLAN_ARGS, '--plan-topic', chunk_topic))
    attempt = the_attempt(run)
    try:
        if moment == 'planner-scanning':
            wait_for('a planner worker', lambda: [s for s in recorded_alive(attempt) if s.get('role') == 'planner-worker'], 300,
                     cli=cli, step=0.02)
        else:
            wait_for('the dispatcher and a writer', lambda: live(attempt, component='planned-dispatcher')
                     and live(attempt, component='planned-edge-writer'), 300, cli=cli, step=0.02)
        before = recorded_alive(attempt)
        roles = sorted(s.get('role') or (s.get('labels') or {}).get('component') for s in before)
        killed_at = kill_cli(cli)
        seconds = lifeline_seconds(attempt, killed_at)
    finally:
        for state in recorded_alive(attempt):                      # never leave a process behind if an assertion failed
            try:
                os.kill(int(state['pid']), signal.SIGKILL)
            except OSError:
                pass
        cli.kill()
    if moment == 'planner-scanning':
        assert 'planner-worker' in roles, roles
    else:
        # the planner workers end with the scan, before the dispatcher and the writers start; the multiprocessing helper stays
        assert 'planned-dispatcher' in roles and 'planned-edge-writer' in roles and 'planner-helper' in roles, roles
    assert recorded_alive(attempt) == [] and registered_children(attempt / 'registry.jsonl') == []
    on_disk = attempt_files(attempt)
    assert on_disk, 'a killed planner leaves its attempt files behind'
    shell = second_shell(service_config, stack, run, attempt, 'stop', '--recover', timeout=120)
    out = shell.stderr.read_text() + shell.stdout.read_text()
    assert shell.exit_code == 0 and 'signalled=[]' in out and 'remaining=[]' in out, out
    assert f'files={len(on_disk)}' in out and all(name in out for name in on_disk), ('every retained file is reported by path', out)
    assert attempt_files(attempt) == on_disk, 'recovery deleted or changed an attempt file'
    (run / 'logs' / 'observed.json').write_text(json.dumps({'moment': moment, 'alive_at_kill': roles, 'lifeline_seconds': round(seconds, 3),
                                                            'retained': sorted(on_disk)}, indent=1) + '\n')
    assert seconds < 30, seconds

    admin = stack.admin()                                          # the killed planner could not delete its chunk topic
    if chunk_topic in admin.list_topics(timeout=10).topics:
        admin.delete_topics([chunk_topic])[chunk_topic].result(30)
    second_run = run_dir(service_config, f'p7-bulk-kill9-{moment}-2')
    result, attempt2 = run_to_completion(stack, service_config, second_run, 'start-2', text, extra=fx.PLAN_ARGS)
    assert result.exit_code == 0, result.tail
    assert attempt2 != attempt
    assert bulk_own_graph(stack) == (fx.expected_nodes(*scale), fx.expected_relationships(*scale)), 'the fresh run must converge to the exact graph'
    assert_clean(attempt2)
    bulk_clear(stack)


# -- scaling ------------------------------------------------------------------------------------------------------------------
def test_p7_node_loader_replicas_scale_with_the_partitions(stack, service_config):
    run = run_dir(service_config, 'p7-scale')
    stream_clear(stack)
    prefix = f'{stack.topic_prefix}sc-'
    topics = {'citizen': prefix + 'citizen', 'firm': prefix + 'firm', **{kind: prefix + kind.lower() for kind in sf.TYPES}}
    stack.create_topics([topics['citizen']], partitions=3)
    stack.create_topics([t for key, t in topics.items() if key != 'citizen'])
    group = f'{prefix}group'
    text = sf.schema(stack.topic_prefix, topics, group).replace(f"    topic: {topics['citizen']}\n",
                                                                 f"    topic: {topics['citizen']}\n    replicas: 3\n", 1)
    assert 'replicas: 3' in text
    sf.produce_nodes(stack, topics, 'big', with_firms=True)
    graph = sf.expected(['big'])
    fleet = Fleet(stack, service_config, run, 'start', text)
    try:
        fleet.wait('the nodes', lambda: stream_own_graph(stack)[0] == graph[0], 240)      # endpoints first: no DLQ in this schema
        sf.produce_edges(stack, topics, 'big')
        fleet.wait('the whole big batch', lambda: stream_own_graph(stack) == graph, 300)
        end = stream_ends(stack, topics)
        fleet.wait('committed offsets at the log ends', lambda: stream_committed(stack, group, topics) == end, 120)
        started = components_started(fleet.attempt)
        assert started == {'bucket-lease-server': 1, 'node-loader': 4, 'edge-loader': 3}, started
        assert len({s['labels'].get('node_label') for s in children(fleet.attempt, component='node-loader')}) == 2
        code = fleet.stop()
    finally:
        fleet.close()
    assert code == 0
    finished(stack, fleet.cli, fleet.attempt, {'bucket-lease-server': 1, 'node-loader': 4, 'edge-loader': 3}, code=0)
    stream_clear(stack)


def test_p7_no_process_left_from_this_module(service_config):
    from pathlib import Path
    root = Path(service_config['run_dir'])
    survivors = []
    for registry in root.glob('p7-*/runs/*/registry.jsonl'):
        survivors += [s['name'] for s in registered_children(registry)]
    assert survivors == []
