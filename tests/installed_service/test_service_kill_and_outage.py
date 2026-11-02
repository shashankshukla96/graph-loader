"""E09.F1.S5 outages, supervision and recovery on owned Neo4j/Kafka (native shared Docker Engine substrate).

A small scenario table through the installed CLI and installed images; each row says what the product promises for its mode:

| mode | event | expected |
| --- | --- | --- |
| stream (leases) | Neo4j stopped for a while, relationships published meanwhile | no container exits; the loaders ride the outage out and the graph is exact afterwards |
| stream (leases) | lease server killed mid-load | supervised restart, the fleet keeps running, exact graph, later writes still land |
| stream (leases) | one edge loader killed mid-load | supervision stops the fleet (exit 1); the same command resumes with no loss or duplicates |
| stream (leases) | SIGTERM with relationships outstanding | exit 0; the same command resumes the rest |
| planned bulk | Neo4j stopped during the edge phase | fails closed (exit 1, no ledger, clean teardown); a full replay converges |
| classic / lease bulk | one edge loader (or the lease server) killed mid-load | the run ends and cleans up; a full replay of the same topics converges to the exact graph |

Bulk modes (classic, lease bulk) lose a loader or the lease server mid-load and recover only by a full replay, like planned bulk.

Not covered: retry consumer or node loader loss, Neo4j loss while
rows are parked, a 10-minute authentication fatal, outages longer than one short stop, kill -9 of the CLI, arm64. The kills and the
stop are timing races against a load of a few seconds; every test asserts that the event really hit work in flight.
"""
from __future__ import annotations

import contextlib
import json
from pathlib import Path
import re
import threading
import time

import pytest

from tests.installed_service import bulk_fixture as fx
from tests.installed_service import query_window as qw
from tests.installed_service import stream_fixture as sf
from tests.installed_service import test_service_load_bulk as lb
from tests.installed_service.live_cli import AppObserver, cli_environment, run_cli
from tests.installed_service.observers import committed_offsets, graph_observations
from tests.installed_service.stream_run import StreamRun

pytestmark = pytest.mark.integration

STREAM_COMPONENTS = {'preflight': 1, 'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': 3}
TOTAL_BIG = 6000
WRITE_PREFIXES = ('UNWIND range(0, size($rows) - 1) AS row_index', 'UNWIND $batch AS record MERGE (n:')


def own_graph(stack):
    return sf.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def relationships(stack):
    return own_graph(stack)[1]


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Citizen|Firm) DETACH DELETE n').consume()


def topics_for(stack, tag):
    prefix = f'e09-{stack.owner}-{tag}-'
    topics = {'citizen': prefix + 'citizen', 'firm': prefix + 'firm', **{kind: prefix + kind.lower() for kind in sf.TYPES}}
    stack.create_topics(list(topics.values()))
    return topics


def committed(stack, group, topics):
    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'
    result = {}
    for kind, key in [('Citizen', 'citizen'), ('Firm', 'firm')] + [(t, t) for t in sf.TYPES]:
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=f'{group}-{kind}',
                                 partitions=[(topics[key], 0)], broker_guard=running)
        result[key] = rows[0]['offset']
    return result


def ends(stack, topics):
    return {key: stack.end_offsets(topic)[0] for key, topic in topics.items()}


def stopped_cleanly(stack, docker_client, out, cli, components, *, exit_code, neo4j_restarted=False):
    snapshot = out['snapshot']
    assert out['exit_code'] == exit_code, (exit_code, out['exit_code'], cli.text()[-1500:])
    assert out['stop_seconds'] < 60 and cli.redactions == 0
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    if neo4j_restarted:
        refusals_only_the_restarted_stack(stack, snapshot)
    else:
        assert snapshot['refused'] == {}
    assert out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids], 'a container outlived the run'
    seen = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info and not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        assert all('docker.sock' not in source for _, source, _, _ in info['mounts'])
        seen[info['labels']['component']] = seen.get(info['labels']['component'], 0) + 1
    assert seen == components, seen


def start_loaded_stream(stack, docker_client, registry, config, run, tag, name, *, track_queries=True):
    """Nodes of the big batch first, a fleet started on them, then the big batch's relationships: returns (stream, topics, group)."""
    topics = topics_for(stack, tag)
    group = f'e09-{stack.owner}-{tag}-group'
    sf.produce_nodes(stack, topics, 'big', with_firms=True)
    stream = StreamRun(stack, docker_client, registry, config, run, name, sf.schema(f'e09-{stack.owner}-', topics, group),
                       track_queries=track_queries)
    nodes, _ = sf.expected(['big'])
    stream.wait('nodes', lambda: own_graph(stack)[0] == nodes, 180)
    return stream, topics, group


def count_relationships(stack):
    with stack.driver.session(database=stack.database) as session:
        return session.run('MATCH (:Citizen)-[r:WORKS_FOR|BUYS_FROM|KNOWS_OF]->() RETURN count(r) AS n').single()['n']


@contextlib.contextmanager
def producing(stack, topics, batch):
    """Publish a batch's relationships from a thread while the body runs, and join it on exit.

    The loaders start writing as soon as the first relationship topic has records, and a load of this size takes about two seconds on an
    idle host. Publishing all three topics first and only then watching for work in flight left the event (a stop or a kill) landing
    after the load had nearly finished. With the body running while the batch is still being published, the event hits work in flight and
    records are still unconsumed whenever it lands, however fast the host is."""
    box = {}

    def publish():
        try:
            sf.produce_edges(stack, topics, batch)
        except BaseException as error:  # re-raised in the test thread once the body is done
            box['error'] = error

    thread = threading.Thread(target=publish, name='publish-edges')
    thread.start()
    try:
        yield
    finally:
        thread.join()
    if 'error' in box:
        raise box['error']


def mid_load(stack, stream, timeout=120):
    """Wait, polling a cheap count, until some but not all of the big batch's relationships exist; returns the count seen."""
    end = time.monotonic() + timeout
    while True:
        assert stream.cli.running(), 'the streaming CLI exited while waiting for relationships in flight: ' + stream.cli.text()[-1500:]
        count = count_relationships(stack)
        if 0 < count < TOTAL_BIG:
            return count
        assert count == 0 or time.monotonic() < end, f'the whole batch ({count}) loaded between two polls: no work was in flight'
        assert time.monotonic() < end, 'timed out waiting for relationships in flight'
        time.sleep(0.05)


def refusals_only_the_restarted_stack(stack, snapshot):
    """After a Neo4j restart Docker reconnects the stack's own Neo4j to the owned network: the observer rightly refuses it."""
    allowed = {stack.neo.id: 'existed before the run'}
    assert snapshot['refused'] == {k: v for k, v in allowed.items() if k in snapshot['refused']}, snapshot['refused']


def outage_case(run_name, tag, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / run_name
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, tag)
    group = f'e09-{stack.owner}-{tag}-group'
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    nodes, rels_a = sf.expected(['a'])
    stream = StreamRun(stack, docker_client, registry, service_config, run, 'start', sf.schema(f'e09-{stack.owner}-', topics, group),
                       track_queries=False)
    notes = {}
    try:
        stream.wait('nodes', lambda: own_graph(stack)[0] == nodes, 120)
        sf.produce_edges(stack, topics, 'a')
        stream.wait('first batch', lambda: own_graph(stack) == (nodes, rels_a), 120)
        before = {cid for kind in ('edge-loader', 'node-loader', 'bucket-lease-server') for cid, _ in stream.components(kind)}
        assert stack.stop_neo4j(timeout=30) == 'exited'
        sf.produce_edges(stack, topics, 'a2')  # relationships over existing nodes, published while Neo4j is down
        down_until = time.monotonic() + 15
        while time.monotonic() < down_until:
            assert stream.cli.running(), 'the fleet must ride out a short Neo4j outage: ' + stream.cli.text()[-1500:]
            stream.poll_logs()
            time.sleep(1)
        stack.start_neo4j()
        _, rels_all = sf.expected(['a', 'a2'])
        notes['recovered_s'] = stream.wait('both batches after the outage', lambda: own_graph(stack) == (nodes, rels_all), 240)
        end = ends(stack, topics)
        stream.wait('committed offsets at the log ends', lambda: committed(stack, group, topics) == end, 120)
        after = {cid for kind in ('edge-loader', 'node-loader', 'bucket-lease-server') for cid, _ in stream.components(kind)}
        assert after == before, 'a loader or the lease server was replaced during the outage'
        stream.poll_logs()
        edge_logs = '\n'.join(text for name, text in stream.logs.items() if 'graph-loader-edge-' in name)
        assert 'stage=write-outage' in edge_logs and 'stage=write-outage-recovered' in edge_logs, 'no outage and recovery log in the edge loaders'
        assert stream.cli.running()
    finally:
        out = stream.finish()
    stopped_cleanly(stack, docker_client, out, stream.cli, STREAM_COMPONENTS, exit_code=0, neo4j_restarted=True)
    assert own_graph(stack) == (nodes, rels_all)
    clear(stack)


def test_s5_stream_rides_out_a_short_neo4j_outage(docker_client, registry, stack, service_config):
    outage_case('s5-outage', 'ou', docker_client, registry, stack, service_config)


@pytest.mark.parametrize('repeat', range(1, 3))
def test_s5_short_neo4j_outage_repeats(repeat, docker_client, registry, stack, service_config):
    """Executions 2 and 3 of the short-outage case (timing-sensitive), under the same body as the original test."""
    outage_case(f's5-outage-{repeat}', f'ou{repeat}', docker_client, registry, stack, service_config)


@pytest.mark.parametrize('repeat', range(2))
def test_s5_stream_survives_a_lease_server_kill(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's5-lease-{repeat}'
    run.mkdir()
    clear(stack)
    stream, topics, group = start_loaded_stream(stack, docker_client, registry, service_config, run, f'ls{repeat}', 'start')
    nodes_big, _ = sf.expected(['big'])
    try:
        sf.produce_edges(stack, topics, 'big')
        in_flight = mid_load(stack, stream)
        servers = stream.components('bucket-lease-server')
        assert len(servers) == 1
        killed = stream.observer.kill_observed(servers[0][0], timeout=60)
        assert killed['exit_code'] == 137 and killed['oom_killed'] is not True
        stream.wait('supervised restart', lambda: 'stage=lease-server-restart' in stream.cli.text(), 90)
        _, rels_big = sf.expected(['big'])
        stream.wait('the whole batch after the restart', lambda: own_graph(stack) == (nodes_big, rels_big), 240)
        # A write published after the restart proves the loaders reconnected to the replacement server.
        sf.produce_nodes(stack, topics, 'c', with_firms=False)
        sf.produce_edges(stack, topics, 'c')
        nodes_all, rels_all = sf.expected(['big', 'c'])
        stream.wait('a batch published after the restart', lambda: own_graph(stack) == (nodes_all, rels_all), 180)
        end = ends(stack, topics)
        stream.wait('committed offsets at the log ends', lambda: committed(stack, group, topics) == end, 120)
        assert stream.cli.running()
    finally:
        out = stream.finish()
    stopped_cleanly(stack, docker_client, out, stream.cli, {**STREAM_COMPONENTS, 'bucket-lease-server': 2}, exit_code=0)
    declare_open_writes(stack)
    assert 0 < in_flight < TOTAL_BIG
    assert own_graph(stack) == sf.expected(['big', 'c'])
    clear(stack)


@pytest.mark.parametrize('repeat', range(2))
def test_s5_stream_stops_when_an_edge_loader_is_lost_and_resumes(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's5-loader-{repeat}'
    run.mkdir()
    clear(stack)
    stream, topics, group = start_loaded_stream(stack, docker_client, registry, service_config, run, f'el{repeat}', 'start-1')
    nodes_big, rels_big = sf.expected(['big'])
    try:
        with producing(stack, topics, 'big'):
            in_flight = mid_load(stack, stream)
            loaders = stream.components('edge-loader')
            assert len(loaders) == 3
            killed = stream.observer.kill_observed(loaders[0][0], timeout=60)
            assert killed['exit_code'] == 137
        end = time.monotonic() + 120
        while stream.cli.running():
            assert time.monotonic() < end, 'the fleet must stop itself when an edge loader is lost'
            stream.poll_logs()
            time.sleep(0.5)
        code = stream.cli.process.poll()
    finally:
        out1 = stream.finish()
    text = stream.cli.text()
    assert code == 1 and 'exited before drain acknowledgement' in text, (code, text[-1500:])
    stopped_cleanly(stack, docker_client, out1, stream.cli, STREAM_COMPONENTS, exit_code=1)
    declare_open_writes(stack)
    nodes, partial = own_graph(stack)
    assert nodes == nodes_big and set(partial) <= set(rels_big) and len(partial) == len(set(partial))
    assert 0 < in_flight < TOTAL_BIG
    assert len(partial) < TOTAL_BIG, 'the lost loader must have left work outstanding for the restart'

    second = StreamRun(stack, docker_client, registry, service_config, run, 'start-2', sf.schema(f'e09-{stack.owner}-', topics, group))
    try:
        second.wait('the same command resumes to the exact graph', lambda: own_graph(stack) == (nodes_big, rels_big), 240)
        end_offsets = ends(stack, topics)
        second.wait('committed offsets at the log ends', lambda: committed(stack, group, topics) == end_offsets, 120)
        assert second.cli.running()
    finally:
        out2 = second.finish()
    stopped_cleanly(stack, docker_client, out2, second.cli, STREAM_COMPONENTS, exit_code=0)
    assert own_graph(stack) == (nodes_big, rels_big)
    (run / 'logs' / 'summary.json').write_text(json.dumps({'in_flight': in_flight, 'after_loss': len(partial)}) + '\n')
    clear(stack)


@pytest.mark.parametrize('repeat', range(2))
def test_s5_stream_stops_gracefully_with_work_outstanding_and_resumes(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's5-stop-{repeat}'
    run.mkdir()
    clear(stack)
    stream, topics, group = start_loaded_stream(stack, docker_client, registry, service_config, run, f'gs{repeat}', 'start-1')
    nodes_big, rels_big = sf.expected(['big'])
    with producing(stack, topics, 'big'):
        try:
            in_flight = mid_load(stack, stream)
        finally:
            out1 = stream.finish()  # SIGTERM while relationships are outstanding (and still being published)
    stopped_cleanly(stack, docker_client, out1, stream.cli, STREAM_COMPONENTS, exit_code=0)
    nodes, partial = own_graph(stack)
    assert nodes == nodes_big and set(partial) <= set(rels_big) and len(partial) == len(set(partial))
    assert 0 < in_flight < TOTAL_BIG
    assert len(partial) < TOTAL_BIG, 'the stop must have left work outstanding for the restart'

    second = StreamRun(stack, docker_client, registry, service_config, run, 'start-2', sf.schema(f'e09-{stack.owner}-', topics, group))
    try:
        second.wait('the same command resumes to the exact graph', lambda: own_graph(stack) == (nodes_big, rels_big), 240)
        end_offsets = ends(stack, topics)
        second.wait('committed offsets at the log ends', lambda: committed(stack, group, topics) == end_offsets, 120)
    finally:
        out2 = second.finish()
    stopped_cleanly(stack, docker_client, out2, second.cli, STREAM_COMPONENTS, exit_code=0)
    assert own_graph(stack) == (nodes_big, rels_big)
    (run / 'logs' / 'summary.json').write_text(json.dumps({'in_flight': in_flight, 'after_stop': len(partial)}) + '\n')
    clear(stack)


def planned_drive(stack, client, registry, config, run, name, topics, group, *, outage):
    """The S1 planned-bulk run, optionally with Neo4j stopped once the dispatcher says the edge phase is ready. No query window:
    a Neo4j restart starts a new query-ID epoch."""
    prefix = f'e09-{stack.owner}-'
    (run / 'runs').mkdir(parents=True, exist_ok=True)
    (run / 'logs').mkdir(exist_ok=True)
    (run / 'schema.yaml').write_text(fx.schema(prefix, topics, group))
    observer = AppObserver(client, registry, stack.network.id, owner=stack.owner,
                           candidate_images={row['id'] for row in config['images'].values()}, barrier_tag=f'{run.name}-{name}-')
    observer.start()
    known = lb.attempt_dirs(run)
    box, logs, note = {}, {}, {}
    env = cli_environment(stack, config['images'], config['docker_host'])
    plan_args = ('--plan-chunk-rows', '300', '--plan-writers', '2')

    def target():
        try:
            box['result'] = run_cli(Path(config['venv_bin']) / 'graph-loader',
                                    ['start', '--mode', 'bulk', '--config', str(run / 'schema.yaml'), '--network', stack.network.name,
                                     '--skip-image-build', '--run-dir', str(run / 'runs'), *plan_args],
                                    env=env, cwd=run, log_dir=run / 'logs', name=name, timeout=lb.CLI_TIMEOUT, redact=[stack.password])
        except BaseException as error:
            box['error'] = repr(error)

    def stop_neo4j():
        note['stop'] = stack.stop_neo4j(timeout=30)
        note['stopped_at'] = time.monotonic()

    thread, stopper = threading.Thread(target=target), None
    started = time.monotonic()
    thread.start()
    try:
        while thread.is_alive():
            lb.poll_containers(client, observer, logs, only='planned-dispatcher' if outage else None)
            dispatcher = next((text for n, text in logs.items() if 'planned-dispatcher' in n), '')
            if outage and stopper is None and 'stage=planned-ready' in dispatcher:
                note['done_before'] = len(re.findall(r'stage=planned-done', dispatcher))
                stopper = threading.Thread(target=stop_neo4j)
                stopper.start()
            time.sleep(0.02 if outage else 0.2)
    finally:
        thread.join(timeout=lb.CLI_TIMEOUT + 60)
        if stopper is not None:
            stopper.join(timeout=60)
    assert not thread.is_alive(), 'the CLI thread outlived its deadline'
    if outage:
        stack.start_neo4j()
    lb.poll_containers(client, observer, logs)
    try:
        observer.barrier()
        observer.drain()
    finally:
        observer.stop()
    snapshot = observer.snapshot()
    leftovers = observer.reap()
    (run / 'logs' / f'{name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
    for container_name, text in logs.items():
        (run / 'logs' / f'{name}-container-{container_name}.log').write_text(text)
    assert 'error' not in box, box.get('error')
    result = box['result']
    new = lb.attempt_dirs(run) - known
    assert len(new) == 1
    return {'result': result, 'stderr': result.stderr.read_text() + result.stdout.read_text(), 'snapshot': snapshot, 'leftovers': leftovers,
            'kill': {}, 'records': [], 'capture': None, 'elapsed': time.monotonic() - started, 'attempt': new.pop(), 'note': note}


@pytest.mark.parametrize('repeat', range(2))
def test_s5_planned_bulk_fails_closed_when_neo4j_stops_and_a_replay_converges(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's5-planned-{repeat}'
    run.mkdir()
    lb.clear(stack)
    scale = lb.LARGE
    topics = lb.fresh_topics(stack, f'po{repeat}', scale)
    group = f'e09-{stack.owner}-po{repeat}-group'
    total = 2 * scale[0]
    first = planned_drive(stack, docker_client, registry, service_config, run, 'start-1', topics, group, outage=True)
    assert first['note'].get('stop') == 'exited', first['note']
    result = first['result']
    assert result.timed_out is False and result.exit_code not in (0, None), 'the planned run must fail closed when Neo4j stops'
    assert result.redactions == 0
    snapshot = first['snapshot']
    assert snapshot['errors'] == [] and first['leftovers'] == {'removed': [], 'failed': []}
    refusals_only_the_restarted_stack(stack, snapshot)
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids]
    assert not (first['attempt'] / 'plans' / 'edge-plan.json.ledger.json').exists(), 'a failed attempt persisted a ledger'
    nodes, edges = lb.own_graph(stack)
    assert nodes == fx.expected_nodes(*scale)
    assert set(edges) <= set(fx.expected_relationships(*scale)) and len(edges) == len(set(edges)) and len(edges) < total
    lb.committed(stack, group, topics, {'worker': scale[0], 'employer': scale[1]})

    second = planned_drive(stack, docker_client, registry, service_config, run, 'start-2', topics, group, outage=False)
    lb.check_cleanup(stack, docker_client, second)
    plan, ledger, stats = lb.assert_completed_run(stack, second, total, scale)
    assert stats['state'] == 'complete'
    (run / 'logs' / 'summary.json').write_text(json.dumps({'edges_after_outage': len(edges), 'chunks': ledger['chunks'],
                                                          'done_before_stop': first['note'].get('done_before')}) + '\n')
    lb.clear(stack)


def declare_open_writes(stack):
    """Neo4j never logs the end of a query whose client was killed mid-write. Declare exactly such leftovers (and only those: node or
    relationship write statements) so later strict query-log captures in the shared session accept them and still reject anything else."""
    capture = qw.capture(stack.neo, stack.marker, allow_open=True)
    started = {r['query_id']: ' '.join(r['query'].split()) for r in capture.records if r['phase'] == 'started'}
    new = [i for i in capture.open_ids if (capture.epochs, i) not in qw.ORPHANS]
    for query_id in new:
        assert started[query_id].startswith(WRITE_PREFIXES), ('an unexpected query was left open', started[query_id][:200])
    qw.declare_orphans(capture)
    return len(new)


def running_components(observer, name):
    with observer._lock:
        rows = [(cid, row.get('inspected') or {}) for cid, row in observer.containers.items() if cid not in observer.ended]
    return [(cid, info['name']) for cid, info in rows if info and info['labels'].get('component') == name]


def kill_when_in_flight(stack, component):
    """Watcher for ``bulk_run``: SIGKILL one container of ``component`` as soon as some, but not all, relationships exist."""
    state = {}

    def watch(observer, _logs):
        if 'killed' in state:
            return
        count = count_relationships(stack)
        if 0 < count < TOTAL_BIG:
            targets = running_components(observer, component)
            if targets:
                state.update(count=count, target=targets[0][1], killed=observer.kill_observed(targets[0][0], timeout=60))
    watch.state = state
    return watch


def bulk_run(stack, client, registry, config, run, name, schema_text, args, *, watcher=None, deadline=300, poll_every=1.0):
    """Installed ``graph-loader start --mode bulk ARGS`` under an observer; ``watcher(observer, logs)`` runs every 50 ms. No query
    window (a killed loader can leave queries open and these runs share the lane's log). Container logs are sampled every
    ``poll_every`` seconds and only the newest copy is kept: a run whose containers live about two seconds needs a short interval."""
    (run / 'runs').mkdir(parents=True, exist_ok=True)
    (run / 'logs').mkdir(exist_ok=True)
    (run / f'{name}.yaml').write_text(schema_text)
    known = lb.attempt_dirs(run)
    observer = AppObserver(client, registry, stack.network.id, owner=stack.owner,
                           candidate_images={row['id'] for row in config['images'].values()}, barrier_tag=f'{run.name}-{name}-')
    observer.start()
    box, logs = {}, {}
    env = cli_environment(stack, config['images'], config['docker_host'])

    def target():
        try:
            box['result'] = run_cli(Path(config['venv_bin']) / 'graph-loader',
                                    ['start', '--mode', 'bulk', '--config', str(run / f'{name}.yaml'), '--network', stack.network.name,
                                     '--skip-image-build', '--run-dir', str(run / 'runs'), *args],
                                    env=env, cwd=run, log_dir=run / 'logs', name=name, timeout=deadline, redact=[stack.password])
        except BaseException as error:
            box['error'] = repr(error)

    thread = threading.Thread(target=target)
    started, last_poll = time.monotonic(), 0.0
    thread.start()
    try:
        while thread.is_alive():
            if watcher is not None:
                watcher(observer, logs)
            if time.monotonic() - last_poll > poll_every:
                lb.poll_containers(client, observer, logs)
                last_poll = time.monotonic()
            time.sleep(0.05)
    finally:
        thread.join(timeout=deadline + 60)
    assert not thread.is_alive(), 'the CLI thread outlived its deadline'
    lb.poll_containers(client, observer, logs)
    try:
        observer.barrier()
        observer.drain()
    finally:
        observer.stop()
    snapshot = observer.snapshot()
    leftovers = observer.reap()
    (run / 'logs' / f'{name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
    for container_name, text in logs.items():
        (run / 'logs' / f'{name}-container-{container_name}.log').write_text(text)
    assert 'error' not in box, box.get('error')
    result = box['result']
    new = lb.attempt_dirs(run) - known
    assert len(new) == 1
    return {'result': result, 'stderr': result.stderr.read_text() + result.stdout.read_text(), 'snapshot': snapshot,
            'leftovers': leftovers, 'elapsed': time.monotonic() - started, 'attempt': new.pop(), 'schema': run / f'{name}.yaml'}


def bulk_cleanup(stack, docker_client, out, components=None):
    result = out['result']
    assert result.timed_out is False and result.redactions == 0 and out['elapsed'] < 360
    snapshot = out['snapshot']
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids], 'a container outlived the run'
    seen = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info and not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        assert all('docker.sock' not in source for _, source, _, _ in info['mounts'])
        seen[info['labels']['component']] = seen.get(info['labels']['component'], 0) + 1
    if components is not None:
        assert seen == components, seen
    return seen
