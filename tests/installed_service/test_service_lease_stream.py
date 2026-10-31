"""E09.F1.S2 streaming leases on owned Neo4j/Kafka (native shared Docker Engine substrate).

Installed ``graph-loader start --mode stream`` (the default bucket-lease fleet) with the installed candidate images:

* nodes first, then relationships published while the fleet runs: exact graph, stable consumer-group offsets at the log
  ends, zero database lock waits on every node and relationship write (positive control on the same detector), the lease
  server's own counters read as a different quantity;
* graceful SIGTERM stop (exit 0, every container removed, offsets and graph unchanged) and resume from the stable groups: a
  relationship tampered with between the runs keeps its value (committed records are not read again) while a batch published
  during the stop is loaded, nodes and relationships together;
* relationships published with no nodes and no dead-letter queue fail closed (exit 1, documented message, nothing written).

Not covered: lease-server or writer loss, Neo4j outage (S5), dead-letter/retry (S4), scale, replicas > 1, the deprecated
clock/rotation path, arm64. The second start publishes nodes and edges together while stopped, which races the node loaders
against the edge loaders; it is repeated.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import time

import pytest

from tests.installed_service import query_window as qw
from tests.installed_service import stream_fixture as sf
from tests.installed_service.lockwait import lock_conflict
from tests.installed_service.live_cli import AppObserver, RunningCli, cli_environment
from tests.installed_service.observers import committed_offsets, graph_observations

pytestmark = pytest.mark.integration


class Stream:
    """One running installed ``graph-loader start --mode stream`` under an observer."""

    def __init__(self, stack, client, registry, config, run, name, topics, group, *, extra=(), deadline=900):
        (run / 'logs').mkdir(parents=True, exist_ok=True)
        (run / 'schema.yaml').write_text(sf.schema(f'e09-{stack.owner}-', topics, group))
        self.stack, self.client, self.run, self.name = stack, client, run, name
        self.before = qw.capture(stack.neo, stack.marker)
        self.observer = AppObserver(client, registry, stack.network.id, owner=stack.owner,
                                    candidate_images={row['id'] for row in config['images'].values()},
                                    barrier_tag=f'{run.name}-{name}-')
        self.observer.start()
        self.logs = {}
        env = cli_environment(stack, config['images'], config['docker_host'])
        self.cli = RunningCli(Path(config['venv_bin']) / 'graph-loader',
                              ['start', '--mode', 'stream', '--config', str(run / 'schema.yaml'), '--network', stack.network.name,
                               '--skip-image-build', '--run-dir', str(run / 'runs'), *extra],
                              env=env, cwd=run, log_dir=run / 'logs', name=name, deadline=deadline, redact=[stack.password])

    def poll_logs(self):
        with self.observer._lock:
            rows = list(self.observer.containers.items())
        for cid, row in rows:
            info = row.get('inspected') or {}
            name = info.get('name') or row.get('event_name') or cid[:12]
            try:
                self.logs[name] = self.client.api.logs(cid, tail=300, stdout=True, stderr=True).decode(errors='replace')
            except Exception as error:
                self.logs.setdefault(name, f'<unreadable: {type(error).__name__}>')

    def wait(self, what, predicate, timeout):
        end = time.monotonic() + timeout
        began = time.monotonic()
        while True:
            assert self.cli.running(), f'the streaming CLI exited while waiting for {what}: {self.cli.text()[-1500:]}'
            value = predicate()
            if value:
                return time.monotonic() - began
            assert time.monotonic() < end, f'timed out waiting for {what}'
            self.poll_logs()
            time.sleep(0.5)

    def finish(self):
        code = self.cli.terminate(timeout=90)
        self.poll_logs()
        try:
            self.observer.barrier()
            self.observer.drain()
        finally:
            self.observer.stop()
        snapshot = self.observer.snapshot()
        leftovers = self.observer.reap()
        (self.run / 'logs' / f'{self.name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
        for container, text in self.logs.items():
            (self.run / 'logs' / f'{self.name}-container-{container}.log').write_text(text)
        after = qw.capture(self.stack.neo, self.stack.marker)
        (self.run / 'logs' / f'{self.name}-query-log-delta.log').write_bytes(after.content[len(self.before.content):])
        records = qw.window(self.before, after)
        return {'exit_code': code, 'stop_seconds': self.cli.stop_seconds, 'snapshot': snapshot, 'leftovers': leftovers,
                'records': records}


def own_graph(stack):
    return sf.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


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
    for kind, key, topic in [('Citizen', 'citizen', topics['citizen']), ('Firm', 'firm', topics['firm'])] + \
            [(t, t, topics[t]) for t in sf.TYPES]:
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=f'{group}-{kind}',
                                 partitions=[(topic, 0)], broker_guard=running)
        result[key] = rows[0]['offset']
    return result


def ends(stack, topics):
    return {key: stack.end_offsets(topic)[0] for key, topic in topics.items()}


COMPONENTS = {'preflight': 1, 'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': 3}
EDGE_WRITE = 'UNWIND range(0, size($rows) - 1) AS row_index'
NODE_WRITE = 'UNWIND $batch AS record MERGE (n:'


def check_stopped(stack, docker_client, out, cli):
    snapshot = out['snapshot']
    assert out['exit_code'] == 0, ('SIGTERM must end a supervising stream run with exit 0', cli.text()[-1500:])
    assert out['stop_seconds'] < 30
    assert cli.redactions == 0, 'the scratch password reached CLI output'
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids], 'a container outlived the stop'
    components = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info, 'an application container disappeared before it could be inspected'
        assert not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        assert all('docker.sock' not in source for _, source, _, _ in info['mounts'])
        components[info['labels']['component']] = components.get(info['labels']['component'], 0) + 1
    assert components == COMPONENTS
    text = cli.text()
    assert 'stage=isolation' not in text, 'a shared or self-referencing type ran in an isolated phase'
    return text


def check_no_lock_waits(out):
    def kind(record):
        text = ' '.join(record['query'].split())
        return 'edge' if text.startswith(EDGE_WRITE) else 'node' if text.startswith(NODE_WRITE) else None
    writes = [r for r in out['records'] if r['phase'] == 'completed' and kind(r)]
    counts = {name: len([r for r in writes if kind(r) == name]) for name in ('edge', 'node')}
    assert counts['edge'] and counts['node'], 'no node or relationship write was identified in the query window'
    assert all(r['waiting_ms'] == 0 for r in writes), 'a leased write waited for a database lock'
    assert not [r for r in out['records'] if r['phase'] == 'failed']
    return counts


def lease_stats(text):
    found = re.findall(r'stage=lease-server-stats generation=\w+ connections=(\d+) grants=(\d+) extensions=(\d+) waited_grants=(\d+) '
                       r'denials=(\d+) wait_timeouts=(\d+) releases=(\d+) disconnect_releases=(\d+) active=(\d+) '
                       r'max_concurrent=(\d+) max_waiters=(\d+) total_wait_ms=([\d.]+)', text)
    assert found, 'no lease-server stats line'
    keys = ('connections', 'grants', 'extensions', 'waited_grants', 'denials', 'wait_timeouts', 'releases', 'disconnect_releases',
            'active', 'max_concurrent', 'max_waiters', 'total_wait_ms')
    return dict(zip(keys, (float(v) if k == 'total_wait_ms' else int(v) for k, v in zip(keys, found[-1]))))


def positive_control(stack):
    before = qw.capture(stack.neo, stack.marker)
    contended = lock_conflict(stack, 700)
    positive = qw.waits(qw.window(before, qw.capture(stack.neo, stack.marker)), query=contended, database=stack.database,
                        user='neo4j', expected=1)
    assert positive[0] >= 500, positive
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:LockProbe) DELETE n').consume()


def tampered(rels):
    old = json.dumps({'source_labels': ['Citizen'], 'source': {'id': 'c1', 'name': 'c1'}, 'type': 'WORKS_FOR',
                      'properties': {'kind': 'works'}, 'target_labels': ['Firm'], 'target': {'id': 'f1', 'name': 'f1'}}, sort_keys=True)
    assert old in rels
    return sorted([r for r in rels if r != old] + [old.replace('"kind": "works"', '"kind": "tampered"')])


@pytest.mark.parametrize('repeat', range(3))
def test_s2_stream_leases_load_stop_and_resume(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's2-cycle-{repeat}'
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, f'c{repeat}')
    group = f'e09-{stack.owner}-c{repeat}-group'
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    nodes_a, rels_a = sf.expected(['a'])
    first = Stream(stack, docker_client, registry, service_config, run, 'start-1', topics, group)
    try:
        first.wait('nodes', lambda: own_graph(stack)[0] == nodes_a, 120)
        sf.produce_edges(stack, topics, 'a')
        first.wait('relationships', lambda: own_graph(stack) == (nodes_a, rels_a), 120)
        end_a = ends(stack, topics)
        assert end_a == {'citizen': 24, 'firm': 6, 'WORKS_FOR': 24, 'BUYS_FROM': 24, 'KNOWS_OF': 24}
        first.wait('committed offsets at the log ends', lambda: committed(stack, group, topics) == end_a, 90)
        assert first.cli.running(), 'the supervising CLI must keep running after the load'
        with stack.driver.session(database=stack.database) as session:
            session.run("MATCH (:Citizen {id: 'c1'})-[r:WORKS_FOR]->() SET r.kind = 'tampered'").consume()
    finally:
        out1 = first.finish()
    text1 = check_stopped(stack, docker_client, out1, first.cli)
    writes1 = check_no_lock_waits(out1)
    stats1 = lease_stats(text1)
    assert stats1['grants'] > 0 and stats1['releases'] == stats1['grants'] and stats1['denials'] == 0 and stats1['active'] == 0
    assert committed(stack, group, topics) == end_a, 'the stable groups must keep their offsets across the stop'
    assert own_graph(stack) == (nodes_a, tampered(rels_a)), 'the graph changed across the stop'

    sf.produce_nodes(stack, topics, 'b', with_firms=False)
    sf.produce_edges(stack, topics, 'b')
    nodes_ab, rels_ab = sf.expected(['a', 'b'])
    final_rels = tampered(rels_ab)
    second = Stream(stack, docker_client, registry, service_config, run, 'start-2', topics, group)
    try:
        second.wait('the batch published during the stop', lambda: own_graph(stack) == (nodes_ab, final_rels), 150)
        end_b = ends(stack, topics)
        assert end_b == {'citizen': 36, 'firm': 6, 'WORKS_FOR': 36, 'BUYS_FROM': 36, 'KNOWS_OF': 36}
        second.wait('committed offsets at the new log ends', lambda: committed(stack, group, topics) == end_b, 90)
        assert second.cli.running()
    finally:
        out2 = second.finish()
    text2 = check_stopped(stack, docker_client, out2, second.cli)
    writes2 = check_no_lock_waits(out2)
    stats2 = lease_stats(text2)
    assert stats2['grants'] > 0 and stats2['releases'] == stats2['grants'] and stats2['denials'] == 0 and stats2['active'] == 0
    assert committed(stack, group, topics) == end_b
    assert own_graph(stack) == (nodes_ab, final_rels), 'a relationship tampered before the restart was re-read and overwritten, or the graph differs'
    (run / 'logs' / 'summary.json').write_text(json.dumps({'writes': [writes1, writes2], 'lease_stats': [stats1, stats2],
                                                          'stop_seconds': [out1['stop_seconds'], out2['stop_seconds']]}, indent=1) + '\n')
    positive_control(stack)
    clear(stack)


def test_s2_relationships_before_nodes_without_dead_letter_fail_closed(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 's2-edges-first'
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, 'n')
    group = f'e09-{stack.owner}-n-group'
    sf.produce_edges(stack, topics, 'a')
    stream = Stream(stack, docker_client, registry, service_config, run, 'start', topics, group, deadline=240)
    began = time.monotonic()
    try:
        while stream.cli.running() and time.monotonic() - began < 90:
            stream.poll_logs()
            time.sleep(0.5)
        exited = not stream.cli.running()
        code = stream.cli.process.poll()
        elapsed = time.monotonic() - began
    finally:
        out = stream.finish()
    text = stream.cli.text()
    assert exited and code == 1, ('the fleet must stop itself with exit 1', code, elapsed)
    assert 'did not resolve exactly one source and target endpoint' in text
    assert stream.cli.redactions == 0
    assert own_graph(stack) == ([], []), 'a relationship was written without its endpoints'
    snapshot = out['snapshot']
    assert snapshot['errors'] == [] and snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids]
    clear(stack)
