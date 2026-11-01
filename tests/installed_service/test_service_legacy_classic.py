"""E09.F1.S3 classic bulk (``--edge-classic``) on owned Neo4j/Kafka (native shared Docker Engine substrate).

Classic is the per-type APOC-locked loader and the one bulk mode that can wait on Neo4j locks. Installed CLI, installed images:

* default overlap on the small fixture: exact graph, the classic mode line and a succeeded outcome per type, node groups and
  the run-scoped relationship groups committed at the log ends, components, cleanup;
* ``execution.mode: native_disjoint`` (only classic can run it): a plain ``start`` with such a type is refused before any
  container launches, and under ``--edge-classic`` it fails closed because it needs Cypher 25 and the Neo4j 5.26 target has none;
* ``--bulk-edge-overlap shared`` on a larger fixture with several write sessions per type and four partitions: exact graph,
  offsets, cleanup, and database lock waits actually observed in the query log (repeated 3 times, because whether two
  transactions collide is timing): planned bulk (S1) and the lease modes (S1, S2) show none on the same detector.

Not covered: precedence, DLQ in classic (S4), failure and restart (S5), scale, arm64.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import threading
import time

import pytest

from tests.installed_service import query_window as qw
from tests.installed_service import stream_fixture as sf
from tests.installed_service.live_cli import AppObserver, cli_environment, run_cli
from tests.installed_service.inspect_strict import list_groups
from tests.installed_service.observers import committed_offsets, graph_observations

pytestmark = pytest.mark.integration


def drive(stack, client, registry, config, run, name, topics, group, *, args, tuning, deadline=420, classic=True):
    (run / 'runs').mkdir(parents=True, exist_ok=True)
    (run / 'logs').mkdir(exist_ok=True)
    (run / 'schema.yaml').write_text(sf.schema(f'e09-{stack.owner}-', topics, group, bulk=tuning))
    before = qw.capture(stack.neo, stack.marker)
    observer = AppObserver(client, registry, stack.network.id, owner=stack.owner,
                           candidate_images={row['id'] for row in config['images'].values()},
                           barrier_tag=f'{run.name}-{name}-')
    observer.start()
    box, logs = {}, {}
    env = cli_environment(stack, config['images'], config['docker_host'])

    def target():
        try:
            box['result'] = run_cli(Path(config['venv_bin']) / 'graph-loader',
                                    ['start', '--mode', 'bulk', *(['--edge-classic'] if classic else []), '--config', str(run / 'schema.yaml'),
                                     '--network', stack.network.name, '--skip-image-build', '--run-dir', str(run / 'runs'), *args],
                                    env=env, cwd=run, log_dir=run / 'logs', name=name, timeout=deadline, redact=[stack.password])
        except BaseException as error:
            box['error'] = repr(error)

    thread = threading.Thread(target=target)
    started = time.monotonic()
    thread.start()
    try:
        while thread.is_alive():
            with observer._lock:
                rows = list(observer.containers.items())
            for cid, row in rows:
                info = row.get('inspected') or {}
                cname = info.get('name') or row.get('event_name') or cid[:12]
                try:
                    logs[cname] = client.api.logs(cid, tail=300, stdout=True, stderr=True).decode(errors='replace')
                except Exception as error:
                    logs.setdefault(cname, f'<unreadable: {type(error).__name__}>')
            time.sleep(0.5)
    finally:
        thread.join(timeout=deadline + 60)
    assert not thread.is_alive(), 'the CLI thread outlived its deadline'
    try:
        observer.barrier()
        observer.drain()
    finally:
        observer.stop()
    snapshot = observer.snapshot()
    leftovers = observer.reap()
    (run / 'logs' / f'{name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
    for cname, text in logs.items():
        (run / 'logs' / f'{name}-container-{cname}.log').write_text(text)
    after = qw.capture(stack.neo, stack.marker)
    (run / 'logs' / f'{name}-query-log-delta.log').write_bytes(after.content[len(before.content):])
    records = qw.window(before, after)
    return {'result': box.get('result'), 'error': box.get('error'), 'snapshot': snapshot, 'leftovers': leftovers,
            'records': records, 'elapsed': time.monotonic() - started}


def own_graph(stack):
    return sf.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Citizen|Firm) DETACH DELETE n').consume()


def topics_for(stack, tag, partitions=1):
    prefix = f'e09-{stack.owner}-{tag}-'
    topics = {'citizen': prefix + 'citizen', 'firm': prefix + 'firm', **{kind: prefix + kind.lower() for kind in sf.TYPES}}
    stack.create_topics(list(topics.values()), partitions=partitions)
    return topics


COMPONENTS = {'preflight': 1, 'node-loader': 2, 'edge-loader': 3}
EDGE_WRITE = 'UNWIND range(0, size($rows) - 1) AS row_index'


def edge_writes(out):
    return [r for r in out['records'] if r['phase'] == 'completed' and ' '.join(r['query'].split()).startswith(EDGE_WRITE)]


def check_run(stack, docker_client, out, batch, topics, group, partitions):
    result = out['result']
    assert out['error'] is None and result.timed_out is False and result.exit_code == 0, (out['error'], result.tail)
    assert result.redactions == 0, 'the scratch password reached CLI output'
    snapshot = out['snapshot']
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids]
    components = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info and not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        assert all('docker.sock' not in source for _, source, _, _ in info['mounts'])
        components[info['labels']['component']] = components.get(info['labels']['component'], 0) + 1
    assert components == COMPONENTS
    text = result.stdout.read_text() + result.stderr.read_text()
    assert 'stage=launch edge_mode=classic reason=flag' in text
    for kind in sf.TYPES:
        assert re.search(rf'stage=bulk-edge-complete edge={kind} elapsed_s=[\d.]+ outcome=succeeded', text), kind
    assert own_graph(stack) == sf.expected([batch]), 'the classic graph differs from the fixture'
    assert not [r for r in out['records'] if r['phase'] == 'failed']

    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'
    names = list_groups(stack)
    for label, key in (('Citizen', 'citizen'), ('Firm', 'firm')):
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=f'{group}-{label}',
                                 partitions=[(topics[key], 0)], broker_guard=running)
        assert rows[0]['offset'] == stack.end_offsets(topics[key])[0], label
    for kind in sf.TYPES:
        matching = [n for n in names if n.startswith(f'{group}-{kind}-')]
        assert len(matching) == 1, (kind, matching)
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=matching[0],
                                 partitions=[(topics[kind], p) for p in range(partitions)], broker_guard=running)
        ends = stack.end_offsets(topics[kind], partitions=partitions)
        assert {row['partition']: row['offset'] for row in rows} == ends, (kind, rows, ends)
    return text


def test_s3_classic_default_overlap_loads_the_exact_graph(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 's3-labels'
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, 'l')
    group = f'e09-{stack.owner}-l-group'
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    sf.produce_edges(stack, topics, 'a')
    out = drive(stack, docker_client, registry, service_config, run, 'start', topics, group, args=[],
                tuning={'worker_count': 2, 'batch_size': 50})
    check_run(stack, docker_client, out, 'a', topics, group, 1)
    assert edge_writes(out), 'no classic relationship write was identified in the query window'
    clear(stack)


@pytest.mark.parametrize('repeat', range(3))
def test_s3_classic_shared_overlap_waits_on_database_locks(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's3-shared-{repeat}'
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, f's{repeat}', partitions=4)
    group = f'e09-{stack.owner}-s{repeat}-group'
    sf.produce_nodes(stack, topics, 'big', with_firms=True)
    sf.produce_edges(stack, topics, 'big')
    out = drive(stack, docker_client, registry, service_config, run, 'start', topics, group,
                args=['--bulk-edge-overlap', 'shared', '--max-concurrent-bulk-edges', '3'],
                tuning={'worker_count': 4, 'batch_size': 100})
    check_run(stack, docker_client, out, 'big', topics, group, 4)
    writes = edge_writes(out)
    waited = [r['waiting_ms'] for r in writes if r['waiting_ms'] > 0]
    (run / 'logs' / 'summary.json').write_text(json.dumps({'edge_writes': len(writes), 'waited': len(waited),
                                                          'max_wait_ms': max(waited, default=0)}) + '\n')
    assert len(writes) >= 30
    assert waited, 'classic shared overlap with concurrent sessions showed no database lock wait'
    clear(stack)


def test_s3_native_disjoint_is_refused_by_a_plain_start_and_fails_closed_on_neo4j_5_26(docker_client, registry, stack, service_config):
    """``native_disjoint`` only runs under ``--edge-classic`` and needs Cypher 25, which the Neo4j 5.26 target does not have."""
    run = Path(service_config['run_dir']) / 's3-native'
    run.mkdir()
    clear(stack)
    topics = topics_for(stack, 'nd')
    group = f'e09-{stack.owner}-nd-group'
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    sf.produce_edges(stack, topics, 'a')
    tuning = {'worker_count': 2, 'batch_size': 50, 'mode': 'native_disjoint'}
    refused = drive(stack, docker_client, registry, service_config, run, 'plain', topics, group, args=[], tuning=tuning, classic=False)
    result = refused['result']
    text = result.stdout.read_text() + result.stderr.read_text()
    assert result.exit_code not in (0, None) and result.timed_out is False
    assert 'native_disjoint: it needs a Neo4j server with the Cypher 25 dialect and is not supported on Neo4j 5.26' in text
    assert refused['snapshot']['containers'] == {} and refused['leftovers'] == {'removed': [], 'failed': []}
    assert own_graph(stack) == ([], []), 'a refused start must write nothing'

    out = drive(stack, docker_client, registry, service_config, run, 'classic', topics, group, args=[], tuning=tuning)
    result = out['result']
    text = result.stdout.read_text() + result.stderr.read_text()
    assert out['error'] is None and result.timed_out is False and result.exit_code not in (0, None)
    assert 'Execution mode native_disjoint requires Cypher 25; server=Neo4j/5.26' in text
    assert 'stage=launch edge_mode=classic reason=flag' in text and result.redactions == 0
    snapshot = out['snapshot']
    assert snapshot['errors'] == [] and snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids]
    nodes, rels = own_graph(stack)
    assert rels == [] and nodes == sf.expected(['a'])[0], 'the nodes load, no relationship may be written'
    # The native writer probes for Cypher 25; Neo4j 5.26 rejects the probe before running it (logged once at ERROR).
    assert {r['query'] for r in out['records'] if r['phase'] == 'rejected'} == {'CYPHER 25 RETURN 1 AS supported'}
    assert not edge_writes(out)
    clear(stack)
