"""E09.F1.S7 live harness checks on owned Neo4j/Kafka (native shared Docker Engine substrate).

These tests prove the harness (owned stack, candidate image identity, installed
package provenance, installed CLI invocation, observers, outage controls and the
lock-wait detector). They are not planned-bulk, streaming, classic, DLQ or
failure-mode evidence; those stories add their own scenarios.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time

import psutil
import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import neo4j_graph_loader
from tests.installed_service import query_window as qw
from tests.installed_service.lockwait import lock_conflict
from tests.installed_service.live_cli import AppObserver, cli_environment, run_cli
from tests.installed_service.live_stack import FOREIGN_PORTS, OWNER_LABEL, owned_name
from tests.installed_service.observers import committed_offsets, graph_observations, plan_ledger
from tests.installed_service.ownership import labelled

pytestmark = pytest.mark.integration

ROLES = ('cli', 'node', 'edge', 'clock')
PROBE = '''
import hashlib, importlib.util, json, pathlib
spec = importlib.util.find_spec("neo4j_graph_loader")
root = pathlib.Path(spec.origin).parent
files = {p.relative_to(root.parent).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
         for p in sorted(root.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}
print(json.dumps({"origin": spec.origin, "files": files}))
'''


def run_owned_container(client, registry, owner, role, image, command, *, network=None, entrypoint=None, timeout=60):
    """Create, run to exit, read bounded stdout, and remove one owned helper container by ID."""
    name = owned_name(owner, role)
    registry.write('intent', resource='container', name=name)
    container = client.containers.create(image, name=name, command=command, entrypoint=entrypoint,
                                         labels={OWNER_LABEL: owner}, network=network, network_mode=None if network else 'none',
                                         read_only=True)
    registry.write('created', resource='container', name=name, id=container.id)
    try:
        container.start()
        status = container.wait(timeout=timeout)
        output = container.logs(stdout=True, stderr=False)
        assert len(output) <= 1024 * 1024, 'helper output exceeds its bound'
        return status['StatusCode'], output
    finally:
        container.reload()
        if not labelled(container.attrs['Config'].get('Labels'), owner):
            raise RuntimeError('refusing to remove a container without the run owner label')
        container.remove(force=True, v=True)
        registry.write('removed', resource='container', id=container.id)


def test_h1_owned_stack_and_settings(stack, docker_client):
    facts = stack.facts()
    assert facts['neo4j']['version'].startswith('5.26.') and facts['neo4j']['edition'].lower() == 'enterprise'
    assert facts['neo4j']['apoc'] == ['apoc.lock.nodes', 'apoc.util.sleep']
    assert facts['neo4j']['query_log'] == {'db.logs.query.enabled': 'VERBOSE', 'db.logs.query.threshold': '0s',
                                           'db.logs.query.parameter_logging_enabled': 'false'}
    assert facts['kafka_release'].startswith('7.7.0')
    assert stack.database not in ('neo4j', 'system') and stack.database.startswith('e09')
    for container in (stack.neo, stack.kafka):
        container.reload()
        labels = container.attrs['Config']['Labels']
        assert labels[OWNER_LABEL] == stack.owner and container.name.startswith(f'e09-{stack.owner}-')
        ports = [b for bindings in container.attrs['NetworkSettings']['Ports'].values() for b in bindings or []]
        assert ports and all(b['HostIp'] == '127.0.0.1' and int(b['HostPort']) not in FOREIGN_PORTS for b in ports)
        assert list(container.attrs['NetworkSettings']['Networks']) == [stack.network.name]
    assert stack.container_reachable(stack.neo, stack.kafka_name, 9092)
    assert stack.container_reachable(stack.kafka, stack.neo_name, 7687)
    with stack.driver.session(database='system') as session:
        databases = {row['name'] for row in session.run('SHOW DATABASES YIELD name RETURN name')}
    assert stack.database in databases


def test_h2_candidate_image_identity(docker_client, service_config):
    images = service_config['images']
    assert sorted(images) == sorted(ROLES) and len({row['id'] for row in images.values()}) == 4
    for role, row in images.items():
        image = docker_client.images.get(row['id'])
        labels = image.attrs['Config']['Labels']
        assert image.id == row['id'] == image.attrs['Id']
        assert labels['io.graph-loader.component'] == role and labels['io.graph-loader.image-contract'] == '1'
        assert labels['org.opencontainers.image.version'] == neo4j_graph_loader.__version__
        assert f"{image.attrs['Os']}/{image.attrs['Architecture']}" == row['platform']
        assert row['tag'] in image.tags


def test_h3_installed_package_provenance(docker_client, registry, stack, service_config):
    payload = service_config['payload']
    site = Path(neo4j_graph_loader.__file__).resolve().parents[1]
    assert site.is_relative_to(Path(service_config['venv_bin']).parent), 'package is not from the target environment'
    installed = {p.relative_to(site).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted((site / 'neo4j_graph_loader').rglob('*')) if p.is_file() and '__pycache__' not in p.parts}
    assert installed == payload
    for role, row in service_config['images'].items():
        status, output = run_owned_container(docker_client, registry, stack.owner, f'probe-{role}', row['id'], ['-I', '-c', PROBE],
                                             entrypoint=['python'])
        assert status == 0
        probe = json.loads(output)
        assert probe['origin'].startswith('/usr/local/lib/python3.12/site-packages/neo4j_graph_loader/')
        assert probe['files'] == payload, role


EXPECTED_COMPONENTS = {'preflight': 1, 'node-loader': 2, 'planned-dispatcher': 1, 'planned-edge-writer': 1}


def test_h4_installed_cli_tiny_planned_bulk(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 'h4'
    (run / 'runs').mkdir(parents=True)
    (run / 'logs').mkdir()
    prefix = f'e09-{stack.owner}-'
    topics = {name: prefix + name for name in ('person', 'company', 'works-at')}
    stack.create_topics(list(topics.values()))
    schema = f'''loading:
  mode: bulk
  consumer_group_id: {prefix}group
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/{prefix}nodes.jsonl
nodes:
  - label: Person
    topic: {topics['person']}
    key_property: id
    properties:
      id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
  - label: Company
    topic: {topics['company']}
    key_property: id
    properties:
      id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
edges:
  - type: WORKS_AT
    topic: {topics['works-at']}
    nodes: {{source: Person, target: Company}}
    source_key_property: id
    target_key_property: id
    properties:
      role: {{type: string}}
'''
    (run / 'schema.yaml').write_text(schema)
    people = [(1, 'p1'), (2, 'p2'), (3, 'p3')]
    companies = [('a', 'ca'), ('b', 'cb')]
    works = [(1, 'a', 'r1'), (2, 'a', 'r2'), (3, 'b', 'r3')]
    stack.produce(topics['person'], [(i, {'id': i, 'name': n}) for i, n in people])
    stack.produce(topics['company'], [(i, {'id': i, 'name': n}) for i, n in companies])
    stack.produce(topics['works-at'], [(p, {'source': {'id': p}, 'target': {'id': c}, 'properties': {'role': r}}) for p, c, r in works])
    end = {'person': stack.end_offsets(topics['person']), 'company': stack.end_offsets(topics['company'])}
    assert end == {'person': {0: 3}, 'company': {0: 2}}

    before = qw.capture(stack.neo, stack.marker)
    observer = AppObserver(docker_client, registry, stack.network.id, owner=stack.owner,
                           candidate_images={row['id'] for row in service_config['images'].values()})
    observer.start()
    try:
        env = cli_environment(stack, service_config['images'], service_config['docker_host'])
        result = run_cli(Path(service_config['venv_bin']) / 'graph-loader',
                         ['start', '--mode', 'bulk', '--config', str(run / 'schema.yaml'), '--network', stack.network.name,
                          '--skip-image-build', '--run-dir', str(run / 'runs')],
                         env=env, cwd=run, log_dir=run / 'logs', name='start', timeout=240, redact=[stack.password])
        observer.barrier()
        observer.drain()
    finally:
        observer.stop()
    snapshot = observer.snapshot()
    (run / 'logs' / 'observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
    leftovers = observer.reap()
    assert (result.exit_code, result.timed_out) == (0, False), result.tail
    assert result.redactions == 0, 'the scratch password reached CLI output'
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {}, 'a container with another owner label attached to the owned network'
    assert leftovers == {'removed': [], 'failed': []}
    assert observer.unobserved_candidates() == []
    assert set(snapshot['created_from_candidates']) == set(snapshot['containers'])

    images = service_config['images']
    components = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info, 'an application container disappeared before it could be inspected'
        role = info['labels']['io.graph-loader.component']
        assert info['image_id'] == images[role]['id'] and info['networks'] == [stack.network.id]
        assert not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        for kind, source, destination, _ in info['mounts']:
            assert 'docker.sock' not in source and (kind != 'bind' or Path(source).is_relative_to(run)), (kind, source)
        components[info['labels']['component']] = components.get(info['labels']['component'], 0) + 1
    assert components == EXPECTED_COMPONENTS
    assert not [c for c in docker_client.containers.list(all=True) if c.id in snapshot['containers']]

    graph = graph_observations(stack.driver, owner=stack.owner, database=stack.database)
    nodes = sorted(json.dumps({'labels': [label], 'properties': {'id': i, 'name': n}}, sort_keys=True)
                   for label, rows in (('Person', people), ('Company', companies)) for i, n in rows)
    assert sorted(json.dumps(row, sort_keys=True) for row in graph['nodes']) == nodes
    relationships = sorted(json.dumps({'source_labels': ['Person'], 'source': {'id': p, 'name': f'p{p}'}, 'type': 'WORKS_AT',
                                       'properties': {'role': r}, 'target_labels': ['Company'],
                                       'target': {'id': c, 'name': 'c' + c}}, sort_keys=True) for p, c, r in works)
    assert sorted(json.dumps(row, sort_keys=True) for row in graph['relationships']) == relationships

    attempts = [p for p in (run / 'runs').iterdir() if p.name.startswith('graph-loader-')]
    assert len(attempts) == 1
    plan_bytes = (attempts[0] / 'plans' / 'edge-plan.json').read_bytes()
    plan = json.loads(plan_bytes)
    ledger = plan_ledger(attempts[0], expected_rows=len(works), expected_digest=hashlib.sha256(plan_bytes).hexdigest(),
                         expected_plan_id=plan['plan_id'])
    logged = re.search(r'stage=planned-edges-complete plan_id=(\w+) chunks=(\d+) rows=(\d+)',
                       result.stderr.read_text() + result.stdout.read_text())
    assert logged and logged.groups() == (plan['plan_id'], str(ledger['chunks']), str(len(works)))
    assert len({chunk[0] for chunk in plan['chunks']}) == len(plan['chunks']) == ledger['chunks']

    admin = stack.admin()
    groups = [g.group_id for g in admin.list_consumer_groups().result().valid if g.group_id.startswith(prefix)]
    assert groups, 'no scratch consumer group was committed'

    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'
    committed = {}
    for group in groups:
        for name in ('person', 'company'):
            offsets = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=group,
                                        partitions=[(topics[name], 0)], broker_guard=running)
            if offsets[0]['offset'] == end[name][0]:
                committed[name] = group
    assert set(committed) == {'person', 'company'}, 'node topic offsets were not committed at the log end'

    after = qw.capture(stack.neo, stack.marker)
    window = qw.window(before, after)
    completed = [r for r in window if r['phase'] == 'completed' and not r.get('duplicate')]
    started = [r for r in window if r['phase'] == 'started']
    assert len(started) == len(completed) > 0
    assert any('WORKS_AT' in r['query'] for r in completed)


def test_h5_neo4j_outage_and_termination_controls(docker_client, registry, stack):
    """Neo4j and Kafka stop/start on owned volumes, plus SIGKILL of owned helper containers (name kept: frozen test ID)."""
    def count():
        with stack.driver.session(database=stack.database) as session:
            return session.run('MATCH (n) WHERE NOT n:Outage RETURN count(n) AS c').single()['c']

    def outages():
        with stack.driver.session(database=stack.database) as session:
            return session.run('MATCH (o:Outage) RETURN count(o) AS c').single()['c']

    baseline = count()
    assert baseline == 5
    for attempt in range(5):
        assert stack.stop_neo4j(timeout=30) == 'exited'
        probe = GraphDatabase.driver(stack.neo_uri, auth=('neo4j', stack.password), connection_timeout=2)
        try:
            with pytest.raises(ServiceUnavailable):
                probe.verify_connectivity()
        finally:
            probe.close()
        time.sleep(5)
        stack.start_neo4j()
        assert (count(), outages()) == (baseline, attempt), attempt
        with stack.driver.session(database=stack.database) as session:
            session.run('MERGE (:Outage {attempt: $a})', a=attempt).consume()
    assert outages() == 5
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (o:Outage) DELETE o').consume()
    assert (count(), outages()) == (baseline, 0)

    topic = f'e09-{stack.owner}-outage'
    stack.create_topics([topic])
    for attempt in range(5):
        stack.produce(topic, [(attempt, {'attempt': attempt})])
        assert stack.stop_kafka(timeout=30) == 'exited'
        time.sleep(3)
        stack.start_kafka()
        # Only the partition end offset is read back: payloads and committed consumer offsets are not.
        assert stack.end_offsets(topic) == {0: attempt + 1}, attempt

    for attempt in range(5):
        sleeper = stack.run_owned(f'sleeper-{attempt}', 'python:3.12-slim', ['sleep', '600'])
        try:
            sleeper.reload()
            assert sleeper.status == 'running'
            assert stack.kill_owned(sleeper, timeout=30) == {'exit_code': 137, 'status': 'exited', 'oom_killed': False}
        finally:
            stack.remove_owned(sleeper)
    assert [c.name for c in docker_client.containers.list(all=True) if 'sleeper' in c.name] == []


def test_h6_lock_wait_detector_positive_and_negative_controls(stack):
    for attempt in range(5):
        before = qw.capture(stack.neo, stack.marker)
        contended = lock_conflict(stack, 100 + attempt)
        with stack.driver.session(database=stack.database) as session:
            uncontended = f'MATCH (n:LockProbe {{id: 1}}) SET n.v = {200 + attempt} RETURN n.v AS v'
            assert session.run(uncontended).single()['v'] == 200 + attempt
        after = qw.capture(stack.neo, stack.marker)
        window = qw.window(before, after)
        positive = qw.waits(window, query=contended, database=stack.database, user='neo4j', expected=1)
        negative = qw.waits(window, query=uncontended, database=stack.database, user='neo4j', expected=1)
        assert positive[0] >= 500, positive
        assert negative == [0], negative
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:LockProbe) DELETE n').consume()


def test_h7_no_helper_process_outlives_its_test():
    stray = []
    for process in psutil.Process().children(recursive=True):
        try:
            if process.status() != psutil.STATUS_ZOMBIE:
                stray.append(process.pid)
        except psutil.NoSuchProcess:
            pass
    assert stray == [], 'a CLI or helper child outlived its test'
