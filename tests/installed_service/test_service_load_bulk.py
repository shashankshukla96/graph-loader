"""E09.F1.S1 planned bulk on owned Neo4j/Kafka (native shared Docker Engine substrate).

Installed ``graph-loader start --mode bulk`` (planned edges, the default) with the installed candidate images:

* a clean run on the small fixture (graph, plan ledger, committed node offsets, component set, cleanup);
* a clean run on a larger fixture whose edge-writer statements all show zero database lock waits, with a
  positive control on the same detector, and the dispatcher's own wait counters read as a different quantity;
* one planned-edge-writer SIGKILLed while it holds a chunk (no external lock holder): the run fails closed, no chunk
  is granted after the loss, and a full MERGE replay on the same topics converges to the fixture.

Not covered: distributed exactly-once writes, hubs/spill/scale, node-loader recovery, Neo4j outages, DLQ, arm64.
The kill is a timing race against 100-250 ms chunks; the test asserts that the writer really held a chunk and fails
otherwise, so it is repeated.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import threading
import time

import pytest

from tests.installed_service import bulk_fixture as fx
from tests.installed_service import query_window as qw
from tests.installed_service.live_cli import AppObserver, cli_environment, run_cli
from tests.installed_service.lockwait import lock_conflict
from tests.installed_service.observers import committed_offsets, graph_observations, plan_ledger

pytestmark = pytest.mark.integration

LARGE = (3000, 30)
LARGE_ARGS = ('--plan-chunk-rows', '1000', '--plan-writers', '2')
COMPONENTS = {'preflight': 1, 'node-loader': 2, 'planned-dispatcher': 1, 'planned-edge-writer': 2}
CLI_TIMEOUT = 240


def poll_containers(client, observer, sink, *, only=None):
    """Keep the newest bounded log copy of every adopted application container (or only the dispatcher)."""
    with observer._lock:
        rows = list(observer.containers.items())
    for cid, row in rows:
        info = row.get('inspected') or {}
        name = info.get('name') or row.get('event_name') or cid[:12]
        if only is not None and only not in name:
            continue
        try:
            sink[name] = client.api.logs(cid, tail=400, stdout=True, stderr=True).decode(errors='replace')
        except Exception as error:
            sink.setdefault(name, f'<unreadable: {type(error).__name__}>')


def kill_a_chunk_holder(observer, logs, kill, started):
    """After the first planned-done, SIGKILL the writer that reported it (it most likely holds its next chunk)."""
    dispatcher = next((text for name, text in logs.items() if 'planned-dispatcher' in name), '')
    done = re.findall(r'stage=planned-done chunk=(\d+) .*? writer=(\S+)', dispatcher)
    if not done:
        return
    writer_id = done[-1][1]
    with observer._lock:
        candidates = [(cid, (row.get('inspected') or {}).get('name') or row.get('event_name') or '')
                      for cid, row in observer.containers.items()]
    matches = [cid for cid, name in candidates if f'-planned-{writer_id}-' in name]
    kill.update(writer_id=writer_id, done_before=len(done), at_s=time.monotonic() - started, matches=len(matches))
    if len(matches) == 1:
        kill.update(container=matches[0], state=observer.kill_observed(matches[0], timeout=60),
                    killed_at_s=time.monotonic() - started)


def attempt_dirs(run):
    return {p for p in (run / 'runs').iterdir() if p.name.startswith('graph-loader-')} if (run / 'runs').is_dir() else set()


def drive(stack, docker_client, registry, config, run, name, topics, group, *, plan_args, kill_a_writer=False, carried=None):
    """One installed ``graph-loader start --mode bulk`` under an observer, with bounded deadlines everywhere."""
    prefix = f'e09-{stack.owner}-'
    (run / 'runs').mkdir(parents=True, exist_ok=True)
    (run / 'logs').mkdir(exist_ok=True)
    (run / 'schema.yaml').write_text(fx.schema(prefix, topics, group))
    known = attempt_dirs(run)
    before = carried or qw.capture(stack.neo, stack.marker)
    observer = AppObserver(docker_client, registry, stack.network.id, owner=stack.owner,
                           candidate_images={row['id'] for row in config['images'].values()},
                           barrier_tag=f'{run.name}-{name}-')
    observer.start()
    box, logs, kill = {}, {}, {}
    env = cli_environment(stack, config['images'], config['docker_host'])

    def target():
        try:
            box['result'] = run_cli(Path(config['venv_bin']) / 'graph-loader',
                                    ['start', '--mode', 'bulk', '--config', str(run / 'schema.yaml'), '--network', stack.network.name,
                                     '--skip-image-build', '--run-dir', str(run / 'runs'), *plan_args],
                                    env=env, cwd=run, log_dir=run / 'logs', name=name, timeout=CLI_TIMEOUT, redact=[stack.password])
        except BaseException as error:
            box['error'] = repr(error)

    thread = threading.Thread(target=target)
    started = time.monotonic()
    thread.start()
    try:
        while thread.is_alive():
            poll_containers(docker_client, observer, logs, only='planned-dispatcher' if kill_a_writer else None)
            if kill_a_writer and 'state' not in kill:
                kill_a_chunk_holder(observer, logs, kill, started)
            time.sleep(0.02 if kill_a_writer else 0.2)
    finally:
        thread.join(timeout=CLI_TIMEOUT + 60)
    assert not thread.is_alive(), 'the CLI thread outlived its deadline'
    poll_containers(docker_client, observer, logs)
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
    after = qw.capture(stack.neo, stack.marker, allow_open=kill_a_writer or carried is not None)
    (run / 'logs' / f'{name}-query-log-delta.log').write_bytes(after.content[len(before.content):])
    (run / 'logs' / f'{name}-query-log-size.json').write_text(json.dumps({'before': len(before.content), 'after': len(after.content)}) + '\n')
    records = qw.window(before, after)
    assert 'error' not in box, box.get('error')
    result = box['result']
    new = attempt_dirs(run) - known
    assert len(new) == 1, 'the run should create exactly one attempt directory'
    return {'result': result, 'stderr': result.stderr.read_text() + result.stdout.read_text(), 'snapshot': snapshot,
            'leftovers': leftovers, 'kill': kill, 'records': records, 'capture': after, 'elapsed': time.monotonic() - started,
            'attempt': new.pop()}


def check_cleanup(stack, docker_client, out):
    snapshot = out['snapshot']
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {}, 'a container with another owner label attached to the owned network'
    assert out['leftovers'] == {'removed': [], 'failed': []}
    assert out['result'].redactions == 0, 'the scratch password reached CLI output'
    assert out['result'].timed_out is False and out['elapsed'] < CLI_TIMEOUT + 60
    observer_ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == observer_ids and observer_ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in observer_ids]
    components = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info, 'an application container disappeared before it could be inspected'
        assert not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        for kind, source, _, _ in info['mounts']:
            assert 'docker.sock' not in source, (kind, source)
        component = info['labels']['component']
        components[component] = components.get(component, 0) + 1
    assert components == COMPONENTS


def own_graph(stack):
    return fx.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Worker|Employer) DETACH DELETE n').consume()


def fresh_topics(stack, tag, scale):
    prefix = f'e09-{stack.owner}-'
    topics = {key: f'{prefix}{tag}-{key}' for key in ('worker', 'employer', 'edge')}
    stack.create_topics(list(topics.values()))
    fx.produce(stack, topics, *scale)
    return topics


def committed(stack, group, topics, ends):
    """Node-topic offsets committed by the per-label consumer groups the CLI derives from the schema group."""
    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'
    result = {}
    for label, key in (('Worker', 'worker'), ('Employer', 'employer')):
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=f'{group}-{label}',
                                 partitions=[(topics[key], 0)], broker_guard=running)
        result[key] = rows[0]['offset']
    assert result == ends, result


def settled_edge_transactions(stack, timeout=60):
    """Seconds until no transaction running the edge-writer statement remains (a lost writer's may outlive it)."""
    end = time.monotonic() + timeout
    started = time.monotonic()
    while True:
        with stack.driver.session(database='system') as session:
            rows = session.run("SHOW TRANSACTIONS YIELD currentQuery, database WHERE currentQuery CONTAINS $edge AND "
                               "database = $db RETURN count(*) AS c", edge=fx.EDGE_TYPE, db=stack.database).single()['c']
        if rows == 0:
            return time.monotonic() - started
        assert time.monotonic() < end, 'the lost writer\'s transaction is still running'
        time.sleep(0.5)


def plan_of(attempt):
    raw = (attempt / 'plans' / 'edge-plan.json').read_bytes()
    return raw, json.loads(raw)


def dispatcher_stats(text):
    found = re.findall(r'stage=planned-dispatcher-stats state=(\w+) done=(\d+)/(\d+) rows=(\d+)/(\d+) grants=(\d+) .*? '
                       r'waits=(\d+) wait_timeouts=(\d+) total_wait_ms=([\d.]+) active=(\d+) max_active=(\d+) completed=(\d+)', text)
    assert found, 'no dispatcher stats line'
    keys = ('state', 'done', 'chunks', 'rows', 'plan_rows', 'grants', 'waits', 'wait_timeouts', 'total_wait_ms', 'active',
            'max_active', 'completed')
    return {k: (v if k == 'state' else float(v) if k == 'total_wait_ms' else int(v)) for k, v in zip(keys, found[-1])}


def edge_statements(records):
    rows = [r for r in records if fx.EDGE_TYPE in r['query'] and 'MERGE' in r['query']]
    assert len({r['query'] for r in rows}) == 1, 'the edge writer should run one statement text'
    return rows


def assert_completed_run(stack, out, expected_rows, scale):
    assert (out['result'].exit_code, out['result'].timed_out) == (0, False), out['result'].tail
    nodes, edges = own_graph(stack)
    assert nodes == fx.expected_nodes(*scale) and edges == fx.expected_relationships(*scale)
    raw, plan = plan_of(out['attempt'])
    ledger = plan_ledger(out['attempt'], expected_rows=expected_rows, expected_digest=hashlib.sha256(raw).hexdigest(),
                         expected_plan_id=plan['plan_id'])
    logged = re.search(r'stage=planned-edges-complete plan_id=(\w+) chunks=(\d+) rows=(\d+)', out['stderr'])
    assert logged and logged.groups() == (plan['plan_id'], str(ledger['chunks']), str(expected_rows))
    assert len({chunk[0] for chunk in plan['chunks']}) == len(plan['chunks']) == ledger['chunks'] >= 4
    stats = dispatcher_stats(out['stderr'])
    assert (stats['state'], stats['completed'], stats['grants'], stats['active'], stats['rows']) == (
        'complete', ledger['chunks'], ledger['chunks'], 0, expected_rows)
    return plan, ledger, stats


def test_b1_planned_bulk_clean_small_fixture(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 's1-b1'
    run.mkdir()
    clear(stack)
    scale = (fx.WORKERS, fx.EMPLOYERS)
    topics = fresh_topics(stack, 'b1', scale)
    group = f'e09-{stack.owner}-b1-group'
    ends = {'worker': stack.end_offsets(topics['worker'])[0], 'employer': stack.end_offsets(topics['employer'])[0]}
    assert ends == {'worker': fx.WORKERS, 'employer': fx.EMPLOYERS}
    out = drive(stack, docker_client, registry, service_config, run, 'start', topics, group, plan_args=fx.PLAN_ARGS)
    check_cleanup(stack, docker_client, out)
    plan, ledger, stats = assert_completed_run(stack, out, 2 * fx.WORKERS, scale)
    committed(stack, group, topics, ends)
    assert len(edge_statements(out['records'])) == 2 * ledger['chunks']
    assert not [r for r in out['records'] if r['phase'] == 'failed']


def test_b2_no_database_lock_waits_between_planned_edge_writers(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 's1-b2'
    run.mkdir()
    clear(stack)
    topics = fresh_topics(stack, 'b2', LARGE)
    group = f'e09-{stack.owner}-b2-group'
    out = drive(stack, docker_client, registry, service_config, run, 'start', topics, group, plan_args=LARGE_ARGS)
    check_cleanup(stack, docker_client, out)
    plan, ledger, stats = assert_completed_run(stack, out, 2 * LARGE[0], LARGE)
    writers = edge_statements(out['records'])
    done = [r for r in writers if r['phase'] == 'completed']
    assert len(done) == ledger['chunks'] == len([r for r in writers if r['phase'] == 'started'])
    assert [r['waiting_ms'] for r in done] == [0] * ledger['chunks'], 'an edge-writer query waited for a database lock'
    # The dispatcher's own waiting (writers idle until the node phase is ready) is a different quantity.
    assert stats['waits'] > 0 and stats['total_wait_ms'] > 0
    # Positive control on the same detector and server: a real lock conflict is seen as a wait.
    before = qw.capture(stack.neo, stack.marker)
    contended = lock_conflict(stack, 900)
    positive = qw.waits(qw.window(before, qw.capture(stack.neo, stack.marker)), query=contended, database=stack.database,
                        user='neo4j', expected=1)
    assert positive[0] >= 500, positive
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:LockProbe) DELETE n').consume()


@pytest.mark.parametrize('repeat', range(3))
def test_b3_b4_lost_chunk_holder_fails_closed_and_replay_converges(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's1-b3-{repeat}'
    run.mkdir()
    clear(stack)
    topics = fresh_topics(stack, f'b3-{repeat}', LARGE)
    group = f'e09-{stack.owner}-b3-{repeat}-group'
    ends = {'worker': LARGE[0], 'employer': LARGE[1]}
    total = 2 * LARGE[0]

    first = drive(stack, docker_client, registry, service_config, run, 'start-1', topics, group, plan_args=LARGE_ARGS,
                  kill_a_writer=True)
    check_cleanup(stack, docker_client, first)
    kill = first['kill']
    assert kill.get('matches') == 1 and kill['state'] == {'exit_code': 137, 'status': 'exited', 'oom_killed': False}, kill
    assert first['result'].exit_code not in (0, None)
    held = re.search(r'writer (w\d+) disconnected while holding chunk (\d+)', first['stderr'])
    assert held and held.group(1) == kill['writer_id'], 'the killed writer did not hold a chunk when it was lost'
    stats = dispatcher_stats(first['stderr'])
    assert stats['state'] == 'failed' and stats['active'] >= 1 and stats['completed'] < stats['chunks']
    assert stats['grants'] == stats['completed'] + stats['active'], 'a chunk was granted after the loss'
    open_ids = first['capture'].open_ids
    started_edge = {r['query_id'] for r in edge_statements(first['records']) if r['phase'] == 'started'}
    assert len(open_ids) <= 1 and set(open_ids) <= started_edge, ('a killed writer leaves at most its own query open', open_ids)
    settled_s = settled_edge_transactions(stack)
    (run / 'logs' / 'settled.json').write_text(json.dumps({'edge_transactions_gone_after_s': settled_s, 'open_query_ids': list(open_ids)}) + '\n')
    raw, plan = plan_of(first['attempt'])
    rows = {chunk[0]: chunk[3] for chunk in plan['chunks']}
    assert not (first['attempt'] / 'plans' / 'edge-plan.json.ledger.json').exists(), 'a failed attempt persisted a ledger'
    nodes, edges = own_graph(stack)
    assert nodes == fx.expected_nodes(*LARGE)
    assert set(edges) <= set(fx.expected_relationships(*LARGE)) and len(edges) == len(set(edges)), \
        'a partial graph must be a duplicate-free subset of the fixture'
    held_rows = rows[int(held.group(2))]
    assert stats['rows'] <= len(edges) <= stats['rows'] + held_rows < total, (stats['rows'], len(edges), held_rows)
    committed(stack, group, topics, ends)
    failed = [r for r in first['records'] if r['phase'] == 'failed']
    assert all(fx.EDGE_TYPE in r['query'] and 'MERGE' in r['query'] and r['waiting_ms'] == 0 for r in failed), failed

    second = drive(stack, docker_client, registry, service_config, run, 'start-2', topics, group, plan_args=LARGE_ARGS,
                   carried=first['capture'])
    check_cleanup(stack, docker_client, second)
    replay_plan, ledger, replay_stats = assert_completed_run(stack, second, total, LARGE)
    assert replay_plan['plan_id'] != plan['plan_id'], 'the replay must be a new plan'
    committed(stack, group, topics, ends)
    mine = [r for r in second['records'] if r['query_id'] > first['capture'].max_id]
    assert not [r for r in mine if r['phase'] == 'failed']
    assert [r['waiting_ms'] for r in edge_statements(mine) if r['phase'] == 'completed'] == [0] * ledger['chunks']
