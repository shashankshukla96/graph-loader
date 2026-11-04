#!/usr/bin/env python3
"""E15.F2.S12 cell A: the status / events / logs live matrix (runs on the Docker host against the stack of ``e15_stack.py``).

{stream, planned bulk, --edge-classic, --edge-leases} x {docker, process runtime} x {success, loader kill} x {json, sql store} = 32 cells.
Each cell: fresh topics, a small deterministic dataset (``tests/installed_service/bulk_fixture.py``), one ``graph-loader start`` with
``--log-format json`` and an explicit ``--run-dir`` inside the work directory, then assertions on

* the CLI exit code and the terminal state in the chosen store (status.json / the PostgreSQL rows),
* the event history (gap-free ``seq`` from 1, first event ``execution.*``, last ``execution.finished`` agreeing with the state),
* the roles recorded for the run (and the killed one for a kill cell),
* every JSON log line (parses; the CLI's lines carry the execution id; relayed children carry theirs),
* for a success the loaded graph (node and relationship counts).

Nothing is printed that holds a password. Memory guard: a cell starts only with MemAvailable >= START_KB and is stopped below STOP_KB.
Usage:  e15_live_matrix.py WORKDIR [--only SUBSTRING ...] [--list]
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

START_KB, STOP_KB = 3_500_000, 1_800_000
WORKERS, EMPLOYERS = 1500, 6
MODES = ('stream', 'planned', 'classic', 'leases')
RUNTIMES = ('process', 'docker')
OUTCOMES = ('success', 'kill')
STORES = ('json', 'sql')


def available_kb() -> int:
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1])
    raise RuntimeError('no MemAvailable')


class Guard(threading.Thread):
    def __init__(self, stop_callback):
        super().__init__(daemon=True)
        self.minimum, self.breached, self.done, self.stop_callback = available_kb(), False, threading.Event(), stop_callback

    def run(self):
        while not self.done.wait(5):
            value = available_kb()
            self.minimum = min(self.minimum, value)
            if value < STOP_KB and not self.breached:
                self.breached = True
                self.stop_callback()


def load_env(path: Path) -> dict:
    return dict(line.split('=', 1) for line in path.read_text().splitlines() if '=' in line)


class Cell:
    def __init__(self, mode, runtime, outcome, store):
        self.mode, self.runtime, self.outcome, self.store = mode, runtime, outcome, store
        self.name = f'{mode}-{runtime}-{outcome}-{store}'


def cells():
    return [Cell(*parts) for parts in itertools.product(MODES, RUNTIMES, OUTCOMES, STORES)]


class Lab:
    """One stack, one set of images, one work directory."""

    def __init__(self, workdir: Path, tree: Path):
        self.workdir, self.tree = workdir, tree
        self.facts = json.loads((workdir / 'stack.json').read_text())
        self.credentials = load_env(workdir / 'credentials.env')
        self.images = json.loads((workdir / 'images.json').read_text()) if (workdir / 'images.json').exists() else {}
        sys.path.insert(0, str(tree / 'tests' / 'installed_service'))
        import bulk_fixture
        self.fixture = bulk_fixture
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
        from neo4j import GraphDatabase
        self.Producer, self.AdminClient, self.NewTopic = Producer, AdminClient, NewTopic
        self.driver = GraphDatabase.driver(self.credentials['NEO4J_URI'], auth=('neo4j', self.credentials['NEO4J_PASSWORD']))
        self.bootstrap = self.credentials['KAFKA_BOOTSTRAP_SERVERS']
        self.token = self.facts['token']
        self.stamp = int(time.time())
        self.executable = str(Path(sys.executable).parent / 'graph-loader')

    # -- stack helpers
    def clear_graph(self):
        with self.driver.session() as session:
            session.run('MATCH (n) DETACH DELETE n').consume()

    def counts(self):
        with self.driver.session() as session:
            nodes = {r['l']: r['c'] for r in session.run('MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS c')}
            rels = session.run('MATCH ()-[r]->() RETURN count(r) AS c').single()['c']
        return nodes, rels

    def create_topics(self, names):
        admin = self.AdminClient({'bootstrap.servers': self.bootstrap})
        for name, future in admin.create_topics([self.NewTopic(n, num_partitions=1, replication_factor=1) for n in names]).items():
            future.result(timeout=30)

    def produce(self, topic, rows):
        failures = []
        producer = self.Producer({'bootstrap.servers': self.bootstrap, 'enable.idempotence': True})
        for key, value in rows:
            producer.produce(topic, key=str(key).encode(), value=json.dumps(value).encode(),
                             on_delivery=lambda error, message: failures.append(error) if error else None)
        if producer.flush(30) != 0 or failures:
            raise RuntimeError('publish was not fully acknowledged')

    def produce_nodes(self, topics):
        f = self.fixture
        self.produce(topics['worker'], [(i, {'id': i, 'name': n}) for i, n in f.workers(WORKERS)])
        self.produce(topics['employer'], [(i, {'id': i, 'name': n}) for i, n in f.employers(EMPLOYERS)])

    def produce_edges(self, topics):
        f = self.fixture
        self.produce(topics['edge'], [(f'{w}-{e}', {'source': {'id': w}, 'target': {'id': e}, 'properties': {'role': r, 'since': s}})
                                      for w, e, r, s in f.relationships(WORKERS, EMPLOYERS)])

    def environment(self, cell: Cell) -> dict:
        env = {k: os.environ[k] for k in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR') if k in os.environ}
        env.update(NEO4J_URI=self.credentials['NEO4J_URI'], NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=self.credentials['NEO4J_PASSWORD'],
                   KAFKA_BOOTSTRAP_SERVERS=self.bootstrap, GRAPH_LOADER_STATUS_INTERVAL_S='2')
        if cell.runtime == 'docker':
            env.update(LOADER_NEO4J_URI=self.credentials['LOADER_NEO4J_URI'],
                       LOADER_KAFKA_BOOTSTRAP_SERVERS=self.credentials['LOADER_KAFKA_BOOTSTRAP_SERVERS'])
            env.update({f'GRAPH_LOADER_{role.upper()}_IMAGE': ref for role, ref in self.images.items()})
        if cell.store == 'sql':
            env['GRAPH_LOADER_STATUS_DB_URL'] = self.credentials['GRAPH_LOADER_STATUS_DB_URL']
        return env


def schema_text(lab: Lab, cell: Cell, topics: dict, group: str) -> str:
    text = lab.fixture.schema(f'{cell.name}-', topics, group)
    if cell.mode == 'stream':
        text = text.replace('  mode: bulk\n', '', 1)
    return text


def arguments(lab: Lab, cell: Cell, schema: Path, run_dir: Path) -> list[str]:
    args = ['--log-format', 'json', 'start', '--mode', 'stream' if cell.mode == 'stream' else 'bulk', '--config', str(schema),
            '--run-dir', str(run_dir), '--status-store', cell.store, '--runtime', cell.runtime]
    if cell.runtime == 'docker':
        args += ['--network', lab.facts['network'], '--skip-image-build']
    args += {'stream': [], 'planned': ['--plan-chunk-rows', '300', '--plan-writers', '2'], 'classic': ['--edge-classic'],
             'leases': ['--edge-leases']}[cell.mode]
    return args


def attempt_dir(run_dir: Path, wait=60) -> Path:
    end = time.monotonic() + wait
    while time.monotonic() < end:
        found = sorted(p for p in run_dir.glob('graph-loader-*') if (p / 'status-locator.json').is_file()) if run_dir.is_dir() else []
        if found:
            return found[0]
        time.sleep(0.2)
    raise RuntimeError('no attempt directory')


def wait_until(predicate, timeout, process=None, step=0.4):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        if process is not None and process.poll() is not None:
            return predicate()
        time.sleep(step)
    return False


def kill_a_loader(lab: Lab, cell: Cell, attempt: Path, process, timeout=120) -> dict:
    """SIGKILL a node loader as soon as it exists; returns what was killed."""
    if cell.runtime == 'process':
        from neo4j_graph_loader.orchestrator.process_service import registered_children
        from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, fold

        def find():
            try:
                names = {s['id'] for s in fold(RunRegistry.read(attempt / 'registry.jsonl')).values()
                         if not s.get('external') and (s.get('labels') or {}).get('component') == 'node-loader'}
                live = [s for s in registered_children(attempt / 'registry.jsonl') if s['id'] in names]
            except Exception:
                return None
            return live[0] if live else None
        end = time.monotonic() + timeout
        while time.monotonic() < end and process.poll() is None:
            state = find()
            if state:
                os.kill(int(state['pid']), signal.SIGKILL)
                return {'killed': state['id'], 'pid': state['pid']}
            time.sleep(0.05)
        return {'killed': None}
    import docker
    client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
    end = time.monotonic() + timeout
    while time.monotonic() < end and process.poll() is None:
        for container in client.containers.list(filters={'label': 'graph_loader.kind=node', 'network': lab.facts['network']}):
            container.kill()
            return {'killed': container.name}
        time.sleep(0.05)
    return {'killed': None}


def read_state(cell: Cell, attempt: Path, env: dict):
    os.environ.update({k: v for k, v in env.items() if k.startswith('GRAPH_LOADER_STATUS')})
    from neo4j_graph_loader.status import iter_events, read_status
    return read_status(attempt), list(iter_events(attempt))


def check_logs(stderr: Path, stdout: Path, status: dict) -> tuple[list[str], dict]:
    problems, stats = [], {'lines': 0, 'with_execution_id': 0, 'with_incarnation': 0, 'kinds': {}}
    for path in (stderr, stdout):
        for line in path.read_text(errors='replace').splitlines():
            if not line.strip():
                continue
            if not line.startswith('{'):
                if path is stderr:
                    problems.append(f'non-JSON stderr line: {line[:120]!r}')
                continue
            try:
                row = json.loads(line)
            except ValueError:
                problems.append(f'unparseable JSON line: {line[:120]!r}')
                continue
            if not isinstance(row, dict) or 'level' not in row:
                continue
            stats['lines'] += 1
            if row.get('execution_id') == status['execution_id']:
                stats['with_execution_id'] += 1
            elif row.get('execution_id'):
                problems.append(f"a log line carries another execution id: {row.get('execution_id')}")
            if row.get('incarnation'):
                stats['with_incarnation'] += 1
            stats['kinds'][row.get('kind') or '?'] = stats['kinds'].get(row.get('kind') or '?', 0) + 1
    if stats['with_execution_id'] == 0:
        problems.append('no log line carries the execution id')
    return problems, stats


def sql_cross_check(lab: Lab, execution_id: str, status: dict, events: list) -> list[str]:
    import psycopg
    url = lab.credentials['GRAPH_LOADER_STATUS_DB_URL'].replace('postgresql+psycopg://', 'postgresql://')
    with psycopg.connect(url) as connection:
        row = connection.execute('SELECT state, exit_code FROM graph_loader.executions WHERE execution_id = %s', (execution_id,)).fetchone()
        count = connection.execute('SELECT count(*), max(seq) FROM graph_loader.execution_events WHERE execution_id = %s', (execution_id,)).fetchone()
        roles = connection.execute('SELECT count(*) FROM graph_loader.execution_roles WHERE execution_id = %s', (execution_id,)).fetchone()[0]
    problems = []
    if row is None or row[0] != status['state']:
        problems.append(f'PostgreSQL row state {row} differs from the status {status["state"]}')
    if count[0] != len(events) or count[1] != len(events):
        problems.append(f'PostgreSQL has {count} events, the reader returned {len(events)}')
    if roles != len(status.get('roles') or []):
        problems.append(f'PostgreSQL has {roles} roles, the status lists {len(status.get("roles") or [])}')
    return problems


def run_cell(lab: Lab, cell: Cell, index: int) -> dict:
    started = time.monotonic()
    result = {'cell': cell.name, 'problems': [], 'facts': {}}
    if available_kb() < START_KB:
        result['problems'].append(f'memory guard: MemAvailable {available_kb()} kB below the start threshold')
        result['skipped'] = True
        return result
    directory = lab.workdir / 'cells' / cell.name
    directory.mkdir(parents=True)
    run_dir = directory / 'runs'
    prefix = f'e15.{lab.token}.{lab.stamp}.{index:02d}'          # unique per invocation: topics outlive an earlier attempt
    topics = {'worker': f'{prefix}.worker', 'employer': f'{prefix}.employer', 'edge': f'{prefix}.edge'}
    lab.create_topics(list(topics.values()))
    lab.clear_graph()
    schema = directory / 'schema.yaml'
    schema.write_text(schema_text(lab, cell, topics, f'e15-{lab.token}-{lab.stamp}-{index:02d}'))
    if cell.mode == 'stream':
        lab.produce_nodes(topics)
    else:
        lab.produce_nodes(topics)
        lab.produce_edges(topics)
    env = lab.environment(cell)
    out, err = open(directory / 'stdout.log', 'wb'), open(directory / 'stderr.log', 'wb')
    process = subprocess.Popen([lab.executable, *arguments(lab, cell, schema, run_dir)], cwd=directory, env=env, stdout=out, stderr=err,
                               start_new_session=True)
    guard = Guard(lambda: os.killpg(process.pid, signal.SIGTERM))
    guard.start()
    expected_nodes = {'Worker': WORKERS, 'Employer': EMPLOYERS}
    try:
        attempt = attempt_dir(run_dir)
        result['facts']['attempt'] = str(attempt)
        if cell.mode == 'stream':
            if cell.outcome == 'success':
                if not wait_until(lambda: lab.counts()[0] == expected_nodes, 240, process):
                    result['problems'].append('stream: the nodes were not loaded in time')
                lab.produce_edges(topics)
                if not wait_until(lambda: lab.counts()[1] == 2 * WORKERS, 240, process):
                    result['problems'].append('stream: the relationships were not loaded in time')
                process.send_signal(signal.SIGTERM)
            else:
                if not wait_until(lambda: lab.counts()[0].get('Worker', 0) > 0, 240, process):
                    result['problems'].append('stream: nothing was loaded before the kill')
                result['facts']['kill'] = kill_a_loader(lab, cell, attempt, process)
        elif cell.outcome == 'kill':
            result['facts']['kill'] = kill_a_loader(lab, cell, attempt, process)
        try:
            exit_code = process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            result['problems'].append('the CLI did not end within 300 s; terminating it')
            os.killpg(process.pid, signal.SIGTERM)
            exit_code = process.wait(timeout=120)
    finally:
        guard.done.set()
        out.close()
        err.close()
    result['facts'].update(exit_code=exit_code, seconds=round(time.monotonic() - started, 1), min_available_kb=guard.minimum)
    if guard.breached:
        result['problems'].append('memory watchdog fired')
    if cell.outcome == 'kill' and not result['facts'].get('kill', {}).get('killed'):
        result['problems'].append('the kill missed: the loader was not seen alive (the run finished first)')
    try:
        status, events = read_state(cell, attempt, env)
    except Exception as exc:                                   # noqa: BLE001
        result['problems'].append(f'status unreadable: {type(exc).__name__}: {exc}')
        return result
    result['facts'].update(state=status['state'], run_state_exit=status.get('outcome'), roles=[
        {k: r.get(k) for k in ('role', 'name', 'state', 'exit_code', 'exit_reason', 'restart_count')} for r in status.get('roles') or []],
        events=len(events), history_incomplete=status.get('history_incomplete'))
    want = {'success': 'stopped' if cell.mode == 'stream' else 'succeeded', 'kill': 'failed'}[cell.outcome]
    if status['state'] != want:
        result['problems'].append(f'terminal state {status["state"]!r}, expected {want!r}')
    if cell.outcome == 'success' and cell.mode != 'stream' and exit_code != 0:
        result['problems'].append(f'exit code {exit_code}, expected 0')
    if cell.outcome == 'kill' and exit_code == 0:
        result['problems'].append('exit code 0 after a loader kill')
    if status.get('history_incomplete'):
        result['problems'].append('history_incomplete is true')
    if not status.get('finished_at'):
        result['problems'].append('no finished_at in a terminal status')
    seqs = [e['seq'] for e in events]
    if seqs != list(range(1, len(seqs) + 1)):
        result['problems'].append(f'event seq is not gap-free from 1: {seqs[:5]}...{seqs[-3:]}')
    if not events or events[-1]['type'] != 'execution.finished' or events[-1].get('state') != status['state']:
        result['problems'].append('the last event is not execution.finished agreeing with the state')
    if not events or not events[0]['type'].startswith('execution.'):
        result['problems'].append('the first event is not an execution event')
    if not status.get('roles'):
        result['problems'].append('no roles recorded')
    elif cell.outcome == 'kill' and not any(r.get('state') == 'failed' for r in status['roles']):
        result['problems'].append('no role is recorded as failed after the kill')
    log_problems, result['facts']['logs'] = check_logs(directory / 'stderr.log', directory / 'stdout.log', status)
    result['problems'] += log_problems
    if cell.store == 'sql':
        result['problems'] += sql_cross_check(lab, status['execution_id'], status, events)
    else:
        names = {p.name for p in attempt.iterdir()}
        if 'status.json' not in names or not any(n.startswith('events.') for n in names):
            result['problems'].append('the JSON store files are missing')
    if cell.outcome == 'success':
        nodes, rels = lab.counts()
        result['facts']['graph'] = {'nodes': nodes, 'relationships': rels}
        if nodes != expected_nodes or rels != 2 * WORKERS:
            result['problems'].append(f'graph {nodes} / {rels} differs from {expected_nodes} / {2 * WORKERS}')
    if cell.runtime == 'docker':
        import docker
        client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
        left = [c.name for c in client.containers.list(all=True, filters={'network': lab.facts['network']})
                if c.labels.get('io.graph-loader.e15-owner') != lab.token]
        if left:
            result['problems'].append(f'role containers left on the owned network: {left}')
            for c in client.containers.list(all=True, filters={'network': lab.facts['network']}):
                if c.labels.get('io.graph-loader.e15-owner') != lab.token:
                    c.remove(force=True, v=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('workdir', type=Path)
    parser.add_argument('--only', nargs='*', default=[])
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--tree', type=Path, default=Path.cwd())
    args = parser.parse_args()
    selected = [c for c in cells() if not args.only or any(o in c.name for o in args.only)]
    if args.list:
        print('\n'.join(c.name for c in selected))
        return 0
    lab = Lab(args.workdir.resolve(), args.tree.resolve())
    results = args.workdir / 'results.jsonl'
    failures = 0
    for index, cell in enumerate(cells()):
        if cell.name not in {c.name for c in selected}:
            continue
        try:
            result = run_cell(lab, cell, index)
        except Exception as exc:                               # noqa: BLE001
            result = {'cell': cell.name, 'problems': [f'driver error: {type(exc).__name__}: {exc}'], 'facts': {}}
        with open(results, 'a') as handle:
            handle.write(json.dumps(result, sort_keys=True, default=str) + '\n')
        failures += bool(result['problems'])
        print(f"{'FAIL' if result['problems'] else 'ok  '} {cell.name} {result['facts'].get('seconds', '-')}s state={result['facts'].get('state')} "
              f"exit={result['facts'].get('exit_code')} {'; '.join(result['problems'])[:300]}", flush=True)
    print(f'{len(selected)} cell(s), {failures} with problems')
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
