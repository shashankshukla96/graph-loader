"""Shared plumbing of the process-runtime live cells (E14.F2.S4 runtime cells, E14.F2.S7 supervision cells).

Everything here drives the installed ``graph-loader`` executable with ``--runtime process`` and ``DOCKER_HOST`` pointing at a
missing socket, against the owned Neo4j/Kafka test stack. Nothing in this module is a test.
"""
from __future__ import annotations

import contextlib
import json
import os
import signal
import threading
import time
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator.process_service import registered_children
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, fold, signal_registered_cli
from tests.installed_service import stream_fixture as sf
from tests.installed_service.live_cli import ENV_KEEP, RunningCli, run_cli
from tests.installed_service.observers import committed_offsets, graph_observations

CLI_TIMEOUT = 300
NO_DOCKER = 'unix:///nonexistent/e14-no-docker.sock'


START_MIN_AVAILABLE_KB = 2_500_000       # E13 found memory the binding limit beside the user's services
STOP_BELOW_AVAILABLE_KB = 1_500_000       # a running cell is stopped through its normal stop path below this
SAMPLE_SECONDS = 5


def available_kb():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1])
    raise RuntimeError('no MemAvailable in /proc/meminfo')


class MemoryWatch(threading.Thread):
    """Samples MemAvailable every few seconds during one cell; on a breach stops every live CLI of the module the normal way."""

    def __init__(self, run_dir):
        super().__init__(name='memory-watch', daemon=True)
        self.run_dir, self.done = Path(run_dir), threading.Event()
        self.minimum = available_kb()
        self.samples = 1
        self.breached = False

    def run(self):
        while not self.done.wait(SAMPLE_SECONDS):
            value = available_kb()
            self.samples += 1
            self.minimum = min(self.minimum, value)
            if value < STOP_BELOW_AVAILABLE_KB and not self.breached:
                self.breached = True
                self.stop_clis()

    def stop_clis(self):
        """SIGTERM (the CLI's ordinary ordered stop) to every live CLI recorded in this module's run directories."""
        for registry in self.run_dir.glob('*/runs/*/registry.jsonl'):
            try:
                signal_registered_cli(registry, signal.SIGTERM)
            except Exception:
                pass


@pytest.fixture(autouse=True)
def memory_guard(request, service_config):
    """Refuse to start a cell below ~2.4 GB; sample every 5 s during it; record the minimum; fail the cell if the stop path fired."""
    start = available_kb()
    assert start >= START_MIN_AVAILABLE_KB, f'host memory guard: MemAvailable {start} kB < {START_MIN_AVAILABLE_KB} kB'
    watch = MemoryWatch(service_config['run_dir'])
    watch.start()
    yield watch
    watch.done.set()
    watch.join(timeout=10)
    with open(Path(service_config['run_dir']) / 'memory.jsonl', 'a') as out:
        out.write(json.dumps({'test': request.node.name, 'start_kb': start, 'min_available_kb': watch.minimum,
                              'peak_used_kb': start - watch.minimum, 'samples': watch.samples, 'breached': watch.breached}) + '\n')
    assert not watch.breached, f'MemAvailable fell below {STOP_BELOW_AVAILABLE_KB} kB; the cell was stopped (min {watch.minimum} kB)'


# -- plumbing ---------------------------------------------------------------------------------------------------
def process_env(stack, extra=None):
    env = {key: os.environ[key] for key in ENV_KEEP if key in os.environ}
    env.update(DOCKER_HOST=NO_DOCKER, NEO4J_URI=stack.neo_uri, NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=stack.password,
               NEO4J_DATABASE=stack.database, KAFKA_BOOTSTRAP_SERVERS=stack.bootstrap, GRAPH_LOADER_METRICS_ENABLED='0')
    env.update(extra or {})
    return env


def executable(config):
    return Path(config['venv_bin']) / 'graph-loader'


def attempt_dirs(run):
    base = run / 'runs'
    return sorted(p for p in base.iterdir() if (p / 'registry.jsonl').is_file()) if base.is_dir() else []


def the_attempt(run, *, wait=30):
    end = time.monotonic() + wait
    while True:
        found = attempt_dirs(run)
        if found:
            assert len(found) == 1, f'one attempt directory expected, found {found}'
            return found[0]
        assert time.monotonic() < end, 'the CLI never created its attempt directory'
        time.sleep(0.2)


def records(attempt):
    return RunRegistry.read(attempt / 'registry.jsonl')


def children(attempt, **where):
    """Registry state of the (non-external) children matching ``where`` on their labels/fields."""
    rows = []
    for state in fold(records(attempt)).values():
        if state.get('external'):
            continue
        labels = state.get('labels') or {}
        if all(labels.get(k, state.get(k)) == v for k, v in where.items()):
            rows.append(state)
    return rows


def live(attempt, **where):
    names = {s['id'] for s in children(attempt, **where)}
    return [s for s in registered_children(attempt / 'registry.jsonl') if s['id'] in names]


def wait_for(what, predicate, timeout, *, cli=None, step=0.5):
    end = time.monotonic() + timeout
    while True:
        if cli is not None:
            assert cli.running(), f'the CLI exited while waiting for {what}: {cli.text()[-2000:]}'
        value = predicate()
        if value:
            return value
        assert time.monotonic() < end, f'timed out waiting for {what}'
        time.sleep(step)


def second_shell(config, stack, run, attempt, *args, timeout=60):
    out = run_cli(executable(config), [*args, '--runtime', 'process', '--run-dir', str(attempt)], env=process_env(stack),
                  cwd=run, log_dir=run / 'logs', name=f'shell-{len(list((run / "logs").glob("shell-*.out")))}-{args[0]}',
                  timeout=timeout, redact=[stack.password])
    return out


def assert_clean(attempt):
    """Every child has an exit receipt; nothing registered is still alive; no stray planner worker either."""
    started = [s for s in fold(records(attempt)).values() if not s.get('external')]
    assert started, 'the registry shows no child at all'
    assert all(s['phase'] == 'exit' for s in started), [(s['name'], s['phase']) for s in started if s['phase'] != 'exit']
    assert registered_children(attempt / 'registry.jsonl') == []
    external = [s for s in fold(records(attempt)).values() if s.get('external')]
    for state in external:
        try:
            os.kill(int(state['pid']), 0)
        except OSError:
            continue
        pytest.fail(f'a planner process outlived the run: {state["name"]} pid {state["pid"]}')


def assert_no_secret(cli_or_result, stack):
    assert getattr(cli_or_result, 'redactions', 0) == 0, 'the scratch password reached CLI output'


def start_stream(stack, config, run, name, schema_text, *, extra=(), mode='stream', deadline=900):
    (run / 'logs').mkdir(parents=True, exist_ok=True)
    (run / 'runs').mkdir(exist_ok=True)
    schema = run / f'{name}.yaml'
    schema.write_text(schema_text)
    return RunningCli(executable(config), ['start', '--mode', mode, '--config', str(schema), '--runtime', 'process',
                                           '--run-dir', str(run / 'runs'), *extra],
                      env=process_env(stack), cwd=run, log_dir=run / 'logs', name=name, deadline=deadline, redact=[stack.password])


def run_to_completion(stack, config, run, name, schema_text, *, extra=(), mode='bulk', timeout=CLI_TIMEOUT):
    (run / 'logs').mkdir(parents=True, exist_ok=True)
    (run / 'runs').mkdir(exist_ok=True)
    schema = run / f'{name}.yaml'
    schema.write_text(schema_text)
    result = run_cli(executable(config), ['start', '--mode', mode, '--config', str(schema), '--runtime', 'process',
                                          '--run-dir', str(run / 'runs'), *extra],
                     env=process_env(stack), cwd=run, log_dir=run / 'logs', name=name, timeout=timeout, redact=[stack.password])
    return result, the_attempt(run, wait=1)



# -- stream helpers ----------------------------------------------------------------------------------------------
def stream_own_graph(stack):
    return sf.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def stream_clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Citizen|Firm) DETACH DELETE n').consume()


def stream_topics(stack, tag):
    prefix = f'{stack.topic_prefix}{tag}-'
    topics = {'citizen': prefix + 'citizen', 'firm': prefix + 'firm', **{kind: prefix + kind.lower() for kind in sf.TYPES}}
    stack.create_topics(list(topics.values()))
    return topics


def stream_committed(stack, group, topics):
    def running():
        stack.kafka.reload()
        return stack.kafka.status == 'running'
    result = {}
    for kind, key, topic in [('Citizen', 'citizen', topics['citizen']), ('Firm', 'firm', topics['firm'])] + \
            [(t, t, topics[t]) for t in sf.TYPES]:
        rows = committed_offsets(owner=stack.owner, bootstrap=stack.bootstrap, group=f'{group}-{kind}',
                                 partitions=[(topic, 0)], broker_guard=running, prefix_name=stack.prefix)
        result[key] = rows[0]['offset']
    return result


def stream_ends(stack, topics):
    return {key: stack.end_offsets(topic)[0] for key, topic in topics.items()}


def stop_via_second_shell(config, stack, run, attempt, cli, *, timeout=90):
    began = time.monotonic()
    shell = second_shell(config, stack, run, attempt, 'stop')
    assert shell.exit_code == 0, shell.tail
    end = time.monotonic() + timeout
    while cli.process.poll() is None and time.monotonic() < end:
        time.sleep(0.2)
    code = cli.process.poll()
    if code is None:
        cli.kill()
    cli._finish()
    return code, time.monotonic() - began


def lease_port(attempt):
    ready = [r for r in records(attempt) if r['kind'] == 'ready' and 'control' in (r.get('ports') or {})
             and any(s['id'] == r['id'] and (s.get('labels') or {}).get('component') == 'bucket-lease-server'
                     for s in children(attempt))]
    return ready[-1]['ports']['control'] if ready else None


def with_fast_restarts(text):
    return text.replace('    tx_timeout_ms: 6000\n',
                        '    tx_timeout_ms: 6000\n    max_consecutive_restarts: 2\n    restart_backoff_base_ms: 200\n'
                        '    restart_backoff_max_ms: 400\n', 1)


def loaded_and_supervising(stack, cli, topics, attempt):
    """Batch A is fully loaded (nodes and relationships), so the CLI is past its start-up lease-readiness wait and supervising."""
    nodes_a, rels_a = sf.expected(['a'])
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    wait_for('nodes', lambda: stream_own_graph(stack)[0] == nodes_a, 180, cli=cli)
    sf.produce_edges(stack, topics, 'a')
    wait_for('relationships', lambda: stream_own_graph(stack) == (nodes_a, rels_a), 240, cli=cli)
    return wait_for('the lease server port', lambda: lease_port(attempt), 60, cli=cli)


# -- supervision helpers (E14.F2.S7) ---------------------------------------------------------------------------------
TOTAL_BIG = 6000
STREAM_COMPONENTS = {'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': 3}


def components_started(attempt):
    """{component: how many children of it the registry ever started} (replacements count)."""
    counts = {}
    for state in children(attempt):
        component = (state.get('labels') or {}).get('component')
        counts[component] = counts.get(component, 0) + 1
    return counts


def child_ids(attempt):
    return {s['id'] for s in children(attempt)}


def exited_children(attempt):
    return [s['name'] for s in children(attempt) if s.get('phase') == 'exit']


def kill_child(attempt, **where):
    """SIGKILL one live registered child matching ``where`` (through its pidfd, identity-checked); returns its registry state."""
    from neo4j_graph_loader.orchestrator.process_service import _signal_child
    found = live(attempt, **where)
    assert found, f'no live child matches {where}'
    _signal_child(found[0], signal.SIGKILL)
    return found[0]


def child_log(attempt, state):
    path = attempt / 'logs' / f'{state["name"]}.log'
    return path.read_text(errors='replace') if path.is_file() else ''


def count_relationships(stack):
    with stack.driver.session(database=stack.database) as session:
        return session.run('MATCH (:Citizen)-[r:WORKS_FOR|BUYS_FROM|KNOWS_OF]->() RETURN count(r) AS n').single()['n']



@contextlib.contextmanager
def producing(stack, topics, batch):
    """Publish a batch's relationships from a thread while the body runs, so an event (a kill, a stop) lands on work in flight."""
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


def mid_load(stack, cli, timeout=120):
    """Wait, polling a cheap count, until some but not all of the big batch's relationships exist; returns the count seen."""
    end = time.monotonic() + timeout
    while True:
        assert cli.running(), 'the streaming CLI exited while waiting for relationships in flight: ' + cli.text()[-1500:]
        count = count_relationships(stack)
        if 0 < count < TOTAL_BIG:
            return count
        assert count == 0 or time.monotonic() < end, f'the whole batch ({count}) loaded between two polls: no work was in flight'
        assert time.monotonic() < end, 'timed out waiting for relationships in flight'
        time.sleep(0.05)


def wait_exit(cli, timeout, what):
    end = time.monotonic() + timeout
    while cli.running():
        assert time.monotonic() < end, what
        time.sleep(0.5)
    return cli.process.poll()


def run_dir(config, name):
    path = Path(config['run_dir']) / name
    path.mkdir()
    return path


def finished(stack, cli, attempt, components, *, code):
    """The CLI has ended with ``code``, no secret reached its output, every child has an exit receipt and none is alive, and
    the registry started exactly ``components`` children (replacements included)."""
    cli._finish()
    assert cli.process.poll() == code, (code, cli.process.poll(), cli.text()[-2500:])
    assert cli.redactions == 0, 'the scratch password reached CLI output'
    assert_clean(attempt)
    assert components_started(attempt) == components, components_started(attempt)


class Fleet:
    """One ``graph-loader start --mode stream --runtime process`` under test: wait on predicates, stop it the ordered way."""

    def __init__(self, stack, config, run, name, schema_text, *, extra=()):
        self.stack, self.config, self.run = stack, config, run
        self.cli = start_stream(stack, config, run, name, schema_text, extra=extra)
        self.attempt = the_attempt(run)
        self.code = None

    def wait(self, what, predicate, timeout):
        return wait_for(what, predicate, timeout, cli=self.cli)

    def stop(self):
        """``graph-loader stop`` from a second shell; returns the CLI's exit code."""
        self.code, _ = stop_via_second_shell(self.config, self.stack, self.run, self.attempt, self.cli)
        return self.code

    def close(self):
        if self.cli.process.poll() is None:
            self.cli.kill()
        self.cli._finish()

    def shell(self, *args, timeout=60):
        return second_shell(self.config, self.stack, self.run, self.attempt, *args, timeout=timeout)


def recorded_alive(attempt):
    """Every process the registry recorded for the attempt (children and externals) that still runs, identity re-verified."""
    from neo4j_graph_loader.orchestrator.run_registry import process_identity
    alive = []
    for state in fold(records(attempt)).values():
        if state.get('phase') == 'exit' or 'pid' not in state:
            continue
        current = process_identity(int(state['pid']))
        if current is not None and (current.get('start_time'), current.get('boot_id')) == (state.get('start_time'), state.get('boot_id')):
            alive.append(state)
    return alive


def lifeline_seconds(attempt, since, *, besides=(), timeout=60):
    """Seconds from ``since`` (the moment the CLI was killed) until no recorded process but ``besides`` is alive."""
    end = time.monotonic() + timeout
    while True:
        left = [s['name'] for s in recorded_alive(attempt) if s['name'] not in besides]
        if not left:
            return time.monotonic() - since
        assert time.monotonic() < end, f'the lifeline left {left} alive {timeout}s after the CLI died'
        time.sleep(0.02)


def kill_cli(cli):
    """SIGKILL the CLI process only; returns the monotonic time of the kill."""
    since = time.monotonic()
    os.kill(cli.process.pid, signal.SIGKILL)
    cli.process.wait(timeout=10)
    return since


def attempt_files(attempt):
    """{relative path: (size, mtime_ns)} of the planner artifacts an attempt keeps: plans/, spill/, rejections/ and any lock file."""
    found = {}
    for sub in ('plans', 'spill', 'rejections'):
        for path in (attempt / sub).rglob('*'):
            if path.is_file():
                info = path.stat()
                found[str(path.relative_to(attempt))] = (info.st_size, info.st_mtime_ns)
    for path in attempt.rglob('*.lock'):
        info = path.stat()
        found[str(path.relative_to(attempt))] = (info.st_size, info.st_mtime_ns)
    return found


def free_port_for_health():
    """A free loopback port for ``--health-addr``."""
    import socket
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]
