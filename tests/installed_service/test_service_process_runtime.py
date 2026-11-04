"""E14.F2.S4 live role matrix: ``graph-loader start --runtime process`` on owned Neo4j/Kafka (Linux host).

The CLI runs every role as a child process; no Docker daemon is involved (``DOCKER_HOST`` points at a socket that does not
exist, so a stray Docker call fails loudly). The owned stack is the same ``LiveStack`` the Docker lanes use, reached through
its published loopback ports. Observation is the run's own registry (``registry.jsonl``) instead of container events.

Cells (each compares the whole fixture graph with the closed-form oracle that the E09 Docker lanes assert - exact sorted JSON):

* planned bulk (default) with ``--bulk-edge-overlap`` unset / ``labels`` / ``shared`` (the option is classic-only and ignored with a
  warning), ``--edge-classic`` with ``labels`` / ``shared``, ``--edge-leases``;
* stream (bucket-lease fleet): load, ``status`` and ordered ``stop`` from a second shell (exit 0), resume;
* stream with a lease-server kill: the replacement rebinds the same port and the run continues; with the port occupied the
  replacement cannot start and the run fails closed (exit 1);
* bulk ``--edge-leases`` with a lease-server kill: the run fails (bulk never restarts the server);
* deprecated ``--stream-clock-rotation`` on a schema without self-references;
* ``stop --loader`` from a second shell fails the stream run like Docker mode does.

Every cell ends with the same cleanliness check: every child the registry started has an exit receipt and no registered
child is alive. Not covered here: Neo4j outages, DLQ, replica scaling (S7).
"""
from __future__ import annotations

import json
import re
import signal
import socket
import time
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator.process_service import _signal_child, registered_children
from tests.installed_service import bulk_fixture as fx
from tests.installed_service import stream_fixture as sf
from tests.installed_service.live_cli import run_cli
from tests.installed_service.observers import graph_observations
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    CLI_TIMEOUT,
    NO_DOCKER,
    START_MIN_AVAILABLE_KB,
    STOP_BELOW_AVAILABLE_KB,
    SAMPLE_SECONDS,
    available_kb,
    MemoryWatch,
    memory_guard,
    process_env,
    executable,
    attempt_dirs,
    the_attempt,
    records,
    children,
    live,
    wait_for,
    second_shell,
    assert_clean,
    assert_no_secret,
    start_stream,
    run_to_completion,
    stream_own_graph,
    stream_clear,
    stream_topics,
    stream_committed,
    stream_ends,
    stop_via_second_shell,
    lease_port,
    with_fast_restarts,
    loaded_and_supervising,
)

pytestmark = pytest.mark.integration


# -- bulk fixture helpers -----------------------------------------------------------------------------------------
def bulk_own_graph(stack):
    return fx.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def bulk_clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Worker|Employer) DETACH DELETE n').consume()


def bulk_topics(stack, tag, scale=(fx.WORKERS, fx.EMPLOYERS)):
    topics = {key: f'{stack.topic_prefix}{tag}-{key}' for key in ('worker', 'employer', 'edge')}
    stack.create_topics(list(topics.values()))
    fx.produce(stack, topics, *scale)
    return topics


def bulk_cell(stack, service_config, tag, *, extra=(), scale=(fx.WORKERS, fx.EMPLOYERS)):
    run = Path(service_config['run_dir']) / f'p4-{tag}'
    run.mkdir()
    bulk_clear(stack)
    topics = bulk_topics(stack, tag, scale)
    group = f'{stack.topic_prefix}{tag}-group'
    result, attempt = run_to_completion(stack, service_config, run, tag, fx.schema(stack.topic_prefix, topics, group),
                                        extra=(*extra,))
    text = result.stderr.read_text() + result.stdout.read_text()
    assert result.timed_out is False, text[-2000:]
    assert result.exit_code == 0, (result.exit_code, text[-3000:])
    assert_no_secret(result, stack)
    nodes, relationships = fx.expected_nodes(*scale), fx.expected_relationships(*scale)
    assert bulk_own_graph(stack) == (nodes, relationships), 'the process-mode graph differs from the fixture oracle'
    assert_clean(attempt)
    bulk_clear(stack)
    return text, attempt


# -- bulk cells ---------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('overlap', [None, 'labels', 'shared'])
def test_p4_planned_bulk(overlap, stack, service_config):
    extra = (*fx.PLAN_ARGS, *(('--bulk-edge-overlap', overlap) if overlap else ()))
    text, attempt = bulk_cell(stack, service_config, f'planned-{overlap or "default"}', extra=extra)
    assert 'stage=planned-edges-complete' in text
    kinds = {s['labels'].get('component') for s in children(attempt)}
    assert {'node-loader', 'planned-dispatcher', 'planned-edge-writer'} <= kinds
    plan = json.loads((attempt / 'plans' / 'edge-plan.json').read_bytes())
    assert plan['chunks'], 'the plan is empty'


@pytest.mark.parametrize('overlap', ['labels', 'shared'])
def test_p4_edge_classic(overlap, stack, service_config):
    text, attempt = bulk_cell(stack, service_config, f'classic-{overlap}', extra=('--edge-classic', '--bulk-edge-overlap', overlap))
    assert {s['labels'].get('component') for s in children(attempt)} >= {'node-loader', 'edge-loader'}


def test_p4_edge_leases(stack, service_config):
    text, attempt = bulk_cell(stack, service_config, 'leases', extra=('--edge-leases',))
    assert {s['labels'].get('component') for s in children(attempt)} >= {'node-loader', 'edge-loader', 'bucket-lease-server'}


def test_p4_stream_load_status_stop_resume(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-stream'
    run.mkdir()
    stream_clear(stack)
    topics = stream_topics(stack, 's')
    group = f'{stack.topic_prefix}s-group'
    schema_text = sf.schema(stack.topic_prefix, topics, group)
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    nodes_a, rels_a = sf.expected(['a'])
    cli = start_stream(stack, service_config, run, 'start-1', schema_text)
    attempt = the_attempt(run)
    try:
        wait_for('nodes', lambda: stream_own_graph(stack)[0] == nodes_a, 180, cli=cli)
        sf.produce_edges(stack, topics, 'a')
        wait_for('relationships', lambda: stream_own_graph(stack) == (nodes_a, rels_a), 180, cli=cli)
        end_a = stream_ends(stack, topics)
        wait_for('committed offsets at the log ends', lambda: stream_committed(stack, group, topics) == end_a, 120, cli=cli)
        status = second_shell(service_config, stack, run, attempt, 'status')
        text = status.stderr.read_text() + status.stdout.read_text()
        assert status.exit_code == 0 and 'Loader ' in text and '[Node: Citizen]' in text and '[Node: Firm]' in text, text
        kinds = {s['labels'].get('component') for s in children(attempt)}
        assert kinds >= {'node-loader', 'edge-loader', 'bucket-lease-server'}
        code, seconds = stop_via_second_shell(service_config, stack, run, attempt, cli)
    finally:
        if cli.process.poll() is None:
            cli.kill()
    assert code == 0, ('an ordered second-shell stop must end the stream run with exit 0', code, cli.text()[-2000:])
    assert seconds < 60
    assert_no_secret(cli, stack)
    assert stream_own_graph(stack) == (nodes_a, rels_a)
    assert_clean(attempt)

    # resume from the stable groups with a new CLI: a batch published during the stop is loaded
    sf.produce_nodes(stack, topics, 'b', with_firms=False)
    sf.produce_edges(stack, topics, 'b')
    nodes_ab, rels_ab = sf.expected(['a', 'b'])
    second_run = Path(service_config['run_dir']) / 'p4-stream-2'
    second_run.mkdir()
    cli2 = start_stream(stack, service_config, second_run, 'start-2', schema_text)
    attempt2 = the_attempt(second_run)
    try:
        wait_for('the batch published during the stop', lambda: stream_own_graph(stack) == (nodes_ab, rels_ab), 240, cli=cli2)
        code2, _ = stop_via_second_shell(service_config, stack, second_run, attempt2, cli2)
    finally:
        if cli2.process.poll() is None:
            cli2.kill()
    assert code2 == 0
    assert_clean(attempt2)
    stream_clear(stack)


def test_p4_stream_lease_server_kill_replacement_rebinds_the_same_port(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-lease-kill'
    run.mkdir()
    stream_clear(stack)
    topics = stream_topics(stack, 'k')
    group = f'{stack.topic_prefix}k-group'
    schema_text = with_fast_restarts(sf.schema(stack.topic_prefix, topics, group))
    cli = start_stream(stack, service_config, run, 'start', schema_text)
    attempt = the_attempt(run)
    try:
        port = loaded_and_supervising(stack, cli, topics, attempt)
        first = live(attempt, component='bucket-lease-server')
        assert len(first) == 1
        _signal_child(first[0], signal.SIGKILL)
        replacement = wait_for('a replacement lease server', lambda: [s for s in live(attempt, component='bucket-lease-server')
                                                                      if s['id'] != first[0]['id']], 120, cli=cli)
        assert lease_port(attempt) == port, 'the replacement must rebind the same port'
        nodes_ab, rels_ab = sf.expected(['a', 'b'])
        sf.produce_nodes(stack, topics, 'b', with_firms=False)
        wait_for('the new nodes', lambda: stream_own_graph(stack)[0] == nodes_ab, 180, cli=cli)   # endpoints first: no DLQ here
        sf.produce_edges(stack, topics, 'b')
        wait_for('the batch published after the replacement', lambda: stream_own_graph(stack) == (nodes_ab, rels_ab), 300, cli=cli)
        assert replacement
        code, _ = stop_via_second_shell(service_config, stack, run, attempt, cli)
    finally:
        if cli.process.poll() is None:
            cli.kill()
    assert code == 0, (code, cli.text()[-2500:])
    assert 'stage=lease-server-restart' in cli.text()
    assert_clean(attempt)
    stream_clear(stack)


def test_p4_stream_lease_server_kill_with_the_port_occupied_fails_the_run(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-lease-rebind-fail'
    run.mkdir()
    stream_clear(stack)
    topics = stream_topics(stack, 'r')
    group = f'{stack.topic_prefix}r-group'
    schema_text = with_fast_restarts(sf.schema(stack.topic_prefix, topics, group))
    cli = start_stream(stack, service_config, run, 'start', schema_text)
    attempt = the_attempt(run)
    squatter = None
    try:
        port = loaded_and_supervising(stack, cli, topics, attempt)
        first = live(attempt, component='bucket-lease-server')[0]
        squatter = socket.socket()
        squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _signal_child(first, signal.SIGKILL)
        end = time.monotonic() + 15
        while True:                                   # take the freed port before the supervisor's replacement does
            try:
                squatter.bind(('127.0.0.1', port))
                squatter.listen()
                break
            except OSError:
                assert time.monotonic() < end, 'could not occupy the lease port'
                time.sleep(0.01)
        wait_for('the run to fail closed', lambda: cli.process.poll() is not None, 180)
    finally:
        if squatter is not None:
            squatter.close()
        if cli.process.poll() is None:
            cli.kill()
    cli._finish()
    text = cli.text()
    assert cli.process.returncode == 1, (cli.process.returncode, text[-2500:])
    assert 'did not become ready' not in text, 'the run must fail in supervision, not in the start-up readiness wait'
    assert re.search(r'lease server exceeded \d+ restarts|lease-server|bucket-lease-server', text), text[-2500:]
    assert_clean(attempt)
    stream_clear(stack)


def test_p4_bulk_lease_server_kill_fails_the_run(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-bulk-lease-kill'
    run.mkdir()
    bulk_clear(stack)
    scale = (3000, 30)
    topics = bulk_topics(stack, 'bl', scale)
    group = f'{stack.topic_prefix}bl-group'
    cli = start_stream(stack, service_config, run, 'start', fx.schema(stack.topic_prefix, topics, group), mode='bulk',
                       extra=('--edge-leases',))
    attempt = the_attempt(run)
    try:
        server = wait_for('the lease server', lambda: live(attempt, component='bucket-lease-server'), 180, cli=cli)
        _signal_child(server[0], signal.SIGKILL)
        wait_for('the run to fail', lambda: cli.process.poll() is not None, 180)
    finally:
        if cli.process.poll() is None:
            cli.kill()
    cli._finish()
    assert cli.process.returncode == 1, (cli.process.returncode, cli.text()[-2500:])
    assert_clean(attempt)
    bulk_clear(stack)


def test_p4_stop_loader_from_a_second_shell_fails_the_stream_run(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-stop-loader'
    run.mkdir()
    stream_clear(stack)
    topics = stream_topics(stack, 'l')
    group = f'{stack.topic_prefix}l-group'
    sf.produce_nodes(stack, topics, 'a', with_firms=True)
    nodes_a, _ = sf.expected(['a'])
    cli = start_stream(stack, service_config, run, 'start', sf.schema(stack.topic_prefix, topics, group))
    attempt = the_attempt(run)
    try:
        wait_for('nodes', lambda: stream_own_graph(stack)[0] == nodes_a, 180, cli=cli)
        shell = second_shell(service_config, stack, run, attempt, 'stop', '--loader', 'Citizen')
        assert shell.exit_code == 0, shell.tail
        wait_for('the run to fail', lambda: cli.process.poll() is not None, 120)
    finally:
        if cli.process.poll() is None:
            cli.kill()
    cli._finish()
    assert cli.process.returncode == 1 and 'node loader exited' in cli.text(), (cli.process.returncode, cli.text()[-2500:])
    assert_clean(attempt)
    stream_clear(stack)


def test_p4_stream_clock_rotation_on_a_schema_without_self_references(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-clock'
    run.mkdir()
    bulk_clear(stack)
    topics = {key: f'{stack.topic_prefix}clk-{key}' for key in ('worker', 'employer', 'edge')}
    stack.create_topics(list(topics.values()))
    group = f'{stack.topic_prefix}clk-group'
    schema_text = fx.schema(stack.topic_prefix, topics, group).replace('mode: bulk', 'mode: stream', 1)
    fx.produce(stack, topics)
    nodes, relationships = fx.expected_nodes(), fx.expected_relationships()
    cli = start_stream(stack, service_config, run, 'start', schema_text, extra=('--stream-clock-rotation',))
    attempt = the_attempt(run)
    try:
        wait_for('the graph', lambda: bulk_own_graph(stack) == (nodes, relationships), 300, cli=cli)
        assert {s['labels'].get('component') for s in children(attempt)} >= {'node-loader', 'edge-loader', 'global-batch-clock'}
        code, _ = stop_via_second_shell(service_config, stack, run, attempt, cli)
    finally:
        if cli.process.poll() is None:
            cli.kill()
    assert code == 0, (code, cli.text()[-2500:])
    assert_clean(attempt)
    bulk_clear(stack)


def test_p4_docker_only_options_are_rejected_before_anything_starts(stack, service_config):
    run = Path(service_config['run_dir']) / 'p4-reject'
    (run / 'logs').mkdir(parents=True)
    for index, flags in enumerate((['--network', 'graph-loader-net'], ['--skip-image-build'])):
        result = run_cli(executable(service_config), ['start', '--mode', 'bulk', '--runtime', 'process', *flags], env=process_env(stack),
                         cwd=run, log_dir=run / 'logs', name=f'reject-{index}', timeout=60, redact=[stack.password])
        assert result.exit_code == 1 and 'applies to Docker containers' in result.tail
    assert not (run / 'runs').exists()


def test_p4_no_process_left_from_this_module(service_config):
    """Last cell: nothing from any run directory of this module is still alive."""
    root = Path(service_config['run_dir'])
    survivors = []
    for registry in root.glob('p4-*/runs/*/registry.jsonl'):
        survivors += [s['name'] for s in registered_children(registry)]
    assert survivors == []
