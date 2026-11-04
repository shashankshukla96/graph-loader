"""E14 D14-5 live: a stream schema without relationship types under ``--runtime process`` is supervised in the foreground.

Docker mode returns 0 and leaves the node containers running; the process runtime cannot (the roles belong to the command), so the
command stays up as their supervisor: nodes load, ``stop`` from a second shell ends it with exit 0, and a node loader killed with
SIGKILL ends it with exit 1 and no survivor."""
from __future__ import annotations

import time

import pytest

from tests.installed_service import stream_fixture as sf
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    assert_clean, children, kill_child, memory_guard, run_dir, second_shell, start_stream, stream_clear, stream_own_graph, stream_topics,
    the_attempt, wait_exit, wait_for, live,
)

pytestmark = pytest.mark.integration


def node_only_schema(stack, topics, group):
    text = sf.schema(stack.topic_prefix, topics, group)
    return text[:text.index('edges:')].rstrip() + '\nedges: []\n'


def start_node_only(stack, config, tag):
    run = run_dir(config, f'p-node-only-{tag}')
    stream_clear(stack)
    topics = stream_topics(stack, tag)
    group = f'{stack.topic_prefix}{tag}-group'
    cli = start_stream(stack, config, run, 'start', node_only_schema(stack, topics, group))
    return run, topics, cli, the_attempt(run)


def test_node_only_stream_stays_up_loads_nodes_and_stops_with_exit_zero(stack, service_config):
    run, topics, cli, attempt = start_node_only(stack, service_config, 'no1')
    try:
        wait_for('the node loaders', lambda: len(live(attempt, component='node-loader')) == 2, 120, cli=cli)
        sf.produce_nodes(stack, topics, 'a', with_firms=True)
        nodes, _ = sf.expected(['a'])
        wait_for('the nodes', lambda: stream_own_graph(stack)[0] == nodes, 180, cli=cli)
        time.sleep(5)
        assert cli.running(), 'the command must stay up as the loaders\' supervisor (Docker mode would have returned 0 by now)'
        shell = second_shell(service_config, stack, run, attempt, 'stop')
        assert shell.exit_code == 0, shell.tail
        code = wait_exit(cli, 60, 'the ordered stop must end the command')
    finally:
        if cli.process.poll() is None:
            cli.kill()
    cli._finish()
    assert code == 0, (code, cli.text()[-2000:])
    assert 'supervising 2 node loader(s) until stopped' in cli.text()
    assert_clean(attempt)
    assert sorted(s.get('code') for s in children(attempt)) == [0, 0]          # Citizen, Firm
    stream_clear(stack)


def test_node_only_stream_fails_with_exit_one_when_a_node_loader_dies(stack, service_config):
    run, topics, cli, attempt = start_node_only(stack, service_config, 'no2')
    try:
        wait_for('the node loaders', lambda: len(live(attempt, component='node-loader')) == 2, 120, cli=cli)
        time.sleep(3)
        killed = kill_child(attempt, component='node-loader')
        code = wait_exit(cli, 60, 'a lost node loader must stop the run')
    finally:
        if cli.process.poll() is None:
            cli.kill()
    cli._finish()
    assert code == 1 and 'node loader exited while the stream run was active' in cli.text(), (code, cli.text()[-2000:])
    assert_clean(attempt)
    assert next(s for s in children(attempt) if s['id'] == killed['id'])['code'] == 137
    stream_clear(stack)
