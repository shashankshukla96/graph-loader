"""E14.F3.S11: the quick starts of ``docs/PROCESS_RUNTIME.md`` section 3, run literally with the freshly installed wheel on the host.

One ``--run-dir`` base for everything, as the page shows: validate the schema, a bulk load, then a stream run in the foreground
with ``--health-addr``, ``/readyz`` from a second shell, ``status`` and ``stop`` addressed by the *base* directory (two attempts live
under it), the start command exits 0; plus the refusals the page documents."""
from __future__ import annotations

import re
import urllib.request

import pytest

from tests.installed_service import bulk_fixture as fx
from tests.installed_service import stream_fixture as sf
from tests.installed_service.live_cli import run_cli
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    assert_clean, executable, free_port_for_health, memory_guard, process_env, run_dir, run_to_completion, second_shell, start_stream,
    stream_clear, stream_own_graph, stream_topics, the_attempt, wait_exit, wait_for,
)
from tests.installed_service.test_service_process_runtime import bulk_clear, bulk_own_graph, bulk_topics

pytestmark = pytest.mark.integration


def cli(config, stack, run, name, *args, timeout=120):
    return run_cli(executable(config), list(args), env=process_env(stack), cwd=run, log_dir=run / 'logs', name=name, timeout=timeout,
                   redact=[stack.password])


def test_the_documented_quick_starts_work_with_the_fresh_wheel(stack, service_config):
    run = run_dir(service_config, 'qs')
    (run / 'logs').mkdir()
    base = run / 'runs'
    base.mkdir()

    version = cli(service_config, stack, run, 'version', '--version')
    assert version.exit_code == 0 and re.search(r'\d+\.\d+\.\d+', version.stdout.read_text() + version.stderr.read_text())

    # -- bulk quick start ----------------------------------------------------------------------------------------------------
    bulk_clear(stack)
    scale = (600, 12)
    topics = bulk_topics(stack, 'qb', scale)
    bulk_yaml = run / 'my_graph.yaml'
    bulk_yaml.write_text(fx.schema(stack.topic_prefix, topics, f'{stack.topic_prefix}qb-group'))
    validated = cli(service_config, stack, run, 'validate', 'validate-schema', '--config', str(bulk_yaml))
    assert validated.exit_code == 0, validated.tail
    bulk = cli(service_config, stack, run, 'bulk', 'start', '--mode', 'bulk', '--runtime', 'process', '--config', str(bulk_yaml),
               '--run-dir', str(base), timeout=300)
    text = bulk.stderr.read_text() + bulk.stdout.read_text()
    assert bulk.exit_code == 0, text[-2500:]
    assert re.search(r'stage=runtime directory=\S+', text) and 'stage=sizing' in text, 'the lines the page names'
    assert bulk_own_graph(stack) == (fx.expected_nodes(*scale), fx.expected_relationships(*scale))
    bulk_attempt = the_attempt(run)
    assert_clean(bulk_attempt)
    bulk_clear(stack)

    # -- stream quick start --------------------------------------------------------------------------------------------------
    stream_clear(stack)
    stopics = stream_topics(stack, 'qs')
    sgroup = f'{stack.topic_prefix}qs-group'
    health = free_port_for_health()
    sf.produce_nodes(stack, stopics, 'a', with_firms=True)
    nodes, rels = sf.expected(['a'])
    live_cli = start_stream(stack, service_config, run, 'stream', sf.schema(stack.topic_prefix, stopics, sgroup),
                            extra=('--health-addr', f'127.0.0.1:{health}'))
    # start_stream used its own --run-dir (run/runs) = the same base: two attempts now live under it
    try:
        def ready():
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{health}/readyz', timeout=3) as response:
                    return response.status == 200
            except OSError:
                return False
        wait_for('/readyz 200', ready, 240, cli=live_cli, step=1)
        with urllib.request.urlopen(f'http://127.0.0.1:{health}/healthz', timeout=3) as response:        # liveness: the supervision heartbeat
            assert response.status == 200
        wait_for('the nodes', lambda: stream_own_graph(stack)[0] == nodes, 180, cli=live_cli)
        sf.produce_edges(stack, stopics, 'a')
        wait_for('relationships', lambda: stream_own_graph(stack) == (nodes, rels), 240, cli=live_cli)
        status = cli(service_config, stack, run, 'status', 'status', '--config', str(run / 'stream.yaml'), '--runtime', 'process',
                     '--run-dir', str(base))
        stext = status.stderr.read_text() + status.stdout.read_text()
        assert status.exit_code == 0 and '[Node: Citizen]' in stext and '[Node: Firm]' in stext, stext
        stop = cli(service_config, stack, run, 'stop', 'stop', '--runtime', 'process', '--run-dir', str(base))
        assert stop.exit_code == 0, stop.tail
        code = wait_exit(live_cli, 90, 'the start command must exit after `stop`')
    finally:
        if live_cli.process.poll() is None:
            live_cli.kill()
    live_cli._finish()
    assert code == 0, (code, live_cli.text()[-2000:])
    assert stream_own_graph(stack) == (nodes, rels)
    stream_clear(stack)

    # -- refusals the page documents ------------------------------------------------------------------------------------------
    for name, args, needle in (
            ('refuse-recover', ('stop', '--runtime', 'docker', '--recover'), '--recover applies to --runtime process only'),
            ('refuse-metrics', ('start', '--mode', 'stream', '--config', str(bulk_yaml), '--runtime', 'docker', '--metrics-addr', '127.0.0.1'),
             '--metrics-addr applies to --runtime process only'),
            ('refuse-network', ('start', '--mode', 'stream', '--config', str(bulk_yaml), '--runtime', 'process', '--network', 'x'),
             '--network applies to Docker containers and is not accepted with --runtime process'),
            ('refuse-env', ('stop',), 'GRAPH_LOADER_RUNTIME must be one of docker, process')):
        env = process_env(stack, {'GRAPH_LOADER_RUNTIME': 'bogus'} if name == 'refuse-env' else None)
        result = run_cli(executable(service_config), list(args), env=env, cwd=run, log_dir=run / 'logs', name=name, timeout=60,
                         redact=[stack.password])
        assert result.exit_code == 1 and needle in result.stderr.read_text() + result.stdout.read_text(), (name, result.tail)
    assert_clean(the_attempt_of_stream(base, bulk_attempt))


def the_attempt_of_stream(base, bulk_attempt):
    found = [p for p in base.iterdir() if (p / 'registry.jsonl').is_file() and p != bulk_attempt]
    assert len(found) == 1, found
    return found[0]
