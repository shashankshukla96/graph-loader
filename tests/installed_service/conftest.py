"""Fixtures for the live installed-service lanes (native shared Docker Engine substrate)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import time

import docker
import pytest

from tests.installed_service.live_stack import LiveStack, Registry


@pytest.fixture(scope='session')
def service_config():
    path = os.environ.get('E09_SERVICE_CONFIG')
    if not path:
        pytest.fail('E09_SERVICE_CONFIG is required: live service tests run only through scripts/verify_installed.py')
    config = json.loads(Path(path).read_text())
    for key in ('docker_host', 'run_dir', 'images', 'payload', 'venv_bin', 'deadline_seconds'):
        assert key in config, key
    return config


@pytest.fixture(scope='session')
def docker_client(service_config):
    client = docker.DockerClient(base_url=service_config['docker_host'], timeout=30)
    yield client
    client.close()


@pytest.fixture(scope='session')
def registry(service_config):
    return Registry(Path(service_config['run_dir']) / 'registry.jsonl', secrets.token_hex(16))


@pytest.fixture(scope='session')
def stack(docker_client, registry, service_config):
    live = LiveStack(docker_client, registry, deadline=time.monotonic() + service_config['deadline_seconds'],
                     prefix=service_config.get('prefix', 'e09'), limits=service_config.get('limits'),
                     expected_images=service_config.get('expected_images'), pin_neo_port=bool(service_config.get('pin_neo_port')))
    try:
        live.start()
        yield live
    finally:
        result = live.close()
        registry.write('close', **result)
        if result['failed']:
            raise RuntimeError('owned stack cleanup failed: ' + repr(result['failed']))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    setattr(item, 'rep_' + call.when, outcome.get_result())


@pytest.fixture(autouse=True)
def wipe_the_owned_graph_after_a_failed_test(request):
    """The tests share one scratch Neo4j and each ends with its own ``clear``; a test that fails first never reaches it. What it
    left behind then pushes a later whole-database graph observation past its byte bound, and an abandoned observation leaves a
    started-but-never-completed query in the query log that every later strict capture waits for in vain. Whatever fails, the next
    test therefore starts on an empty scratch graph."""
    yield
    report = getattr(request.node, 'rep_call', None)
    if report is None or not report.failed or 'stack' not in request.fixturenames:
        return
    live = request.getfixturevalue('stack')
    try:
        with live.driver.session(database=live.database) as session:
            while session.run('MATCH (n) WITH n LIMIT 2000 DETACH DELETE n RETURN count(*) AS deleted').single()['deleted']:
                pass
    except Exception as error:  # the original failure is the one to report
        print('could not wipe the owned graph after a failed test:', type(error).__name__)

