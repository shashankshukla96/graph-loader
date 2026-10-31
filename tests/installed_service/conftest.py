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
    live = LiveStack(docker_client, registry, deadline=time.monotonic() + service_config['deadline_seconds'])
    try:
        live.start()
        yield live
    finally:
        result = live.close()
        registry.write('close', **result)
        if result['failed']:
            raise RuntimeError('owned stack cleanup failed: ' + repr(result['failed']))
