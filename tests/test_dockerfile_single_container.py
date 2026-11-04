"""E14.F3.S8 static controls on the CLI image: tini is PID 1, and nothing of the image contract or of Docker mode changes.

The live acceptance (the container itself) is ``tests/installed_service/test_service_single_container.py``; these read the Dockerfiles
and the frozen 0.1.0a1 parity golden only, so they run anywhere.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests.installed_service.container_support import dockerfile_entrypoint

ROOT = Path(__file__).resolve().parents[1]
CLI = (ROOT / 'Dockerfile').read_text()
ROLE_ENTRYPOINTS = {
    'Dockerfile.node_loader': ['python', '-m', 'neo4j_graph_loader.loader.node_loader'],
    'Dockerfile.edge_loader': ['python', '-m', 'neo4j_graph_loader.loader.edge_loader'],
    'Dockerfile.clock': ['python', '-m', 'neo4j_graph_loader.orchestrator.clock_runner'],
}


def instructions(text):
    """Dockerfile instructions with continuation lines joined and comments dropped."""
    joined = re.sub(r'\\\n\s*', ' ', text)
    return [re.sub(r'\s+', ' ', line.strip()) for line in joined.splitlines() if line.strip() and not line.lstrip().startswith('#')]


def test_tini_is_pid_one_in_exec_form_and_arguments_still_reach_the_cli() -> None:
    assert dockerfile_entrypoint(ROOT / 'Dockerfile') == ['tini', '--', 'python', '-m', 'neo4j_graph_loader.cli']
    lines = instructions(CLI)
    assert sum(line.startswith('ENTRYPOINT') for line in lines) == 1
    assert not any(line.startswith(('CMD', 'USER', 'HEALTHCHECK', 'STOPSIGNAL')) for line in lines), 'the CLI image contract has none of these'
    assert '-g' not in dockerfile_entrypoint(ROOT / 'Dockerfile'), 'tini -g would signal every process group before the CLI stops its planners'


def test_tini_is_installed_from_the_distribution_and_its_version_is_asserted_at_build() -> None:
    run = next(line for line in instructions(CLI) if line.startswith('RUN apt-get'))
    assert 'apt-get install -y --no-install-recommends tini' in run
    assert 'rm -rf /var/lib/apt/lists/*' in run
    assert 'tini --version | grep -F "tini version ${TINI_VERSION}"' in run
    assert re.search(r'^ARG TINI_VERSION=\d+\.\d+\.\d+$', CLI, re.M)
    assert CLI.index('apt-get install') < CLI.index('pip install'), 'tini is installed before the dependency layers so they stay cached'


def test_image_contract_labels_and_version_check_are_unchanged() -> None:
    lines = instructions(CLI)
    label = next(line for line in lines if line.startswith('LABEL'))
    assert label == ('LABEL org.opencontainers.image.version=$GRAPH_LOADER_VERSION '
                     'org.opencontainers.image.source="https://github.com/shashankshukla96/graph-loader" '
                     'io.graph-loader.component="cli" io.graph-loader.image-contract="1"')
    assert 'ARG GRAPH_LOADER_VERSION' in lines
    assert any(line.startswith('RUN python -c') and 'm.version("neo4j-graph-loader") == sys.argv[1]' in line
               and line.endswith('"$GRAPH_LOADER_VERSION"') for line in lines)
    assert 'WORKDIR /app' in lines and 'ENV PYTHONUNBUFFERED=1' in lines
    assert lines[0] == 'FROM python:3.12-slim'


@pytest.mark.parametrize('dockerfile,entrypoint', ROLE_ENTRYPOINTS.items())
def test_role_images_are_untouched(dockerfile: str, entrypoint: list) -> None:
    text = (ROOT / dockerfile).read_text()
    assert dockerfile_entrypoint(ROOT / dockerfile) == entrypoint
    assert 'tini' not in text and 'apt-get' not in text, 'Docker mode starts roles with Docker\'s own init; only the CLI image gains tini'


def test_docker_mode_never_runs_a_container_from_the_cli_image() -> None:
    """Docker mode's role containers use the node/edge/clock images with an explicit entrypoint, so the CLI image's entrypoint is
    irrelevant to them: the frozen parity golden names no CLI image and records only ``python -m <role module>`` entrypoints."""
    golden = (ROOT / 'tests' / 'golden' / 'docker_parity_0.1.0a1.json').read_text()
    assert 'graph-loader-cli' not in golden
    entrypoints = re.findall(r'"entrypoint": (\[[^\]]*\])', golden)
    assert entrypoints
    for found in {json.dumps(json.loads(e)) for e in entrypoints}:
        value = json.loads(found)
        assert value[:2] == ['python', '-m'] and 'tini' not in value, value
