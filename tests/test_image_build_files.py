"""E19.F2.S5: the four image Dockerfiles take everything from a generated build context and carry no credential.

Static controls; the builds themselves are proved by the image build harness (a BuildKit builder, an owned index and mirror).
"""
from __future__ import annotations

from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE_DIR = ROOT / 'src' / 'neo4j_graph_loader' / 'image_build' / 'dockerfiles'
DOCKERFILES = {'cli': 'Dockerfile', 'node': 'Dockerfile.node_loader', 'edge': 'Dockerfile.edge_loader', 'clock': 'Dockerfile.clock'}
ENTRYPOINTS = {
    'cli': ['tini', '--', 'python', '-m', 'neo4j_graph_loader.cli'],
    'node': ['python', '-m', 'neo4j_graph_loader.loader.node_loader'],
    'edge': ['python', '-m', 'neo4j_graph_loader.loader.edge_loader'],
    'clock': ['python', '-m', 'neo4j_graph_loader.orchestrator.clock_runner'],
}
# The only build arguments; none of them can carry a credential (the index URL arrives as a BuildKit secret, INDEX_URL is for a mirror without one).
ARGS = {'BASE_IMAGE', 'GRAPH_LOADER_VERSION', 'PACKAGE_WHEEL', 'PACKAGE_WHEEL_SHA256', 'DEPENDENCY_SOURCE', 'INDEX_URL', 'INSTALL_FLAGS',
        'TINI_VERSION', 'DEBIAN_MIRROR'}


def text(role):
    return (DOCKERFILE_DIR / DOCKERFILES[role]).read_text(encoding='utf-8')


def instructions(content):
    joined = re.sub(r'\\\n\s*', ' ', content)
    return [re.sub(r'\s+', ' ', line.strip()) for line in joined.splitlines() if line.strip() and not line.lstrip().startswith('#')]


@pytest.mark.parametrize('role', sorted(DOCKERFILES))
def test_the_base_image_is_a_build_argument_and_nothing_pulls_a_frontend(role):
    lines = instructions(text(role))
    assert lines[0] == 'ARG BASE_IMAGE=python:3.12-slim' and lines[1] == 'FROM ${BASE_IMAGE}'
    assert sum(line.startswith('FROM') for line in lines) == 1
    assert not re.search(r'^\s*#\s*syntax\s*=', text(role), re.I | re.M), 'a syntax line would pull a frontend image from a registry'


@pytest.mark.parametrize('role', sorted(DOCKERFILES))
def test_only_the_listed_arguments_exist_and_no_name_suggests_a_credential(role):
    names = set(re.findall(r'^ARG ([A-Z0-9_]+)', text(role), re.M))
    assert names <= ARGS and {'BASE_IMAGE', 'GRAPH_LOADER_VERSION', 'PACKAGE_WHEEL', 'PACKAGE_WHEEL_SHA256', 'DEPENDENCY_SOURCE'} <= names
    assert not any(re.search('PASS|SECRET|TOKEN|CRED|AUTH|USER', name) for name in names)
    assert not re.search(r'^(ENV|ARG) .*(pip_index|PIP_INDEX|PIP_EXTRA|PIP_CERT|PROXY)', text(role), re.I | re.M)


@pytest.mark.parametrize('role', sorted(DOCKERFILES))
def test_the_install_goes_through_the_helper_with_the_context_mounted_and_the_secret_optional(role):
    lines = instructions(text(role))
    install = next(line for line in lines if 'install.py' in line)
    assert install.startswith('RUN --mount=type=bind,source=.,target=/ctx --mount=type=secret,id=pip_index,required=false')
    assert 'python -I /ctx/install.py' in install and '--index-secret /run/secrets/pip_index' in install
    assert '--lock /ctx/lock.txt' in install and '--wheelhouse /ctx/wheelhouse' in install and '--ca-bundle /ctx/pip-ca.pem' in install
    assert not any(line.startswith('COPY') or line.startswith('ADD') for line in lines), 'the context is bind-mounted: no wheel, lock or wheelhouse becomes a layer'
    assert not any('pip install' in line or 'pip3' in line for line in lines), 'pip is only ever started by the helper, in its fresh environment'
    assert not any(re.search(r'curl|wget|http://|https://', line) for line in lines if not line.startswith('LABEL'))


@pytest.mark.parametrize('role', sorted(DOCKERFILES))
def test_labels_entrypoints_and_user_are_those_of_the_released_images(role):
    lines = instructions(text(role))
    label = next(line for line in lines if line.startswith('LABEL'))
    assert label == (f'LABEL org.opencontainers.image.version=$GRAPH_LOADER_VERSION org.opencontainers.image.source="https://github.com/Neburyx/graph-loader" '
                     f'io.graph-loader.component="{role}" io.graph-loader.image-contract="1"')
    entry = next(line for line in lines if line.startswith('ENTRYPOINT'))
    import json
    assert json.loads(entry[len('ENTRYPOINT '):]) == ENTRYPOINTS[role]
    assert 'WORKDIR /app' in lines and 'ENV PYTHONUNBUFFERED=1' in lines
    assert not any(line.startswith(('USER', 'CMD', 'HEALTHCHECK', 'EXPOSE', 'VOLUME')) for line in lines)
    assert any(line.startswith('RUN python -I -c') and 'm.version("neo4j-graph-loader") == sys.argv[1]' in line for line in lines)


def test_tini_belongs_to_the_cli_image_only_and_has_a_skip_and_a_mirror_path():
    cli = text('cli')
    run = next(line for line in instructions(cli) if line.startswith('RUN') and 'apt-get install' in line)
    assert 'tini --version 2>/dev/null | grep -qF "tini version ${TINI_VERSION}"' in run and 'apt is skipped' in run, 'a base that carries tini skips apt'
    assert 'if [ -n "${DEBIAN_MIRROR}" ]' in run and '/etc/apt/sources.list.d/debian.sources' in run and 'Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg' in run
    assert run.endswith('tini --version | grep -F "tini version ${TINI_VERSION}"'), 'the version is asserted whichever way tini got there'
    for role in ('node', 'edge', 'clock'):
        assert 'tini' not in text(role) and 'apt-get' not in text(role) and 'DEBIAN_MIRROR' not in text(role)


@pytest.mark.parametrize('role,has_schema', [('cli', True), ('node', True), ('edge', True), ('clock', False)])
def test_the_default_config_schema_is_taken_from_the_installed_package(role, has_schema):
    lines = instructions(text(role))
    copy = [line for line in lines if 'graph_schema.yaml' in line and line.startswith('RUN')]
    assert bool(copy) is has_schema
    if copy:
        assert 'neo4j_graph_loader' in copy[0] and 'data", "graph_schema.yaml"' in copy[0] and 'config/graph_schema.yaml' in copy[0]


def test_the_bundled_schema_equals_the_checkout_schema_the_old_images_copied():
    assert (ROOT / 'config' / 'graph_schema.yaml').read_bytes() == (ROOT / 'src' / 'neo4j_graph_loader' / 'data' / 'graph_schema.yaml').read_bytes()


def test_the_four_files_share_one_install_block_so_they_cannot_drift():
    blocks = []
    for role in sorted(DOCKERFILES):
        content = text(role)
        start, end = content.index('ARG PACKAGE_WHEEL\n'), content.index('RUN python -I -c')
        blocks.append(content[start:end])
    assert len(set(blocks)) == 1
