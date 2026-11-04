"""E19.F1.S1: the product's public identity is the Neburyx organization.

Every place that names the image namespace or the repository is checked against literals written here, independent of the
package constant, so a wrong constant cannot make its own test pass.
"""
import re
import tomllib
from pathlib import Path

import pytest

from neo4j_graph_loader import _images
from neo4j_graph_loader._version import __version__
from scripts import build_candidate_images, service_lane

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE_DIR = ROOT / 'src' / 'neo4j_graph_loader' / 'image_build' / 'dockerfiles'
IMAGE_NAMESPACE = 'ghcr.io/neburyx'
REPOSITORY_URL = 'https://github.com/Neburyx/graph-loader'
# Built from parts: the previous owner's account name is also the public contact, which stays.
PREVIOUS_ACCOUNT = 'shashank' + 'shukla96'
DOCKERFILES = ('Dockerfile', 'Dockerfile.node_loader', 'Dockerfile.edge_loader', 'Dockerfile.clock')


@pytest.mark.parametrize('role', ['cli', 'node', 'edge', 'clock'])
def test_package_constant_is_the_neburyx_namespace(role, monkeypatch) -> None:
    monkeypatch.delenv(f'GRAPH_LOADER_{role.upper()}_IMAGE', raising=False)
    assert _images.NAMESPACE == IMAGE_NAMESPACE
    assert _images.image_reference(role) == f'{IMAGE_NAMESPACE}/graph-loader-{role}:{__version__}'


def test_scripts_use_the_same_namespace() -> None:
    assert build_candidate_images.NAMESPACE == IMAGE_NAMESPACE == _images.NAMESPACE
    assert service_lane.CANDIDATE_REPOSITORIES == (IMAGE_NAMESPACE + '/graph-loader-',)


def test_the_installed_runner_accepts_only_neburyx_candidate_tags() -> None:
    source = (ROOT / 'scripts' / 'installed_service.py').read_text(encoding='utf-8')
    assert "f'ghcr.io/neburyx/graph-loader-{role}:{version}-'" in source


@pytest.mark.parametrize('dockerfile', DOCKERFILES)
def test_every_dockerfile_labels_the_neburyx_repository(dockerfile: str) -> None:
    text = (DOCKERFILE_DIR / dockerfile).read_text(encoding='utf-8')
    assert f'org.opencontainers.image.source="{REPOSITORY_URL}"' in text


def test_project_urls_point_at_the_neburyx_repository() -> None:
    urls = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']['urls']
    assert urls == {
        'Repository': REPOSITORY_URL,
        'Documentation': REPOSITORY_URL + '/blob/main/docs/QUICKSTART.md',
        'Changelog': REPOSITORY_URL + '/blob/main/CHANGELOG.md',
        'Issues': REPOSITORY_URL + '/issues',
    }


def test_the_public_contact_is_unchanged() -> None:
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    contact = PREVIOUS_ACCOUNT + '@gmail.com'
    assert project['authors'] == [{'name': 'Shashank Shukla', 'email': contact}]
    assert project['maintainers'] == [{'name': 'Shashank Shukla', 'email': contact}]


# Files that legitimately keep the previous account name as history: the frozen 0.1.0a1 golden, the parity harness and its test (they
# define the 0.1.0a1 namespace normalization), the 0.1.0a1 changelog entry and the internal release-process records (not exported).
# The test inventory keeps the original assertions of the tests as a baseline, which holds the namespace of their first freeze.
HISTORY = {'tests/golden/docker_parity_0.1.0a1.json', 'tests/docker_parity_support.py', 'tests/test_docker_parity.py', 'CHANGELOG.md',
           'tests/inventory/test-inventory.json',
           'docs/PYPI_LIBRARY_CHECKLIST.md', 'docs/PYPI_CHECKLIST_REVIEW.md', 'tests/test_neburyx_namespace.py'}
SCANNED = ('src', 'scripts', 'tests', 'docs', 'examples', 'config', 'monitoring')
SCANNED_FILES = ('README.md', 'PACKAGE_README.md', 'SECURITY.md', 'pyproject.toml', 'Makefile')
OLD_ADDRESS = re.compile(rf'(?:github\.com/|ghcr\.io/){PREVIOUS_ACCOUNT}\b', re.IGNORECASE)


def scanned_files():
    for name in SCANNED_FILES:
        yield ROOT / name
    for folder in SCANNED:
        for path in (ROOT / folder).rglob('*'):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix not in {'.pyc', '.png', '.gz', '.whl'}:
                yield path


def test_no_product_file_names_the_previous_repository_or_registry_namespace() -> None:
    found = []
    for path in scanned_files():
        relative = path.relative_to(ROOT).as_posix()
        if relative in HISTORY:
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        if OLD_ADDRESS.search(text):
            found.append(relative)
    assert found == []


def test_the_scan_catches_the_previous_namespace_in_each_spelling() -> None:
    """Control: the pattern the scan uses matches both former addresses (so an empty result above is meaningful)."""
    assert OLD_ADDRESS.search(f'https://github.com/{PREVIOUS_ACCOUNT}/graph-loader')
    assert OLD_ADDRESS.search(f'ghcr.io/{PREVIOUS_ACCOUNT}/graph-loader-node')
    assert not OLD_ADDRESS.search(f'contact {PREVIOUS_ACCOUNT}@gmail.com')
    assert not OLD_ADDRESS.search(f'{IMAGE_NAMESPACE}/graph-loader-node')
