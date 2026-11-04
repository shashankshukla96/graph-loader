"""Candidate identity is derived from the package, never a release placeholder."""
import importlib.util
from pathlib import Path

import pytest

from neo4j_graph_loader._version import __version__

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('candidate_builder', ROOT / 'scripts/build_candidate_images.py')
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


def test_builder_uses_literal_version_authority(tmp_path):
    assert builder.package_version() == __version__
    version_file = tmp_path / 'src/neo4j_graph_loader/_version.py'
    version_file.parent.mkdir(parents=True)
    version_file.write_text('__version__ = "0.2.0a2.dev7"\n')
    assert builder.package_version(tmp_path) == '0.2.0a2.dev7'


@pytest.mark.parametrize('role,dockerfile', builder.DOCKERFILES.items())
def test_each_candidate_checks_version_and_defines_role(role, dockerfile):
    content = (ROOT / 'src/neo4j_graph_loader/image_build/dockerfiles' / dockerfile).read_text()
    assert 'ARG GRAPH_LOADER_VERSION\n' in content
    assert 'org.opencontainers.image.version=$GRAPH_LOADER_VERSION' in content
    assert f'io.graph-loader.component="{role}"' in content
    assert 'io.graph-loader.image-contract="1"' in content
    assert 'm.version("neo4j-graph-loader") == sys.argv[1]' in content
    assert 'python -I /ctx/install.py' in content, 'the install goes through the helper that starts pip in a fresh environment'
    assert 'pip install' not in content


def test_copy_closure_includes_nested_and_non_python_inputs(tmp_path):
    """The candidate's build context is generated from package data plus the candidate wheel: the four Dockerfiles, the helper, the lock of the
    platform, the wheel, the CA bundle file (empty here) and, offline, the wheelhouse; nothing is copied from the checkout."""
    from neo4j_graph_loader.image_build import builder as core
    wheel = tmp_path / 'neo4j_graph_loader-1.0-py3-none-any.whl'
    wheel.write_bytes(b'wheel')
    house = tmp_path / 'house'
    house.mkdir()
    (house / 'dep-1-py3-none-any.whl').write_bytes(b'dep')
    context = tmp_path / 'context'
    context.mkdir()
    core.build_context(context, core.Settings(registry='r.example/team', wheelhouse=house), 'py312-linux-arm64.txt', wheel)
    names = sorted(str(p.relative_to(context)) for p in context.rglob('*') if p.is_file())
    assert names == sorted([*core.DOCKERFILES.values(), 'install.py', 'lock.txt', wheel.name, 'pip-ca.pem', 'wheelhouse/dep-1-py3-none-any.whl'])
    assert (context / 'pip-ca.pem').read_bytes() == b''
    assert (context / 'Dockerfile.clock').read_bytes() == (ROOT / 'src/neo4j_graph_loader/image_build/dockerfiles/Dockerfile.clock').read_bytes()
    assert not any(part in ('src', 'config', 'plan', '.env') for name in names for part in Path(name).parts), 'no checkout file is in the context'
