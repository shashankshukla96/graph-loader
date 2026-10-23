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
    content = (ROOT / dockerfile).read_text()
    assert 'ARG GRAPH_LOADER_VERSION\n' in content
    assert 'org.opencontainers.image.version=$GRAPH_LOADER_VERSION' in content
    assert f'io.graph-loader.component="{role}"' in content
    assert 'io.graph-loader.image-contract="1"' in content
    assert 'm.version("neo4j-graph-loader") == sys.argv[1]' in content
    assert 'pip install --no-cache-dir --no-deps .' in content


def test_copy_closure_includes_nested_and_non_python_inputs(tmp_path):
    for name in (*builder.DOCKERFILES.values(), 'pyproject.toml', 'requirements.txt', 'MANIFEST.in',
                 'PACKAGE_README.md', 'LICENSE', '.dockerignore', '.env.example',
                 'scripts/build_candidate_images.py', 'src/neo4j_graph_loader/x.py',
                 'src/neo4j_graph_loader/data/sample.yaml', 'config/neo4j/server-logs.xml', 'config/.gitkeep'):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    ignored = tmp_path / 'src/neo4j_graph_loader/__pycache__/x.pyc'
    ignored.parent.mkdir(); ignored.write_bytes(b'ignored')
    inputs = builder.build_inputs(tmp_path)
    assert 'config/neo4j/server-logs.xml' in inputs and 'config/.gitkeep' in inputs
    assert 'src/neo4j_graph_loader/data/sample.yaml' in inputs
    assert not any('egg-info' in n or '__pycache__' in n for n in inputs)
    (tmp_path / 'config/link').symlink_to('neo4j/server-logs.xml')
    with pytest.raises(ValueError, match='unsafe candidate build input'):
        builder.build_inputs(tmp_path)
