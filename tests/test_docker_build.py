import subprocess
import sys
import uuid
from pathlib import Path

import docker
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.integration
def test_docker_build_node_loader(tmp_path):
    """The images build from a generated context (the candidate builder: package wheel + hashed lock + the public PyPI) and keep their entrypoints.

    Needs Docker with buildx and network access; it builds all four candidates under a unique suffix and removes them afterwards.
    """
    suffix = 'it' + uuid.uuid4().hex[:8]
    output = tmp_path / 'candidates'
    client = docker.from_env()
    tags = [f'ghcr.io/neburyx/graph-loader-{role}:{version}-{suffix}' for role in ('cli', 'node', 'edge', 'clock')
            for version in [__import__('neo4j_graph_loader._version', fromlist=['x']).__version__]]
    try:
        done = subprocess.run([sys.executable, str(ROOT / 'scripts' / 'build_candidate_images.py'), '--output', str(output), '--suffix', suffix],
                              capture_output=True, text=True, timeout=1800)
        assert done.returncode == 0, done.stdout + done.stderr
        image = client.images.get(tags[1])
        assert tags[1] in image.tags
        assert image.attrs.get('Config', {}).get('Entrypoint') == ['python', '-m', 'neo4j_graph_loader.loader.node_loader']
    finally:
        for tag in tags:
            try:
                client.images.remove(tag)
            except docker.errors.DockerException:
                pass
