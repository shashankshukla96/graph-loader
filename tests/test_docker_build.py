import docker
import pytest
from docker.errors import BuildError, APIError

@pytest.mark.integration
def test_docker_build_node_loader():
    client = docker.from_env()
    try:
        image, build_logs = client.images.build(
            path=".",
            dockerfile="Dockerfile.node_loader",
            tag="graph-loader-node:test",
            rm=True
        )
        assert image is not None
        assert "graph-loader-node:test" in image.tags
        
        # Verify ENTRYPOINT
        attrs = image.attrs
        entrypoint = attrs.get("Config", {}).get("Entrypoint")
        assert entrypoint == ["python", "-m", "src.loader.node_loader"]
        
    except (BuildError, APIError) as e:
        pytest.fail(f"Docker build failed: {e}")
