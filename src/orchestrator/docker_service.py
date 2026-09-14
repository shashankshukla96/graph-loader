import uuid
import os
import docker
from typing import List, Dict, Optional

class DockerService:
    def __init__(self, client: docker.DockerClient = None):
        self.client = client or docker.from_env()

    def build_image(self, tag: str = "graph-loader-node:latest", dockerfile: str = "Dockerfile.node_loader"):
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag, rm=True)

    def run_node_loader(
        self,
        node_label: str,
        topic: str,
        mode: str,
        config_path: str,
        replicas: int = 1,
        network: str = "graph_loader_default",
        environment: Optional[Dict[str, str]] = None,
    ) -> List[docker.models.containers.Container]:
        
        env = environment or {
            "NEO4J_URI": os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        }

        labels = {
            "app": "graph-loader",
            "component": "node-loader",
            "node_label": node_label,
        }

        command = ["--config", "/app/runtime_schema.yaml", "--node-label", node_label, "--mode", mode, "--topic", topic]

        # Use an absolute path for the volume mount
        host_config_path = os.path.abspath(config_path)
        volumes = {
            host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"}
        }

        started_containers = []
        try:
            for i in range(replicas):
                container_name = f"graph-loader-node-{node_label}-{i}"
                container = self.client.containers.run(
                    image="graph-loader-node:latest",
                    name=container_name,
                    command=command,
                    environment=env,
                    network=network,
                    labels=labels,
                    volumes=volumes,
                    detach=True,
                    remove=True,
                )
                started_containers.append(container)
        except Exception as e:
            # Rollback started containers for this node_label if a subsequent replica fails
            for container in started_containers:
                try:
                    container.stop()
                except Exception:
                    pass
            raise e

        return started_containers

    def stop_node_loaders(self, node_label: Optional[str] = None):
        labels = ["component=node-loader"]
        if node_label:
            labels.append(f"node_label={node_label}")
        filters = {"label": labels}

        containers = self.client.containers.list(all=True, filters=filters)
        for container in containers:
            container.stop()

    def list_node_loaders(self) -> List[Dict]:
        filters = {"label": "component=node-loader"}
        containers = self.client.containers.list(all=True, filters=filters)
        result = []
        for container in containers:
            result.append({
                "id": container.short_id,
                "name": container.name,
                "status": container.status,
                "node_label": container.labels.get("node_label")
            })
        return result
