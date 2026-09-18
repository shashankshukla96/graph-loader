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
        rejection_dir: str = "var/rejections",
        run_id: Optional[str] = None,
    ) -> List[docker.models.containers.Container]:
        
        # The CLI runs on the host, whereas node loaders run on the Docker
        # network. Do not leak host endpoints such as ``localhost:9092`` into a
        # loader container: there, localhost is the loader itself. Deployments
        # with a non-default internal network can explicitly use the LOADER_*
        # variables or pass ``environment``.
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
                "LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        }

        labels = {
            "app": "graph-loader",
            "component": "node-loader",
            "node_label": node_label,
        }

        command = ["--config", "/app/runtime_schema.yaml", "--node-label", node_label, "--mode", mode, "--topic", topic]

        # Use an absolute path for the volume mount
        host_config_path = os.path.abspath(config_path)
        host_rejection_dir = os.path.abspath(rejection_dir)
        os.makedirs(host_rejection_dir, exist_ok=True)
        volumes = {
            host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"},
            host_rejection_dir: {"bind": "/app/rejections", "mode": "rw"},
        }

        started_containers = []
        try:
            for i in range(replicas):
                replica_id = str(i)
                container_name = f"graph-loader-node-{node_label}-{replica_id}"
                replica_environment = dict(env)
                replica_environment["REJECTION_LOG_PATH"] = (
                    f"/app/rejections/{node_label}-{replica_id}.jsonl"
                )
                replica_command = [*command, "--replica-id", replica_id]
                replica_labels = {**labels, "replica_id": replica_id}
                if run_id is not None:
                    # Hex encoding preserves every byte of the external id while
                    # restricting the Docker name suffix to a safe alphabet. In
                    # particular, ``a-b`` and ``ab`` cannot collapse to one name.
                    encoded_run_id = run_id.encode("utf-8").hex()
                    if not encoded_run_id:
                        raise ValueError("run_id must not be empty")
                    container_name = f"{container_name}-{encoded_run_id}"
                    replica_environment["GRAPH_LOADER_RUN_ID"] = run_id
                    replica_command.extend(["--run-id", run_id])
                    replica_labels["run_id"] = run_id
                container = self.client.containers.run(
                    image="graph-loader-node:latest",
                    name=container_name,
                    command=replica_command,
                    environment=replica_environment,
                    network=network,
                    labels=replica_labels,
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
