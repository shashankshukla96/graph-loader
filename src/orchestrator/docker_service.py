import os
import docker
from typing import List, Dict, Optional

class DockerService:
    def __init__(self, client: docker.DockerClient = None):
        self.client = client or docker.from_env()

    def build_image(self, tag: str = "graph-loader-node:latest", dockerfile: str = "Dockerfile.node_loader"):
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag, rm=True)

    def build_edge_image(self, tag: str = "graph-loader-edge:latest", dockerfile: str = "Dockerfile.edge_loader"):
        """Build the dedicated relationship-loader image."""
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag, rm=True)

    def run_edge_loader(
        self,
        *,
        edge_type: str,
        topic: str,
        mode: str,
        config_path: str,
        replicas: int = 1,
        network: str = "graph-loader-net",
        environment: Optional[Dict[str, str]] = None,
        rejection_dir: str = "var/rejections",
        run_id: Optional[str] = None,
        consumer_group_prefix: str = "graph-loader",
        slot_gating: bool = False,
        coordination_topic: Optional[str] = None,
    ) -> List[docker.models.containers.Container]:
        """Launch exact relationship loader replicas, rolling back only this call."""
        if replicas < 1:
            raise ValueError("replicas must be positive")
        if coordination_topic is not None and not slot_gating:
            raise ValueError("coordination_topic requires slot_gating")
        if slot_gating:
            if not isinstance(coordination_topic, str) or not coordination_topic.strip():
                raise ValueError("slot_gating requires a nonblank coordination_topic")
            if not isinstance(run_id, str) or not run_id.strip():
                raise ValueError("slot_gating requires a nonblank run_id")
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
                "LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        }
        host_config_path = os.path.abspath(config_path)
        host_rejection_dir = os.path.abspath(rejection_dir)
        os.makedirs(host_rejection_dir, exist_ok=True)
        volumes = {
            host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"},
            host_rejection_dir: {"bind": "/app/rejections", "mode": "rw"},
        }
        started_containers = []
        try:
            for index in range(replicas):
                replica_id = str(index)
                command = [
                    "--config", "/app/runtime_schema.yaml", "--edge-type", edge_type,
                    "--mode", mode, "--topic", topic, "--replica-id", replica_id,
                ]
                labels = {
                    "app": "graph-loader", "component": "edge-loader",
                    "edge_type": edge_type, "replica_id": replica_id,
                }
                name = f"graph-loader-edge-{edge_type}-{replica_id}"
                replica_environment = dict(env)
                rejection_suffix = ""
                if run_id is not None:
                    encoded_run_id = run_id.encode("utf-8").hex()
                    if not encoded_run_id:
                        raise ValueError("run_id must not be empty")
                    name = f"{name}-{encoded_run_id}"
                    command.extend(["--run-id", run_id])
                    labels["run_id"] = run_id
                    replica_environment["GRAPH_LOADER_RUN_ID"] = run_id
                    replica_environment["KAFKA_GROUP_ID"] = (
                        f"{consumer_group_prefix}-{edge_type}-{run_id}"
                    )
                    rejection_suffix = f"-{encoded_run_id}"
                if slot_gating:
                    command.extend(["--slot-gating", "--coordination-topic", coordination_topic])
                    replica_environment["KAFKA_COORDINATION_GROUP_ID"] = (
                        f"{consumer_group_prefix}-{edge_type}-{run_id}-clock-{replica_id}"
                    )
                replica_environment["REJECTION_LOG_PATH"] = (
                    f"/app/rejections/{edge_type}-{replica_id}{rejection_suffix}.jsonl"
                )
                started_containers.append(self.client.containers.run(
                    image="graph-loader-edge:latest", name=name, command=command,
                    environment=replica_environment, network=network, labels=labels,
                    volumes=volumes, detach=True, remove=True,
                ))
        except Exception:
            for container in started_containers:
                try:
                    container.stop()
                except Exception:
                    pass
            raise
        return started_containers

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
