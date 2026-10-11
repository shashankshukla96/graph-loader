# Implementation Plan for Story 2: Docker Orchestration Service

## Files to Create
- Create: `src/orchestrator/docker_service.py`
- Create: `tests/test_docker_service.py`

## Logic / Contents
`src/orchestrator/docker_service.py`:
- Class `DockerService`:
  - `__init__(self, client: docker.DockerClient = None)`
  - `build_image(self, tag="graph-loader-node:latest", dockerfile="Dockerfile.node_loader")`
  - `run_node_loader(self, node_label, topic, mode, config_path, replicas=1, network="graph-loader_default", environment=None)`
    - Network: use `network=network` argument to allow connecting to the docker-compose network (e.g., `graph-loader_default`).
    - Env vars: Use `environment` dict if provided; else build from `os.environ`. (Note: Caller should configure `NEO4J_URI=bolt://neo4j:7687` and `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` if on compose network).
    - `KAFKA_GROUP_ID`: `loader_group_{node_label}`
    - Pass args to entrypoint: `["--config", config_path, "--node-label", node_label]` (mode is handled at CLI but maybe passed in env or skipped).
    - Labels: `{"app": "graph-loader", "component": "node-loader", "node_label": node_label}`
    - Container names: `graph-loader-node-{node_label}-{uuid.uuid4().hex[:6]}` (append short uuid to avoid naming collisions).
    - Return list of started containers.
  - `stop_node_loaders(self, node_label=None)`
    - Use `client.containers.list(all=True, filters={"label": "component=node-loader"})`
    - Filter further by `node_label` if provided.
    - `container.stop()`, `container.remove()`
  - `list_node_loaders(self)`
    - Return list of dicts with container id, name, status, and node_label.

`tests/test_docker_service.py`:
- Use `unittest.mock` to mock `docker.DockerClient` and its methods.
- Test `build_image`.
- Test `run_node_loader` (verifying network_mode, env vars, labels).
- Test `stop_node_loaders`.
- Test `list_node_loaders`.

## Rollback
- Delete the created files.
