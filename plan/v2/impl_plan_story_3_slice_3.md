# Implementation Plan for Story 3: Integrate Fleet Launch into CLI

## Files to Modify
- Modify: `src/cli.py`
- Modify: `tests/test_cli.py` (if it exists, create if not)

## Logic / Contents
`src/cli.py`:
- Import `DockerService` from `src.orchestrator.docker_service`.
- In `build_parser`: Add `--network` argument to `start` command with default `graph-loader_default`.
- In `handle_start`:
  - After `logger.info("Schema initialization complete. Ready for ingestion.")`:
  - Instantiate `docker_service = DockerService()`
  - Log `Building node loader image...` and call `docker_service.build_image()`
  - Iterate over `schema.nodes`:
  - Inside a try block, call `docker_service.run_node_loader`:
    ```python
    node_label = node_config.label
    topic = node_config.topic
    docker_service.run_node_loader(
        node_label=node_label,
        topic=topic,
        mode=args.mode,
        config_path=args.config,
        network=args.network
    )
    ```
  - If any exception occurs during `run_node_loader`, log the error, call `docker_service.stop_node_loaders()`, and return 1.
  - Return 0 on success.
- In `handle_stop`:
  - Instantiate `docker_service = DockerService()`
  - Call `docker_service.stop_node_loaders(args.loader)`
  - Return 0.
- In `handle_status`:
  - Instantiate `docker_service = DockerService()`
  - Call `containers = docker_service.list_node_loaders()`
  - Print container info (id, name, status, node_label).
  - Return 0.

`tests/test_cli.py`:
- Test `handle_start`, `handle_stop`, `handle_status` with mocked `DockerService`.

## Rollback
- Revert changes to `src/cli.py` and delete `tests/test_cli.py`.
