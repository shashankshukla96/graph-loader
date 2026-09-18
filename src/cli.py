"""
src/cli.py
──────────
Command-line interface for the Neo4j Graph Loader.
"""
import argparse
import sys
import logging
import os
import uuid
from collections.abc import Iterable

from docker.errors import NotFound

from src.utils.schema_loader import load_schema
from src.orchestrator.schema_initializer import (
    get_neo4j_driver,
    apply_schema,
    SchemaInitializationError
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_neo4j_credentials() -> tuple[str, str, str]:
    """Return Neo4j credentials, preferring the documented username variable."""
    return (
        os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        os.environ.get("NEO4J_USERNAME", os.environ.get("NEO4J_USER", "neo4j")),
        os.environ.get("NEO4J_PASSWORD", "changeme"),
    )


from src.orchestrator.docker_service import DockerService
from src.orchestrator.bulk_monitor import BulkMonitor


def _is_running(container: object) -> bool:
    """Return whether an exact tracked Docker container still needs a stop attempt."""
    try:
        container.reload()
    except NotFound:
        return False
    except Exception:
        return True
    state = getattr(container, "attrs", {}).get("State", {})
    status = state.get("Status", getattr(container, "status", ""))
    return status in {"running", "created", "restarting"}


def _stop_exact_containers(containers: Iterable[object]) -> list[str]:
    """Stop only supplied containers, retrying a failed exact target once if live."""
    failures: list[tuple[object, Exception]] = []
    for container in containers:
        try:
            container.stop()
        except Exception as exc:
            failures.append((container, exc))

    unresolved: list[str] = []
    for container, first_error in failures:
        if not _is_running(container):
            continue
        try:
            container.stop()
        except Exception as retry_error:
            if _is_running(container):
                unresolved.append(f"{getattr(container, 'name', '<unknown>')}: {retry_error}")
            continue
        if _is_running(container):
            unresolved.append(f"{getattr(container, 'name', '<unknown>')}: {first_error}")
    return unresolved

def handle_start(args: argparse.Namespace) -> int:
    logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")

    # 1. Load schema
    try:
        schema = load_schema(args.config)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return 1

    # 2. Get DB config
    uri, user, password = get_neo4j_credentials()

    # 3. Apply schema
    try:
        driver = get_neo4j_driver(uri, user, password)
        try:
            apply_schema(driver, schema)
        finally:
            driver.close()
        logger.info("Schema initialization complete. Ready for ingestion.")
    except SchemaInitializationError as e:
        logger.error(f"Schema initialization failed: {e}")
        return 1

    docker_service = DockerService()
    if getattr(args, "skip_image_build", False):
        logger.info("Using the pre-built node loader image.")
    else:
        logger.info("Building node loader image...")
        try:
            docker_service.build_image()
        except Exception as e:
            logger.error(f"Failed to build node loader image: {e}")
            return 1

    is_bulk = args.mode == "bulk"
    bulk_timeout_seconds = getattr(args, "bulk_timeout_seconds", 300.0)
    if is_bulk and bulk_timeout_seconds <= 0:
        logger.error("--bulk-timeout-seconds must be positive")
        return 1
    run_id = uuid.uuid4().hex if is_bulk else None
    started_containers = []
    tracked_containers = {}
    monitor = None
    drain_started = False
    for node_config in schema.nodes:
        try:
            containers = docker_service.run_node_loader(
                node_label=node_config.label,
                topic=node_config.topic,
                mode=args.mode,
                config_path=args.config,
                replicas=node_config.replicas,
                network=args.network,
                **({"run_id": run_id} if run_id is not None else {}),
            )
            # Preserve every returned object before validating the result so a
            # malformed launch response is still safely cleaned up.
            started_containers.extend(containers)
            if len(containers) != node_config.replicas:
                raise RuntimeError(
                    f"loader launch returned {len(containers)} replicas for {node_config.label}; "
                    f"expected {node_config.replicas}"
                )
            if is_bulk:
                for replica_index, container in enumerate(containers):
                    tracked_containers[(node_config.label, str(replica_index))] = container
            logger.info(f"Started node loader for {node_config.label}")
        except Exception as e:
            logger.error(f"Failed to launch node loader for {node_config.label}: {e}")
            _stop_exact_containers(started_containers)
            return 1

    if not is_bulk:
        return 0

    try:
        monitor = BulkMonitor(
            schema,
            run_id,
            set(tracked_containers),
            tracked_containers,
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        )
        monitor.wait_for_assignment_coverage(bulk_timeout_seconds)
        monitor.capture_boundary()
        monitor.wait_for_completion(bulk_timeout_seconds)
        drain_started = True
        stop_failures = _stop_exact_containers(started_containers)
        if stop_failures:
            raise RuntimeError(f"unable to stop bulk loader fleet: {'; '.join(stop_failures)}")
        monitor.wait_for_drain_complete(bulk_timeout_seconds)
        monitor.verify_zero_lag()
        return 0
    except Exception as exc:
        logger.error("Bulk run failed: %s", exc)
        if not drain_started:
            _stop_exact_containers(started_containers)
        return 1
    finally:
        if monitor is not None:
            monitor.close()


def handle_stop(args: argparse.Namespace) -> int:
    loader = args.loader
    logger.info(f"Stopping loader(s)...")
    docker_service = DockerService()
    docker_service.stop_node_loaders(node_label=loader)
    logger.info("Loaders stopped.")
    return 0


def handle_status(args: argparse.Namespace) -> int:
    logger.info(f"Checking status for config: {args.config}")
    docker_service = DockerService()
    containers = docker_service.list_node_loaders()
    if not containers:
        logger.info("No loaders running.")
    for c in containers:
        logger.info(f"Loader {c['name']} ({c['id']}): {c['status']} [Node: {c['node_label']}]")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Neo4j Graph Loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Start command
    parser_start = subparsers.add_parser("start", help="Start the ingestion pipeline")
    parser_start.add_argument("--config", default="config/graph_schema.yaml", help="Path to schema YAML")
    parser_start.add_argument("--mode", choices=["bulk", "stream"], required=True, help="Ingestion mode")
    parser_start.add_argument("--network", default="graph-loader-net", help="Docker network for containers")
    parser_start.add_argument(
        "--skip-image-build",
        action="store_true",
        help="use an image built earlier by an external orchestrator",
    )
    parser_start.add_argument(
        "--bulk-timeout-seconds",
        type=float,
        default=300.0,
        help="Maximum seconds for each bulk lifecycle stage",
    )
    parser_start.set_defaults(func=handle_start)

    # Stop command
    parser_stop = subparsers.add_parser("stop", help="Stop running ingestion containers")
    parser_stop.add_argument("--loader", help="Specific loader name to stop")
    parser_stop.set_defaults(func=handle_stop)

    # Status command
    parser_status = subparsers.add_parser("status", help="Check pipeline status and lags")
    parser_status.add_argument("--config", default="config/graph_schema.yaml", help="Path to schema YAML")
    parser_status.set_defaults(func=handle_status)

    return parser


def main(args_list: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(args_list)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
