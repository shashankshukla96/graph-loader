"""
src/cli.py
──────────
Command-line interface for the Neo4j Graph Loader.
"""
import argparse
import sys
import logging
import os

from src.utils.schema_loader import load_schema
from src.orchestrator.schema_initializer import (
    get_neo4j_driver,
    apply_schema,
    SchemaInitializationError
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def handle_start(args: argparse.Namespace) -> int:
    logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")
    
    # 1. Load schema
    try:
        schema = load_schema(args.config)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return 1
        
    # 2. Get DB config
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    
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
        
    return 0


def handle_stop(args: argparse.Namespace) -> int:
    loader = args.loader if args.loader else "ALL"
    logger.info(f"Stopping loader: {loader}")
    return 0


def handle_status(args: argparse.Namespace) -> int:
    logger.info(f"Checking status for config: {args.config}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Neo4j Graph Loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Start command
    parser_start = subparsers.add_parser("start", help="Start the ingestion pipeline")
    parser_start.add_argument("--config", default="config/graph_schema.yaml", help="Path to schema YAML")
    parser_start.add_argument("--mode", choices=["bulk", "stream"], required=True, help="Ingestion mode")
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
