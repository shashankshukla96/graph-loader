# Implementation Plan: Story 1 — CLI Core & Subcommand Stubs

## Objective
Create the primary entry point for the application using standard `argparse`, defining the required subcommands (`start`, `stop`, `status`) and their specific flags.

## Files to Modify/Create
| File | Change |
|---|---|
| `src/cli.py` | Create root CLI parser and handlers |
| `tests/test_cli.py` | Create unit tests for parsing arguments and verifying handlers |

## Key Logic

**Code Structure (`src/cli.py`):**
```python
import argparse
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def handle_start(args: argparse.Namespace) -> int:
    logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")
    return 0

def handle_stop(args: argparse.Namespace) -> int:
    logger.info(f"Stopping loader: {args.loader if args.loader else 'ALL'}")
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
```

## Unit Tests (`tests/test_cli.py`)
- `test_parser_start_valid`: Pass `["start", "--mode", "bulk"]`, assert `args.mode == "bulk"`, `args.config == "config/graph_schema.yaml"`.
- `test_parser_start_missing_mode`: Use `pytest.raises(SystemExit)` and redirect `stderr` to verify the missing `--mode` error.
- `test_parser_stop`: Verify `--loader` parsing.
- `test_parser_status`: Verify `--config` default.
- `test_main_routes_to_handler`: Mock `handle_start` and call `main(["start", "--mode", "stream"])` to verify correct routing and return code.

## Validation Commands
```bash
PYTHONPATH=. .venv/bin/pytest tests/test_cli.py -v --cov=src.cli --cov-report=term-missing
```

## Rollback
Delete `src/cli.py`, `tests/test_cli.py`, and `plan/v1/slice-5/impl_plan_story_1.md`.
