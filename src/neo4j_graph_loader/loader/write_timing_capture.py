"""Optional file capture for structured loader write timing log records."""
from __future__ import annotations

import logging
import os


def configure_write_timing_capture(logger: logging.Logger, stage: str) -> None:
    """Copy successful write log lines to the run's mounted timing file."""
    path = os.environ.get("GRAPH_LOADER_TIMING_LOG_PATH")
    if not path:
        return
    for existing in tuple(logger.handlers):
        if getattr(existing, "_graph_loader_timing", False):
            logger.removeHandler(existing)
            existing.close()
    handler = logging.FileHandler(path, mode="w", encoding="utf-8")
    handler._graph_loader_timing = True
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    prefix = f"stage={stage} "
    handler.addFilter(lambda record: record.getMessage().startswith(prefix))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
