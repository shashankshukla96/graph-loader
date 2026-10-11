"""Loader write timing capture used by the IMDb report."""
import logging
from unittest.mock import patch

from src.loader.write_timing_capture import configure_write_timing_capture


def test_write_timing_capture_keeps_only_successful_batch_lines(tmp_path):
    path = tmp_path / "node-Person-0.log"
    logger = logging.getLogger("test.write.timing.capture")
    with patch.dict("os.environ", {"GRAPH_LOADER_TIMING_LOG_PATH": str(path)}):
        configure_write_timing_capture(logger, "node-write")
        try:
            logger.info("stage=node-write label=Person topic=imdb-person records=2 write_ms=1.500")
            logger.error("Node batch write failed records=2 write_ms=2.000")
        finally:
            for handler in tuple(logger.handlers):
                logger.removeHandler(handler)
                handler.close()
    assert path.read_text(encoding="utf-8") == (
        "stage=node-write label=Person topic=imdb-person records=2 write_ms=1.500\n"
    )
