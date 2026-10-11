"""v7-S5 story 5.4: precedence sub-stage rows sit under their type in the run report."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from examples.imdb.publish_and_load_nodes import (  # noqa: E402
    read_edge_stage_timings,
    read_edge_substage_timings,
    write_run_report,
)


def test_precedence_substage_rows_sit_under_the_type_with_the_type_total(tmp_path: Path) -> None:
    def row(name: str, **extra) -> None:
        (tmp_path / name).write_text(json.dumps({
            "edge_type": "WORKS_AT", "started_utc": "2026-10-01T00:00:00+00:00",
            "completed_utc": "2026-10-01T00:00:20+00:00", "succeeded": True, **extra,
        }), encoding="utf-8")

    row("stage-WORKS_AT.json", elapsed_seconds=20.0)
    row("substage-WORKS_AT.t0.json", elapsed_seconds=11.0, rank=0, topic="works-at-a")
    row("substage-WORKS_AT.t1.json", elapsed_seconds=9.0, rank=1, topic="works-at-b")
    assert read_edge_stage_timings(tmp_path)["WORKS_AT"].elapsed_seconds == 20.0  # type total, not the last topic
    assert [(r, t) for r, t, _ in read_edge_substage_timings(tmp_path)["WORKS_AT"]] == [
        (0, "works-at-a"), (1, "works-at-b"),
    ]
    report = write_run_report(
        tmp_path, started_at=datetime(2026, 10, 1, tzinfo=timezone.utc), total_seconds=30.0, partitions=4,
        publish_workers=1, consumer_workers=1, topic_setup_seconds=0.1, parallel_phase_seconds=1.0,
        image_build_seconds=1.0, topic_results=[("WORKS_AT", "works-at-a", 3, 0.1)],
        consumer_lifecycle_seconds=20.0, consumer_exit_code=0, write_timings={}, edge_writers=1,
        node_batch_size=1, edge_batch_size=1, timing_directory=tmp_path,
    )
    lines = report.read_text(encoding="utf-8").splitlines()
    total = next(i for i, line in enumerate(lines) if line.startswith("| WORKS_AT |") and "20.000s" in line)
    assert "topic 0 `works-at-a`" in lines[total + 1] and "11.000s" in lines[total + 1]
    assert "topic 1 `works-at-b`" in lines[total + 2] and "9.000s" in lines[total + 2]
