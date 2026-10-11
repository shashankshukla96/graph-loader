"""Producer-side edge planning for the IMDb example (``--edge-plan``, default ``--plan-source files``).

The planning itself is the library's (:mod:`src.loader.edge_planner`); this
module only turns the generated relationship TSVs into
:class:`~src.loader.edge_planner.EdgeRecordSource` objects.  Each source
parses its file with the per-type publisher's encoder
(:class:`EdgeRowEncoder`, falling back to :func:`edge_payload`) and yields
*prepared* rows ``(source key, target key, record JSON)``, so the planner
never re-parses JSON; the planned writers validate every record against the
schema before writing.

Large files are split into source-bucket shards (``bucket(source) % shards``,
the library's bucket function), so every ``(source, target)`` pair -- and
every unit, whose source resource is a bucket or a hub of one bucket -- stays
in one shard and keeps file order; the shard index is the source ``order``.

Why keep this next to the library's Kafka planner (``--plan-source kafka``)?
Planning from the files skips publishing the per-type edge topics and
overlaps planning with the node publication, so the plan is ready before the
loader even starts; reading the topics back costs a second pass over every
edge record after publishing (measured on 4 cores for the 20k subset, 8.9M
edges: plan ready 10.2 s after start from the files vs ~19.7 s via Kafka).
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.loader.edge_plan import DEFAULT_PLAN_BUCKETS, planned_bucket  # noqa: E402
from src.loader.edge_planner import (  # noqa: E402
    PLAN_PRODUCER_CONFIG, EdgeRecordSource, ParallelEdgePlanner, PlanResult, PlanSettings, producer_edge_planner,
)
from src.models.schema import EdgeConfig  # noqa: E402

# Full IMDb exports contain quoted fields longer than csv's 128 KiB default.
csv.field_size_limit(sys.maxsize)


DEFAULT_PLAN_TOPIC = "imdb-edge-chunks"
DEFAULT_PLAN_WRITERS = 10
_PLAN_PRODUCER_CONFIG = PLAN_PRODUCER_CONFIG

__all__ = [
    "DEFAULT_PLAN_TOPIC", "DEFAULT_PLAN_WRITERS", "ParallelEdgePlanner", "PlanResult", "PlanSettings",
    "TsvEdgeSource", "imdb_edge_sources", "imdb_edge_planner",
]


@dataclass(frozen=True)
class TsvEdgeSource(EdgeRecordSource):
    """One relationship TSV, or one source-bucket shard of it."""

    edge_type: str
    path: str
    source_column: str
    target_column: str
    size_hint: int = 0
    order: int = 0
    shard_count: int = 1
    buckets: int = DEFAULT_PLAN_BUCKETS

    @property
    def name(self) -> str:
        return self.edge_type if self.shard_count == 1 else f"{self.edge_type}#{self.order}"

    def describe(self) -> str:
        return self.path if self.shard_count == 1 else f"{self.path} shard {self.order}/{self.shard_count}"

    def records(self, edge: EdgeConfig):
        """Yield ``(source, target, record JSON)``; invalid rows raise with file and line."""
        from examples.imdb.publish_and_load_nodes import EdgeRowEncoder, _dict_row, edge_payload

        path = Path(self.path)
        with path.open("r", encoding="utf-8", newline="") as input_file:
            reader = csv.reader(input_file, delimiter="\t")
            fieldnames = next(reader, None)
            if fieldnames is None:
                return
            encoder = (
                EdgeRowEncoder(fieldnames, edge, self.source_column, self.target_column)
                if len(set(fieldnames)) == len(fieldnames) else None
            )
            # ``edge_payload`` reads the source from ``dict(zip(fieldnames, row))``,
            # i.e. the last column of that name; any valid row's source key is
            # therefore ``row[source_index]``.
            source_index = max(
                (index for index, name in enumerate(fieldnames) if name == self.source_column), default=None,
            )
            shard_count, shard_index, buckets = self.shard_count, self.order, self.buckets
            source_property, target_property = edge.source_key_property, edge.target_key_property
            line_number = 1
            for row in reader:
                if not row:
                    continue
                line_number += 1
                if shard_count > 1:
                    # A row without a source key is invalid; shard 0 rejects it once.
                    direct = row[source_index] if source_index is not None and len(row) > source_index else ""
                    shard = planned_bucket(direct, buckets) % shard_count if direct else 0
                    if shard != shard_index:
                        continue
                encoded = None
                if encoder is not None:
                    try:
                        encoded = encoder.encode(row)
                    except Exception:
                        encoded = None
                if encoded is not None:
                    source, record = encoded
                    yield source, row[encoder.target_index], record
                    continue
                try:
                    payload = edge_payload(
                        _dict_row(fieldnames, row), edge,
                        source_column=self.source_column, target_column=self.target_column,
                    )
                except ValueError as exc:
                    raise ValueError(f"{path}:{line_number}: {exc}") from exc
                yield payload["source"][source_property], payload["target"][target_property], json.dumps(payload)


def imdb_edge_sources(files: dict[str, tuple[str, str, str]], workers: int,
                      buckets: int = DEFAULT_PLAN_BUCKETS) -> list[TsvEdgeSource]:
    """Sources for ``edge type -> (path, source column, target column)``.

    Files larger than a worker's fair share are split into source-bucket
    shards; a shard still parses the whole file (skipping other shards'
    rows), which the size hints account for.
    """
    sizes = {edge_type: Path(path).stat().st_size for edge_type, (path, _, _) in files.items()}
    share = (sum(sizes.values()) or 1) / max(1, workers)
    sources = []
    for edge_type, (path, source_column, target_column) in files.items():
        size = sizes[edge_type]
        shards = max(1, min(workers, math.ceil(size / share))) if workers > 1 else 1
        for index in range(shards):
            sources.append(TsvEdgeSource(
                edge_type, str(path), source_column, target_column,
                size_hint=int(size * (0.25 + 0.75 / shards)), order=index, shard_count=shards, buckets=buckets,
            ))
    return sources


def imdb_edge_planner(schema, files: dict[str, tuple[str, str, str]], settings: PlanSettings,
                      bootstrap_servers: str, **options) -> ParallelEdgePlanner:
    """The library producer planner over the IMDb relationship files."""
    return producer_edge_planner(
        schema, imdb_edge_sources(files, settings.workers, settings.buckets), settings, bootstrap_servers,
        **options,
    )
