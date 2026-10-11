# Story 1 implementation plan: Metrics Facade Skeleton, Dependency, and Label Policy

## Orchestrator correction applied in this plan (binding, overrides `stories.md`)

`stories.md` Story 1/3 picks port `9090` for `GRAPH_LOADER_METRICS_PORT` and
argues the collision with Prometheus server's own default port is
acceptable. The orchestrator has overridden this: the default metrics port
is **9464** (the IANA/Prometheus-registered default for
"default exporter port" ranges used by `prometheus_client`'s own
`start_http_server` examples), not 9090 — 9090 is reserved for the
Prometheus server itself, which may run as its own container on the same
`graph-loader-net` in Slice 3, and a stray identically-numbered internal
port invites exactly the kind of confusion the stories doc's own comment
worried about, even though the doc argued it was technically fine. Every
occurrence of `9090` in `stories.md` (Stories 1, 3, 8) is superseded by
`9464` in this and all following impl plans. `plan/v6/completion_slice-2.md`
will record this deviation once at the end of the slice.

## Files

- New: `src/utils/metrics.py`
- New: `tests/test_metrics.py`
- Edit: `requirements.txt` (done: `prometheus_client>=0.20` added)

## Design

`src/utils/metrics.py`:

```python
"""Shared Prometheus metrics facade for every graph-loader process kind.

Label policy (verbatim, single source of truth alongside
plan/v6/slice-2/stories.md Story 1's table):

| Label            | Allowed values                                              | Why bounded |
|------------------|--------------------------------------------------------------|-------------|
| kind             | node, edge, planned-writer, planned-dispatcher,
|                  | lease-server, clock, cli (closed 7-value enum)               | one value per process type |
| name             | a node label / edge type declared in the schema, or "-"      | bounded by schema size |
| label_or_type    | same domain as name (records_consumed_total only)             | forward-looking, epic's literal shape |
| stage            | node-write | edge-write                                      | matches write_timing_capture.py |
| reason           | closed enum (see RETRY_REASONS)                                | never free text |
| failure_class    | malformed | missing_endpoint | ambiguous_endpoint (dead_letter.FAILURE_CLASSES) | fixed by dead_letter.py's own envelope enum |
| topic            | a schema-declared Kafka topic                                  | bounded by schema size |

Never a label: a Kafka message key, a node/relationship key property, a raw
exception message, a run_id, an offset, a partition, or a replica/container
id.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

DEFAULT_METRICS_PORT = 9464  # orchestrator correction: NOT 9090 (Prometheus server's own default)

WRITE_DURATION_BUCKETS = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
)

RETRY_REASONS = ("neo4j-transient", "missing-endpoint")

_AT_LEAST_ONCE_NOTE = (
    "at-least-once: a crash between a durable write/DLQ-publish and this "
    "counter's increment can double-count on restart; the graph itself "
    "stays exact via idempotent MERGE. Do not treat a raw counter delta as "
    "an exact retry/DLQ count."
)


def build_registry() -> CollectorRegistry:
    """One fresh, independent registry. Never shared across instances."""
    return CollectorRegistry()


class GraphLoaderMetrics:
    """Every metric object in the catalogue, bound to one registry.

    One instance per process in production (see process_metrics()); one
    instance per test case in tests, each with its own CollectorRegistry, so
    values never leak between tests or between production replicas (each
    replica is a separate OS process with its own registry either way).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else build_registry()
        # Story 2 populates the catalogue here.


_process_metrics: GraphLoaderMetrics | None = None


def process_metrics() -> GraphLoaderMetrics:
    """The one GraphLoaderMetrics instance shared within this process."""
    global _process_metrics
    if _process_metrics is None:
        _process_metrics = GraphLoaderMetrics()
    return _process_metrics
```

- `import src.utils.metrics` performs no `prometheus_client` calls at module
  scope: `CollectorRegistry`/`Counter`/etc. are only *imported* (a class
  reference, not a call), and `_process_metrics` stays `None` until
  `process_metrics()` runs. Verified by a test that imports the module and
  asserts `metrics._process_metrics is None`.
- `GraphLoaderMetrics(registry=CollectorRegistry())` constructed twice in
  one process must not raise `ValueError: Duplicated timeseries...` — since
  each instance gets its own registry, this is true by construction; add a
  regression test anyway per the story's explicit acceptance criterion.
- `process_metrics()` called twice returns the identical object (`is`
  check).
- Env vars are read only inside `start_metrics_server`/CLI call sites
  (Story 3+), never at `src/utils/metrics.py` import time — Story 1 only
  defines the constants (`DEFAULT_METRICS_PORT`) other stories reference.

## Tests (`tests/test_metrics.py`)

1. `test_import_performs_no_prometheus_calls` — import the module fresh
   (via `importlib.reload` or a subprocess, whichever is simpler/least
   flaky — subprocess is simplest since `prometheus_client`'s own default
   `REGISTRY` is a process-global we must not touch either) and assert no
   metric families exist on `prometheus_client.REGISTRY` that this module
   would have added (in practice: assert `metrics._process_metrics is
   None` immediately after import, which is sufficient given the module
   never touches the *default* registry at all — it only builds its own via
   `build_registry()`).
2. `test_independent_registries_no_duplicate_timeseries` — construct two
   `GraphLoaderMetrics(registry=CollectorRegistry())` in the same process;
   no exception.
3. `test_process_metrics_singleton` — call `process_metrics()` twice,
   assert identity equality. Reset `metrics._process_metrics = None` in a
   fixture teardown so this test doesn't leak into other test files that
   also call `process_metrics()` later in the slice.
4. `test_label_policy_docstring_present` — assert the module docstring
   contains the literal strings `"kind"`, `"failure_class"`, and
   `"Never a label"` (cheap proof the policy table lives in the module,
   per the story's explicit AC, not just in this plan document).

## Deviation note for the completion doc

`RETRY_REASONS` includes `"missing-endpoint"` (hyphenated) per
`stories.md`'s own table, but `dead_letter.py`'s `FAILURE_CLASSES` uses
underscores (`missing_endpoint`, `ambiguous_endpoint`) — these are two
different label domains (`reason` on `retry_total` vs. `failure_class` on
`dlq_routed_total`) that happen to describe similar concepts with different
spellings inherited from two different Phase 5/6 documents. Kept as-is,
each faithful to its own source of truth, rather than silently
reconciling casing across two independently-owned enums; flagged for the
reviewer.
