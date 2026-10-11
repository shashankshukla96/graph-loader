"""Kafka consumer-group names for relationship loaders and the retry consumer.

Every launcher, monitor and migration step derives these names here, so they
cannot drift apart.

* Bulk groups are run-scoped: ``<prefix>-<type>-<run id>``. The run id is the
  phase id the CLI derives (``<R>.bulk.0.<h>``, ``<R>.isolated.<i>.<h>``,
  ``<R>...topic.<i>.<h>``), so every bulk stage proves its own drain from an
  empty group. This module does not change those names.
* Stream groups are stable, independent of the run id: ``<prefix>-<type>``
  for a relationship type and ``<prefix>-retry-consumer`` for the retry
  consumer. A stream restart resumes from the committed offsets, with or
  without ``--run-id``.
* Before this naming, stream groups were ``<prefix>-<type>-<run id>`` and
  ``graph-loader-retry-consumer-<run id>``. The ``legacy_*`` helpers name them
  for the one-time offset migration (``src.orchestrator.stream_groups``).

Edge types are Cypher identifiers (no ``-``), so ``<prefix>-retry-consumer``
can never equal ``<prefix>-<type>``.
"""
from __future__ import annotations

RETRY_CONSUMER_GROUP_SUFFIX = "retry-consumer"


def bulk_edge_group(prefix: str, edge_type: str, run_id: str) -> str:
    """Run-scoped bulk relationship group (unchanged since v3)."""
    return f"{prefix}-{edge_type}-{run_id}"


def stream_edge_group(prefix: str, edge_type: str) -> str:
    """Stable stream relationship group: one per type, every topic and label pair."""
    return f"{prefix}-{edge_type}"


def stream_retry_group(prefix: str) -> str:
    """Stable retry-consumer group, namespaced by the schema's ``consumer_group_id``."""
    return f"{prefix}-{RETRY_CONSUMER_GROUP_SUFFIX}"


def edge_group(prefix: str, edge_type: str, *, mode: str, run_id: str | None) -> str:
    """The group a relationship loader of ``mode`` uses.

    Stream mode is always stable. Bulk keeps its run-scoped name; a bulk
    launch without a run id falls back to the loader's own default
    (``<prefix>-<type>``), as before.
    """
    if mode == "stream" or run_id is None:
        return stream_edge_group(prefix, edge_type)
    return bulk_edge_group(prefix, edge_type, run_id)


def legacy_stream_edge_group(prefix: str, edge_type: str, run_id: str) -> str:
    """The run-scoped stream group of releases before stable stream groups."""
    return f"{prefix}-{edge_type}-{run_id}"


def legacy_retry_group(run_id: str) -> str:
    """The retry-consumer group of releases before stable stream groups.

    Still the default of a standalone ``python -m src.loader.retry_consumer``
    started without ``KAFKA_GROUP_ID``; the CLI always sets the stable name.
    """
    return f"graph-loader-retry-consumer-{run_id}"
