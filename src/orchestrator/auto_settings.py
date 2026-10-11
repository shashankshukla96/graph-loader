"""Scheduling parameters derived from the schema, the data and the Neo4j server.

A new graph model needs only its YAML schema and data on the schema-declared
topics; everything below has a documented, data-driven default that a CLI
flag may override:

* **Planned writers** ``K`` = Neo4j's available processors + 2 (one
  transaction per core plus a little slack for fetch/commit round trips),
  probed from the server's JVM; :data:`DEFAULT_NEO4J_CORES` when the probe is
  unavailable.  Never more writers than planned chunks.
* **Buckets per label** ``B = clamp(K, 8, 64)``: one relationship type
  between two labels can run at most ``B`` node-disjoint transactions at a
  time (a permutation of the ``B x B`` bucket grid), so ``B >= K`` lets a
  single large type keep every writer busy -- but at most
  ``isqrt(R / (T * 500))`` (at least 2) for ``T`` edge types with records,
  so small inputs are not cut into thousands of tiny units.
  **Self-referencing rows** (a label pair ``(L, L)``, e.g. ``FRIEND_OF``
  Person->Person) hold *two* buckets of the same label per chunk, so only
  about ``B / 2`` of them fit at once.  When such rows are a share ``s``
  above :data:`SELF_REF_SHARE_THRESHOLD` of all records, ``B =
  clamp(round(K * (1 + s)), 8, 64)`` instead (same volume cap): ``K``
  writers whose chunks hold ``1 + s`` buckets of the shared label on
  average then still find about ``K`` node-disjoint chunks.  Loads without
  such rows (or with only a few) get exactly ``clamp(K, 8, 64)``.
* **Chunk rows** ``C = clamp(ceil(R / (20 K)), 500, 5000)`` for ``R``
  relationship records: at least ~20 chunks per writer for load balance on
  small inputs, at most 5000 rows so a transaction's fixed cost stays a few
  percent (fitted server cost: 3 ms + 15 us/row).
* **Hub degree** ``H = C``: a node with at least one chunk's worth of rows
  gets a resource of its own, so its (necessarily serial) rows never block
  the other nodes of its bucket.
* **Node-loader replicas** for labels whose YAML omits ``replicas``: one
  replica when the node topic retains fewer than
  :data:`DEFAULT_NODE_REPLICAS_MIN_RECORDS` records, else one per topic
  partition (a consumer group cannot use more), capped at
  :data:`DEFAULT_MAX_NODE_REPLICAS`.
"""
from __future__ import annotations

import logging
import math
import os
from typing import Iterable, Mapping
import uuid


logger = logging.getLogger(__name__)

DEFAULT_NEO4J_CORES = 8
MAX_PLAN_WRITERS = 64
MIN_AUTO_BUCKETS, MAX_AUTO_BUCKETS = 8, 64
MIN_VOLUME_BUCKETS = 2
MIN_AUTO_CHUNK_ROWS, MAX_AUTO_CHUNK_ROWS = 500, 5000
# Self-referencing share of the relationship records above which the bucket
# count grows to ``K * (1 + share)``; at or below it (IMDb's EPISODE_OF is
# 0.3%) the derivation is exactly ``clamp(K, 8, 64)``.
SELF_REF_SHARE_THRESHOLD = 0.10
CHUNKS_PER_WRITER = 20
DEFAULT_NODE_REPLICAS_MIN_RECORDS = 10_000
DEFAULT_MAX_NODE_REPLICAS = 8
DEFAULT_MAX_PLAN_WORKERS = 4

_CORE_QUERIES = (
    "CALL dbms.queryJmx('java.lang:type=OperatingSystem') YIELD attributes "
    "RETURN attributes.AvailableProcessors.value AS cores",
)


def probe_neo4j_cores(driver) -> int | None:
    """Return the Neo4j server's available processors, or ``None`` if unknown."""
    from src.utils.neo4j_session import open_session

    for query in _CORE_QUERIES:
        try:
            with open_session(driver) as session:
                record = session.run(query).single()
            cores = None if record is None else record["cores"]
            if isinstance(cores, int) and not isinstance(cores, bool) and cores > 0:
                return cores
        except Exception as exc:  # procedure missing, permissions, old server
            logger.info("stage=auto-settings Neo4j core probe unavailable: %s", exc)
    return None


def default_plan_writers(neo4j_cores: int | None) -> int:
    cores = neo4j_cores if neo4j_cores else DEFAULT_NEO4J_CORES
    return max(2, min(MAX_PLAN_WRITERS, cores + 2))


def default_plan_workers(cpu_count: int | None = None) -> int:
    cpus = cpu_count if cpu_count is not None else (os.cpu_count() or 1)
    return max(1, min(DEFAULT_MAX_PLAN_WORKERS, cpus))


def self_ref_share(records: int, self_ref_records: int) -> float:
    """Share of ``records`` that belong to self-referencing label pairs, in ``[0, 1]``."""
    return min(1.0, max(0, self_ref_records) / records) if records > 0 else 0.0


def derive_plan_buckets(records: int, writers: int, *, edge_types: int = 1,
                        self_ref_records: int = 0) -> tuple[int, str]:
    """Return the derived bucket count ``B`` and the reason, for the settings log line.

    ``self_ref_records`` counts the records of self-referencing label pairs
    (source label == target label; for a type of several label pairs only
    its same-label pairs' topics, see
    :func:`~src.loader.edge_planner.self_referencing_rows`).
    The reason is ``writers`` (``clamp(K, 8, 64)``) or ``self_ref_share=S``
    (``clamp(round(K * (1 + S)), 8, 64)``, for ``S`` above
    :data:`SELF_REF_SHARE_THRESHOLD`), prefixed with ``volume_cap=V,`` when
    the volume cap ``V = isqrt(R / (T * 500))`` (at least 2) is lower.
    """
    writers = max(1, writers)
    records = max(0, records)
    share = self_ref_share(records, self_ref_records)
    if share > SELF_REF_SHARE_THRESHOLD:
        wanted = math.floor(writers * (1.0 + share) + 0.5)  # round half up
        reason = f"self_ref_share={share:.2f}"
    else:
        wanted, reason = writers, "writers"
    by_writers = max(MIN_AUTO_BUCKETS, min(MAX_AUTO_BUCKETS, wanted))
    by_volume = math.isqrt(records // (max(1, edge_types) * MIN_AUTO_CHUNK_ROWS))
    buckets = max(MIN_VOLUME_BUCKETS, min(by_writers, by_volume))
    if buckets < by_writers:
        reason = f"volume_cap={buckets},{reason}"
    return buckets, reason


def derive_plan_parameters(records: int, writers: int, *, edge_types: int = 1, buckets: int | None = None,
                           hub_degree: int | None = None, chunk_rows: int | None = None,
                           self_ref_records: int = 0) -> dict[str, int]:
    """Return ``buckets``, ``hub_degree`` and ``chunk_rows``; explicit values win.

    ``edge_types`` is the number of relationship types with records: small
    inputs get fewer buckets so a unit (one type's rows between two buckets)
    still averages at least :data:`MIN_AUTO_CHUNK_ROWS` rows.
    ``self_ref_records`` (records of same-label pairs) raises the bucket
    count of self-reference-heavy loads (:func:`derive_plan_buckets`); it
    changes neither ``chunk_rows`` nor ``hub_degree``.
    """
    writers = max(1, writers)
    if buckets is None:
        buckets, _ = derive_plan_buckets(records, writers, edge_types=edge_types,
                                         self_ref_records=self_ref_records)
    if chunk_rows is None:
        target = math.ceil(max(0, records) / (CHUNKS_PER_WRITER * writers)) if records > 0 else MAX_AUTO_CHUNK_ROWS
        chunk_rows = max(MIN_AUTO_CHUNK_ROWS, min(MAX_AUTO_CHUNK_ROWS, target))
    if hub_degree is None:
        hub_degree = chunk_rows
    return {"buckets": buckets, "hub_degree": hub_degree, "chunk_rows": chunk_rows}


def topic_sizes(bootstrap_servers: str, topics: Iterable[str], *, consumer_factory=None,
                timeout: float = 10.0) -> dict[str, tuple[int, int]]:
    """Return ``topic -> (retained records, partitions)``; missing topics are omitted."""
    if consumer_factory is None:
        from confluent_kafka import Consumer as consumer_factory
    from confluent_kafka import TopicPartition

    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-sizes-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(timeout=timeout)
        sizes: dict[str, tuple[int, int]] = {}
        for topic in sorted(set(topics)):
            item = metadata.topics.get(topic)
            if item is None or item.error is not None or not item.partitions:
                continue
            total = 0
            for partition in item.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
                total += max(0, high - low)
            sizes[topic] = (total, len(item.partitions))
        return sizes
    finally:
        consumer.close()


def replicas_are_explicit(node) -> bool:
    """Whether the YAML set ``replicas`` for ``node`` (non-pydantic objects: always)."""
    fields = getattr(node, "model_fields_set", None)
    return fields is None or "replicas" in fields


def node_replicas_for(records: int, partitions: int, *, min_records: int = DEFAULT_NODE_REPLICAS_MIN_RECORDS,
                      max_replicas: int = DEFAULT_MAX_NODE_REPLICAS) -> int:
    """The size policy: 1 below ``min_records`` records, else one per partition up to ``max_replicas``.

    A tiny label gains nothing from several loader containers, but each costs
    a container launch, a consumer-group rebalance while it joins, and a stop.
    """
    if min_records and records < min_records:
        return 1
    return max(1, min(partitions, max_replicas))


def auto_node_replicas(nodes, sizes: Mapping[str, tuple[int, int]], *,
                       min_records: int = DEFAULT_NODE_REPLICAS_MIN_RECORDS,
                       max_replicas: int = DEFAULT_MAX_NODE_REPLICAS) -> dict[str, int]:
    """Return ``label -> replicas``: explicit YAML values, else the size policy."""
    result = {}
    for node in nodes:
        if replicas_are_explicit(node) or min_records <= 0 or node.topic not in sizes:
            result[node.label] = node.replicas
            continue
        records, partitions = sizes[node.topic]
        result[node.label] = node_replicas_for(records, partitions, min_records=min_records,
                                               max_replicas=max_replicas)
    return result
