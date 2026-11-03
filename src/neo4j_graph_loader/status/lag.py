"""``graph-loader status --refresh-lag`` (E15.F1.S6): ONE bounded Kafka query for the consumer lag of a run.

Everything here has a deadline: the whole refresh takes at most ``timeout`` seconds, whether Kafka is down, slow or
answers. It reads the run's schema copy (``runtime_schema.yaml`` in the attempt directory) and the status document, asks
the brokers for each entity's high watermarks and its group's committed offsets, and returns ``{(kind, name): lag}``.
It writes nothing anywhere and never touches the run.
"""
from __future__ import annotations

import time
import uuid
from pathlib import Path

from neo4j_graph_loader.status import model


class LagRefreshError(RuntimeError):
    """The brokers could not answer within the deadline (or the run's schema is not available)."""


def _groups(schema, status: dict) -> dict[tuple[str, str], tuple[str, list[str]]]:
    from neo4j_graph_loader.utils.consumer_groups import edge_group
    prefix = schema.loading.consumer_group_id
    mode, run_id = status.get("mode"), status.get("run_id")
    wanted: dict[tuple[str, str], tuple[str, list[str]]] = {}
    for node in schema.nodes:
        wanted[("node", node.label)] = (f"{prefix}-{node.label}", [node.topic])
    for edge in schema.edges:
        phase = next((p["phase_id"] for p in status.get("phases", ()) if p.get("edge_type") == edge.type
                      and p.get("kind") in ("bulk", "isolated")), run_id)
        wanted[("edge", edge.type)] = (edge_group(prefix, edge.type, mode=mode or "stream", run_id=phase), list(edge.topics))
    return wanted


def refresh_lag(attempt: Path | str, status: dict, *, bootstrap_servers: str, timeout: float = 5.0) -> dict[tuple[str, str], int]:
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient
    from neo4j_graph_loader.utils.schema_loader import load_schema

    started = time.monotonic()

    def remaining() -> float:
        left = timeout - (time.monotonic() - started)
        if left <= 0:
            raise LagRefreshError(f"no answer from {bootstrap_servers} within {timeout:g}s")
        return left

    try:
        schema = load_schema(Path(attempt) / "runtime_schema.yaml")
    except Exception as exc:                                              # noqa: BLE001
        raise LagRefreshError(f"the run's schema copy cannot be read: {exc}") from None
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"graph-loader-status-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, "socket.timeout.ms": int(min(timeout, 5) * 1000)})
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": int(min(timeout, 5) * 1000)})
    result: dict[tuple[str, str], int] = {}
    try:
        try:
            metadata = consumer.list_topics(timeout=remaining())
        except KafkaException as exc:
            raise LagRefreshError(f"cannot read the broker metadata: {exc}") from None
        for key, (group, topics) in _groups(schema, status).items():
            partitions: list[tuple[str, int, int, int]] = []
            for topic in topics:
                item = metadata.topics.get(topic)
                if item is None or item.error is not None:
                    continue
                for partition in sorted(item.partitions):
                    low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=remaining())
                    partitions.append((topic, partition, low, high))
            if not partitions:
                continue
            request = ConsumerGroupTopicPartitions(group, [TopicPartition(t, p) for t, p, _l, _h in partitions])
            try:
                response = admin.list_consumer_group_offsets([request])[group].result(timeout=remaining())
            except KafkaException as exc:
                raise LagRefreshError(f"cannot read the committed offsets of group {group}: {exc}") from None
            committed = {(item.topic, item.partition): item.offset for item in response.topic_partitions}
            lag = 0
            for topic, partition, low, high in partitions:
                offset = committed.get((topic, partition))
                offset = offset if isinstance(offset, int) and offset >= 0 else low
                lag += max(0, high - max(offset, low))
            result[key] = lag
    except LagRefreshError:
        raise
    except Exception as exc:                                              # noqa: BLE001 - KafkaException, timeouts, ...
        raise LagRefreshError(f"the lag query failed: {exc}") from None
    finally:
        try:
            consumer.close()
        except Exception:                                                 # noqa: BLE001
            pass
    return result


def overlay(status: dict, lag: dict[tuple[str, str], int], *, now: float | None = None) -> dict:
    """``status`` with the refreshed lag in each entity's ``lag`` field (source ``status --refresh-lag``)."""
    stamp = model.timestamp(time.time() if now is None else now)
    types = []
    for entry in status["types"]:
        if (entry["kind"], entry["name"]) in lag:
            entry = {**entry, "lag": model.observed(lag[(entry["kind"], entry["name"])], stamp, "status --refresh-lag")}
        types.append(entry)
    return {**status, "types": types}
