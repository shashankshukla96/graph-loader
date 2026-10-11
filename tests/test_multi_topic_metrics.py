"""v7-S6 story 6.1: metrics cardinality with multi-topic relationship types.

A type adds one lag series per topic (``{kind, name=<type>, topic}``) and nothing else: no per-record,
per-pair or per-sub-stage label.  ``tests/test_metrics.py`` keeps owning the exact label sets.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry

from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
from src.utils.metrics import GraphLoaderMetrics
from tests.test_metrics import EXPECTED_CATALOGUE

TYPES = ("LIKES", "FOLLOWS", "RATED")
TOPICS_PER_TYPE = 4


def _topics(edge_type: str) -> list[str]:
    return [f"{edge_type.lower()}-t{i}" for i in range(TOPICS_PER_TYPE)]


def _lag_series(metrics: GraphLoaderMetrics) -> list:
    return [s for family in metrics.registry.collect() if family.name == "graph_loader_consumer_lag_records"
            for s in family.samples]


def _monitor(metrics: GraphLoaderMetrics, edges=None) -> RelationshipBulkMonitor:
    edges = edges or [SimpleNamespace(type=t, topics=_topics(t), replicas=1) for t in TYPES]
    marks = {(topic, 0): (0, 10) for edge in edges for topic in edge.topics}
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        topic: SimpleNamespace(error=None, partitions={0: SimpleNamespace()}) for (topic, _p) in marks
    })
    watermark.get_watermark_offsets.side_effect = lambda tp, timeout=None: marks[(tp.topic, tp.partition)]
    future = MagicMock()
    future.result.return_value = SimpleNamespace(topic_partitions=[TopicPartition(t, p, 4) for (t, p) in marks])
    admin.list_consumer_group_offsets.side_effect = lambda requests, require_stable=True: {requests[0].group_id: future}
    control.poll.return_value = None
    return RelationshipBulkMonitor(
        edges, "run-1", {(e.type, "0") for e in edges}, {(e.type, "0"): MagicMock() for e in edges},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01, metrics=metrics,
    )


def test_three_types_with_four_topics_each_yield_exactly_twelve_lag_series() -> None:
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    monitor = _monitor(metrics)
    monitor._pretouch_lag_series()
    series = _lag_series(metrics)
    assert len(series) == len(TYPES) * TOPICS_PER_TYPE == 12
    assert {(s.labels["name"], s.labels["topic"]) for s in series} == {(t, topic) for t in TYPES for topic in _topics(t)}
    assert {frozenset(s.labels) for s in series} == {frozenset({"kind", "name", "topic"})}

    monitor._refresh_consumer_lag()  # a refresh updates the same series, it never adds one
    series = _lag_series(metrics)
    assert len(series) == 12
    assert {s.value for s in series} == {6.0}  # high 10, committed 4, per topic


def test_only_the_lag_gauge_has_a_topic_label_and_no_label_names_a_record_or_substage() -> None:
    forbidden = {"pair", "key", "source_key", "target_key", "record", "offset", "partition", "substage", "rank",
                 "run_id", "plan_id", "replica", "group"}
    with_topic = {name for name, (_type, labels) in EXPECTED_CATALOGUE.items() if "topic" in labels}
    assert with_topic == {"graph_loader_consumer_lag_records"}
    for name, (_type, labels) in EXPECTED_CATALOGUE.items():
        assert not forbidden & set(labels), f"{name} has an unbounded label: {labels}"


def test_precedence_substage_monitors_on_one_registry_add_no_series_beyond_the_types_topics() -> None:
    """Sub-stages (one single-topic monitor per topic, run one after another) reuse the type's own series."""
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    topics = _topics("LIKES")
    for rank, topic in enumerate(topics):
        sub_edge = SimpleNamespace(type="LIKES", topics=[topic], replicas=1)
        monitor = _monitor(metrics, edges=[sub_edge])
        monitor._refresh_consumer_lag()
        series = _lag_series(metrics)
        assert len(series) == rank + 1  # grows by exactly the topic of this sub-stage
    assert {(s.labels["name"], s.labels["topic"]) for s in series} == {("LIKES", t) for t in topics}
    assert {frozenset(s.labels) for s in series} == {frozenset({"kind", "name", "topic"})}
    # a rerun of a sub-stage updates the same series
    _monitor(metrics, edges=[SimpleNamespace(type="LIKES", topics=[topics[0]], replicas=1)])._refresh_consumer_lag()
    assert len(_lag_series(metrics)) == len(topics)
