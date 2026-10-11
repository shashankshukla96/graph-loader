"""v7-S7 story 7.9: a multi-pair type exports exactly the series a multi-topic type exports.

No metric label names a label pair: every family keeps ``{kind, name=<type>}`` (lag adds ``topic``, the DLQ
backlog ``queue``). The pair appears only in logs (``pair=Person>Comment``) and in the plan's ``types[T].pairs``.

Compared here: the lease write path (writer and lease series, one ``lease_wait`` per flush), the zero series and the
lag series of the real EdgeConfig. The EdgeLoader-level, DLQ and retry series are labelled by type exactly like
these and are not exercised separately.
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes, likes_schema_dict, record
from src.loader.dead_letter import DeadLetterRecordSink
from src.loader.edge_execution import LabelledEndpointSink, build_edge_execution
from src.loader.edge_loader import PendingEdgeRecord
from src.loader.edge_records import normalize_edge_record
from src.loader.lease_client import BucketCellRouter, BucketLeaseClient
from src.loader.retry_consumer import (
    build_pair_retry_writers, build_retry_base_writer, build_retry_lease_coordinator, build_retry_writer,
)
from src.models.schema import GraphSchema
from src.utils import metrics as metrics_module
from src.utils.metrics import GraphLoaderMetrics
from test_multi_pair_leases import _LeaseServer
from test_multi_topic_metrics import _monitor

TOPICS = (POSTS, BACKFILL, COMMENTS)


def _single_pair_three_topics():
    """The same type and topics, all three binding Person->Post."""
    data = likes_schema_dict()
    for entry in data["edges"][0]["topics"]:
        entry.update(nodes={"source": "Person", "target": "Post"}, target_key_property="post_id")
    return GraphSchema.model_validate(data).edges[0]


def _series(edge) -> tuple[set, set]:
    """Run what a stream loader of ``edge`` exports for one lease flush (both pairs' rows), the zero series and
    the type's lag series, on a fresh registry. Returns ``({(family, label names)}, {label values})``."""
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    metrics_module.initialize_zero_series(metrics, kind="edge", name=edge.type)
    store = GraphStore({"Person": set(range(5)), "Post": set(range(5)), "Comment": set(range(5))})
    pending = []
    for i in range(30):
        topic = TOPICS[i % 3]
        raw = record(topic, i % 5, (i * 3) % 5) if edge.is_multi_pair else record(POSTS, i % 5, (i * 3) % 5)
        pending.append(PendingEdgeRecord(normalize_edge_record(raw, edge.topic_view(topic)), topic, i % 2, i))
    with _LeaseServer(4) as address:
        _w, _p, _b, coordinator = build_edge_execution(
            edge, store.driver(), metrics=metrics, mode="stream",
            environ={"GRAPH_LOADER_LEASE_ADDR": address, "GRAPH_LOADER_LEASE_BUCKETS": "4"},
            lease_client_factory=lambda: BucketLeaseClient(address, bucket_count=4),
        )
        try:
            assert all(result.success for result in coordinator.execute_pending(tuple(pending)))
        finally:
            coordinator.close()
    monitor = _monitor(metrics, [edge])  # the real EdgeConfig: the monitor must not read a pair field
    monitor._pretouch_lag_series()
    monitor._refresh_consumer_lag()
    shapes, values = set(), set()
    for family in metrics.registry.collect():
        for sample in family.samples:
            shapes.add((sample.name, tuple(sorted(sample.labels))))
            values.update(sample.labels.values())
    lease_counts = [s for f in metrics.registry.collect() if f.name == "graph_loader_lease_wait_seconds"
                    for s in f.samples if s.name.endswith("_count")]
    return shapes, values, lease_counts


def test_a_two_pair_three_topic_type_exports_the_series_of_a_one_pair_three_topic_type() -> None:
    multi_shapes, multi_values, multi_lease = _series(likes())
    single_shapes, single_values, single_lease = _series(_single_pair_three_topics())
    assert multi_shapes == single_shapes
    assert multi_values == single_values
    assert [s.value for s in multi_lease] == [s.value for s in single_lease] == [1.0]  # one observation per flush


class _CapturingProducer:
    def __init__(self) -> None:
        self.envelopes = []

    def publish(self, topic, envelope, **_kwargs) -> None:
        self.envelopes.append(envelope)


def _dlq_envelopes(edge, count: int) -> list:
    """``count`` DLQ envelopes of ``edge``, cycling over its three topics, keyed as the live loader keys them."""
    producer = _CapturingProducer()
    sink = DeadLetterRecordSink(producer=producer, topic="LIKES.dlq", run_id="r", replica_id="0", kind="edge",
                                clock=lambda: 1)
    for i in range(count):
        topic = TOPICS[i % 3]
        view = edge.topic_view(topic)
        target = LabelledEndpointSink(sink, edge.pair_for_topic(topic).labels) if edge.is_multi_pair else sink
        raw = record(topic, i % 5, (i * 3) % 5) if edge.is_multi_pair else record(POSTS, i % 5, (i * 3) % 5)
        target.route_unresolved_endpoint(
            topic=topic, partition=0, offset=i, record=normalize_edge_record(raw, view), edge_type="LIKES",
            source_key_property=view.source_key_property, target_key_property=view.target_key_property,
            failure_class="missing_endpoint", reason_code="missing_target", error_text="x", retry_after_seconds=0.0,
        )
    return producer.envelopes


def _retry_lease_wait_counts(edge) -> list:
    """Three ``retry_batch`` calls through lease-coordinated retry writers wired like ``retry_consumer.main``:
    pair coordinators that do not observe ``lease_wait`` and a split writer that does, once per call."""
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    store = GraphStore({"Person": set(range(5)), "Post": set(range(5)), "Comment": set(range(5))})
    envelopes = _dlq_envelopes(edge, 30)
    with _LeaseServer(4) as address:
        def make(config, deadline):
            base = build_retry_base_writer(store.driver(), config, tx_timeout_seconds=None, leased=True)
            coordinator = build_retry_lease_coordinator(
                base, config, router=BucketCellRouter(config.nodes.source, config.nodes.target, 4),
                client_factory=lambda: BucketLeaseClient(address, bucket_count=4), worker_count=2,
                tx_timeout_seconds=None, metrics=metrics, drain_deadline=deadline,
                observe_lease_wait=not edge.is_multi_pair,
            )
            return build_retry_writer(base, coordinator=coordinator)

        writer = build_pair_retry_writers(edge, make, concurrent=True, metrics=metrics, observe_lease_wait=True)
        try:
            for call in range(3):
                outcomes = writer.retry_batch(envelopes[call * 10:(call + 1) * 10])
                assert len(outcomes) == 10
        finally:
            writer.close()
    return [(dict(s.labels), s.value) for f in metrics.registry.collect() if f.name == "graph_loader_lease_wait_seconds"
            for s in f.samples if s.name.endswith("_count")]


def test_retry_lease_wait_is_observed_once_per_call_for_a_multi_pair_type() -> None:
    """Final follow-ups S1: the retry path observed ``lease_wait`` once per pair per call (each pair coordinator
    observed its own); now the split writer observes the sum once, like a single-pair type's coordinator."""
    expected = [({"kind": "retry-consumer", "name": "LIKES"}, 3.0)]
    assert _retry_lease_wait_counts(likes()) == expected
    assert _retry_lease_wait_counts(_single_pair_three_topics()) == expected


def test_retry_consumer_main_builds_pair_coordinators_that_leave_lease_wait_to_the_split_writer() -> None:
    from contextlib import ExitStack
    from unittest.mock import MagicMock, patch

    from multi_pair_support import likes_schema
    from src.loader.retry_consumer import PairSplitRetryWriter, main as retry_consumer_main
    from tests.test_multi_topic_dlq import _main_patches

    schema = likes_schema(order_by=False, topic_conflict="arrival", dead_letter={"enabled": True})
    with ExitStack() as stack:
        rc_cls, _sink = _main_patches(stack, schema)
        stack.enter_context(patch("src.loader.retry_consumer.ParkedPairIndex", return_value=MagicMock()))
        stack.enter_context(patch("src.loader.lease_client.BucketLeaseClient"))
        build = stack.enter_context(patch("src.loader.retry_consumer.build_retry_lease_coordinator"))
        stack.enter_context(patch.dict("os.environ", {"GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease:7700",
                                                      "GRAPH_LOADER_LEASE_BUCKETS": "16"}))
        assert retry_consumer_main(["--dlq-topics", "LIKES.dlq", "--lease-address", "lease:7700"]) == 0
    assert [call.kwargs["observe_lease_wait"] for call in build.call_args_list] == [False, False]
    writer = rc_cls.call_args.kwargs["writer_by_edge_type"]["LIKES"]
    assert isinstance(writer, PairSplitRetryWriter) and writer._observe_lease_wait is True


def test_no_label_value_names_a_node_label_of_a_pair() -> None:
    _shapes, values, _lease = _series(likes())
    assert not {value for value in values if "Comment" in value or "Post" in value or ">" in value}
    assert {"LIKES", *TOPICS} <= values
