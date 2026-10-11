"""Final follow-ups S2: migration guards for a type whose topics or label pairs change while its DLQ is not drained.

* A multi-pair parked-pair index stamps each entry's label pair (``"labels": [S, T]`` in the value; single-pair
  values are unchanged), so a rebuilt index can tell entries of a removed topic, of a topic since bound to
  another pair, and of a type collapsed back to one pair. Loaders and the retry consumer log a WARNING;
  ``start`` refuses (``--accept-dlq-leftovers TYPE`` overrides), reading only the compacted index topic.
* A single-pair type reads a DLQ key labelled with its own pair (left from its multi-pair days) back as
  ``[sk, tk]``, the mirror of ``canonical_pair_key``. The fuzz version is in ``test_retry_pair_order_fuzz.py``.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from argparse import Namespace
from unittest.mock import MagicMock, patch

from multi_pair_support import BACKFILL, COMMENTS, POSTS, likes_schema, likes_schema_dict
from src import cli
from src.loader.parked_pair_index import ParkedPairIndex, index_leftovers, warn_index_leftovers
from src.loader.retry_consumer import RetryConsumer, single_pair_key
from src.models.schema import GraphSchema
from test_parked_pair_index import _FakeTopicLog, _wait_caught_up
from tests.test_multi_topic_dlq import _AckingIndexProducer
from tests.test_parked_pair_index import _FakeConsumer as _IndexConsumer
from tests.test_retry_consumer import _Clock, _dlq_message, _envelope, _FakeConsumer, _FakeSink
from tests.test_retry_pair_order_fuzz import RecordingWriter, SeqIndex

INDEX = "LIKES.dlq.parked-pairs"
_REAL_REQUIRE_NO_LEFTOVERS = cli._require_no_dlq_leftovers  # conftest replaces it in every unit test
_DL = {"enabled": True, "topic": "LIKES.dlq"}  # an explicit name: the DLQ survives every schema change below


def _schema(**kwargs) -> GraphSchema:
    return likes_schema(order_by=False, topic_conflict="arrival", dead_letter=_DL, **kwargs)


def _edge(**kwargs):
    return _schema(**kwargs).edges[0]


def _collapsed_edge():
    """LIKES back to one pair, Person->Post, over topics a and b."""
    return _edge(topics=(POSTS, BACKFILL))


def _rebound_edge():
    """likes-comments now binds Person->Post: one pair over three topics."""
    data = likes_schema_dict(order_by=False, topic_conflict="arrival", dead_letter=_DL)
    for entry in data["edges"][0]["topics"]:
        entry.update(nodes={"source": "Person", "target": "Post"}, target_key_property="post_id")
    return GraphSchema.model_validate(data).edges[0]


def _index(edge, log: _FakeTopicLog) -> ParkedPairIndex:
    index = ParkedPairIndex(
        topic=INDEX, bootstrap_servers="kafka:9092", writable=True, sequenced=edge.is_multi_topic,
        pair_of_topic=edge.pair_labels_for_topic if edge.is_multi_pair else None,
        consumer_factory=lambda config: _IndexConsumer(config, log, INDEX),
        producer_factory=lambda config: _AckingIndexProducer(config, log), poll_timeout_seconds=0.01,
    )
    index.start()
    return index


def _written_by_the_multi_pair_type() -> _FakeTopicLog:
    """One outstanding row per topic, written by the multi-pair LIKES."""
    log = _FakeTopicLog()
    index = _index(_edge(), log)
    try:
        for offset, topic in enumerate((POSTS, BACKFILL, COMMENTS)):
            index.add(7, 7, original_topic=topic, original_partition=0, original_offset=offset, first_failure_at_ms=1)
    finally:
        index.stop()
    return log


def _leftovers(edge, log):
    index = _index(edge, log)
    try:
        _wait_caught_up(index)
        return [(topic, labels, records, reason) for topic, labels, records, reason in index_leftovers(index, edge)]
    finally:
        index.stop()


# ── the index stamps the pair ────────────────────────────────────────────────


def test_a_multi_pair_index_stamps_each_entrys_pair_and_a_single_pair_value_is_unchanged() -> None:
    log = _written_by_the_multi_pair_type()
    assert [json.loads(value) for _key, value in log.records] == [
        {"first_failure_at_ms": 1, "labels": ["Person", "Post"], "seq": 1},
        {"first_failure_at_ms": 1, "labels": ["Person", "Post"], "seq": 2},
        {"first_failure_at_ms": 1, "labels": ["Person", "Comment"], "seq": 1},
    ]
    single = _FakeTopicLog()
    index = _index(_collapsed_edge(), single)
    try:
        index.add(7, 7, original_topic=POSTS, original_partition=0, original_offset=0, first_failure_at_ms=1)
    finally:
        index.stop()
    assert single.records[0][1] == b'{"first_failure_at_ms":1,"seq":1}'  # no stamp: single-pair bytes as before


def test_an_unchanged_type_has_no_leftovers() -> None:
    assert _leftovers(_edge(), _written_by_the_multi_pair_type()) == []


def test_a_removed_topic_a_rebound_topic_and_a_collapsed_type_are_reported() -> None:
    log = _written_by_the_multi_pair_type()
    assert _leftovers(_edge(topics=(POSTS, COMMENTS)), log) == [(BACKFILL, ("Person", "Post"), 1, "removed")]
    assert _leftovers(_rebound_edge(), log) == [(COMMENTS, ("Person", "Comment"), 1, "rebound")]
    assert _leftovers(_collapsed_edge(), log) == [(COMMENTS, ("Person", "Comment"), 1, "removed")]


def test_a_type_collapsed_onto_the_other_pair_reports_the_rebound_rows() -> None:
    """LIKES now reads only likes-posts and likes-comments, both as Person->Comment."""
    data = likes_schema_dict(order_by=False, topic_conflict="arrival", dead_letter=_DL, topics=(POSTS, COMMENTS))
    for entry in data["edges"][0]["topics"]:
        entry.update(nodes={"source": "Person", "target": "Comment"}, target_key_property="comment_id")
    edge = GraphSchema.model_validate(data).edges[0]
    assert not edge.is_multi_pair
    assert sorted(_leftovers(edge, _written_by_the_multi_pair_type())) == [
        (POSTS, ("Person", "Post"), 1, "rebound"), (BACKFILL, ("Person", "Post"), 1, "removed"),
    ]


def test_loaders_and_the_retry_consumer_warn_once_the_index_is_rebuilt(caplog) -> None:
    edge = _collapsed_edge()
    index = _index(edge, _written_by_the_multi_pair_type())
    try:
        _wait_caught_up(index)
        with caplog.at_level(logging.WARNING):
            assert warn_index_leftovers(index, edge, stage="edge-loader")
    finally:
        index.stop()
    assert "reason=index_holds_entries_the_type_no_longer_binds" in caplog.text
    assert f"1 record(s) of topic {COMMENTS} written as Person->Comment, which LIKES no longer reads" in caplog.text


def test_the_warning_is_wired_after_each_index_start() -> None:
    import inspect

    from src.loader import edge_loader, retry_consumer

    assert 'warn_index_leftovers(parked_pair_index, edge_config, stage="edge-loader")' in inspect.getsource(edge_loader)
    assert 'warn_index_leftovers(index, edge, stage="retry-consumer")' in inspect.getsource(retry_consumer.main)


# ── the start preflight ──────────────────────────────────────────────────────


class _Admin:
    def __init__(self, topics) -> None:
        self.topics = topics

    def list_topics(self, timeout=None):
        return MagicMock(topics={name: MagicMock(error=None) for name in self.topics})


def _found(schema, log, *, accepted=(), topics=(INDEX,)):
    started = []

    def factory(edge, topic):
        index = ParkedPairIndex(
            topic=topic, bootstrap_servers="kafka:9092", writable=False, sequenced=edge.is_multi_topic,
            pair_of_topic=edge.pair_labels_for_topic if edge.is_multi_pair else None,
            consumer_factory=lambda config: _IndexConsumer(config, log, INDEX), poll_timeout_seconds=0.01,
        )
        started.append(topic)
        return index

    found = cli._dlq_index_leftovers(schema, "kafka:9092", accepted=accepted,
                                     admin_factory=lambda config: _Admin(topics), index_factory=factory)
    return [(edge.type, topic, leftovers) for edge, topic, leftovers in found], started


def test_start_preflight_finds_a_removed_topics_entries_and_honours_the_override() -> None:
    log = _written_by_the_multi_pair_type()
    schema = _schema(topics=(POSTS, COMMENTS))
    assert _found(schema, log) == ([("LIKES", INDEX, [(BACKFILL, ("Person", "Post"), 1, "removed")])], [INDEX])
    assert _found(schema, log, accepted=("LIKES",)) == ([], [])
    assert _found(schema, log, topics=()) == ([], [])  # no index topic: nothing to read
    assert _found(_schema(), log) == ([], [INDEX])


def test_start_preflight_skips_order_by_types_without_an_index() -> None:
    schema = likes_schema(order_by=True, dead_letter=_DL, topics=(POSTS, COMMENTS))
    assert _found(schema, _written_by_the_multi_pair_type()) == ([], [])


def test_start_refuses_before_any_topic_is_created(caplog) -> None:
    schema = _schema(topics=(POSTS, COMMENTS))
    edge = schema.edges[0]
    found = [(edge, INDEX, [(BACKFILL, ("Person", "Post"), 2, "removed")])]
    args =Namespace(mode="stream", config="c.yaml", network="n", skip_image_build=True)
    with patch.object(cli, "load_schema", return_value=schema), \
            patch.object(cli, "_require_no_dlq_leftovers", _REAL_REQUIRE_NO_LEFTOVERS), \
            patch.object(cli, "_dlq_index_leftovers", return_value=found), \
            patch.object(cli, "ensure_dead_letter_topic") as ensure, \
            patch.object(cli, "get_neo4j_driver") as driver, \
            patch.object(cli, "get_neo4j_credentials", return_value=("bolt://x", "u", "p")):
        assert cli.handle_start(args) == 1
    ensure.assert_not_called()
    driver.assert_not_called()
    assert f"2 record(s) of topic {BACKFILL} written as Person->Post, which LIKES no longer reads" in caplog.text
    assert "--accept-dlq-leftovers LIKES" in caplog.text


def test_start_passes_every_accept_dlq_leftovers_flag_to_the_preflight() -> None:
    args = cli.build_parser().parse_args(["start", "--mode", "stream", "--accept-dlq-leftovers", "LIKES",
                                          "--accept-dlq-leftovers", "KNOWS"])
    assert args.accept_dlq_leftovers == ["LIKES", "KNOWS"]
    seen = {}

    def fake(schema_arg, bootstrap, *, accepted=()):
        seen["accepted"] = accepted
        raise ValueError("stop here")

    args.skip_image_build, args.network = True, "n"
    with patch.object(cli, "load_schema", return_value=_schema()), \
            patch.object(cli, "_require_no_dlq_leftovers", fake), \
            patch.object(cli, "get_neo4j_credentials", return_value=("bolt://x", "u", "p")):
        assert cli.handle_start(args) == 1
    assert seen["accepted"] == ("LIKES", "KNOWS")


# ── a single-pair type reads its own labelled keys back as [sk, tk] ─────────


def _labelled(labels, *, offset=0, attempt=0):
    envelope = _envelope(original_topic=POSTS, original_offset=offset, source_key="p1", target_key="p2")
    key = json.dumps([*labels, "p1", "p2"], separators=(",", ":")).encode()
    return dataclasses.replace(envelope, key_bytes=key, attempt_count=attempt)


def test_single_pair_key_rewrites_only_its_own_pairs_labelled_keys() -> None:
    own, other = _labelled(("Person", "Post")), _labelled(("Person", "Comment"))
    assert single_pair_key(own, ("Person", "Post")).key_bytes == b'["p1","p2"]'
    assert single_pair_key(other, ("Person", "Post")) is other
    legacy = _envelope()
    assert single_pair_key(legacy, ("Person", "Post")) is legacy


def test_the_retry_consumer_groups_a_collapsed_types_labelled_and_legacy_copies_as_one_pair() -> None:
    """An older labelled copy and a newer legacy copy of one relationship: one key_bytes identity, so the
    pair's order is kept and the writer sees legacy keys only."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = SeqIndex()
    index.add("p1", "p2", (POSTS, 0, 5))
    index.add("p1", "p2", (POSTS, 0, 9))
    writer = RecordingWriter()
    rc = RetryConsumer(
        consumer=consumer, writer_by_edge_type={"KNOWS": writer}, sink_by_edge_type={"KNOWS": _FakeSink()},
        parked_pair_index_by_edge_type={"KNOWS": index}, max_retry_attempts_by_edge_type={"KNOWS": 3},
        parked_topic_by_edge_type={"KNOWS": "p"}, base_delay_seconds_by_edge_type={"KNOWS": 1.0},
        max_delay_seconds_by_edge_type={"KNOWS": 2.0}, due_batch_coalesce_seconds=0.0,
        wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
        single_pair_labels_by_edge_type={"KNOWS": ("Person", "Post")},
    )
    newer = dataclasses.replace(_envelope(original_topic=POSTS, original_offset=9, source_key="p1", target_key="p2"))
    consumer.push(_dlq_message(newer, offset=100))
    consumer.push(_dlq_message(_labelled(("Person", "Post"), offset=5), offset=101))
    for _ in range(5):
        rc.tick()
        wall.advance(1_000)
    assert [(key, offset) for key, offset, _attempt in writer.writes] == [(b'["p1","p2"]', 5), (b'["p1","p2"]', 9)]


def test_retry_consumer_main_passes_each_single_pair_types_labels() -> None:
    from contextlib import ExitStack

    from src.loader.retry_consumer import main as retry_consumer_main
    from tests.test_multi_topic_dlq import _main_patches

    schema = likes_schema(order_by=True, dead_letter=_DL, topics=(POSTS, BACKFILL))
    with ExitStack() as stack:
        rc_cls, _sink = _main_patches(stack, schema)
        assert retry_consumer_main(["--dlq-topics", "LIKES.dlq"]) == 0
    assert rc_cls.call_args.kwargs["single_pair_labels_by_edge_type"] == {"LIKES": ("Person", "Post")}
    assert rc_cls.call_args.kwargs["endpoint_pairs_by_edge_type"] == {}


# ── review follow-up: refuse only where the rows cannot be written through the type as it is now ──


class _FixedIndex:
    def __init__(self, counts) -> None:
        self.counts = counts

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def outstanding_by_topic(self):
        return dict(self.counts)


def _single_pair_two_topics() -> GraphSchema:
    """p_s2_single: a single-pair (S4-shape) type over likes-a and likes-b."""
    def node(label, key):
        return {"label": label, "topic": label.lower() + "s", "key_property": key,
                "properties": {key: {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}}
    return GraphSchema.model_validate({"nodes": [node("Person", "pid"), node("Post", "post_id")], "edges": [{
        "type": "LIKES", "topics": ["likes-a", "likes-b"], "topic_conflict": "arrival",
        "nodes": {"source": "Person", "target": "Post"}, "source_key_property": "pid", "target_key_property": "post_id",
        "properties": {"x": {"type": "integer"}}, "dead_letter": {"enabled": True}}]})


def _found_fixed(schema, counts):
    topic = cli.edge_parked_pair_index_topic_name(schema.edges[0])
    found = cli._dlq_index_leftovers(schema, "k:1", admin_factory=lambda config: _Admin([topic]),
                                     index_factory=lambda edge, topic: _FixedIndex(counts))
    return [leftovers for _edge, _topic, leftovers in found]


def test_a_single_pair_type_that_dropped_a_topic_only_warns(caplog) -> None:
    """p_s2_single: its leftovers are retried through its one pair, as before S2 -- no new refusal."""
    with caplog.at_level(logging.WARNING):
        assert _found_fixed(_single_pair_two_topics(), {("likes-old-backfill", None): 3, ("likes-a", None): 2}) == []
    assert ("3 record(s) of topic likes-old-backfill, which LIKES no longer reads (their DLQ rows are retried "
            "through the type's one pair Person->Post)") in caplog.text


def test_a_collapsed_types_stamped_rows_of_the_removed_pair_still_refuse() -> None:
    schema = _schema(topics=(POSTS, BACKFILL))
    assert not schema.edges[0].is_multi_pair
    assert _found_fixed(schema, {(COMMENTS, ("Person", "Comment")): 1, (POSTS, ("Person", "Post")): 4}) == [
        [(COMMENTS, ("Person", "Comment"), 1, "removed")]]
    # stamped with the type's own pair, topic removed: retried through that pair, so no refusal
    assert _found_fixed(_schema(topics=(POSTS,)), {(BACKFILL, ("Person", "Post")): 2}) == []
