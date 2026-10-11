"""Stable stream consumer groups, their one-time migration, and unchanged bulk groups.

Stream relationship groups are ``<prefix>-<type>`` and the retry group is
``<prefix>-retry-consumer``, whatever the run id, so a stream restart resumes.
Bulk groups stay run-scoped and phase-derived; every bulk path is pinned below.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import TopicPartition

from src.orchestrator.docker_service import DockerService
from src.orchestrator.stream_groups import (
    StreamGroup, StreamGroupError, migrate_legacy_offsets, prepare_stream_groups,
)
from src.utils import consumer_groups
from src.utils.schema_loader import load_schema


# -- helpers --------------------------------------------------------------------------------------------------------

def _launch_group(*, mode: str, run_id: str | None, edge_type: str = "WORKS_AT", topics=("works",),
                  prefix: str = "loader", **kwargs) -> list[str]:
    client = MagicMock()
    client.containers.run.return_value = MagicMock()
    with tempfile.TemporaryDirectory() as rejection_dir:
        DockerService(client=client).run_edge_loader(
            edge_type=edge_type, topics=list(topics), mode=mode, config_path="config.yaml", network="net",
            rejection_dir=rejection_dir, run_id=run_id, consumer_group_prefix=prefix, **kwargs,
        )
    return [call.kwargs["environment"].get("KAFKA_GROUP_ID") for call in client.containers.run.call_args_list]


def _monitor(edges, run_id: str, *, mode: str | None = None, prefix: str = "loader"):
    from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor

    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        topic: SimpleNamespace(error=None, partitions={0: SimpleNamespace()})
        for edge in edges for topic in edge.topics
    })
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    expected = {(edge.type, "0") for edge in edges}
    kwargs = {} if mode is None else {"mode": mode}
    return RelationshipBulkMonitor(
        list(edges), run_id, expected, {replica: container for replica in expected}, bootstrap_servers="kafka:9092",
        consumer_group_prefix=prefix, admin_client=admin, watermark_consumer=MagicMock(),
        control_consumer=MagicMock(), **kwargs,
    )


def _phase_id(parent: str, kind: str, index: int, edge_type: str) -> str:
    digest = hashlib.sha256(f"{parent}|{kind}|{index}|{edge_type}".encode()).hexdigest()[:24]
    return f"{parent}.{kind}.{index}.{digest}"


# -- the helper -----------------------------------------------------------------------------------------------------

def test_helper_names() -> None:
    assert consumer_groups.stream_edge_group("p", "WORKS_AT") == "p-WORKS_AT"
    assert consumer_groups.stream_retry_group("p") == "p-retry-consumer"
    assert consumer_groups.bulk_edge_group("p", "WORKS_AT", "R") == "p-WORKS_AT-R"
    assert consumer_groups.legacy_stream_edge_group("p", "WORKS_AT", "R") == "p-WORKS_AT-R"
    assert consumer_groups.legacy_retry_group("R") == "graph-loader-retry-consumer-R"
    assert consumer_groups.edge_group("p", "T", mode="stream", run_id="R") == "p-T"
    assert consumer_groups.edge_group("p", "T", mode="bulk", run_id="R") == "p-T-R"
    assert consumer_groups.edge_group("p", "T", mode="bulk", run_id=None) == "p-T"


# -- stream: stable across run ids ----------------------------------------------------------------------------------

def test_stream_edge_group_is_stable_across_run_ids() -> None:
    groups = {
        group
        for run_id in ("run-A", "run-B", "0123456789abcdef0123456789abcdef", None)
        for group in _launch_group(mode="stream", run_id=run_id, lease_address="lease:7700", lease_buckets=8)
    }
    assert groups == {"loader-WORKS_AT"}


def test_stream_group_has_one_group_per_type_for_multi_topic_and_replicas() -> None:
    groups = _launch_group(mode="stream", run_id="run-A", edge_type="RATED", topics=("a", "b"), replicas=3,
                           lease_address="lease:7700", lease_buckets=8)
    assert groups == ["loader-RATED"] * 3


@pytest.mark.parametrize("path", [
    "examples/multi_topic/graph_schema.yaml", "examples/multi_topic/graph_schema_multi_pair.yaml",
    "examples/social/graph_schema.yaml",
])
def test_stream_launcher_and_monitor_agree_for_shipped_schemas(path) -> None:
    schema = load_schema(path)
    prefix = schema.loading.consumer_group_id
    monitor = _monitor(schema.edges, "run-A", mode="stream", prefix=prefix)
    try:
        for edge in schema.edges:
            launched = set(_launch_group(mode="stream", run_id="run-A", edge_type=edge.type, topics=edge.topics,
                                         prefix=prefix, lease_address="lease:7700", lease_buckets=8))
            assert launched == {f"{prefix}-{edge.type}"} == {monitor._groups[edge.type]}
    finally:
        monitor.close()


def test_stream_slot_gated_launch_keeps_a_run_scoped_clock_group() -> None:
    """Deprecated clock path: the work group is stable, the coordination (clock) group stays per run."""
    client = MagicMock()
    client.containers.run.return_value = MagicMock()
    schema = SimpleNamespace(edges=(SimpleNamespace(type="WORKS_AT", nodes=SimpleNamespace(
        source="Person", target="Company", is_self_referencing=False)),))
    with patch("src.orchestrator.docker_service.load_schema", return_value=schema), \
            tempfile.TemporaryDirectory() as rejection_dir:
        DockerService(client=client).run_edge_loader(
            edge_type="WORKS_AT", topic="works", mode="stream", config_path="c.yaml", rejection_dir=rejection_dir,
            run_id="run-A", consumer_group_prefix="loader", slot_gating=True, coordination_topic="clock",
            fleet_edge_types="WORKS_AT",
        )
    env = client.containers.run.call_args.kwargs["environment"]
    assert env["KAFKA_GROUP_ID"] == "loader-WORKS_AT"
    assert env["KAFKA_COORDINATION_GROUP_ID"] == "loader-WORKS_AT-run-A-clock-0"


def test_stream_monitor_lag_reads_the_stable_group() -> None:
    edge = SimpleNamespace(type="WORKS_AT", topics=["works"])
    monitor = _monitor([edge], "run-B", mode="stream")
    try:
        assert monitor._groups == {"WORKS_AT": "loader-WORKS_AT"}
        future = MagicMock()
        future.result.return_value = SimpleNamespace(topic_partitions=[TopicPartition("works", 0, 7)])
        monitor._admin.list_consumer_group_offsets.return_value = {"loader-WORKS_AT": future}
        assert monitor._commits() == {("WORKS_AT", "works", 0): 7}
        request = monitor._admin.list_consumer_group_offsets.call_args.args[0][0]
        assert request.group_id == "loader-WORKS_AT"
    finally:
        monitor.close()


def test_monitor_rejects_an_unknown_mode() -> None:
    from src.orchestrator.bulk_monitor import BulkMonitorError

    with pytest.raises(BulkMonitorError):
        _monitor([SimpleNamespace(type="WORKS_AT", topics=["works"])], "run-A", mode="rotating")


# -- bulk: unchanged, one explicit assertion per path ---------------------------------------------------------------

BULK_PARENT = "bulk-run"


def test_bulk_monitor_default_stays_run_scoped() -> None:
    edge = SimpleNamespace(type="WORKS_AT", topics=["works"])
    for mode in (None, "bulk"):
        monitor = _monitor([edge], "R.bulk.0.abc", mode=mode)
        try:
            assert monitor._groups == {"WORKS_AT": "loader-WORKS_AT-R.bulk.0.abc"}
        finally:
            monitor.close()


def test_bulk_shared_and_lease_stage_groups() -> None:
    """Completion-driven shared and ``--edge-leases`` stages: ``<prefix>-<type>-<R>.bulk.0.<h>``."""
    from src.cli import _derive_relationship_phase_run_id

    run_id = _derive_relationship_phase_run_id(BULK_PARENT, phase_kind="bulk", phase_index=0, edge_type="WORKS_AT")
    assert run_id == _phase_id(BULK_PARENT, "bulk", 0, "WORKS_AT")
    expected = f"loader-WORKS_AT-{run_id}"
    assert _launch_group(mode="bulk", run_id=run_id) == [expected]
    assert _launch_group(mode="bulk", run_id=run_id, lease_address="lease:7700", lease_buckets=8) == [expected]
    assert _launch_group(mode="bulk", run_id=run_id, replicas=2) == [expected, expected]


def test_bulk_isolated_phase_groups() -> None:
    from src.cli import _derive_relationship_phase_run_id

    run_id = _derive_relationship_phase_run_id(BULK_PARENT, phase_kind="isolated", phase_index=1, edge_type="KNOWS")
    assert run_id == _phase_id(BULK_PARENT, "isolated", 1, "KNOWS")
    assert _launch_group(mode="bulk", run_id=run_id, edge_type="KNOWS") == [f"loader-KNOWS-{run_id}"]


def test_bulk_precedence_substage_groups_and_rerun_check() -> None:
    from src.cli import _check_precedence_rerun, _derive_relationship_phase_run_id, _precedence_substage_plan

    schema = load_schema("examples/multi_topic/graph_schema.yaml")
    edge = next(e for e in schema.edges if e.conflict_rule == "precedence")
    stage_run = _derive_relationship_phase_run_id(BULK_PARENT, phase_kind="bulk", phase_index=0, edge_type=edge.type)
    plan = _precedence_substage_plan(edge, stage_run)
    for index, (_rank, topic, sub_run_id, sub_edge) in enumerate(plan):
        assert sub_run_id == _phase_id(stage_run, "topic", index, topic)
        assert _launch_group(mode="bulk", run_id=sub_run_id, edge_type=edge.type, topics=sub_edge.topics,
                             prefix="p") == [f"p-{edge.type}-{sub_run_id}"]
    seen = []
    with patch("src.cli._group_commits", side_effect=lambda group, keys: (seen.append(group), {})[1]):
        _check_precedence_rerun(edge, plan, {}, "p")
    assert seen == [f"p-{edge.type}-{sub_run_id}" for _, _, sub_run_id, _ in plan]


@patch("src.cli.RelationshipBulkMonitor")
def test_bulk_relationship_edge_launches_and_monitors_run_scoped(monitor_cls) -> None:
    from src.cli import _run_bulk_relationship_edge

    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", topics=["works"], replicas=1, is_multi_pair=False, conflict_rule="single",
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
        endpoint_labels=frozenset({"Person", "Company"}), has_self_reference=False,
    )
    client = MagicMock()
    client.containers.run.return_value = MagicMock()
    args = argparse.Namespace(config="config.yaml", network="net", bulk_timeout_seconds=1)
    with tempfile.TemporaryDirectory() as rejection_dir, \
            patch("src.orchestrator.docker_service.REJECTION_DIR", rejection_dir):
        service = DockerService(client=client)
        original = service.run_edge_loader
        service.run_edge_loader = lambda **kw: original(**{**kw, "rejection_dir": rejection_dir})
        assert _run_bulk_relationship_edge(edge, service, args, run_id="R.bulk.0.abc", consumer_group_prefix="loader") == 0
    assert client.containers.run.call_args.kwargs["environment"]["KAFKA_GROUP_ID"] == "loader-WORKS_AT-R.bulk.0.abc"
    assert client.containers.run.call_args.kwargs["command"][5] == "bulk"
    assert monitor_cls.call_args.kwargs.get("mode", "bulk") == "bulk"


# -- the migration --------------------------------------------------------------------------------------------------

class FakeAdmin:
    """Commits per group, members per group; records alter requests."""

    def __init__(self, partitions, commits=None, members=None, watermarks=None, errors=None):
        self.partitions = partitions  # {topic: n}
        self.commits = {group: dict(offsets) for group, offsets in (commits or {}).items()}
        self.members = dict(members or {})
        self.watermarks = dict(watermarks or {})  # {(topic, partition): (low, high)}, default (0, 10**6)
        self.errors = dict(errors or {})  # {group: {(topic, partition): KafkaError}}
        self.altered: list[tuple[str, dict]] = []
        self.describe_calls = 0

    def list_offsets(self, requests, request_timeout=None):
        out = {}
        for tp, spec in requests.items():
            low, high = self.watermarks.get((tp.topic, tp.partition), (0, 10 ** 6))
            offset = low if type(spec).__name__ == "EarliestSpec" else high
            out[tp] = self._future(SimpleNamespace(offset=offset))
        return out

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics={
            topic: SimpleNamespace(error=None, partitions={p: None for p in range(n)})
            for topic, n in self.partitions.items()
        })

    def _future(self, value):
        future = MagicMock()
        future.result.return_value = value
        return future

    def list_consumer_group_offsets(self, requests):
        (request,) = requests
        offsets = self.commits.get(request.group_id, {})
        errors = self.errors.get(request.group_id, {})
        partitions = []
        for tp in request.topic_partitions:
            key = (tp.topic, tp.partition)
            partitions.append(SimpleNamespace(topic=tp.topic, partition=tp.partition,
                                              offset=-1001 if key in errors else offsets.get(key, -1001),
                                              error=errors.get(key)))
        return {request.group_id: self._future(SimpleNamespace(topic_partitions=partitions))}

    def alter_consumer_group_offsets(self, requests):
        (request,) = requests
        assert not self.members.get(request.group_id), "Kafka refuses to alter a group with members"
        offsets = {(tp.topic, tp.partition): tp.offset for tp in request.topic_partitions}
        self.altered.append((request.group_id, offsets))
        self.commits.setdefault(request.group_id, {}).update(offsets)
        return {request.group_id: self._future(SimpleNamespace(topic_partitions=[
            TopicPartition(tp.topic, tp.partition, tp.offset) for tp in request.topic_partitions
        ]))}

    def describe_consumer_groups(self, groups, request_timeout=None):
        self.describe_calls += 1
        return {group: self._future(SimpleNamespace(members=list(self.members.get(group, ())))) for group in groups}


STABLE = StreamGroup("loader-WORKS_AT", ("works",), "loader-WORKS_AT-run-A")


def test_migration_copies_legacy_offsets_into_an_uncommitted_stable_group(caplog) -> None:
    admin = FakeAdmin({"works": 3}, commits={"loader-WORKS_AT-run-A": {("works", 0): 10, ("works", 2): 4}})
    with caplog.at_level(logging.INFO, logger="src.orchestrator.stream_groups"):
        assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is True
    assert admin.altered == [("loader-WORKS_AT", {("works", 0): 10, ("works", 2): 4})]
    warning = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning) == 1
    assert "from_group=loader-WORKS_AT-run-A" in warning[0].getMessage()
    assert "to_group=loader-WORKS_AT" in warning[0].getMessage()


def test_migration_never_overwrites_a_stable_group_with_commits() -> None:
    admin = FakeAdmin({"works": 2}, commits={
        "loader-WORKS_AT": {("works", 1): 3}, "loader-WORKS_AT-run-A": {("works", 0): 10, ("works", 1): 9},
    })
    assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is False
    assert admin.altered == []


def test_migration_without_run_id_does_not_guess(caplog) -> None:
    admin = FakeAdmin({"works": 1}, commits={"loader-WORKS_AT-run-A": {("works", 0): 10}})
    group = StreamGroup("loader-WORKS_AT", ("works",), None)
    with caplog.at_level(logging.INFO, logger="src.orchestrator.stream_groups"):
        assert migrate_legacy_offsets(admin, group, explicit_run_id=None) is False
    assert admin.altered == []
    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any("resume=stable-group" in message and "no --run-id" in message for message in info)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_migration_with_a_legacy_group_without_commits_is_a_no_op() -> None:
    admin = FakeAdmin({"works": 1})
    assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is False
    assert admin.altered == []


def test_migration_ignores_legacy_commits_of_topics_the_type_no_longer_reads() -> None:
    admin = FakeAdmin({"works": 1, "old": 1}, commits={"loader-WORKS_AT-run-A": {("old", 0): 5, ("works", 0): 2}})
    assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is True
    assert admin.altered == [("loader-WORKS_AT", {("works", 0): 2})]


def test_prepare_refuses_groups_that_keep_members() -> None:
    admin = FakeAdmin({"works": 1}, members={"loader-WORKS_AT": [SimpleNamespace(client_id="c", host="/10.0.0.9")]})
    now = [0.0]
    with pytest.raises(StreamGroupError, match="loader-WORKS_AT.*Another stream fleet"):
        prepare_stream_groups(admin, [STABLE], explicit_run_id="run-A", member_timeout_seconds=50,
                              clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + max(s, 1.0)))
    assert admin.altered == []
    assert admin.describe_calls > 1  # it waited for the members to expire before refusing


def test_prepare_waits_for_expiring_members_then_migrates() -> None:
    admin = FakeAdmin({"works": 1}, commits={"loader-WORKS_AT-run-A": {("works", 0): 8}},
                      members={"loader-WORKS_AT": [SimpleNamespace(client_id="c", host="h")]})
    now = [0.0]

    def sleep(seconds):
        now[0] += 1.0
        if now[0] >= 3:
            admin.members.clear()

    migrated = prepare_stream_groups(admin, [STABLE], explicit_run_id="run-A", member_timeout_seconds=50,
                                     clock=lambda: now[0], sleep=sleep)
    assert migrated == ["loader-WORKS_AT"]
    assert admin.commits["loader-WORKS_AT"] == {("works", 0): 8}


def test_prepare_refuses_colliding_names() -> None:
    with pytest.raises(StreamGroupError, match="collide"):
        prepare_stream_groups(FakeAdmin({}), [STABLE, STABLE], explicit_run_id=None, member_timeout_seconds=1)


# -- CLI wiring -----------------------------------------------------------------------------------------------------

def _schema(path: str):
    return load_schema(path)


def test_cli_stream_groups_for_the_lease_fleet_with_and_without_run_id() -> None:
    from src.cli import _stream_consumer_groups

    schema = _schema("examples/multi_topic/graph_schema_dead_letter.yaml")
    args = argparse.Namespace(stream_clock_rotation=False)
    with_id = _stream_consumer_groups(schema, args, explicit_run_id="run-A")
    assert with_id == [
        StreamGroup("multi-topic-dlq-example-RATED", ("mt-rated-app", "mt-rated-import"),
                    "multi-topic-dlq-example-RATED-run-A"),
        StreamGroup("multi-topic-dlq-example-retry-consumer", ("RATED.dlq",), "graph-loader-retry-consumer-run-A"),
    ]
    without = _stream_consumer_groups(schema, args, explicit_run_id=None)
    assert [g.group for g in without] == [g.group for g in with_id]
    assert all(g.legacy_group is None for g in without)


def test_cli_stream_groups_for_the_deprecated_clock_path_use_its_phase_ids() -> None:
    from src.cli import _derive_relationship_phase_run_id, _stream_consumer_groups

    schema = _schema("tests/golden/fixtures/single_topic_dlq.yaml")
    groups = _stream_consumer_groups(schema, argparse.Namespace(stream_clock_rotation=True), explicit_run_id="R")
    prefix = schema.loading.consumer_group_id
    by_group = {g.group: g.legacy_group for g in groups}
    assert f"{prefix}-retry-consumer" not in by_group  # that path has no retry consumer
    shared = _derive_relationship_phase_run_id("R", phase_kind="shared", phase_index=0, edge_type="shared")
    assert by_group[f"{prefix}-WORKS_AT"] == f"{prefix}-WORKS_AT-{shared}"
    knows = by_group[f"{prefix}-KNOWS"]  # self-referencing: an isolated phase id
    assert knows.startswith(f"{prefix}-KNOWS-R.isolated.")


def test_cli_refuses_a_type_named_like_a_node_label() -> None:
    from src.cli import _stream_consumer_groups

    schema = SimpleNamespace(
        loading=SimpleNamespace(consumer_group_id="p"),
        nodes=[SimpleNamespace(label="Person")],
        edges=[SimpleNamespace(type="Person", topics=["x"], dead_letter=None)],
    )
    with pytest.raises(ValueError, match="same name as a node label"):
        _stream_consumer_groups(schema, argparse.Namespace(), explicit_run_id=None)


@pytest.mark.real_stream_groups
def test_cli_prepare_passes_the_session_timeout_and_the_groups() -> None:
    from src.cli import _prepare_stream_consumer_groups

    schema = _schema("examples/social/graph_schema.yaml")
    admin = FakeAdmin({edge.topics[0]: 1 for edge in schema.edges})
    with patch("src.cli.prepare_stream_groups") as prepare:
        _prepare_stream_consumer_groups(schema, argparse.Namespace(), explicit_run_id="R",
                                        bootstrap_servers="kafka:9092", admin_factory=lambda: admin)
    kwargs = prepare.call_args.kwargs
    assert kwargs["member_timeout_seconds"] == schema.loading.session_timeout_ms / 1000 + 5
    assert kwargs["explicit_run_id"] == "R"
    assert prepare.call_args.args[0] is admin
    assert [g.group for g in prepare.call_args.args[1]] == [
        f"{schema.loading.consumer_group_id}-{edge.type}" for edge in schema.edges
    ]
    with patch("src.cli.prepare_stream_groups") as prepare:
        _prepare_stream_consumer_groups(schema, argparse.Namespace(stream_group_member_wait_seconds=400.0),
                                        explicit_run_id=None, bootstrap_servers="kafka:9092",
                                        admin_factory=lambda: admin)
    assert prepare.call_args.kwargs["member_timeout_seconds"] == 400.0
    # And the real call path against the fake admin: one member check, nothing altered.
    _prepare_stream_consumer_groups(schema, argparse.Namespace(), explicit_run_id=None,
                                    bootstrap_servers="kafka:9092", admin_factory=lambda: admin)
    assert admin.describe_calls == 1
    assert admin.altered == []


def test_member_wait_flag_parses_positive_seconds() -> None:
    from src.cli import build_parser

    assert build_parser().parse_args(["start", "--mode", "stream", "--stream-group-member-wait-seconds", "360"]) \
        .stream_group_member_wait_seconds == 360.0
    assert build_parser().parse_args(["start", "--mode", "stream"]).stream_group_member_wait_seconds is None
    for bad in ("0", "-1", "nan", "inf"):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["start", "--mode", "stream", "--stream-group-member-wait-seconds", bad])


@pytest.mark.real_stream_groups
def test_bulk_handle_start_never_prepares_stream_groups() -> None:
    from src.cli import handle_start

    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False,
        replicas=1, endpoint_labels=frozenset({"Person", "Company"}), has_self_reference=False, is_multi_pair=False,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False), dead_letter=None,
    )
    schema = SimpleNamespace(nodes=(SimpleNamespace(label="Person", topic="people"),), edges=[edge],
                             loading=SimpleNamespace(consumer_group_id="p", session_timeout_ms=45_000))
    prepare = MagicMock()
    docker = MagicMock()
    docker.run_node_loader.return_value = [MagicMock()]
    args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True,
                              bulk_timeout_seconds=1, edge_leases=False, edge_classic=True)
    with patch("src.cli._prepare_stream_consumer_groups", prepare), \
            patch("src.cli.load_schema", return_value=schema), patch("src.cli.apply_schema"), \
            patch("src.cli.get_neo4j_driver"), patch("src.cli.DockerService", return_value=docker), \
            patch("src.cli._resolve_bulk_edge_mode"), patch("src.cli._require_supported_topic_conflicts"), \
            patch("src.cli._ensure_dead_letter_topics"), \
            patch("src.cli._resolve_node_replicas", return_value={"Person": 1}), \
            patch("src.cli.BulkMonitor", side_effect=RuntimeError("stop the bulk run here")) as bulk_monitor:
        handle_start(args)  # fails at the (mocked) monitor; what matters is what ran before
    assert bulk_monitor.called  # the run got past the point where stream mode prepares its groups
    prepare.assert_not_called()


def test_committed_offsets_refuses_partition_errors() -> None:
    """A stable group whose commits come back with errors must not look uncommitted (and be overwritten)."""
    from confluent_kafka import KafkaError

    from src.orchestrator.stream_groups import committed_offsets

    admin = FakeAdmin({"works": 2}, commits={"loader-WORKS_AT": {("works", 1): 7}},
                      errors={"loader-WORKS_AT": {("works", 0): KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)}})
    with pytest.raises(StreamGroupError, match=r"could not be read: works\[0\]"):
        committed_offsets(admin, "loader-WORKS_AT", [("works", 0), ("works", 1)])
    with pytest.raises(StreamGroupError):
        migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A")
    assert admin.altered == []
    # -1001 without an error is simply "no commit".
    assert committed_offsets(admin, "other", [("works", 0)]) == {}


def test_migration_copies_only_offsets_inside_the_current_range(caplog) -> None:
    admin = FakeAdmin(
        {"works": 3},
        commits={"loader-WORKS_AT-run-A": {("works", 0): 5, ("works", 1): 2, ("works", 2): 900}},
        watermarks={("works", 0): (0, 10), ("works", 1): (4, 10), ("works", 2): (0, 50)},
    )
    with caplog.at_level(logging.INFO, logger="src.orchestrator.stream_groups"):
        assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is True
    assert admin.altered == [("loader-WORKS_AT", {("works", 0): 5})]
    out_of_range = [r.getMessage() for r in caplog.records if "legacy_offsets_out_of_range" in r.getMessage()]
    assert len(out_of_range) == 1
    assert "works[1]=2 range=[4, 10]" in out_of_range[0] and "works[2]=900 range=[0, 50]" in out_of_range[0]
    migrated = [r.getMessage() for r in caplog.records if "migrated_legacy_offsets" in r.getMessage()]
    assert "offsets=works[0]=5" in migrated[0]


def test_migration_with_every_legacy_offset_out_of_range_copies_nothing() -> None:
    admin = FakeAdmin({"works": 1}, commits={"loader-WORKS_AT-run-A": {("works", 0): 3}},
                      watermarks={("works", 0): (8, 20)})
    assert migrate_legacy_offsets(admin, STABLE, explicit_run_id="run-A") is False
    assert admin.altered == []


def test_topic_metadata_is_retried() -> None:
    from confluent_kafka import KafkaError, KafkaException

    from src.orchestrator.stream_groups import _partitions

    admin = FakeAdmin({"works": 2})
    real = admin.list_topics
    calls = []

    def flaky(timeout=None):
        calls.append(1)
        if len(calls) == 1:
            raise KafkaException(KafkaError(KafkaError.NOT_COORDINATOR, "nc"))
        return real(timeout)
    admin.list_topics = flaky
    with patch("src.orchestrator.stream_groups.time.sleep"):
        assert _partitions(admin, ["works"]) == [("works", 0), ("works", 1)]
    assert len(calls) == 2


def test_a_stop_during_the_member_wait_is_reported_as_a_stop() -> None:
    from src.orchestrator.stream_groups import StreamGroupWaitStopped

    admin = FakeAdmin({"works": 1}, members={"loader-WORKS_AT": [SimpleNamespace(client_id="c", host="h")]})
    with pytest.raises(StreamGroupWaitStopped, match="stop requested"):
        prepare_stream_groups(admin, [STABLE], explicit_run_id=None, member_timeout_seconds=600,
                              stop_requested=lambda: True, sleep=lambda _s: None)


def test_the_refusal_names_the_longer_wait_option() -> None:
    admin = FakeAdmin({"works": 1}, members={"loader-WORKS_AT": [SimpleNamespace(client_id="c", host="h")]})
    now = [0.0]
    with pytest.raises(StreamGroupError, match="max_poll_interval_ms plus session_timeout_ms.*"
                                               "--stream-group-member-wait-seconds"):
        prepare_stream_groups(admin, [STABLE], explicit_run_id=None, member_timeout_seconds=5,
                              clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + 1))


def test_handle_start_reports_a_stop_during_the_member_wait_as_a_warning(caplog) -> None:
    from src.orchestrator.stream_groups import StreamGroupWaitStopped

    prepare = MagicMock(side_effect=StreamGroupWaitStopped("stop requested while waiting"))
    with patch("src.cli._prepare_stream_consumer_groups", prepare), caplog.at_level(logging.WARNING):
        result, order, docker = _stream_handle_start(prepare)
    assert result == 1 and order == []
    assert any("stage=shutdown" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)
    assert not any("stage=consumer-groups" in r.getMessage() and r.levelno >= logging.ERROR for r in caplog.records)


# -- post-launch member guard in the stream monitor --------------------------------------------------------------

def _member_monitor(counts: dict, *, retry=0):
    edge = SimpleNamespace(type="WORKS_AT", topics=["works"])
    from src.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor

    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={"works": SimpleNamespace(error=None, partitions={0: None})})

    def describe(groups, request_timeout=None):
        out = {}
        for group in groups:
            future = MagicMock()
            future.result.return_value = SimpleNamespace(members=[object()] * counts.get(group, 0))
            out[group] = future
        return out
    admin.describe_consumer_groups.side_effect = describe
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    now = [0.0]
    expected = {("WORKS_AT", "0"), ("WORKS_AT", "1")}
    monitor = RelationshipBulkMonitor(
        [edge], "run-x", expected, {replica: container for replica in expected}, bootstrap_servers="k:9092",
        consumer_group_prefix="loader", admin_client=admin, watermark_consumer=MagicMock(),
        control_consumer=MagicMock(), mode="stream", clock=lambda: now[0],
        tracked_retry_consumers=[container] * retry,
    )
    return monitor, now


def test_stream_monitor_fails_the_fleet_on_foreign_members(caplog) -> None:
    from src.orchestrator.bulk_monitor import BulkMonitorError

    counts = {"loader-WORKS_AT": 3, "loader-retry-consumer": 1}
    monitor, now = _member_monitor(counts, retry=1)
    try:
        assert monitor._expected_members == {"loader-WORKS_AT": 2, "loader-retry-consumer": 1}
        monitor._check_group_members()  # first sighting of the excess: not yet fatal
        now[0] += 5
        monitor._check_group_members()  # throttled
        now[0] += 6
        with caplog.at_level(logging.ERROR), pytest.raises(BulkMonitorError, match="foreign_members loader-WORKS_AT "
                                                                                  "members=3 expected=2"):
            monitor._check_group_members()
        assert any("stage=consumer-groups" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
    finally:
        monitor.close()


def test_stream_monitor_member_guard_tolerates_its_own_fleet_and_one_off_excess() -> None:
    counts = {"loader-WORKS_AT": 2}
    monitor, now = _member_monitor(counts)
    try:
        for _ in range(5):
            monitor._check_group_members()
            now[0] += 11
        counts["loader-WORKS_AT"] = 3  # e.g. a member that just closed, listed once
        monitor._check_group_members()
        now[0] += 11
        counts["loader-WORKS_AT"] = 2
        monitor._check_group_members()
        monitor._admin.describe_consumer_groups.side_effect = RuntimeError("broker down")
        now[0] += 11
        monitor._check_group_members()  # logged and skipped
    finally:
        monitor.close()


def test_bulk_monitor_has_no_member_guard() -> None:
    edge = SimpleNamespace(type="WORKS_AT", topics=["works"])
    monitor = _monitor([edge], "R.bulk.0.abc")
    try:
        assert monitor._expected_members == {}
        monitor._check_group_members()
        monitor._admin.describe_consumer_groups.assert_not_called()
    finally:
        monitor.close()


@patch("src.cli.RelationshipBulkMonitor")
def test_lease_fleet_wires_stable_groups_into_the_retry_consumer_and_the_monitor(monitor_cls) -> None:
    from src.cli import _run_lease_relationship_fleet
    from src.models.schema import DeadLetterConfig

    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False,
        replicas=1, endpoint_labels=frozenset({"Person", "Company"}), has_self_reference=False, is_multi_pair=False,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
        dead_letter=DeadLetterConfig(enabled=True),
    )
    schema = SimpleNamespace(edges=[edge], nodes=(), loading=SimpleNamespace(
        consumer_group_id="loader", bucket_lease=SimpleNamespace(tx_timeout_ms=30_000, restart_quiet_margin_ms=5_000)))
    lease = MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "lease:7700"
    docker.run_retry_consumer.return_value = [MagicMock()]
    docker.run_edge_loader.return_value = [MagicMock()]
    monitor_cls.return_value.tracked_lease_server = lease
    args = argparse.Namespace(config="c.yaml", network="net", lease_buckets=16, lease_outer_batch_size=32_000,
                              lease_keyed_max_nodes=1_000, retry_consumer_replicas=1)
    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-x") == 0
    assert docker.run_retry_consumer.call_args.kwargs["consumer_group_prefix"] == "loader"
    assert docker.run_edge_loader.call_args.kwargs["mode"] == "stream"
    assert monitor_cls.call_args.kwargs["mode"] == "stream"


def _stream_handle_start(prepare, *, run_id=None):
    from src.cli import handle_start
    from src.models.schema import DeadLetterConfig

    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False,
        replicas=1, endpoint_labels=frozenset({"Person", "Company"}), has_self_reference=False, is_multi_pair=False,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
        dead_letter=DeadLetterConfig(enabled=False),
    )
    schema = SimpleNamespace(nodes=(SimpleNamespace(label="Person", topic="people"),), edges=[edge])
    order: list[str] = []
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda **_kw: (order.append("nodes"), [MagicMock()])[1]
    prepare.side_effect = prepare.side_effect or (lambda *a, **kw: order.append("groups"))
    args = argparse.Namespace(mode="stream", config="c.yaml", network="net", skip_image_build=True,
                              stream_clock_rotation=False, run_id=run_id)
    with patch("src.cli.load_schema", return_value=schema), patch("src.cli.apply_schema"), \
            patch("src.cli.get_neo4j_driver"), patch("src.cli.DockerService", return_value=docker), \
            patch("src.cli._resolve_node_replicas", return_value={"Person": 1}), \
            patch("src.cli._start_stream_lease_for_nodes", return_value=(MagicMock(), {})), \
            patch("src.cli._ensure_dead_letter_topics"), patch("src.cli._ensure_retry_topics"), \
            patch("src.cli._run_lease_relationship_fleet", return_value=0):
        result = handle_start(args)
    return result, order, docker


def test_handle_start_prepares_stream_groups_before_any_launch() -> None:
    prepare = MagicMock()
    with patch("src.cli._prepare_stream_consumer_groups", prepare):
        result, order, _docker = _stream_handle_start(prepare, run_id="run-A")
    assert result == 0
    assert order == ["groups", "nodes"]
    assert prepare.call_args.kwargs["explicit_run_id"] == "run-A"


def test_handle_start_refuses_when_stream_groups_are_busy() -> None:
    prepare = MagicMock(side_effect=StreamGroupError("busy"))
    with patch("src.cli._prepare_stream_consumer_groups", prepare):
        result, order, docker = _stream_handle_start(prepare)
    assert result == 1
    assert order == []
    docker.run_node_loader.assert_not_called()
    assert prepare.call_args.kwargs["explicit_run_id"] is None


def test_group_requests_retry_a_retriable_kafka_error() -> None:
    """A broker that just started answers NOT_COORDINATOR until __consumer_offsets is loaded."""
    from confluent_kafka import KafkaError, KafkaException

    from src.orchestrator.stream_groups import _with_retries

    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            # As a fresh broker answers: NOT_COORDINATOR, not flagged retriable by librdkafka.
            raise KafkaException(KafkaError(KafkaError.NOT_COORDINATOR, "not coordinator"))
        return "ok"

    assert _with_retries(flaky, what="x", sleep=lambda _s: None) == "ok"
    assert len(calls) == 3
    with pytest.raises(KafkaException):
        _with_retries(lambda: (_ for _ in ()).throw(
            KafkaException(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED, "denied"))), what="x",
            sleep=lambda _s: None)
    now = [0.0]

    def always():
        raise KafkaException(KafkaError(KafkaError.NOT_COORDINATOR, "nc", retriable=True))
    with pytest.raises(KafkaException):
        _with_retries(always, what="x", clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + 10),
                      budget_seconds=30)
