"""Story 6: DLQ topic provisioning at CLI launch time, fail-closed."""
from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.cli import _ensure_dead_letter_topics, _ensure_retry_topics, handle_start
from src.models.schema import DeadLetterConfig


def _node(label: str, topic: str, dead_letter=None):
    return SimpleNamespace(label=label, topic=topic, replicas=1, dead_letter=dead_letter)


def _edge(edge_type: str, topic: str, dead_letter=None, order_by=None):
    return SimpleNamespace(type=edge_type, topic=topic, topics=[topic], dead_letter_entity=topic, is_multi_topic=False, replicas=1, dead_letter=dead_letter, order_by=order_by)


@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_dead_letter_topics_calls_once_per_distinct_resolved_topic(mock_ensure) -> None:
    schema = SimpleNamespace(
        nodes=[
            _node("Person", "person-events", DeadLetterConfig(enabled=True)),
            _node("Company", "company-events", None),
        ],
        edges=[
            _edge("WORKS_AT", "works-at-events", DeadLetterConfig(enabled=True, topic="dlq-works-at")),
            _edge("KNOWS", "knows-events", DeadLetterConfig(enabled=False)),
        ],
    )

    _ensure_dead_letter_topics(schema, "kafka:9092")

    assert mock_ensure.call_count == 2
    calls = {call.args[1]: call.kwargs["source_topics"] for call in mock_ensure.call_args_list}
    assert calls == {
        "person-events.dlq": ["person-events"],
        "dlq-works-at": ["works-at-events"],
    }
    for call in mock_ensure.call_args_list:
        assert call.args[0] == "kafka:9092"


@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_dead_letter_topics_makes_zero_calls_when_none_configured(mock_ensure) -> None:
    schema = SimpleNamespace(
        nodes=[_node("Person", "person-events"), _node("Company", "company-events")],
        edges=[_edge("WORKS_AT", "works-at-events")],
    )

    _ensure_dead_letter_topics(schema, "kafka:9092")

    mock_ensure.assert_not_called()


@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_dead_letter_topics_tolerates_missing_attribute_on_loose_test_doubles(mock_ensure) -> None:
    """Defensive getattr fallback: a schema double without .dead_letter is a no-op, not a crash."""
    schema = SimpleNamespace(
        nodes=[SimpleNamespace(label="Person", topic="person-events", replicas=1)],
        edges=[SimpleNamespace(type="WORKS_AT", topic="works-at-events", topics=["works-at-events"], dead_letter_entity="works-at-events", is_multi_topic=False, replicas=1)],
    )

    _ensure_dead_letter_topics(schema, "kafka:9092")

    mock_ensure.assert_not_called()


@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_dead_letter_topics_skips_edges_when_include_edges_is_false(mock_ensure) -> None:
    """Planned mode (include_edges=not planned) never provisions edge DLQ

    topics: planned_writer.py/edge_planner.py never read dead_letter, so a
    misconfigured edge DLQ topic a planned run would never write to must
    not block it. Node DLQ topics are still provisioned unconditionally.
    """
    schema = SimpleNamespace(
        nodes=[_node("Person", "person-events", DeadLetterConfig(enabled=True))],
        edges=[_edge("WORKS_AT", "works-at-events", DeadLetterConfig(enabled=True, topic="dlq-works-at"))],
    )

    _ensure_dead_letter_topics(schema, "kafka:9092", include_edges=False)

    mock_ensure.assert_called_once_with("kafka:9092", "person-events.dlq", source_topics=["person-events"])


@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_dead_letter_topics_dedups_if_two_entities_would_resolve_to_the_same_topic(mock_ensure) -> None:
    """Defensive: even if two entities somehow shared a resolved DLQ topic
    (Story 2's schema validation already forbids this at load_schema()
    time), the provisioning loop still calls ensure_dead_letter_topic only
    once for it, not once per entity.
    """
    schema = SimpleNamespace(
        nodes=[
            _node("Person", "shared", DeadLetterConfig(enabled=True, topic="shared.dlq")),
            _node("Company", "other", DeadLetterConfig(enabled=True, topic="shared.dlq")),
        ],
        edges=[],
    )

    _ensure_dead_letter_topics(schema, "kafka:9092")

    mock_ensure.assert_called_once()


# ── P5-S4 Story 6: retry consumer's own durable topics ──────────────────────


@patch("src.cli.ensure_parked_pair_index_topic")
@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_retry_topics_provisions_parking_lot_and_parked_pairs(mock_ensure_dlq, mock_ensure_pairs) -> None:
    schema = SimpleNamespace(
        nodes=[],
        edges=[_edge("WORKS_AT", "works-at-events", DeadLetterConfig(enabled=True))],
    )

    _ensure_retry_topics(schema, "kafka:9092")

    mock_ensure_dlq.assert_called_once_with(
        "kafka:9092", "works-at-events.dlq.parked", source_topic="works-at-events.dlq",
    )
    mock_ensure_pairs.assert_called_once_with(
        "kafka:9092", "works-at-events.dlq.parked-pairs", source_topic="works-at-events.dlq",
    )


@patch("src.cli.ensure_parked_pair_index_topic")
@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_retry_topics_still_provisions_the_parking_lot_for_an_order_by_edge_type(
    mock_ensure_dlq, mock_ensure_pairs,
) -> None:
    """Review finding M17: an order_by edge type never uses the parked-pair

    index, but it still parks after max_retry_attempts and the retry
    consumer still publishes that park to parked_topic_name(...) -- the
    parking-lot topic must exist for it, or every park publish fails
    forever (no auto topic creation). Only the parked-pairs index topic is
    order_by-conditional.
    """
    schema = SimpleNamespace(
        nodes=[],
        edges=[_edge("WORKS_AT", "works-at-events", DeadLetterConfig(enabled=True), order_by="since")],
    )

    _ensure_retry_topics(schema, "kafka:9092")

    mock_ensure_dlq.assert_called_once_with(
        "kafka:9092", "works-at-events.dlq.parked", source_topic="works-at-events.dlq",
    )
    mock_ensure_pairs.assert_not_called()


@patch("src.cli.ensure_parked_pair_index_topic")
@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_retry_topics_skips_a_dlq_disabled_edge_type(mock_ensure_dlq, mock_ensure_pairs) -> None:
    schema = SimpleNamespace(nodes=[], edges=[_edge("WORKS_AT", "works-at-events", None)])

    _ensure_retry_topics(schema, "kafka:9092")

    mock_ensure_dlq.assert_not_called()
    mock_ensure_pairs.assert_not_called()


@patch("src.cli.ensure_parked_pair_index_topic")
@patch("src.cli.ensure_dead_letter_topic")
def test_ensure_retry_topics_tolerates_missing_attributes_on_loose_test_doubles(mock_ensure_dlq, mock_ensure_pairs) -> None:
    schema = SimpleNamespace(nodes=[], edges=[SimpleNamespace(type="WORKS_AT", topic="works-at-events", topics=["works-at-events"], dead_letter_entity="works-at-events", is_multi_topic=False)])

    _ensure_retry_topics(schema, "kafka:9092")

    mock_ensure_dlq.assert_not_called()
    mock_ensure_pairs.assert_not_called()


@patch("src.cli._ensure_control_topic")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
@patch("src.cli.ensure_dead_letter_topic")
def test_handle_start_aborts_before_any_side_effect_when_dlq_setup_fails(
    mock_ensure, mock_load, mock_get_driver, mock_apply_schema, mock_docker, mock_control_topic,
) -> None:
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")
    mock_get_driver.return_value = MagicMock()
    schema = SimpleNamespace(
        nodes=[_node("Person", "person-events", DeadLetterConfig(enabled=True))],
        edges=[_edge("WORKS_AT", "works-at-events", DeadLetterConfig(enabled=True, topic="dlq-works-at"))],
    )
    mock_load.return_value = schema
    mock_ensure.side_effect = RuntimeError("broker unreachable")
    docker_instance = MagicMock()
    mock_docker.return_value = docker_instance

    assert handle_start(args) == 1

    # Not just "no container launch": nothing downstream of DLQ
    # provisioning ran at all -- no DDL, no image build, and no run_*/
    # start_* container-launching call of any kind (node/edge loaders,
    # clock runner, lease server, planned dispatcher).
    mock_apply_schema.assert_not_called()
    docker_instance.build_image.assert_not_called()
    launch_calls = [
        call for call in docker_instance.method_calls
        if call[0].startswith(("run_", "start_"))
    ]
    assert launch_calls == []


@patch("src.cli._ensure_control_topic")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
@patch("src.cli.ensure_dead_letter_topic")
def test_handle_start_creates_dlq_topics_before_launching_containers(
    mock_ensure, mock_load, mock_get_driver, mock_apply_schema, mock_docker, mock_control_topic,
) -> None:
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")
    mock_get_driver.return_value = MagicMock()
    schema = SimpleNamespace(
        nodes=[_node("Person", "person-events", DeadLetterConfig(enabled=True))],
        edges=[],
    )
    mock_load.return_value = schema
    docker_instance = MagicMock()
    docker_instance.run_node_loader.return_value = [MagicMock()]
    mock_docker.return_value = docker_instance

    manager = MagicMock()
    manager.attach_mock(mock_ensure, "ensure_dead_letter_topic")
    manager.attach_mock(docker_instance.run_node_loader, "run_node_loader")

    assert handle_start(args) == 0

    mock_ensure.assert_called_once()
    docker_instance.run_node_loader.assert_called_once()
    order = [entry[0] for entry in manager.mock_calls]
    assert order.index("ensure_dead_letter_topic") < order.index("run_node_loader")
