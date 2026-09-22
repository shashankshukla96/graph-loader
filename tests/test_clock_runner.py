"""Tests for the exact shared-fleet clock container entry point."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.orchestrator.clock_runner import main


def _schema(topic="clock-topic"):
    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", replicas=1,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
    )
    return SimpleNamespace(
        edges=(edge,),
        loading=SimpleNamespace(
            coordination=SimpleNamespace(topic=topic, bucket_count=1, slot_duration_ms=10, lease_timeout_ms=20),
        ),
    )


def test_clock_runner_rejects_noncanonical_fleet_before_schema_or_kafka():
    with patch("src.orchestrator.clock_runner.load_schema") as load, \
         patch("src.orchestrator.clock_runner.Producer") as producer:
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT,BOUGHT", "--coordination-topic", "clock-topic"]) == 1
    load.assert_not_called()
    producer.assert_not_called()


def test_clock_runner_rejects_topic_split_before_kafka():
    with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()) as load, \
         patch("src.orchestrator.clock_runner.Producer") as producer:
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "other-topic"]) == 1
    load.assert_called_once_with("schema.yaml")
    producer.assert_not_called()


def test_clock_runner_passes_exact_rotation_plan_to_global_clock():
    producer, global_clock = MagicMock(), MagicMock()
    global_clock.run.return_value = 0
    with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()), \
         patch("src.orchestrator.clock_runner.Producer", return_value=producer), \
         patch("src.orchestrator.clock_runner.GlobalBatchClock", return_value=global_clock) as clock_cls, \
         patch("src.orchestrator.clock_runner.signal.signal", return_value=object()):
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "clock-topic"]) == 0
    assert clock_cls.call_args.kwargs["run_id"] == "run"
    assert [edge.type for edge in clock_cls.call_args.kwargs["edges"]] == ["WORKS_AT"]
    assert clock_cls.call_args.kwargs["rotation_plan"].bucket_count == 1
    producer.flush.assert_called_once()
