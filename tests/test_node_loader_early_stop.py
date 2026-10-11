"""F1 (node side) and N1: a stop during startup, the early SIGTERM handler, the logged phase5 action.

A stop during the entrypoint's imports exits 0 at once (it used to be exit 143). Once main() runs,
bulk keeps its exact semantics: a stop that arrives during startup still connects, runs the loop,
drains nothing, publishes DRAIN_COMPLETE and exits 0, and a real startup failure still exits 1 with
an ERROR. Stream exits 0 with a WARNING instead of failing its lease-client connect.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import textwrap
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import pytest

from src.loader import node_loader
from src.loader.dead_letter import DeadLetterRecordSink, RejectionSink
from src.loader.lease_client import LeaseCoordinatorError
from src.loader.node_loader import NodeLoader, main as node_loader_main
from src.utils.schema_loader import load_schema

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


@pytest.fixture(scope="module")
def person_config(schema):
    return next(node for node in schema.nodes if node.label == "Person")


def _producer() -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0
    return producer


def _control_types(producer: MagicMock) -> list[str]:
    return [json.loads(call.kwargs["value"])["type"] for call in producer.produce.call_args_list]


# -- early handler (entrypoint only) -------------------------------------------------------------

def test_importing_the_module_installs_no_handler() -> None:
    """Only ``python -m src.loader.node_loader`` installs the early handler; importers are untouched."""
    assert signal.getsignal(signal.SIGTERM) is not node_loader._exit_stopped_while_importing


def _entrypoint_with_fake_kafka(tmp_path: Path, body: str) -> subprocess.CompletedProcess:
    fake = tmp_path / "confluent_kafka"
    fake.mkdir()
    (fake / "__init__.py").write_text(textwrap.dedent(body), encoding="utf-8")
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": os.pathsep.join([str(tmp_path), str(PROJECT_ROOT)])}
    return subprocess.run(
        [sys.executable, "-m", "src.loader.node_loader", "--node-label", "Person"],
        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True, timeout=60,
    )


def test_a_sigterm_during_the_heavy_imports_exits_zero_at_once(tmp_path: Path) -> None:
    """A stand-in confluent_kafka SIGTERMs its own process while being imported (the heavy-import
    window in which P0 containers exited 143). The early handler exits 0 right away: nothing was
    opened, read, written or committed, and finishing the imports first could outlast Docker's grace."""
    result = _entrypoint_with_fake_kafka(tmp_path, """
        import os, signal, sys, time
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.5)
        sys.stderr.write("imports continued after the stop\\n")
        sys.exit(7)
    """)
    assert result.returncode == 0, (result.returncode, result.stderr)
    assert "stage=node-startup-stopped reason=stop requested while the loader was still importing" in result.stderr
    assert "imports continued" not in result.stderr


def test_without_a_stop_the_entrypoint_imports_and_runs_normally(tmp_path: Path) -> None:
    """Control: the early handler changes nothing for a process that is not stopped."""
    result = _entrypoint_with_fake_kafka(tmp_path, """
        import sys
        sys.stderr.write("fake kafka imported\\n")
        sys.exit(7)
    """)
    assert result.returncode == 7 and "fake kafka imported" in result.stderr


# -- main(): a stop that arrived before main() ---------------------------------------------------

def _patched_main(schema, argv, *, stopped: bool, loader_cls=None, leased=None, driver_error=None):
    consumer, producer, driver = MagicMock(), _producer(), MagicMock()
    consumer.poll.return_value = None
    stop = Event()
    if stopped:
        stop.set()
    patches = [
        patch("src.loader.node_loader.Consumer", return_value=consumer),
        patch("src.loader.node_loader.Producer", return_value=producer),
        patch("src.loader.node_loader.get_neo4j_driver",
              **({"side_effect": driver_error} if driver_error else {"return_value": driver})),
        patch("src.loader.node_loader.load_schema", return_value=schema),
    ]
    if loader_cls is not None:
        patches.append(patch("src.loader.node_loader.NodeLoader", loader_cls))
    if leased is not None:
        patches.append(patch("src.loader.node_loader.build_leased_node_writer", **leased))
    for item in patches:
        item.start()
    try:
        code = node_loader_main(argv, shutdown_requested=stop)
    finally:
        for item in reversed(patches):
            item.stop()
    return code, consumer, producer, driver, stop


def test_bulk_stopped_during_startup_still_drains_and_acknowledges_exactly_as_before(schema, caplog) -> None:
    with caplog.at_level(logging.INFO):
        code, consumer, producer, driver, _stop = _patched_main(
            schema, ["--node-label", "Person", "--mode", "bulk"], stopped=True,
        )
    assert code == 0
    assert _control_types(producer) == ["DRAIN_COMPLETE"]
    consumer.subscribe.assert_called_once()
    consumer.poll.assert_not_called()          # D2: a stopping loader never fetches
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
    driver.close.assert_called_once()
    assert any("Node loader gracefully stopping label=Person" in r.getMessage() for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_bulk_stop_before_or_after_main_has_the_same_outcome(schema) -> None:
    """The early handler only moves when the stop is recorded; bulk sees the very same run."""
    early = _patched_main(schema, ["--node-label", "Person", "--mode", "bulk"], stopped=True)
    stop_on_first_poll = Event()
    consumer = MagicMock()
    consumer.poll.side_effect = lambda _timeout: stop_on_first_poll.set()
    producer = _producer()
    with patch("src.loader.node_loader.Consumer", return_value=consumer), \
         patch("src.loader.node_loader.Producer", return_value=producer), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=MagicMock()), \
         patch("src.loader.node_loader.load_schema", return_value=schema):
        late_code = node_loader_main(["--node-label", "Person", "--mode", "bulk"], shutdown_requested=stop_on_first_poll)
    assert (early[0], _control_types(early[2])) == (late_code, _control_types(producer)) == (0, ["DRAIN_COMPLETE"])


def test_bulk_startup_failures_stay_fail_closed_even_when_a_stop_was_requested(schema, caplog) -> None:
    with caplog.at_level(logging.ERROR):
        code, *_ = _patched_main(
            schema, ["--node-label", "Person", "--mode", "bulk"], stopped=True,
            driver_error=RuntimeError("neo4j unreachable"),
        )
    assert code == 1
    assert any("Unable to start node loader: neo4j unreachable" in r.getMessage() for r in caplog.records)


def test_stream_stopped_before_it_connects_exits_zero_without_connecting(schema, caplog) -> None:
    loader_cls = MagicMock()
    with caplog.at_level(logging.WARNING):
        code, consumer, producer, driver, _ = _patched_main(
            schema, ["--node-label", "Person", "--mode", "stream"], stopped=True, loader_cls=loader_cls,
        )
    assert code == 0
    loader_cls.assert_not_called()
    producer.produce.assert_not_called()
    consumer.subscribe.assert_not_called()
    assert any("stage=node-startup-stopped label=Person" in r.getMessage() for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_stream_stop_during_the_lease_connect_exits_zero(schema, caplog) -> None:
    """The stop lands after the check, while the lease clients connect (they refuse once stopped)."""
    holder: dict = {}

    def connect_refused(*_args, shutdown_requested, **_kwargs):
        shutdown_requested.set()
        holder["stop"] = shutdown_requested
        raise LeaseCoordinatorError("lease reconnect abandoned: shutdown requested")

    with caplog.at_level(logging.WARNING):
        code, *_ = _patched_main(
            schema, ["--node-label", "Person", "--mode", "stream"], stopped=False,
            leased={"side_effect": connect_refused},
        )
    assert holder["stop"].is_set()
    assert code == 0
    assert any("stage=node-startup-stopped" in r.getMessage() and "lease reconnect abandoned" in r.getMessage()
               for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_stream_real_startup_failure_during_a_stop_stays_an_error(schema, caplog) -> None:
    """Reviewer repro: the stop lands while a real failure happens (Neo4j auth): ERROR and exit 1."""
    holder: dict = {}

    def auth_failure(*_args, **_kwargs):
        holder["stop"].set()
        raise RuntimeError("Neo4j AuthError: unauthorized")

    stop = Event()
    holder["stop"] = stop
    with patch("src.loader.node_loader.Consumer", return_value=MagicMock()), \
         patch("src.loader.node_loader.Producer", return_value=_producer()), \
         patch("src.loader.node_loader.get_neo4j_driver", side_effect=auth_failure), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         caplog.at_level(logging.WARNING):
        code = node_loader_main(["--node-label", "Person", "--mode", "stream"], shutdown_requested=stop)
    assert stop.is_set() and code == 1
    assert any(r.levelno == logging.ERROR and "Unable to start node loader: Neo4j AuthError" in r.getMessage()
               for r in caplog.records)
    assert not any("stage=node-startup-stopped" in r.getMessage() for r in caplog.records)


def test_stream_unrelated_lease_error_during_a_stop_stays_an_error(schema, caplog) -> None:
    def lease_failure(*_args, shutdown_requested, **_kwargs):
        shutdown_requested.set()
        raise LeaseCoordinatorError("HELLO rejected: bucket count mismatch")

    with caplog.at_level(logging.WARNING):
        code, *_ = _patched_main(
            schema, ["--node-label", "Person", "--mode", "stream"], stopped=False,
            leased={"side_effect": lease_failure},
        )
    assert code == 1
    assert any(r.levelno == logging.ERROR and "HELLO rejected" in r.getMessage() for r in caplog.records)


def test_an_argument_rejection_never_hands_back_the_early_handler(monkeypatch) -> None:
    """Item 2: after a parse error the early (exit-0) handler is not restored, so a SIGTERM in that
    window cannot turn the rejection (exit 2) into exit 0."""
    signal.signal(signal.SIGTERM, node_loader._exit_stopped_while_importing)
    with pytest.raises(SystemExit) as excinfo:
        node_loader_main(["--no-such-flag"])
    assert excinfo.value.code == 2
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


def test_an_argument_rejection_restores_any_other_previous_handler() -> None:
    def previous(_signum, _frame):
        return None

    signal.signal(signal.SIGTERM, previous)
    with pytest.raises(SystemExit):
        node_loader_main(["--node-label", "Person", "--max-messages", "0"])
    assert signal.getsignal(signal.SIGTERM) is previous


def test_stream_startup_failure_without_a_stop_is_still_an_error(schema, caplog) -> None:
    with caplog.at_level(logging.ERROR):
        code, *_ = _patched_main(
            schema, ["--node-label", "Person", "--mode", "stream"], stopped=False,
            leased={"side_effect": LeaseCoordinatorError("lease coordinator unreachable")},
        )
    assert code == 1
    assert any("Unable to start node loader: lease coordinator unreachable" in r.getMessage() for r in caplog.records)


def test_main_leaves_the_flag_only_handler_bound_to_the_shared_event(schema) -> None:
    """The handler main() (re)installs sets the same event the early handler set."""
    stop = Event()
    with patch("src.loader.node_loader.Consumer", return_value=MagicMock(**{"poll.return_value": None})), \
         patch("src.loader.node_loader.Producer", return_value=_producer()), \
         patch("src.loader.node_loader.get_neo4j_driver", return_value=MagicMock()), \
         patch("src.loader.node_loader.load_schema", return_value=schema), \
         patch("src.loader.node_loader.NodeLoader") as loader_cls:
        loader_cls.return_value.run.side_effect = lambda **_kw: (signal.getsignal(signal.SIGTERM)(15, None), 0)[1]
        assert node_loader_main(["--node-label", "Person", "--mode", "bulk"], shutdown_requested=stop) == 0
    assert stop.is_set()
    assert loader_cls.call_args.kwargs["shutdown_requested"] is stop


# -- N1: the logged phase5 action names the real sink --------------------------------------------

def _malformed(offset: int = 3):
    message = MagicMock()
    message.value.return_value = b"{bad"
    message.error.return_value = None
    message.topic.return_value = "person-events"
    message.partition.return_value = 0
    message.offset.return_value = offset
    message.key.return_value = None
    return message


@pytest.mark.parametrize(
    "sink, action",
    [(MagicMock(spec=DeadLetterRecordSink), "route_to_dlq"), (MagicMock(spec=RejectionSink), "reject_to_file")],
)
def test_malformed_record_log_names_the_real_phase5_action(person_config, caplog, sink, action) -> None:
    consumer = MagicMock()
    loader = NodeLoader(consumer, MagicMock(), person_config, rejection_sink=sink)
    with caplog.at_level(logging.WARNING):
        assert loader._buffer_message(_malformed()) is True
    sink.route.assert_called_once()
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert f"phase5_action={action}" in text
    other = {"route_to_dlq", "reject_to_file"} - {action}
    assert not any(f"phase5_action={name}" in text for name in other)
