"""E14.F1.S1: Docker-mode behavioural parity against the frozen ``0.1.0a1`` source (commit 60ea29f).

``tests/golden/docker_parity_0.1.0a1.json`` was produced by running ``tests/docker_parity_support.py`` against
the source tree at commit 60ea29f:

    git archive 60ea29f | tar -x -C <dir>
    PYTHONPATH=<dir>/src python tests/docker_parity_support.py --emit tests/golden/docker_parity_0.1.0a1.json \\
        --source 60ea29f

Each test replays one scenario against the current source and requires the ordered, per-lane Docker call
trace and the outcome to equal the golden after the normalization documented in ``docker_parity_support``.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest

import docker_parity_support as support

GOLDEN_PATH = Path(__file__).resolve().parent / "golden" / "docker_parity_0.1.0a1.json"
GOLDEN = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

# Every scenario E14.F1.S1 requires (acceptance 2), by name in the harness.
REQUIRED = {
    "stream start/stop": ["stream_start_stop"],
    "stream with --stream-clock-rotation": ["stream_clock_rotation"],
    "planned bulk, both overlap modes": ["planned_bulk_overlap_labels", "planned_bulk_overlap_shared", "planned_bulk"],
    "--edge-classic": ["edge_classic_overlap_labels", "edge_classic_overlap_shared"],
    "--edge-leases": ["edge_leases"],
    "lease-server replacement": ["lease_server_replacement"],
    "partial-launch rollback": ["partial_launch_rollback"],
    "a loader crash": ["loader_crash_stream", "loader_crash_bulk", "crash_detection_real_monitors"],
    "a stop during start-up": ["stop_during_startup"],
    "stop": ["stop_command"],
    "status": ["status_command"],
    "replay-dlq": ["replay_dlq"],
}


def diff(expected: dict, actual: dict) -> list[str]:
    """Human-readable differences between two scenario records (empty when equal)."""
    problems = []
    if expected["outcome"] != actual["outcome"]:
        problems.append(f"outcome: {expected['outcome']!r} != {actual['outcome']!r}")
    if expected["problems"] != actual["problems"]:
        problems.append("error log lines differ")
    for lane in sorted(set(expected["trace"]) | set(actual["trace"])):
        before, after = expected["trace"].get(lane, []), actual["trace"].get(lane, [])
        for index in range(max(len(before), len(after))):
            if index >= len(before) or index >= len(after) or before[index] != after[index]:
                problems.append(f"lane {lane!r} call {index}: {before[index] if index < len(before) else None!r} "
                                f"!= {after[index] if index < len(after) else None!r}")
                break
    return problems


def replay(name: str) -> dict:
    return support.run_scenario(support.SCENARIOS[name])


def test_golden_was_produced_by_the_frozen_source() -> None:
    assert GOLDEN["_source"] == "60ea29f"


def test_every_required_scenario_is_in_the_golden_and_harness() -> None:
    names = {name for group in REQUIRED.values() for name in group}
    assert names <= set(GOLDEN["scenarios"]) and names <= set(support.SCENARIOS)
    assert set(GOLDEN["scenarios"]) == set(support.SCENARIOS), "golden and harness scenarios must match exactly"


@pytest.mark.parametrize("name", sorted(GOLDEN["scenarios"]))
def test_docker_trace_equals_the_frozen_source(name) -> None:
    assert diff(GOLDEN["scenarios"][name], replay(name)) == []


def test_golden_covers_preflight_image_resolution_labels_and_stops() -> None:
    """The traces really contain the call kinds the acceptance names (not just launches)."""
    calls = {(event["subject"] if event["subject"] in {"client", "api"} else "container", event["op"])
             for record in GOLDEN["scenarios"].values() for events in record["trace"].values() for event in events}
    for expected in [("client", "containers.run"), ("client", "containers.list"), ("client", "images.get"),
                     ("client", "networks.get"), ("api", "create_container"), ("api", "wait"), ("api", "logs"),
                     ("api", "remove_container"), ("container", "stop"), ("container", "remove"),
                     ("container", "reload"), ("container", "logs"), ("container", "top")]:
        assert expected in calls, expected
    filters = [event["args"]["filters"] for record in GOLDEN["scenarios"].values()
               for events in record["trace"].values() for event in events if event["op"] == "containers.list"]
    assert {"label": ["component=node-loader"]} in filters
    assert {"label": ["run_id=run-parity", "component=bucket-lease-server"]} in filters
    stop_kwargs = [event["args"]["kwargs"] for record in GOLDEN["scenarios"].values()
                   for events in record["trace"].values() for event in events if event["op"] == "stop"]
    assert {"timeout": 10} in stop_kwargs and {"timeout": 30} in stop_kwargs and {} in stop_kwargs


def test_golden_is_machine_independent() -> None:
    text = GOLDEN_PATH.read_text(encoding="utf-8")
    assert "/Users/" not in text and "/private/" not in text and "/var/folders" not in text
    assert re.search(r"\b0\.1\.0a\d\b", json.dumps(GOLDEN["scenarios"])) is None, "package version must be normalized"
    assert "<VERSION>" in text and "<TMP>" in text and "<ATTEMPT>" in text


def test_replay_is_deterministic() -> None:
    """The heaviest scenarios replay to identical traces back to back (so the parity check cannot flake)."""
    for name in ("stream_start_stop", "planned_bulk"):
        assert diff(replay(name), replay(name)) == []


# -- negative controls: the parity check must fail when the Docker call sequence changes ----------------------
def test_control_reordered_stop_fails_parity() -> None:
    from neo4j_graph_loader import cli
    original = cli._stop_exact_containers

    def reversed_stop(containers, **kwargs):
        return original(list(containers)[::-1], **kwargs)

    with patch.object(cli, "_stop_exact_containers", reversed_stop):
        problems = diff(GOLDEN["scenarios"]["stream_start_stop"], replay("stream_start_stop"))
    assert problems and any("container-stop" in problem or "MainThread" in problem for problem in problems)


def test_control_changed_run_kwarg_fails_parity() -> None:
    from neo4j_graph_loader.orchestrator.docker_service import DockerService
    original = DockerService._run_container

    def without_init(self, **kwargs):
        kwargs["init"] = False
        return original(self, **kwargs)

    with patch.object(DockerService, "_run_container", without_init):
        problems = diff(GOLDEN["scenarios"]["stream_start_stop"], replay("stream_start_stop"))
    assert problems and "'init': False" in " ".join(problems)


def test_control_changed_stop_timeout_fails_parity() -> None:
    from neo4j_graph_loader import cli
    with patch.object(cli, "_NODE_STOP_TIMEOUT_SECONDS", cli._NODE_STOP_TIMEOUT_SECONDS + 1):
        problems = diff(GOLDEN["scenarios"]["stream_start_stop"], replay("stream_start_stop"))
    assert problems and "'timeout': 31" in " ".join(problems)


def test_control_changed_label_filter_fails_parity() -> None:
    from neo4j_graph_loader.orchestrator.docker_service import DockerService
    original = DockerService.list_node_loaders

    def other_filter(self):
        self.client.containers.list(all=True, filters={"label": "component=edge-loader"})
        return original(self)

    with patch.object(DockerService, "list_node_loaders", other_filter):
        problems = diff(GOLDEN["scenarios"]["status_command"], replay("status_command"))
    assert problems


# -- CLI surface ---------------------------------------------------------------------------------------
def _without_runtime_option(text: str) -> str:
    """Help text with the options added by E14 removed, whitespace-collapsed: ``--runtime``; ``--run-dir DIR`` on stop/status;
    ``--metrics-addr``, ``--health-addr`` and ``--child-logs`` on start."""
    text = re.sub(r"\s+", " ", text)
    for fragment in (" [--runtime {docker,process}]", " [--run-dir DIR]", " [--recover]", " [--metrics-addr HOST[:PORT]]",
                     " [--health-addr HOST:PORT]", " [--child-logs {relay,files}]", " [--health-stale-after SECONDS]"):
        text = text.replace(fragment, "")
    text = re.sub(r" --runtime \{docker,process\} How roles are run:.*?no Docker daemon, network or images\)", "", text)
    text = re.sub(r" --run-dir DIR process runtime: .*?\(default: the OS temporary directory\)", "", text)
    text = re.sub(r" --recover process runtime: .*?\(refused while it is alive\)", "", text)
    text = re.sub(r" --metrics-addr HOST\[:PORT\] process runtime: .*?use 0\.0\.0\.0 inside a pod", "", text)
    text = re.sub(r" --health-addr HOST:PORT process runtime: serve /healthz and /readyz here \(off by default\)", "", text)
    text = re.sub(r" --child-logs \{relay,files\} process runtime: .*?logs only", "", text)
    text = re.sub(r" --health-stale-after SECONDS process runtime: .*?\(default 30\)", "", text)
    return text


def test_help_is_unchanged_apart_from_the_new_runtime_option() -> None:
    current = support.help_texts()
    assert set(current) == set(GOLDEN["_help"])
    for name, frozen in GOLDEN["_help"].items():
        collapsed_frozen = re.sub(r"\s+", " ", frozen)
        assert _without_runtime_option(current[name]) == collapsed_frozen, name or "top-level"
    for name in ("start", "stop", "status", "replay-dlq"):
        assert "--runtime {docker,process}" in current[name]
    assert "--runtime" not in current[""] and "--runtime" not in current["validate-schema"]


def test_docker_is_the_default_runtime_and_the_environment_can_select_one(monkeypatch) -> None:
    from neo4j_graph_loader.cli import build_parser
    monkeypatch.delenv("GRAPH_LOADER_RUNTIME", raising=False)
    for command in (["start", "--mode", "bulk"], ["stop"], ["status"], ["replay-dlq", "--node-label", "Person"]):
        assert build_parser().parse_args(command).runtime_backend == "docker"
    assert build_parser().parse_args(["start", "--mode", "bulk", "--runtime", "process"]).runtime_backend == "process"
    monkeypatch.setenv("GRAPH_LOADER_RUNTIME", "process")
    assert build_parser().parse_args(["status"]).runtime_backend == "process"


@pytest.mark.parametrize("command", [["start", "--mode", "bulk"], ["stop"], ["status"]])
def test_process_runtime_is_refused_on_macos_without_touching_docker(command, caplog, monkeypatch) -> None:
    import platform
    from neo4j_graph_loader import cli
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    with patch.object(cli, "DockerService", side_effect=AssertionError("docker touched")), caplog.at_level("ERROR"):
        assert cli.main([*command, "--runtime", "process"]) == 1
    assert "supported on Linux only (this is Darwin)" in caplog.text


@pytest.mark.parametrize("flags,option", [(["--network", "graph-loader-net"], "--network"), (["--skip-image-build"], "--skip-image-build")])
def test_docker_only_options_are_rejected_with_the_process_runtime(flags, option, caplog, monkeypatch) -> None:
    """Even ``--network`` at its default value counts as given: it names a Docker network."""
    from neo4j_graph_loader import cli
    from neo4j_graph_loader.orchestrator import process_service
    monkeypatch.setattr(process_service, "check_platform", lambda *a, **k: None)
    with patch.object(cli, "DockerService", side_effect=AssertionError("docker touched")), caplog.at_level("ERROR"):
        assert cli.main(["start", "--mode", "bulk", "--runtime", "process", *flags]) == 1
    assert f"{option} applies to Docker containers" in caplog.text


def test_an_unknown_runtime_in_the_environment_is_refused(monkeypatch, caplog) -> None:
    from neo4j_graph_loader import cli
    monkeypatch.setenv("GRAPH_LOADER_RUNTIME", "kubernetes")
    with caplog.at_level("ERROR"):
        assert cli.main(["status"]) == 1
    assert "GRAPH_LOADER_RUNTIME must be one of docker, process" in caplog.text
