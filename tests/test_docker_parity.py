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
GOLDEN_A2_PATH = Path(__file__).resolve().parent / "golden" / "docker_parity_0.1.0a2.json"
GOLDEN_A2 = json.loads(GOLDEN_A2_PATH.read_text(encoding="utf-8"))

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


def replay_raw(name: str) -> dict:
    return support.run_scenario(support.SCENARIOS[name])


def replay(name: str) -> dict:
    """The scenario against the frozen 0.1.0a1 golden: the 0.1.0a2 run-status environment keys and the image namespace
    are the only deliberate differences, so they are removed and restored before comparing (``test_a2_differs_...`` pins
    exactly that)."""
    return support.restore_a1_namespace(support.strip_status_env(replay_raw(name)))


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
    ``--metrics-addr``, ``--metrics-port`` (E15), ``--health-addr`` and ``--child-logs`` on start; E15 adds ``--status-store`` and ``--status-store-auto-migrate`` on start and the ``status-store`` command."""
    text = re.sub(r"\s+", " ", text)
    for fragment in (" [--runtime {docker,process}]", " [--run-dir DIR]", " [--recover]", " [--metrics-addr HOST[:PORT]]",
                     " [--metrics-port PORT]", " [--health-addr HOST:PORT]", " [--child-logs {relay,files}]", " [--health-stale-after SECONDS]",
                     " [--status-store STATUS_STORE]", " [--status-store-auto-migrate]"):
        text = text.replace(fragment, "")
    text = re.sub(r" --runtime \{docker,process\} How roles are run:.*?no Docker daemon, network or images\)", "", text)
    text = re.sub(r" --run-dir DIR process runtime: .*?\(default: the OS temporary directory\)", "", text)
    text = re.sub(r" --recover process runtime: .*?\(refused while it is alive\)", "", text)
    text = re.sub(r" --metrics-addr HOST\[:PORT\] process runtime: .*?use 0\.0\.0\.0 inside a pod", "", text)
    text = re.sub(r" --metrics-port PORT process runtime: .*?not both\)", "", text)
    text = re.sub(r" --health-addr HOST:PORT process runtime: serve /healthz and /readyz here \(off by default\)", "", text)
    text = re.sub(r" --child-logs \{relay,files\} process runtime: .*?logs only", "", text)
    text = re.sub(r" --health-stale-after SECONDS process runtime: .*?\(default 30\)", "", text)
    text = re.sub(r" --status-store STATUS_STORE Where this run's status and event history are kept: .*?Env GRAPH_LOADER_STATUS_STORE\.", "", text)
    text = re.sub(r" --status-store-auto-migrate With --status-store sql: .*?\(env GRAPH_LOADER_STATUS_AUTO_MIGRATE=1\)", "", text)
    # E15.F1.S6: the options `status` gained for run status (they are the last ones of its help text)
    text = re.sub(r" \[--(?:run|execution-id ID|json|watch|interval SECONDS|refresh-lag|refresh-timeout SECONDS|list|active|containers|stale-after SECONDS)\]", "", text)
    text = re.sub(r" --run Show the run status of the newest run.*?json or sql", "", text, flags=re.I)
    text = text.replace(" [--log-format {text,json}]", "")                       # E15.F1.S7: the top-level --log-format option
    text = re.sub(r" --log-format \{text,json\} how this command and its roles log: .*?is never affected", "", text)
    text = text.replace("{status-store,", "{")                      # E15: the `status-store` command (listed first, with its help line)
    text = text.replace("{monitoring,", "{")  # E15.F2.S10: the `monitoring` command (listed right after `status-store`, with its help line)
    text = text.replace("{images,", "{")      # E19.F2.S6: the `images` command (listed right after `monitoring`, with its help line)
    text = re.sub(r" images Build the loader images from this installed package, for a registry you control", "", text)
    text = re.sub(r" monitoring Monitoring assets for your own Prometheus and Grafana \(alert rules, dashboards, scrape configuration\)", "", text)
    text = re.sub(r" status-store Maintain the SQL run status database \(URL from GRAPH_LOADER_STATUS_DB_URL, never on the command line\)", "", text)
    return text


def test_help_is_unchanged_apart_from_the_new_runtime_option() -> None:
    current = {name: text for name, text in support.help_texts().items() if name not in ("status-store", "monitoring", "images")}   # E15's and E19's own commands
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


# -- 0.1.0a2: the one additive Docker-mode change (E15.F1.S2): three run-status environment keys per role launch --------
def test_the_a2_golden_has_the_same_scenarios_and_machine_independent_values() -> None:
    assert set(GOLDEN_A2["scenarios"]) == set(GOLDEN["scenarios"])
    text = GOLDEN_A2_PATH.read_text(encoding="utf-8")
    assert "/Users/" not in text and "/private/" not in text and "/var/folders" not in text
    assert "<EXECUTION_ID>" in text and "<INCARNATION>" in text


@pytest.mark.parametrize("name", sorted(GOLDEN_A2["scenarios"]))
def test_docker_trace_equals_the_0_1_0a2_golden(name) -> None:
    assert diff(GOLDEN_A2["scenarios"][name], replay_raw(name)) == []


def to_a1(record):
    """A 0.1.0a2 record reduced to what the frozen 0.1.0a1 golden can hold: no status keys, the 0.1.0a1 namespace."""
    return support.restore_a1_namespace(support.strip_status_env(record))


def _launches(record: dict) -> list[dict]:
    return [event["args"] for events in record["trace"].values() for event in events if event["op"] == "containers.run"]


def test_a2_differs_from_a1_by_exactly_the_three_status_env_keys_per_role_launch() -> None:
    added_total = 0
    for name, record in GOLDEN_A2["scenarios"].items():
        # everything except those three keys is byte-identical to the frozen source's trace and outcome
        assert diff(GOLDEN["scenarios"][name], to_a1(record)) == [], name
        assert json.dumps(to_a1(record), sort_keys=True) == json.dumps(GOLDEN["scenarios"][name], sort_keys=True), name
        for old, new in zip(sorted(_launches(GOLDEN["scenarios"][name]), key=json.dumps),
                            sorted((to_a1(launch) for launch in _launches(record)), key=json.dumps)):
            assert old == new
        for launch in _launches(record):
            env = launch.get("environment") or {}
            keys = set(env) & set(support.STATUS_ENV_KEYS)
            mounted = any(spec.get("bind") == "/app/write-timings" for spec in (launch.get("volumes") or {}).values())
            assert keys == (set(support.STATUS_ENV_KEYS) if mounted else set()), (name, launch.get("name"))
            if keys:
                added_total += 1
                assert env["GRAPH_LOADER_EXECUTION_ID"] == "<EXECUTION_ID>" and env["GRAPH_LOADER_INCARNATION"] == "<INCARNATION>"
                assert env["GRAPH_LOADER_STATUS_SNAPSHOT_DIR"] == "/app/write-timings/<EXECUTION_ID>/<INCARNATION>"
    assert added_total == 75 - 7, "every CLI-driven role launch gets the keys; direct DockerService launches without a timing mount do not"


def test_control_a_fourth_added_env_key_fails_the_a2_comparison() -> None:
    record = json.loads(json.dumps(GOLDEN_A2["scenarios"]["stream_start_stop"]))
    launch = next(args for events in record["trace"].values() for event in events if event["op"] == "containers.run"
                  for args in [event["args"]] if "GRAPH_LOADER_INCARNATION" in (args.get("environment") or {}))
    launch["environment"]["GRAPH_LOADER_SOMETHING_ELSE"] = "x"
    assert diff(record, replay_raw("stream_start_stop")) != []
    assert json.dumps(to_a1(record), sort_keys=True) != json.dumps(GOLDEN["scenarios"]["stream_start_stop"], sort_keys=True)


def test_control_a_missing_status_key_or_a_new_mount_fails_the_a2_comparison() -> None:
    record = json.loads(json.dumps(GOLDEN_A2["scenarios"]["stream_start_stop"]))
    launches = [event["args"] for events in record["trace"].values() for event in events if event["op"] == "containers.run"]
    launches[0]["environment"].pop("GRAPH_LOADER_INCARNATION")
    assert diff(record, replay_raw("stream_start_stop")) != []
    record = json.loads(json.dumps(GOLDEN_A2["scenarios"]["stream_start_stop"]))
    [event["args"] for events in record["trace"].values() for event in events if event["op"] == "containers.run"][0]["volumes"]["/extra"] = {"bind": "/x"}
    assert diff(record, replay_raw("stream_start_stop")) != []


# -- E19.F1.S1: the image namespace is the only other difference between the two goldens -------------------------------
def test_a1_golden_keeps_the_original_namespace_and_a2_has_the_neburyx_one() -> None:
    a1_text, a2_text = GOLDEN_PATH.read_text(encoding="utf-8"), GOLDEN_A2_PATH.read_text(encoding="utf-8")
    assert support.A1_IMAGE_PREFIX in a1_text and "neburyx" not in a1_text.lower()
    assert support.A2_IMAGE_PREFIX in a2_text and support.A1_IMAGE_PREFIX not in a2_text
    assert support.A1_IMAGE_PREFIX == "ghcr.io/shashankshukla96/graph-loader-" and support.A2_IMAGE_PREFIX == "ghcr.io/neburyx/graph-loader-"


def test_the_namespace_normalization_rewrites_the_image_prefix_and_nothing_else() -> None:
    record = {"reference": "ghcr.io/neburyx/graph-loader-node:<VERSION>", "other": ["ghcr.io/neburyx/other:1", "ghcr.io/neburyx"],
              "n": 3, "flag": True, "key ghcr.io/neburyx/graph-loader-": "x"}
    assert support.restore_a1_namespace(record) == {
        "reference": "ghcr.io/shashankshukla96/graph-loader-node:<VERSION>", "other": ["ghcr.io/neburyx/other:1", "ghcr.io/neburyx"],
        "n": 3, "flag": True, "key ghcr.io/neburyx/graph-loader-": "x"}
    assert support.restore_a1_namespace(GOLDEN["scenarios"]) == GOLDEN["scenarios"], "the 0.1.0a1 golden has no 0.1.0a2 prefix to rewrite"


@pytest.mark.parametrize("name", sorted(GOLDEN_A2["scenarios"]))
def test_a2_golden_differs_from_a1_only_by_the_namespace_and_the_status_keys(name) -> None:
    """Restoring the namespace in the a2 golden, then removing the status keys, gives the a1 golden exactly; restoring the
    namespace alone leaves only the status keys, so no other value moved."""
    a2 = GOLDEN_A2["scenarios"][name]
    assert to_a1(a2) == GOLDEN["scenarios"][name]
    assert support.A2_IMAGE_PREFIX not in json.dumps(support.restore_a1_namespace(a2))


def test_control_a_different_namespace_is_not_normalized_and_fails_both_comparisons() -> None:
    from neo4j_graph_loader import _images
    with patch.object(_images, "NAMESPACE", "ghcr.io/someone-else"):
        wrong_a2 = replay_raw("stream_start_stop")
        problems_a1 = diff(GOLDEN["scenarios"]["stream_start_stop"], replay("stream_start_stop"))
    assert diff(GOLDEN_A2["scenarios"]["stream_start_stop"], wrong_a2) != []
    assert problems_a1 and "someone-else" in " ".join(problems_a1)
