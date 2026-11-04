"""E15.F1.S6: ``graph-loader status`` for run status: exit codes, ``--json``, ``--list``, ``--watch``, ``--refresh-lag``."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader.status import StatusTracker, json_store, locator as locator_module, model, schemas, validate
from neo4j_graph_loader.status import lag as status_lag, view

CHILD = Path(__file__).resolve().parent / "fixtures" / "e15_status_child.py"
ROOT = Path(__file__).resolve().parents[1]


class Clock:
    def __init__(self, start):
        self.now = start

    def __call__(self):
        self.now += 0.01
        return self.now


def make_run(base: Path, name: str, *, state: str = "running", age: float = 0.0, run_id: str = "r1", mode: str = "stream",
             started_age: float = 100.0, with_types: bool = True) -> tuple[Path, StatusTracker]:
    """An attempt directory holding a run in ``state``, last heard from ``age`` seconds ago."""
    attempt = base / name
    attempt.mkdir(parents=True)
    now = time.time()
    execution = StatusTracker(mode=mode, run_id=run_id, runtime="docker", clock=Clock(now - started_age))
    store = json_store.JsonStatusStore(attempt, snapshot_interval=0.0, fsync=False)
    execution.attach_journal(store)
    execution.subscribe(on_snapshot=store.offer_snapshot)
    locator_module.write_locator(attempt, locator_module.build_locator(
        store="json", execution_id=execution.execution_id, run_id=run_id, created_at=execution.snapshot()["started_at"]))
    if state != "starting":                     # (starting a role moves the run to running)
        execution.role_started("node-loader", "Person", 0, container_id="c1")
    if with_types and state != "starting":
        execution.update_type("node", "Person", records_written=model.observed(1200, "2026-11-03T00:00:00.000Z", "role-snapshots"),
                              lag=None, ready=model.observed(True, "2026-11-03T00:00:00.000Z", "node-monitor"))
        execution.update_totals(records_written=model.observed(1200, "2026-11-03T00:00:00.000Z", "role-snapshots", lower_bound=True))
    if state in ("succeeded", "failed", "stopped"):
        if state == "stopped":
            execution.request_stop()
        execution.finish(0 if state in ("succeeded", "stopped") else 1)
    elif state == "stopping":
        execution.transition("stopping")
    # the heartbeat is `age` seconds old: rewrite the document the way an old CLI would have left it
    document = execution.snapshot()
    document["heartbeat_at"] = model.timestamp(now - age)
    document["revision"] += 1
    store.close()
    (attempt / "status.json").write_text(json.dumps(document))
    return attempt, execution


TRIGGERS = ("--run", "--json", "--watch", "--list", "--execution-id", "--refresh-lag")


def run_status(*argv) -> int:
    """``graph-loader status`` for RUN status (``--run`` is added when no other option asks for it)."""
    return cli.main(["status", *argv, *(() if any(flag in argv for flag in TRIGGERS) else ("--run",))])


# -- exit codes ---------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("state, code", [("running", 0), ("starting", 0), ("stopping", 0), ("succeeded", 0), ("failed", 1), ("stopped", 3)])
def test_exit_codes_follow_the_state_of_the_run(tmp_path, capsys, state, code):
    attempt, _ = make_run(tmp_path, "graph-loader-a", state=state)
    assert run_status("--run-dir", str(attempt)) == code
    out = capsys.readouterr().out
    assert "execution " in out and state in out


@pytest.mark.parametrize("state", ["running", "starting", "stopping"])
def test_an_unfinished_run_that_stops_heartbeating_is_stale_with_exit_2(tmp_path, capsys, state):
    attempt, _ = make_run(tmp_path, "graph-loader-a", state=state, age=45)
    assert run_status("--run-dir", str(attempt)) == 2
    assert "stale" in capsys.readouterr().out
    assert run_status("--run-dir", str(attempt), "--stale-after", "120") == 0           # the threshold is the reader's to choose


@pytest.mark.parametrize("state, code", [("succeeded", 0), ("failed", 1), ("stopped", 3)])
def test_a_finished_run_is_never_stale_however_old(tmp_path, capsys, state, code):
    attempt, _ = make_run(tmp_path, "graph-loader-a", state=state, age=86_400)
    assert run_status("--run-dir", str(attempt)) == code
    assert "stale" not in capsys.readouterr().out


def test_the_threshold_comes_from_the_environment_too(tmp_path, monkeypatch):
    attempt, _ = make_run(tmp_path, "graph-loader-a", age=10)
    assert run_status("--run-dir", str(attempt)) == 0
    monkeypatch.setenv(view.STALE_AFTER_ENV, "5")
    assert run_status("--run-dir", str(attempt)) == 2
    monkeypatch.setenv(view.STALE_AFTER_ENV, "garbage")
    assert view.stale_after_seconds() == view.DEFAULT_STALE_AFTER_SECONDS == 30.0


def test_nothing_found_and_unreadable_status_are_exit_2_not_a_crash(tmp_path, caplog):
    assert run_status("--run-dir", str(tmp_path), "--json") == 2 and "no run status found" in caplog.text
    attempt, _ = make_run(tmp_path, "graph-loader-a")
    (attempt / "status.json").write_text("{not json")
    assert run_status("--run-dir", str(attempt)) == 2 and "unreadable" in caplog.text
    (attempt / "status.json").write_text(json.dumps({"state": "running"}))
    assert run_status("--run-dir", str(attempt)) == 2                                    # a document that fails its schema
    assert run_status("--execution-id", model.new_id(), "--run-dir", str(tmp_path)) == 2


# -- kill -9 -> stale -> dead --------------------------------------------------------------------------------------------------------
def start_child(attempt: Path, registry: bool) -> subprocess.Popen:
    child = subprocess.Popen([sys.executable, str(CHILD), str(attempt), "registry" if registry else "noregistry"],
                             stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "ready"
    return child


@pytest.mark.parametrize("registry", [True, False])
def test_a_hung_cli_is_stale_then_a_killed_one_is_dead(tmp_path, capsys, registry):
    attempt = tmp_path / "graph-loader-live"
    attempt.mkdir()
    child = start_child(attempt, registry)
    try:
        argv = ("--run-dir", str(attempt), "--stale-after", "1.5", "--json")
        assert run_status(*argv) == 0
        live = json.loads(capsys.readouterr().out)
        assert live["state"] == "running" and live["reader"]["stale"] is False and live["reader"]["label"] == "running"
        validate(live, schemas.STATUS_SCHEMA)
        os.kill(child.pid, signal.SIGSTOP)                                              # alive, but silent: hung
        time.sleep(2.2)
        assert run_status(*argv) == 2
        hung = json.loads(capsys.readouterr().out)
        assert hung["reader"]["stale"] is True and hung["reader"]["dead"] is False and hung["reader"]["label"] == "stale"
        os.kill(child.pid, signal.SIGKILL)                                              # now it is gone
        child.wait(timeout=10)
        assert run_status(*argv) == 2
        dead = json.loads(capsys.readouterr().out)
        assert dead["reader"]["stale"] is True and dead["reader"]["dead"] is True and dead["reader"]["label"] == "dead"
        assert dead["state"] == "running"                                               # what the dead CLI last wrote
        assert run_status("--run-dir", str(attempt), "--stale-after", "1.5") == 2
        assert "controlling CLI process is gone" in capsys.readouterr().out
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


def test_a_recycled_pid_does_not_make_a_stale_run_look_alive(tmp_path):
    attempt, _ = make_run(tmp_path, "graph-loader-a", age=300)
    from neo4j_graph_loader.orchestrator import run_registry
    identity = run_registry.process_identity(os.getpid())
    run_registry.RunRegistry(attempt / "registry.jsonl").append(
        "cli", pid=os.getpid(), pgid=os.getpgid(0), start_time="another start time", boot_id=identity["boot_id"])
    status = json.loads((attempt / "status.json").read_text())
    assert view.cli_is_gone(attempt, status) is True                                    # the pid exists, but it is not that CLI
    run_registry.RunRegistry(attempt / "registry2.jsonl")
    same = tmp_path / "graph-loader-b"
    same.mkdir()
    run_registry.RunRegistry(same / "registry.jsonl").append("cli", pid=os.getpid(), pgid=os.getpgid(0), **identity)
    assert view.cli_is_gone(same, status) is False                                      # the registered CLI is right there


def test_dead_is_only_claimed_where_this_host_can_prove_it(tmp_path):
    attempt, _ = make_run(tmp_path, "graph-loader-a", age=300)
    status = json.loads((attempt / "status.json").read_text())
    assert view.cli_is_gone(None, {**status, "host": "another-host", "pid": 1}) is None          # cannot tell
    assert view.cli_is_gone(None, {**status, "host": __import__("socket").gethostname(), "pid": os.getpid()}) is None   # alive: not proof
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()
    assert view.cli_is_gone(None, {**status, "host": __import__("socket").gethostname(), "pid": gone.pid}) is True
    assessment = view.assess({**status, "host": "another-host"}, attempt=None)
    assert assessment["stale"] is True and assessment["dead"] is False and assessment["label"] == "stale"


# -- --json --------------------------------------------------------------------------------------------------------------------------
def test_json_output_validates_and_carries_the_readers_judgement(tmp_path, capsys):
    attempt, execution = make_run(tmp_path, "graph-loader-a", state="failed")
    assert run_status("--run-dir", str(attempt), "--json") == 1
    out = capsys.readouterr().out
    assert out.count("\n") == 1                                                          # one JSON object, nothing else on stdout
    document = json.loads(out)
    validate(document, schemas.STATUS_SCHEMA)
    reader = document.pop("reader")
    assert reader == {"stale": False, "dead": None, "heartbeat_age_seconds": reader["heartbeat_age_seconds"], "stale_after_seconds": 30.0,
                      "exit_code": 1, "label": "failed"}
    assert document == {**execution.snapshot(), "heartbeat_at": document["heartbeat_at"], "revision": document["revision"]}
    assert document["types"][0]["records_written"]["lower_bound"] if "lower_bound" in document["types"][0]["records_written"] else True


def test_text_output_shows_unknown_as_a_question_mark_and_lower_bounds_with_a_plus(tmp_path, capsys):
    attempt, _ = make_run(tmp_path, "graph-loader-a")
    run_status("--run-dir", str(attempt))
    out = capsys.readouterr().out
    assert "written 1,200" in out and "lag ?" in out and "ready yes" in out and "total" in out and "1,200+" in out
    assert "roles     1: 1 running" in out


# -- execution selection ------------------------------------------------------------------------------------------------------------
def test_the_newest_run_of_a_base_is_shown_unless_an_execution_id_is_given(tmp_path, capsys):
    old, old_run = make_run(tmp_path, "graph-loader-old", state="succeeded", started_age=500, run_id="old")
    new, new_run = make_run(tmp_path, "graph-loader-new", state="failed", started_age=50, run_id="new")
    assert run_status("--run-dir", str(tmp_path), "--json") == 1
    assert json.loads(capsys.readouterr().out)["execution_id"] == new_run.execution_id
    assert run_status("--run-dir", str(tmp_path), "--execution-id", old_run.execution_id, "--json") == 0
    assert json.loads(capsys.readouterr().out)["execution_id"] == old_run.execution_id
    assert old and new


# -- --list ------------------------------------------------------------------------------------------------------------------------------
def test_list_shows_every_run_with_its_label_and_active_filters_the_finished_ones(tmp_path, capsys):
    done, done_run = make_run(tmp_path, "graph-loader-1", state="succeeded", started_age=900, run_id="done")
    live, live_run = make_run(tmp_path, "graph-loader-2", state="running", started_age=60, run_id="live")
    silent, silent_run = make_run(tmp_path, "graph-loader-3", state="running", age=300, started_age=800, run_id="silent")
    assert run_status("--list", "--run-dir", str(tmp_path)) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 3 and lines[0].startswith(live_run.execution_id)                # newest first
    labels = {line.split()[0]: line.split()[1] for line in lines}
    assert labels[done_run.execution_id] == "succeeded" and labels[live_run.execution_id] == "running" and labels[silent_run.execution_id] in ("stale", "dead")
    assert run_status("--list", "--active", "--json", "--run-dir", str(tmp_path)) == 0
    rows = json.loads(capsys.readouterr().out)
    assert {row["execution_id"] for row in rows} == {live_run.execution_id, silent_run.execution_id}
    assert all(row["attempt"] for row in rows) and {row["run_id"] for row in rows} == {"live", "silent"}
    assert run_status("--list", "--run-dir", str(tmp_path / "nothing")) == 0 and "no runs found" in capsys.readouterr().out
    assert done and live and silent


def test_list_reads_the_sql_store_too(tmp_path, monkeypatch, capsys):
    pytest.importorskip("sqlalchemy")
    from neo4j_graph_loader.status import sql_store
    url = f"sqlite:///{tmp_path / 'db.sqlite'}"
    monkeypatch.setenv(sql_store.URL_ENV, url)
    database = sql_store.SqlStatusStore(url)
    database.migrate()
    finished = StatusTracker(mode="stream", run_id="a", runtime="process")
    finished.finish(0)
    live = StatusTracker(mode="bulk", run_id="b", runtime="docker", clock=Clock(time.time() - 600))
    for run in (finished, live):
        database.write_snapshot(run.snapshot())
    assert run_status("--list", "--status-store", "sql", "--json") == 0
    rows = json.loads(capsys.readouterr().out)
    by_id = {row["execution_id"]: row for row in rows}
    assert by_id[finished.execution_id]["label"] == "succeeded" and by_id[live.execution_id]["label"] in ("stale", "dead")
    assert run_status("--list", "--active", "--status-store", "sql", "--json") == 0
    assert [row["execution_id"] for row in json.loads(capsys.readouterr().out)] == [live.execution_id]
    assert run_status("--execution-id", live.execution_id, "--status-store", "sql", "--json") == 2           # stale: its heartbeat is 10 minutes old
    assert json.loads(capsys.readouterr().out)["execution_id"] == live.execution_id
    database.close()


# -- --watch ---------------------------------------------------------------------------------------------------------------------------
def test_watch_follows_a_run_until_it_finishes_and_returns_its_exit_code(tmp_path, capsys):
    attempt = tmp_path / "graph-loader-w"
    attempt.mkdir()
    execution = StatusTracker(mode="stream", run_id="w", runtime="process")
    store = json_store.JsonStatusStore(attempt, snapshot_interval=0.0, fsync=False)
    execution.attach_journal(store)
    execution.subscribe(on_snapshot=store.offer_snapshot)
    locator_module.write_locator(attempt, locator_module.build_locator(
        store="json", execution_id=execution.execution_id, run_id="w", created_at=execution.snapshot()["started_at"]))
    execution.role_started("node-loader", "Person", 0, container_id="c")
    store.write_snapshot()

    def finish_later():
        time.sleep(0.5)
        execution.update_type("node", "Person", records_written=model.observed(5, "2026-11-03T00:00:00.000Z", "t"))
        store.write_snapshot()
        time.sleep(0.5)
        execution.finish(1)
        store.write_snapshot()

    thread = threading.Thread(target=finish_later)
    thread.start()
    started = time.monotonic()
    assert run_status("--run-dir", str(attempt), "--watch", "--interval", "0.1") == 1
    thread.join()
    store.close()
    out = capsys.readouterr().out
    assert out.count("execution ") >= 3 and "written 5" in out and "state     failed" in out
    assert time.monotonic() - started < 15
    assert run_status("--run-dir", str(attempt), "--watch", "--json", "--interval", "0.1") == 1       # a finished run: one frame, at once
    assert json.loads(capsys.readouterr().out)["state"] == "failed"


def test_watch_ends_when_the_run_goes_stale(tmp_path, capsys):
    attempt, _ = make_run(tmp_path, "graph-loader-a", age=100)
    assert run_status("--run-dir", str(attempt), "--watch", "--interval", "0.05") == 2
    assert "stale" in capsys.readouterr().out


# -- --refresh-lag -------------------------------------------------------------------------------------------------------------------------
def with_schema_copy(attempt: Path) -> None:
    (attempt / "runtime_schema.yaml").write_text((ROOT / "config" / "graph_schema.yaml").read_text())


def test_refresh_lag_is_bounded_when_kafka_is_down_and_changes_nothing_else(tmp_path, monkeypatch, capsys):
    attempt, _ = make_run(tmp_path, "graph-loader-a")
    with_schema_copy(attempt)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")
    started = time.monotonic()
    assert run_status("--run-dir", str(attempt), "--refresh-lag", "--refresh-timeout", "1.5", "--json") == 0
    elapsed = time.monotonic() - started
    document = json.loads(capsys.readouterr().out)
    assert elapsed < 8                                                                       # bounded by the deadline, not by Kafka
    assert document["reader"]["lag_refreshed"] is False
    assert ("within 1.5s" in document["reader"]["lag_refresh_error"]) or ("cannot read the broker metadata" in document["reader"]["lag_refresh_error"])
    assert [t for t in document["types"] if t["name"] == "Person"][0]["lag"] is None          # the stored (unknown) value, untouched
    run_status("--run-dir", str(attempt), "--refresh-lag", "--refresh-timeout", "1")
    assert "lag       not refreshed" in capsys.readouterr().out


def test_refresh_lag_without_the_schema_copy_is_a_note_not_a_failure(tmp_path, capsys):
    attempt, _ = make_run(tmp_path, "graph-loader-a")
    assert run_status("--run-dir", str(attempt), "--refresh-lag") == 0
    assert "schema copy cannot be read" in capsys.readouterr().out


def test_refresh_lag_computes_lag_from_watermarks_and_committed_offsets(tmp_path):
    attempt, execution = make_run(tmp_path, "graph-loader-a", mode="stream")
    with_schema_copy(attempt)
    status = execution.snapshot()
    asked: list[str] = []

    class Consumer:
        def __init__(self, config):
            pass

        def list_topics(self, timeout):
            topics = {name: SimpleNamespace(error=None, partitions={0: None, 1: None}) for name in ("person-events", "company-events", "works-at-events", "knows-events")}
            return SimpleNamespace(topics=topics)

        def get_watermark_offsets(self, partition, timeout):
            return (0, 100)

        def close(self):
            pass

    class Admin:
        def __init__(self, config):
            pass

        def list_consumer_group_offsets(self, requests):
            request = requests[0]
            asked.append(request.group_id)
            future = MagicMock()
            future.result.return_value = SimpleNamespace(topic_partitions=[SimpleNamespace(topic=tp.topic, partition=tp.partition, offset=40) for tp in request.topic_partitions])
            return {request.group_id: future}

    with patch("confluent_kafka.Consumer", Consumer), patch("confluent_kafka.admin.AdminClient", Admin):
        lag = status_lag.refresh_lag(attempt, status, bootstrap_servers="x:1", timeout=5)
    assert lag[("node", "Person")] == 120                                               # two partitions, 100 - 40 each
    assert any(group.endswith("-Person") for group in asked)                           # a node group is "<prefix>-<label>"
    assert all(not group.endswith(status["run_id"]) for group in asked)                 # stream groups do not carry the run id
    shown = status_lag.overlay(status, lag, now=1_790_000_000.0)
    person = [t for t in shown["types"] if t["name"] == "Person"][0]
    assert person["lag"] == {"value": 120, "observed_at": model.timestamp(1_790_000_000.0), "source": "status --refresh-lag"}
    assert [t for t in status["types"] if t["name"] == "Person"][0]["lag"] is None     # the input is not modified


def test_a_bulk_edge_group_uses_the_phase_run_id(tmp_path):
    attempt, execution = make_run(tmp_path, "graph-loader-a", mode="bulk", run_id="bulkrun")
    with_schema_copy(attempt)
    execution.phase_started("bulkrun.bulk.0.ab12cd34", "bulk", 0, "WORKS_AT")
    from neo4j_graph_loader.utils.schema_loader import load_schema
    groups = status_lag._groups(load_schema(attempt / "runtime_schema.yaml"), execution.snapshot())
    prefix = load_schema(attempt / "runtime_schema.yaml").loading.consumer_group_id
    assert groups[("edge", "WORKS_AT")][0] == f"{prefix}-WORKS_AT-bulkrun.bulk.0.ab12cd34"
    assert groups[("node", "Person")][0] == f"{prefix}-Person"


# -- the container listing is still there ----------------------------------------------------------------------------------------------------
def test_plain_status_is_the_container_listing_exactly_as_before(caplog):
    service = MagicMock()
    service.list_node_loaders.return_value = [{"name": "n1", "id": "abc", "status": "running", "node_label": "Person"}]
    with patch.object(cli, "_docker_service_or_none", return_value=service), caplog.at_level("INFO"):
        assert cli.main(["status"]) == 0
    assert "Loader n1 (abc): running [Node: Person]" in caplog.text


def test_containers_adds_the_legacy_listing_to_the_run_status(tmp_path, capsys, caplog):
    attempt, _ = make_run(tmp_path, "graph-loader-a", state="failed")
    service = MagicMock()
    service.list_node_loaders.return_value = [{"name": "n1", "id": "abc", "status": "running", "node_label": "Person"}]
    with patch.object(cli, "_docker_service_or_none", return_value=service), caplog.at_level("INFO"):
        assert run_status("--run-dir", str(attempt), "--containers") == 1
    assert "state     failed" in capsys.readouterr().out and "Loader n1 (abc): running" in caplog.text
    caplog.clear()
    with patch.object(cli, "_docker_service_or_none", return_value=service), caplog.at_level("INFO"):
        assert cli.main(["status", "--containers"]) == 0                                    # alone: just the listing
    assert "Loader n1 (abc): running" in caplog.text


def listing_rows():
    return [{"id": "abc", "name": "node-0", "status": "running", "node_label": "Person"}]


@pytest.mark.parametrize("with_locator", [False, True])
def test_status_run_dir_alone_is_e14s_registry_listing_byte_for_byte_even_for_a_run_with_a_locator(tmp_path, capsys, caplog, with_locator):
    """E14 documents `status --runtime process --run-dir DIR` as the registry listing of node loaders; a locator beside
    the registry must not change that command's output."""
    attempt, _ = make_run(tmp_path, "graph-loader-a", state="failed") if with_locator else (tmp_path, None)
    outputs = []
    for _ in range(2):
        caplog.clear()
        with patch("neo4j_graph_loader.orchestrator.process_service.describe_registered", return_value=listing_rows()) as described, \
                patch("neo4j_graph_loader.cli.check_platform" if False else "platform.system", return_value="Linux"), \
                patch("os.pidfd_open", create=True), caplog.at_level("INFO"):
            code = cli.main(["status", "--runtime", "process", "--run-dir", str(attempt)])
        outputs.append((code, [r.getMessage() for r in caplog.records if r.name == "neo4j_graph_loader.cli"], capsys.readouterr().out))
        described.assert_called_once_with(str(attempt))
    assert outputs[0][0] == 0 and outputs[0] == outputs[1]
    assert outputs[0][1][0].startswith("Checking status for config: ") and outputs[0][1][1:] == ["Loader node-0 (abc): running [Node: Person]"]
    assert outputs[0][2] == ""                                                           # nothing on stdout: no run status leaked in
    if with_locator:
        assert (attempt / "status-locator.json").exists()


def test_docker_status_run_dir_alone_still_errors_even_when_the_directory_has_a_locator(tmp_path, caplog):
    attempt, _ = make_run(tmp_path, "graph-loader-a")
    assert cli.main(["status", "--run-dir", str(attempt)]) == 1 and "applies to --runtime process only" in caplog.text
    caplog.clear()
    assert cli.main(["status", "--run-dir", str(attempt), "--run"]) == 0                  # explicit run status reads either runtime's directory
    assert "applies to --runtime process only" not in caplog.text


def test_the_run_option_alone_selects_run_status_and_finds_the_newest_run(tmp_path, capsys):
    older, older_run = make_run(tmp_path, "graph-loader-o", state="succeeded", started_age=400)
    newer, newer_run = make_run(tmp_path, "graph-loader-n", state="failed", started_age=40)
    assert cli.main(["status", "--run-dir", str(tmp_path), "--run"]) == 1
    assert newer_run.execution_id in capsys.readouterr().out
    assert older and newer and older_run
