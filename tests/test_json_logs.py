"""E15.F1.S7: JSON logs. Every entrypoint, in both runtimes, only JSON lines on the log streams; text mode unchanged."""
from __future__ import annotations

import ast
import hashlib
import io
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from neo4j_graph_loader.utils import logging_config as lc

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "neo4j_graph_loader"
BASELINE = json.loads((Path(__file__).resolve().parent / "golden" / "text_logs_1211283.json").read_text())
KEYS = ["ts", "level", "logger", "execution_id", "run_id", "kind", "name", "replica", "incarnation", "stage", "message"]


def run(argv, *, json_logs: bool, python: str | None = None, extra_env: dict | None = None, timeout: float = 120, stdin=None):
    env = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp"), "GRAPH_LOADER_METRICS_ENABLED": "0", **(extra_env or {})}
    if json_logs:
        env["GRAPH_LOADER_LOG_FORMAT"] = "json"
    return subprocess.run([python or sys.executable, *argv], env=env, capture_output=True, text=True, timeout=timeout, input=stdin)


def json_lines(text: str) -> list[dict]:
    lines = [line for line in text.splitlines() if line.strip()]
    parsed = []
    for line in lines:
        value = json.loads(line)                                      # every line must be one JSON object
        assert isinstance(value, dict), line
        parsed.append(value)
    return parsed


def record(message="hello", level=logging.WARNING, name="neo4j_graph_loader.x", args=(), **extra):
    made = logging.LogRecord(name, level, __file__, 1, message, args, None)
    made.created = 1_790_000_000.123456
    for key, value in extra.items():
        setattr(made, key, value)
    return made


@pytest.fixture(autouse=True)
def clean_logging_state():
    yield
    lc.reset_for_tests()


# -- the formatter ----------------------------------------------------------------------------------------------------------------
def test_a_json_line_has_every_documented_key_and_the_identity_from_the_environment(monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_EXECUTION_ID", "e" * 32)
    monkeypatch.setenv("GRAPH_LOADER_INCARNATION", "i" * 32)
    monkeypatch.setenv("GRAPH_LOADER_RUN_ID", "run-1")
    lc.set_log_context(kind="node", name="Person", replica=2)
    line = lc.JsonFormatter().format(record("plain message"))
    entry = json.loads(line)
    assert list(entry) == KEYS and "\n" not in line
    assert entry["level"] == "warning" and entry["logger"] == "neo4j_graph_loader.x" and entry["message"] == "plain message"
    assert (entry["execution_id"], entry["incarnation"], entry["run_id"]) == ("e" * 32, "i" * 32, "run-1")
    assert (entry["kind"], entry["name"], entry["replica"], entry["stage"]) == ("node", "Person", 2, None)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", entry["ts"])


def test_stage_and_fields_are_attached_at_emission_and_message_text_is_never_parsed():
    structured_line = json.loads(lc.JsonFormatter().format(record(
        "stage=node-write label=Person records=5", **lc.structured("node-write", label="Person", records=5, write_ms=1.5))))
    assert structured_line["stage"] == "node-write" and structured_line["label"] == "Person" and structured_line["records"] == 5
    unstructured = json.loads(lc.JsonFormatter().format(record("stage=node-write label=Person records=5")))
    assert unstructured["stage"] is None and "label" not in unstructured and "records" not in unstructured
    assert unstructured["message"] == "stage=node-write label=Person records=5"
    clash = json.loads(lc.JsonFormatter().format(record("x", **lc.structured("s", level="CLASH", message="CLASH"))))
    assert clash["level"] == "warning" and clash["message"] == "x"                  # fields can never overwrite the envelope


def test_stage_logger_gives_every_line_of_a_module_one_stage():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(lc.JsonFormatter())
    base = logging.getLogger("test.stage.logger")
    base.addHandler(handler)
    base.setLevel(logging.INFO)
    base.propagate = False
    try:
        adapter = lc.StageLogger(base, "retry-consumer")
        adapter.info("one %s", "x")
        adapter.error("two", extra=lc.structured("other", reason="r"))
    finally:
        base.removeHandler(handler)
    first, second = json_lines(stream.getvalue())
    assert (first["stage"], first["message"]) == ("retry-consumer", "one x") and (second["stage"], second["reason"]) == ("other", "r")


def test_credentials_are_redacted_from_the_message_the_fields_and_the_traceback():
    try:
        raise RuntimeError("connect failed postgresql://svc:S3cr3tPw@db:5432/x password=S3cr3tPw")
    except RuntimeError:
        made = record("login bolt://neo4j:S3cr3tPw@host token=abc123", exc_info=sys.exc_info(),
                      **lc.structured("runtime", url="redis://u:S3cr3tPw@h/0", nested={"dsn": "password=S3cr3tPw", "items": ["x://a:S3cr3tPw@h"]}))
    line = lc.JsonFormatter().format(made)
    assert "S3cr3tPw" not in line and "abc123" not in line
    assert json.loads(line)["message"] == "login bolt://neo4j:***@host token=***"
    assert "exception" in json.loads(line) and "RuntimeError" in json.loads(line)["exception"]


# -- text mode is unchanged -------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("level, name, message, args, expected", [
    (logging.INFO, "a", "stage=runtime directory=%s", ("/tmp/x",), "INFO: stage=runtime directory=/tmp/x"),
    (logging.WARNING, "b.c", "plain", (), "WARNING: plain"),
    (logging.ERROR, "d", "stage=launch reason=%s", ("bad",), "ERROR: stage=launch reason=bad"),
    (logging.CRITICAL, "e", "50% done", (), "CRITICAL: 50% done"),
])
def test_text_records_format_byte_for_byte_as_before_even_with_structured_fields(level, name, message, args, expected):
    formatter = logging.Formatter(lc.TEXT_FORMAT)
    assert formatter.format(record(message, level=level, name=name, args=args)) == expected
    assert formatter.format(record(message, level=level, name=name, args=args, **lc.structured("runtime", a=1))) == expected


def test_text_mode_configuration_is_exactly_basic_config_and_installs_nothing_else():
    code = ("import logging, sys, argparse, threading\n"
            "from neo4j_graph_loader.utils.logging_config import configure_logging\n"
            "hook, error = sys.excepthook, argparse.ArgumentParser.error\n"
            "print(configure_logging(kind='node'))\n"
            "root = logging.getLogger()\n"
            "print(len(root.handlers), root.handlers[0].formatter._fmt, root.level, sys.excepthook is hook, argparse.ArgumentParser.error is error)\n"
            "import confluent_kafka; print(confluent_kafka.Consumer.__module__)\n"
            "logging.getLogger('x').warning('hello %s', 'world')\n")
    result = run(["-c", code], json_logs=False)
    assert result.stdout.splitlines() == ["text", "1 %(levelname)s: %(message)s 20 True True", "cimpl"]
    assert result.stderr == "WARNING: hello world\n"


@pytest.mark.parametrize("name", sorted(BASELINE))
def test_text_mode_output_equals_the_recorded_output_of_the_previous_source(name):
    expected = BASELINE[name]
    result = run(["-m", *expected["argv"]], json_logs=False)
    assert result.returncode == expected["returncode"] and result.stderr == expected["stderr"]
    assert hashlib.sha256(result.stdout.encode()).hexdigest() == expected["stdout_sha256"]


# -- JSON mode, per entrypoint -----------------------------------------------------------------------------------------------------
def test_the_log_format_is_read_before_parsing_from_the_environment_or_anywhere_on_the_command_line():
    assert lc.requested_format(["start", "--mode", "bulk"]) == "text"
    assert lc.requested_format(["--log-format", "json", "start"]) == "json"
    assert lc.requested_format(["start", "--mode", "bulk", "--log-format=json"]) == "json"
    assert lc.requested_format(["start", "--log-format", "text"]) == "text"
    assert lc.requested_format(["--log-format", "yaml"]) == "text"                 # unknown values fall back to text
    assert lc.strip_log_format(["start", "--log-format", "json", "--mode", "bulk", "--log-format=json"]) == ["start", "--mode", "bulk"]


@pytest.mark.parametrize("argv, code", [
    (["-m", "neo4j_graph_loader.cli", "--log-format", "json", "start", "--mode", "stream", "--run-id", "bad id!"], 1),
    (["-m", "neo4j_graph_loader.cli", "start", "--mode", "bulk", "--bulk-timeout-seconds", "0", "--log-format", "json"], 1),
    (["-m", "neo4j_graph_loader.cli", "--log-format=json", "validate-schema", "--config", "/nonexistent/schema.yaml"], 1),
    (["-m", "neo4j_graph_loader.loader.node_loader", "--node-label", "Person", "--max-messages", "0"], 1),
])
def test_runtime_errors_of_the_cli_and_the_node_loader_are_one_json_line_each(argv, code):
    result = run(argv, json_logs="--log-format" not in " ".join(argv) and "node_loader" in " ".join(argv))
    assert result.returncode == code
    entries = json_lines(result.stderr)
    assert entries and all(list(entry) == KEYS for entry in entries) and entries[-1]["level"] in ("error", "critical", "warning")
    assert result.stdout == ""


def test_the_cli_marks_its_own_identity_and_the_documented_stage_fields():
    result = run(["-m", "neo4j_graph_loader.cli", "--log-format", "json", "start", "--mode", "stream", "--run-id", "bad id!"], json_logs=False)
    (entry,) = json_lines(result.stderr)
    assert (entry["kind"], entry["level"]) == ("cli", "error") and entry["logger"] in ("__main__", "neo4j_graph_loader.cli")
    assert entry["message"].startswith("stage=launch reason=run id must be")


@pytest.mark.parametrize("module, extra", [
    ("neo4j_graph_loader.cli", []),
    ("neo4j_graph_loader.loader.node_loader", []), ("neo4j_graph_loader.loader.edge_loader", []),
    ("neo4j_graph_loader.loader.planned_writer", []), ("neo4j_graph_loader.loader.retry_consumer", []),
    ("neo4j_graph_loader.orchestrator.planned_dispatcher", []), ("neo4j_graph_loader.orchestrator.lease_server", ["--port", "notanumber"]),
    ("neo4j_graph_loader.orchestrator.clock_runner", []),
])
def test_an_argparse_error_is_one_json_line_in_every_entrypoint(module, extra):
    result = run(["-m", module, *extra, "--no-such-option"], json_logs=True)
    assert result.returncode == 2
    (entry,) = json_lines(result.stderr)
    assert entry["logger"] == "argparse" and entry["level"] == "error" and entry["stage"] == "argument-error"
    assert "error:" in entry["message"]
    assert entry["kind"] is not None
    assert result.stdout == ""
    text = run(["-m", module, *extra, "--no-such-option"], json_logs=False)
    assert text.returncode == 2 and text.stderr.startswith("usage:")                 # text mode keeps argparse's own message


def test_command_payloads_are_identical_in_both_formats(tmp_path):
    for argv in (["-m", "neo4j_graph_loader.cli", "schema-example"], ["-m", "neo4j_graph_loader.cli", "validate-schema"],
                 ["-m", "neo4j_graph_loader.cli", "--version"], ["-m", "neo4j_graph_loader.cli", "start", "--help"]):
        plain, structured = run(argv, json_logs=False), run(argv, json_logs=True)
        assert (structured.returncode, structured.stdout) == (plain.returncode, plain.stdout), argv
        assert json_lines(structured.stderr) == [] and plain.stderr == ""
    status = tmp_path / "graph-loader-x"
    status.mkdir()
    for flag in (False, True):                                                            # `status --json` is a payload too
        result = run(["-m", "neo4j_graph_loader.cli", "status", "--json", "--run-dir", str(status)], json_logs=flag)
        assert result.returncode == 2 and result.stdout == ""
        if flag:
            assert json_lines(result.stderr)[-1]["stage"] == "status" or json_lines(result.stderr)[-1]["message"].startswith("stage=status")


def test_an_uncaught_exception_in_the_main_thread_and_in_a_thread_is_a_json_line_in_json_mode_only():
    code = ("import threading, time\n"
            "from neo4j_graph_loader.utils.logging_config import configure_logging\n"
            "configure_logging(kind='node', name='Person', replica=1)\n"
            "t = threading.Thread(target=lambda: 1 / 0, name='worker-7'); t.start(); t.join()\n"
            "raise RuntimeError('boom password=hunter2')\n")
    result = run(["-c", code], json_logs=True)
    assert result.returncode == 1
    thread, main = json_lines(result.stderr)
    assert thread["level"] == "critical" and thread["stage"] == "uncaught-exception" and thread["thread"] == "worker-7"
    assert "ZeroDivisionError" in thread["exception"] and thread["name"] == "Person" and thread["replica"] == 1
    assert main["exception_type"] == "RuntimeError" and "hunter2" not in result.stderr and "password=***" in main["exception"]
    plain = run(["-c", code], json_logs=False)
    assert plain.stderr.startswith("Traceback") or "Traceback" in plain.stderr


def test_the_early_sigterm_handler_of_the_node_loader_writes_a_json_line_or_the_old_bytes():
    code = ("import os, sys\n"
            "import neo4j_graph_loader.loader.node_loader as n\n"
            "n._exit_stopped_while_importing(None, None)\n")
    plain = run(["-c", code], json_logs=False)
    assert plain.returncode == 0 and plain.stderr == ("WARNING: stage=node-startup-stopped reason=stop requested while the loader was still "
                                                       "importing; nothing was read, written or committed\n")
    structured = run(["-c", code], json_logs=True, extra_env={"GRAPH_LOADER_EXECUTION_ID": "e" * 32, "GRAPH_LOADER_INCARNATION": "i" * 32})
    assert structured.returncode == 0
    (entry,) = json_lines(structured.stderr)
    assert list(entry) == KEYS and entry["stage"] == "node-startup-stopped" and entry["level"] == "warning" and entry["kind"] == "node"
    assert (entry["execution_id"], entry["incarnation"]) == ("e" * 32, "i" * 32)
    assert entry["message"].startswith("stage=node-startup-stopped reason=stop requested while the loader was still importing")


def test_a_real_sigterm_during_the_import_window_leaves_only_json_on_stderr():
    env = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp"), "GRAPH_LOADER_LOG_FORMAT": "json", "GRAPH_LOADER_METRICS_ENABLED": "0"}
    child = subprocess.Popen([sys.executable, "-m", "neo4j_graph_loader.loader.node_loader", "--node-label", "Person", "--topic", "t"],
                             env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(0.15)
    child.send_signal(signal.SIGTERM)
    stdout, stderr = child.communicate(timeout=60)
    for entry in json_lines(stderr):                                                 # whichever path ran, every line is JSON
        assert list(entry)[:3] == KEYS[:3]
    assert stdout == ""


# -- the structured stages: statically at every emission site, dynamically for a few ------------------------------------------------------
DOCUMENTED = ("runtime", "metrics-ready", "node-label-ready", "node-write", "edge-write", "edge-plan-rejections", "bulk-substage", "outcome",
              "write-outage")


def _log_calls(path: Path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("debug", "info", "warning", "error", "critical", "exception"):
            if node.args and isinstance(node.args[0], (ast.Constant, ast.JoinedStr, ast.BinOp)):
                text = ast.get_source_segment(path.read_text(), node.args[0]) or ""
                yield node, text


def test_every_log_call_of_a_documented_stage_carries_structured_fields():
    missing = []
    for path in SRC.rglob("*.py"):
        if path.name in ("logging_config.py",):
            continue
        for node, text in _log_calls(path):
            match = re.search(r"stage=([a-z][a-z-]*)", text)
            if not match:
                continue
            stage = match.group(1)
            documented = any(stage == name or stage.startswith(name + "-") and name in ("bulk-substage", "write-outage") for name in DOCUMENTED)
            if stage == "runtime" and "stage=runtime-child" in text:
                documented = False
            if documented and not any(kw.arg == "extra" for kw in node.keywords):
                missing.append(f"{path.relative_to(ROOT)}:{node.lineno} stage={stage}")
    assert not missing, "documented stages without structured fields:\n" + "\n".join(missing)


def test_the_retry_consumer_module_logs_every_line_under_its_stage():
    from neo4j_graph_loader.loader import retry_consumer
    assert isinstance(retry_consumer.logger, lc.StageLogger) and retry_consumer.logger.extra == {}


def test_documented_stages_emit_their_fields_when_they_run(tmp_path, monkeypatch):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(lc.JsonFormatter())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    try:
        from argparse import Namespace
        from prometheus_client import CollectorRegistry
        from neo4j_graph_loader._runtime import prepare_run
        from neo4j_graph_loader.utils import metrics

        schema = tmp_path / "schema.yaml"
        schema.write_text("x: 1\n")
        prepare_run(Namespace(run_dir=str(tmp_path / "runs"), plan_dir=None, plan_spill_dir=None, edge_plan=None), str(schema))
        monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
        server = metrics.start_metrics_server(metrics.GraphLoaderMetrics(registry=CollectorRegistry()), kind="node", name="Person", addr="127.0.0.1", port=0,
                                              allow_ephemeral_fallback=True)
        metrics.stop_metrics_server(server)
    finally:
        root.removeHandler(handler)
    by_stage = {entry["stage"]: entry for entry in json_lines(stream.getvalue()) if entry["stage"]}
    assert by_stage["runtime"]["directory"].startswith(str(tmp_path / "runs"))
    assert by_stage["metrics-ready"]["entity_kind"] == "node" and by_stage["metrics-ready"]["entity"] == "Person" and by_stage["metrics-ready"]["port"] > 0


def test_write_outage_lines_carry_their_stage_and_fields():
    from neo4j_graph_loader.loader import write_outage

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(lc.JsonFormatter())
    log = logging.getLogger("test.write-outage")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False
    try:
        source = (SRC / "loader" / "write_outage.py").read_text()
        assert source.count("extra=structured(") >= 6                                  # every outage line
        assert write_outage.structured is lc.structured
    finally:
        log.removeHandler(handler)


# -- librdkafka: every client factory, both confluent-kafka versions ------------------------------------------------------------------------
KAFKA_PROBE = '''
import json, logging, sys, time
from neo4j_graph_loader.utils.logging_config import configure_logging
configure_logging(kind="probe")
import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
conf = {"bootstrap.servers": "127.0.0.1:1", "socket.timeout.ms": 1000}
clients = {}
clients["consumer-never-subscribed"] = Consumer({**conf, "group.id": "g1"})
clients["producer-no-callbacks"] = Producer(conf)
clients["admin"] = AdminClient(conf)
owned = Consumer({**conf, "group.id": "g2"}); owned.subscribe(["t"]); clients["consumer-subscribed"] = owned
callbacks = Producer(conf); callbacks.produce("t", b"x", on_delivery=lambda e, m: None); clients["producer-with-callbacks"] = callbacks
deadline = time.monotonic() + 8
while time.monotonic() < deadline:
    owned.poll(0.1)                 # what the owner's loop does for the two clients drain thread leaves alone
    callbacks.poll(0.1)
    time.sleep(0.2)
    if all(True for _ in clients): pass
print(json.dumps({"version": confluent_kafka.version()[0], "types": {k: type(v).__mro__[0].__module__ for k, v in clients.items()}}), file=sys.stdout)
'''


def _kafka_entries(python: str):
    result = run(["-c", KAFKA_PROBE], json_logs=True, python=python, timeout=90, extra_env={"PYTHONPATH": str(ROOT / "src")})
    assert result.returncode == 0, result.stderr[-500:]
    info = json.loads(result.stdout.strip().splitlines()[-1])
    entries = json_lines(result.stderr)
    return info, entries, result


@pytest.mark.parametrize("python_env", ["", "E15_KAFKA25_PYTHON"])
def test_librdkafka_logs_of_every_client_kind_come_out_as_json_lines(python_env):
    python = os.environ.get(python_env) if python_env else sys.executable
    if python_env and not python:
        pytest.skip("E15_KAFKA25_PYTHON (a Python with confluent-kafka 2.5.x and this package) is not set")
    info, entries, result = _kafka_entries(python)
    if python_env:
        assert info["version"].startswith("2.5")
    kafka = [entry for entry in entries if entry["logger"] == "librdkafka"]
    assert kafka and all(list(entry) == KEYS for entry in kafka)
    seen = {re.search(r"rdkafka#(consumer|producer)-\d+", entry["message"]).group(0) for entry in kafka if re.search(r"rdkafka#(consumer|producer)-\d+", entry["message"])}
    consumers = {name for name in seen if name.startswith("rdkafka#consumer")}
    producers = {name for name in seen if name.startswith("rdkafka#producer")}
    assert len(consumers) >= 2 and len(producers) >= 2, seen                       # the admin client and the callback producer are producers
    assert "%3|" not in result.stderr and "%4|" not in result.stderr                # no raw librdkafka lines: all of it went through the logger
    assert all(entry["kind"] == "probe" for entry in kafka)
    assert info["types"]["consumer-never-subscribed"] == "neo4j_graph_loader.utils.kafka_logging"


def test_text_mode_leaves_the_kafka_clients_alone():
    result = run(["-c", "import neo4j_graph_loader, confluent_kafka, confluent_kafka.admin as a\n"
                        "print(confluent_kafka.Consumer.__module__, confluent_kafka.Producer.__module__, a.AdminClient.__module__)"], json_logs=False)
    assert result.stdout.split() == ["cimpl", "cimpl", "confluent_kafka.admin"]


FACTORY_SITES = {
    # module-level bindings: replaced because the package installs the wrappers before any submodule is imported
    "neo4j_graph_loader.loader.node_loader": ("Consumer", "Producer"), "neo4j_graph_loader.loader.edge_loader": ("Consumer", "Producer"),
    "neo4j_graph_loader.orchestrator.bulk_monitor": ("Consumer", "AdminClient"),
    "neo4j_graph_loader.orchestrator.relationship_bulk_monitor": ("Consumer", "AdminClient"),
    "neo4j_graph_loader.orchestrator.clock_runner": ("Producer",),
}


def test_every_module_level_client_factory_is_a_logging_wrapper_once_an_entrypoint_configures_json_logging():
    code = ("import importlib, json, sys\n"
            f"sites = {json.dumps({k: list(v) for k, v in FACTORY_SITES.items()})}\n"
            "for module in sites:\n"
            "    importlib.import_module(module)          # imported first, as an entrypoint does (they bind the client classes by name)\n"
            "from neo4j_graph_loader.utils.logging_config import configure_logging\n"
            "configure_logging(kind='probe', argv=sys.argv[1:])\n"
            "out = {module: {n: (getattr(sys.modules[module], n).__init__ if n == 'AdminClient' else getattr(sys.modules[module], n)).__module__ for n in names}\n"
            "       for module, names in sites.items()}\n"
            "import confluent_kafka, confluent_kafka.admin as admin\n"
            "late = {n: (getattr(m, n).__init__ if n == 'AdminClient' else getattr(m, n)).__module__ for n, m in (('Consumer', confluent_kafka), ('Producer', confluent_kafka), ('AdminClient', admin))}\n"
            "print(json.dumps([out, late]))\n")
    result = run(["-c", code], json_logs=True)
    bound, late = json.loads(result.stdout)
    assert all(origin == "neo4j_graph_loader.utils.kafka_logging" for names in bound.values() for origin in names.values()), bound
    assert set(late.values()) == {"neo4j_graph_loader.utils.kafka_logging"}
    argv_form = run(["-c", code, "--log-format", "json"], json_logs=False)                  # `--log-format json` anywhere in argv works too
    assert json.loads(argv_form.stdout)[1] == late


def test_importing_the_package_and_its_library_api_never_touches_confluent_kafka_even_with_json_logging_set():
    """Patching a third-party module is process-global: only this package's own entrypoint processes may do it (never an
    import, never the supported library API ``neo4j_graph_loader.schema`` / ``planning``)."""
    code = ("import confluent_kafka, confluent_kafka.admin as admin\n"
            "originals = (confluent_kafka.Consumer, confluent_kafka.Producer, admin.AdminClient.__init__)\n"
            "import neo4j_graph_loader, neo4j_graph_loader.schema, neo4j_graph_loader.planning\n"
            "from neo4j_graph_loader.schema import load_schema\n"
            "from neo4j_graph_loader.planning import PlanSettings, plan_and_publish_edges, producer_edge_planner\n"
            "import neo4j_graph_loader.utils.logging_config, neo4j_graph_loader.utils.kafka_logging\n"
            "now = (confluent_kafka.Consumer, confluent_kafka.Producer, admin.AdminClient.__init__)\n"
            "print(all(a is b for a, b in zip(originals, now)), confluent_kafka.Consumer.__module__, confluent_kafka.Producer.__module__,\n"
            "      admin.AdminClient.__init__.__module__)\n")
    result = run(["-c", code], json_logs=True)
    assert result.returncode == 0, result.stderr[-400:]
    assert result.stdout.split() == ["True", "cimpl", "cimpl", "confluent_kafka.admin"]
    argv_form = run(["-c", code, "--log-format", "json"], json_logs=False)
    assert argv_form.stdout.split() == ["True", "cimpl", "cimpl", "confluent_kafka.admin"]


def test_clients_that_the_owner_polls_are_never_polled_by_the_drain_thread():
    code = ("import time\n"
            "from neo4j_graph_loader.utils import kafka_logging as k\n"
            "import confluent_kafka\n"
            "k.install()\n"
            "polled = []\n"
            "class Probe:\n"
            "    def __init__(self, owner): self._gl_owner_polls = owner\n"
            "    def poll(self, t): polled.append(self._gl_owner_polls)\n"
            "mine, theirs = Probe(False), Probe(True)\n"
            "k._register(mine); k._register(theirs)\n"
            "time.sleep(1.3)\n"
            "print(polled.count(False) > 0, polled.count(True))\n")
    assert run(["-c", code], json_logs=True).stdout.split() == ["True", "0"]


# -- the children of the process runtime --------------------------------------------------------------------------------------------------------
def test_the_relay_passes_json_lines_through_unchanged_and_wraps_stray_output(monkeypatch):
    from neo4j_graph_loader.orchestrator.process_runtime import LogRelay

    own = json.dumps({"ts": "2026-11-03T00:00:00.000Z", "level": "info", "logger": "x", "message": "m", "execution_id": "e"}) + "\n"
    assert LogRelay._format("node-Person-0", own.encode()) == "[node-Person-0] " + own            # text mode: the old prefix
    monkeypatch.setenv("GRAPH_LOADER_LOG_FORMAT", "json")
    assert LogRelay._format("node-Person-0", own.encode()) == own                                  # JSON mode: unchanged
    wrapped = json.loads(LogRelay._format("node-Person-0", b"a stray print\n"))
    assert wrapped["message"] == "a stray print" and wrapped["child"] == "node-Person-0" and wrapped["stage"] == "child-output"
    assert json.loads(LogRelay._format("n", b'{"not": "closed"\n'))["message"] == '{"not": "closed"'


def test_process_runtime_children_log_json_with_their_identity_and_the_relay_keeps_stdout_json_only(tmp_path, monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)      # (the suite default is "0"; the child should serve metrics)
    from neo4j_graph_loader import _runtime
    from neo4j_graph_loader.orchestrator.process_runtime import LogRelay
    from neo4j_graph_loader.orchestrator.process_service import ProcessService
    from neo4j_graph_loader.status import StatusTracker

    base = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (base / name).mkdir(parents=True)
    schema = base / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    paths = _runtime.RunPaths(base, schema, base / "rejections", base / "plans", base / "spill", base / "timings")
    previous = os.environ.get("GRAPH_LOADER_LOG_FORMAT")
    os.environ["GRAPH_LOADER_LOG_FORMAT"] = "json"
    relayed = io.StringIO()
    try:
        from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
        service = ProcessService(paths, lifecycle=ProcessRuntime(base, relay=LogRelay(relayed), ready_timeout=60))
        execution = StatusTracker(mode="stream", run_id="j1", runtime="process")
        service.status = execution
        handle = service.run_bucket_lease_server(run_id="j1", bucket_count=16, network="n", keyed_labels=[])
        time.sleep(0.5)
        handle.stop(timeout=10)
        time.sleep(0.5)
        service.close()
    finally:
        if previous is None:
            os.environ.pop("GRAPH_LOADER_LOG_FORMAT", None)
        else:
            os.environ["GRAPH_LOADER_LOG_FORMAT"] = previous
    log_file = next((base / "logs").glob("*.log"))
    entries = json_lines(log_file.read_text())
    assert entries and all(list(entry)[:len(KEYS)] == KEYS for entry in entries)
    role = execution.snapshot()["roles"][0]
    assert {entry["execution_id"] for entry in entries} == {execution.execution_id} and {entry["incarnation"] for entry in entries} == {role["incarnation"]}
    assert {entry["kind"] for entry in entries} == {"lease-server"}
    ready = [entry for entry in entries if entry["stage"] == "metrics-ready"]
    assert ready and ready[0]["entity_kind"] == "lease-server"
    if relayed.getvalue():
        assert json_lines(relayed.getvalue())                                                      # relayed lines are JSON too


def test_the_child_wrapper_reports_its_own_errors_as_json_in_json_mode():
    plain = run(["-m", "neo4j_graph_loader._child", "os"], json_logs=False)
    assert plain.returncode == 2 and plain.stderr == "refusing to run a module outside neo4j_graph_loader: os\n"
    structured = run(["-m", "neo4j_graph_loader._child", "os"], json_logs=True)
    (entry,) = json_lines(structured.stderr)
    assert structured.returncode == 2 and entry["stage"] == "runtime-child" and entry["level"] == "error" and "refusing to run a module" in entry["message"]


# -- redaction in both formats ---------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("json_logs", [False, True])
def test_no_credential_reaches_stdout_or_stderr_in_either_format(json_logs):
    password = "Zq7-secret-pw"
    env = {"GRAPH_LOADER_STATUS_DB_URL": f"postgresql+psycopg://svc:{password}@127.0.0.1:1/runs"}
    for argv in (["status-store", "migrate"], ["status-store", "prune", "--older-than", "1d"], ["status", "--list", "--status-store", "sql"]):
        result = run(["-m", "neo4j_graph_loader.cli", *argv], json_logs=json_logs, extra_env=env)
        assert password not in result.stdout + result.stderr and "svc:" not in result.stdout + result.stderr, argv
        if json_logs:
            json_lines(result.stderr)


EXPECTED_FACTORY_CALLS = {
    "_docker_preflight.py": 1, "cli.py": 10, "loader/dead_letter.py": 3, "loader/dead_letter_replay.py": 2, "loader/edge_loader.py": 3,
    "loader/edge_planner.py": 7, "loader/node_loader.py": 2, "loader/parked_pair_index.py": 3, "loader/planned_writer.py": 1,
    "loader/retry_consumer.py": 2, "orchestrator/auto_settings.py": 1, "orchestrator/bulk_monitor.py": 3, "orchestrator/clock_runner.py": 1,
    "orchestrator/planned_edges.py": 3, "orchestrator/relationship_bulk_monitor.py": 4, "status/lag.py": 2,
}


def test_every_kafka_client_factory_has_a_documented_log_drain_disposition():
    """Drift guard: a new Consumer / Producer / AdminClient construction site must be added to the table in
    ``utils/kafka_logging.py`` (which says how its log callbacks are drained) and to the counts here."""
    names = {"Consumer", "Producer", "AdminClient", "consumer_factory", "producer_factory", "admin_factory", "producer_class"}
    found: dict[str, int] = {}
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text())
        count = sum(1 for node in ast.walk(tree) if isinstance(node, ast.Call)
                    and (getattr(node.func, "id", None) in names or getattr(node.func, "attr", None) in names))
        if count:
            found[str(path.relative_to(SRC))] = count
    assert found == EXPECTED_FACTORY_CALLS
    documentation = (SRC / "utils" / "kafka_logging.py").read_text()
    for relative in EXPECTED_FACTORY_CALLS:
        assert Path(relative).name in documentation, f"{relative} has no drain disposition in utils/kafka_logging.py"
