"""Behavioral checks for dependency generation and deployment version reporting."""
from pathlib import Path
import os
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SYNC = ROOT / "scripts/sync_requirements.py"


def _sync(project, *args):
    return subprocess.run([sys.executable, str(SYNC), "--project", str(project), *args],
                          cwd=project.parent, capture_output=True, text=True, timeout=30)


def test_requirement_check_detects_drift_without_rewriting_the_file(tmp_path):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\ndependencies = ["example>=1,<3"]\n')
    assert _sync(project).returncode == 0
    output = tmp_path / "requirements.txt"
    original = output.read_bytes()
    assert _sync(project, "--check").returncode == 0
    project.write_text('[project]\ndependencies = ["example>=2,<3"]\n')
    result = _sync(project, "--check")
    assert result.returncode == 1 and "differs" in result.stderr
    assert output.read_bytes() == original
    assert _sync(project).returncode == 0
    assert "example>=2,<3" in output.read_text()
    assert _sync(project, "--check").returncode == 0


def test_requirement_check_fails_when_generated_input_is_missing(tmp_path):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\ndependencies = ["example>=1"]\n')
    assert _sync(project, "--check").returncode == 1
    assert not (tmp_path / "requirements.txt").exists()


def test_repository_runtime_requirements_are_current():
    assert _sync(ROOT / "pyproject.toml", "--check").returncode == 0


@pytest.mark.parametrize("override", [None, "", "deployment-override"])
def test_checkout_metrics_report_package_version_or_explicit_override(override):
    """Exercise actual process-info labels in a fresh editable-development child."""
    environment = {key: value for key, value in os.environ.items() if key not in {
        "GRAPH_LOADER_VERSION", "GRAPH_LOADER_TIMING_DIR", "GRAPH_LOADER_METRICS_SNAPSHOT_PATH", "PYTHONPATH",
    }}
    if override is not None:
        environment["GRAPH_LOADER_VERSION"] = override
    environment["GRAPH_LOADER_METRICS_ENABLED"] = "1"
    result = subprocess.run([sys.executable, "-c", """
import logging, os, socket, threading
before = logging.getLogger().level, list(logging.getLogger().handlers)
def forbidden(*args, **kwargs): raise AssertionError('unexpected import side effect')
socket.socket.connect = forbidden
socket.socket.connect_ex = forbidden
socket.socket.bind = forbidden
threading.Thread.start = forbidden
from neo4j_graph_loader.utils import metrics
from neo4j_graph_loader._version import __version__
assert before == (logging.getLogger().level, logging.getLogger().handlers)
assert metrics._process_metrics is None
expected = os.environ.get('GRAPH_LOADER_VERSION', __version__)
assert metrics.PROCESS_VERSION == expected
metrics.start_http_server = lambda *a, **k: (object(), object())
facade = metrics.GraphLoaderMetrics(registry=metrics.CollectorRegistry())
assert metrics.start_metrics_server(facade, kind='cli', name='-') is not None
assert facade.registry.get_sample_value('graph_loader_process_info',
    {'kind': 'cli', 'name': '-', 'version': expected}) == 1
"""], cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
