"""Installed CLI entry parity with service creation forbidden in each child."""
import importlib.metadata
import os
from pathlib import Path
import subprocess
import sys

import pytest

from neo4j_graph_loader import __version__


@pytest.mark.parametrize("arguments,code", [(["--help"], 0), (["--version"], 0), ([], 2),
    (["unknown-command"], 2), (["start"], 2), (["start", "--help"], 0)])
def test_entry_parity_without_services(tmp_path, arguments, code):
    (tmp_path / ".env").write_text("GRAPH_LOADER_ENTRY_SENTINEL=private\n")
    sentinel = """
import os, runpy, socket, subprocess, sys, threading
before = dict(os.environ)
def forbidden(*a, **k): raise AssertionError('service started')
socket.socket.connect = socket.socket.connect_ex = socket.socket.bind = forbidden
subprocess.Popen = threading.Thread.start = forbidden
sys.argv = ['graph-loader', *ARGS]
try:
    runpy.run_module(MODULE, run_name='__main__')
finally:
    assert dict(os.environ) == before
"""
    env = {k:v for k,v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME"}}
    executable = Path(sys.executable).parent / "graph-loader"
    results = [subprocess.run([str(executable), *arguments], cwd=tmp_path, env=env,
                             capture_output=True, text=True, timeout=20)]
    for module in ("neo4j_graph_loader", "neo4j_graph_loader.cli"):
        script = sentinel.replace("ARGS", repr(arguments)).replace("MODULE", repr(module))
        results.append(subprocess.run([sys.executable, "-I", "-c", script], cwd=tmp_path,
                                     env=env, capture_output=True, text=True, timeout=20))
    assert all(r.returncode == code for r in results), [(r.returncode,r.stdout,r.stderr) for r in results]
    assert len({(r.stdout, r.stderr) for r in results}) == 1
    if arguments == ["--version"]:
        assert results[0].stdout == f"graph-loader {__version__}\n"
        assert __version__ == importlib.metadata.version("neo4j-graph-loader")


def test_sole_installed_entry_point():
    entries = importlib.metadata.distribution("neo4j-graph-loader").entry_points
    assert [(e.group,e.name,e.value) for e in entries] == [
        ("console_scripts", "graph-loader", "neo4j_graph_loader.cli:main")]
