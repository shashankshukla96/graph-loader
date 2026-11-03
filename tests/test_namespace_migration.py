"""Exercise namespace identity in fresh processes, including spawn serialization."""
import os
import subprocess
import sys

import pytest


def _isolated(code, cwd):
    environment = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME", "GRAPH_LOADER_VERSION"}}
    result = subprocess.run([sys.executable, "-I", "-c", code], cwd=cwd, env=environment,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_migrated_runtime_imports_without_checkout_path(tmp_path):
    _isolated("""
import importlib, importlib.metadata, importlib.util, pkgutil
import neo4j_graph_loader as package
expected = {'models.schema', 'loader.edge_planner', 'utils.metrics', 'cli',
            'orchestrator.docker_service', 'orchestrator.lease_server'}
modules = {m.name for m in pkgutil.walk_packages(package.__path__, package.__name__ + '.')}
assert {package.__name__ + '.' + m for m in expected} <= modules, modules
# E15 (decision D05 as amended): the one module that needs an optional extra at import time, listed explicitly. Without the extra
# it must fail with a ModuleNotFoundError for exactly that dependency; every other module imports on a base install.
optional = {'neo4j_graph_loader.status.sql_store': 'sqlalchemy'}
for name in sorted(modules):
    if name in optional and importlib.util.find_spec(optional[name]) is None:
        try:
            importlib.import_module(name)
        except ModuleNotFoundError as error:
            assert error.name == optional[name], (name, error.name)
        else:
            raise AssertionError(name + ' imported without its optional dependency')
        continue
    module = importlib.import_module(name)
    assert module.__file__, name
from neo4j_graph_loader.utils import metrics
assert metrics.PROCESS_VERSION == importlib.metadata.version('neo4j-graph-loader') == package.__version__
assert not any(n == 'src' or n.startswith('src.') for n in __import__('sys').modules)
""", tmp_path)


@pytest.mark.parametrize("old", ["src.cli", "src.loader.edge_planner", "src.models.schema",
                                 "src.utils.metrics", "src.orchestrator.docker_service"])
def test_old_application_namespace_is_absent(old, tmp_path):
    _isolated(f"""
import importlib
try:
    importlib.import_module({old!r})
except ModuleNotFoundError as error:
    assert error.name == 'src' or {old!r}.startswith(error.name + '.'), error
else:
    raise AssertionError('old application namespace unexpectedly imported')
""", tmp_path)


def test_spawn_unpickles_the_canonical_planner_class(tmp_path):
    script = tmp_path / "spawn_probe.py"
    script.write_text('''
import multiprocessing as mp
from neo4j_graph_loader.loader.edge_planner import PlanSettings

def inspect(settings, result):
    assert type(settings) is PlanSettings
    result.put((type(settings).__module__, settings.topic, settings.buckets))

if __name__ == '__main__':
    context = mp.get_context('spawn')
    result = context.Queue()
    process = context.Process(target=inspect, args=(PlanSettings(topic='migration', buckets=8), result))
    process.start()
    try:
        assert result.get(timeout=20) == ('neo4j_graph_loader.loader.edge_planner', 'migration', 8)
        process.join(20)
        assert process.exitcode == 0, process.exitcode
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        result.close()
        result.join_thread()
''')
    result = subprocess.run([sys.executable, "-I", str(script)], cwd=tmp_path,
                            env={k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME", "GRAPH_LOADER_VERSION"}},
                            capture_output=True, text=True, timeout=50)
    assert result.returncode == 0, result.stdout + result.stderr
