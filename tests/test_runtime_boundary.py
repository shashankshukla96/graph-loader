"""E14.F1.S1: only the Docker boundary imports the Docker SDK; the runtime seam itself is SDK-free."""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "src" / "neo4j_graph_loader"

# The Docker boundary: the Docker backend and the two private helpers it owns (image-contract enforcement
# and the daemon topology preflight). Everything else reaches Docker through ``orchestrator.runtime``.
DOCKER_BOUNDARY = {
    "orchestrator/docker_service.py",
    "_images.py",
    "_docker_preflight.py",
}


def _imports_docker(tree: ast.AST) -> list[int]:
    lines = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "docker" or alias.name.startswith("docker.") for alias in node.names):
                lines.append(node.lineno)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and (node.module == "docker" or node.module.startswith("docker.")):
                lines.append(node.lineno)
        elif isinstance(node, ast.Call):
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            argument = node.args[0] if node.args else None
            if (name in {"__import__", "import_module"} and isinstance(argument, ast.Constant)
                    and isinstance(argument.value, str)
                    and (argument.value == "docker" or argument.value.startswith("docker."))):
                lines.append(node.lineno)
    return lines


def modules_importing_docker(root: Path) -> dict[str, list[int]]:
    found = {}
    for path in sorted(root.rglob("*.py")):
        lines = _imports_docker(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
        if lines:
            found[path.relative_to(root).as_posix()] = lines
    return found


def test_only_the_docker_boundary_imports_docker() -> None:
    importers = set(modules_importing_docker(SOURCE))
    assert importers - DOCKER_BOUNDARY == set(), "modules outside the Docker boundary import docker"
    assert importers, "the scan found no Docker importer at all: the scanner is broken"
    assert DOCKER_BOUNDARY <= importers, "a boundary module no longer needs docker: shrink DOCKER_BOUNDARY"


@pytest.mark.parametrize("statement", [
    "import docker",
    "import docker.errors",
    "from docker import errors",
    "from docker.errors import NotFound",
    "x = __import__('docker')",
    "import importlib\nimportlib.import_module('docker.types')",
])
def test_planted_docker_import_outside_the_boundary_is_found(tmp_path, statement) -> None:
    """Negative control: every spelling of a Docker import in a non-boundary module is reported."""
    package = tmp_path / "pkg"
    (package / "orchestrator").mkdir(parents=True)
    (package / "orchestrator" / "planted.py").write_text(statement + "\n", encoding="utf-8")
    (package / "clean.py").write_text("import os\nimport dockerless_name\n", encoding="utf-8")
    found = modules_importing_docker(package)
    assert set(found) == {"orchestrator/planted.py"}


def test_planted_import_would_fail_the_boundary_assertion(tmp_path) -> None:
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "cli.py").write_text("from docker.errors import NotFound\n", encoding="utf-8")
    assert set(modules_importing_docker(package)) - DOCKER_BOUNDARY == {"cli.py"}


@pytest.mark.parametrize("module", [
    "neo4j_graph_loader.orchestrator.runtime",
    "neo4j_graph_loader.orchestrator.bulk_monitor",
    "neo4j_graph_loader.orchestrator.relationship_bulk_monitor",
    "neo4j_graph_loader.orchestrator.planned_edges",
    "neo4j_graph_loader.orchestrator.process_runtime",
    "neo4j_graph_loader.orchestrator.run_registry",
    "neo4j_graph_loader._child_protocol",
    "neo4j_graph_loader._child",
])
def test_runtime_seam_and_its_callers_load_without_the_docker_sdk(module) -> None:
    """The seam and the call sites behind it import without ever importing the SDK (isolated interpreter)."""
    code = (
        "import sys; import importlib; "
        f"importlib.import_module({module!r}); "
        "sys.exit(1 if any(name == 'docker' or name.startswith('docker.') for name in sys.modules) else 0)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr


def test_docker_handle_maps_not_found_to_handle_gone_and_nothing_else() -> None:
    from docker.errors import APIError, NotFound
    from unittest.mock import MagicMock

    from neo4j_graph_loader.orchestrator.docker_service import DockerHandle
    from neo4j_graph_loader.orchestrator.runtime import Handle, HandleGone

    container = MagicMock()
    handle = DockerHandle(container)
    assert isinstance(handle, Handle)
    for operation, call in {
        "reload": lambda: handle.reload(),
        "logs": lambda: handle.logs(tail=5),
        "stop": lambda: handle.stop(timeout=3),
        "remove": lambda: handle.remove(force=True),
        "top": lambda: handle.top(),
    }.items():
        getattr(container, operation).side_effect = NotFound("gone")
        with pytest.raises(HandleGone) as excinfo:
            call()
        assert isinstance(excinfo.value.__cause__, NotFound)
        getattr(container, operation).side_effect = APIError("daemon error")
        with pytest.raises(APIError):
            call()


def test_stop_hold_dispatches_to_the_handles_own_hold_and_falls_back_to_the_docker_hold() -> None:
    from unittest.mock import MagicMock, patch

    from neo4j_graph_loader.orchestrator import docker_service
    from neo4j_graph_loader.orchestrator.docker_service import DockerHandle
    from neo4j_graph_loader.orchestrator.runtime import wait_until_stop_signal_ready

    class Own:
        def hold_for_stop(self):
            return 1.5

    assert wait_until_stop_signal_ready(Own()) == 1.5
    with patch.object(docker_service, "wait_until_stop_signal_ready", return_value=2.5) as hold:
        raw = MagicMock()                                   # a double without a class-level hold: the Docker hold
        assert wait_until_stop_signal_ready(raw) == 2.5
        handle = DockerHandle(raw)
        assert wait_until_stop_signal_ready(handle) == 2.5
        assert hold.call_args_list[-1].args == (handle,)
