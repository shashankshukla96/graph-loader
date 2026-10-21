#!/usr/bin/env python3
"""E03–E05 archive closure and installed public smoke checks (not E07 regressions)."""
import argparse
import ast
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tomllib
import zipfile

from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[3]
COMPONENTS = ["cli", "loader.node_loader", "loader.edge_loader", "loader.retry_consumer",
              "loader.planned_writer", "orchestrator.planned_dispatcher",
              "orchestrator.lease_server", "orchestrator.clock_runner"]
SDIST_INPUTS = {"pyproject.toml", "PACKAGE_README.md", "LICENSE", "MANIFEST.in", "PKG-INFO", "setup.cfg"}
RESOURCE_FILES = {"data/graph_schema.yaml", "data/social_schema.yaml"}
EGG_FILES = {"PKG-INFO", "SOURCES.txt", "dependency_links.txt", "top_level.txt", "requires.txt", "entry_points.txt"}


def sha(content):
    return hashlib.sha256(content).hexdigest()


def inspect(wheel, sdist):
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    package = ROOT / "src/neo4j_graph_loader"
    modules = {str(p.relative_to(package.parent)): p.read_bytes() for p in package.rglob("*.py")}
    resources = {"neo4j_graph_loader/" + n: (package / n).read_bytes() for n in RESOURCE_FILES}
    payload = modules | resources
    tree = ast.parse((package / "_version.py").read_text())
    version = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets))
    assert len(modules) > 2, "E03 requires the migrated runtime"
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names)), "duplicate wheel paths"
        dist_info = f"neo4j_graph_loader-{version}.dist-info"
        metadata_files = {f"{dist_info}/{name}" for name in (
            "METADATA", "WHEEL", "top_level.txt", "RECORD", "licenses/LICENSE", "entry_points.txt")}
        assert set(names) == set(payload) | metadata_files, names
        for name, content in payload.items():
            assert archive.read(name) == content, name
        metadata = BytesParser().parsebytes(archive.read(next(n for n in names if n.endswith("/METADATA"))))
        assert metadata["Name"] == config["name"]
        assert metadata["Version"] == version
        assert metadata["Requires-Python"] == config["requires-python"]
        assert metadata["Summary"] == config["description"]
        assert metadata["License-Expression"] == "Apache-2.0"
        assert metadata.get_all("License-File") == ["LICENSE"]
        assert {Requirement(r) for r in metadata.get_all("Requires-Dist", [])} == {
            Requirement(r) for r in config["dependencies"]}
        assert metadata.get_payload(decode=True).decode().rstrip() == (ROOT / "PACKAGE_README.md").read_text().rstrip()
        assert archive.read(next(n for n in names if n.endswith("/licenses/LICENSE"))) == (ROOT / "LICENSE").read_bytes()
        assert archive.read(next(n for n in names if n.endswith("/top_level.txt"))).decode().strip() == "neo4j_graph_loader"
        assert config["scripts"] == {"graph-loader": "neo4j_graph_loader.cli:main"}
        assert archive.read(f"{dist_info}/entry_points.txt").decode() == (
            "[console_scripts]\ngraph-loader = neo4j_graph_loader.cli:main\n")
        assert not metadata.get_all("Provides-Extra"), "D05: no extras"
    with tarfile.open(sdist) as archive:
        allowed = SDIST_INPUTS | {"src/" + n for n in payload} | {
            "src/neo4j_graph_loader.egg-info/" + n for n in EGG_FILES}
        snames = []
        for member in archive.getmembers():
            assert member.isdir() or member.isfile(), member.name
            if member.isfile():
                relative = member.name.partition("/")[2]
                assert relative in allowed, member.name
                snames.append(relative)
                if relative.startswith("src/neo4j_graph_loader/"):
                    assert archive.extractfile(member).read() == payload[relative.removeprefix("src/")], relative
                elif relative == "src/neo4j_graph_loader.egg-info/entry_points.txt":
                    assert archive.extractfile(member).read() == b"[console_scripts]\ngraph-loader = neo4j_graph_loader.cli:main\n", relative
                elif relative in SDIST_INPUTS - {"PKG-INFO", "setup.cfg"}:
                    assert archive.extractfile(member).read() == (ROOT / relative).read_bytes(), relative
        assert len(snames) == len(set(snames)), "duplicate sdist paths"
        assert ({"src/" + n for n in payload} | SDIST_INPUTS - {"setup.cfg"}
                | {"src/neo4j_graph_loader.egg-info/" + n for n in EGG_FILES}) <= set(snames)
    return {"version": version, "runtime_modules": {n: sha(b) for n, b in sorted(modules.items())},
            "resources": {n: sha(b) for n, b in sorted(resources.items())},
            "wheel_inventory": names, "sdist_inventory": sorted(snames),
            "artifacts": [{"path": str(p.resolve()), "size": p.stat().st_size, "sha256": sha(p.read_bytes())}
                          for p in (wheel, sdist)]}


def probe(python, modules, cwd):
    """Application imports must resolve exclusively from a clean site-packages."""
    code = '''
import hashlib, importlib, importlib.metadata as metadata, json, logging, pathlib, socket, subprocess, sys, sysconfig, threading
before = logging.getLogger().level, list(logging.getLogger().handlers)
def forbidden(*a, **k): raise AssertionError('unexpected import side effect')
socket.socket.connect = socket.socket.connect_ex = socket.socket.bind = forbidden
threading.Thread.start = subprocess.Popen = forbidden
site = pathlib.Path(sysconfig.get_paths()['purelib']).resolve()
origins = {}
for name in NAMES:
    module = importlib.import_module(name)
    origin = pathlib.Path(module.__file__).resolve()
    assert origin.is_relative_to(site), (name, str(origin), str(site))
    key = name.replace(".", "/")
    key = key + ("/__init__.py" if origin.name == "__init__.py" else ".py")
    assert hashlib.sha256(origin.read_bytes()).hexdigest() == DIGESTS[key], key
    origins[name] = str(origin)
import neo4j_graph_loader as package
from neo4j_graph_loader.utils import metrics
assert package.__version__ == metadata.version('neo4j-graph-loader') == metrics.PROCESS_VERSION
assert metrics._process_metrics is None
assert before == (logging.getLogger().level, logging.getLogger().handlers)
assert not any(n == 'src' or n.startswith('src.') for n in sys.modules)
dist = metadata.distribution('neo4j-graph-loader')
assert [(e.group,e.name,e.value) for e in dist.entry_points] == [("console_scripts","graph-loader","neo4j_graph_loader.cli:main")]
assert not json.loads(dist.read_text('direct_url.json') or '{}').get('dir_info', {}).get('editable')
assert not list(site.glob('__editable__*'))
assert not any(metadata.packages_distributions().get(n) for n in ['pytest','testcontainers','pandas','dotenv','build','twine'])
print(json.dumps({'version': package.__version__, 'origins': origins, 'site': str(site)}))
'''
    names = [n.removesuffix('.py').replace('/', '.').removesuffix('.__init__') for n in modules]
    code = code.replace('NAMES', repr(names)).replace('DIGESTS', repr(modules))
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME", "GRAPH_LOADER_VERSION"}}
    result = subprocess.run([str(python), "-I", "-c", code], cwd=cwd, env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    record = {"import_command": [str(python), "-I", "-c", code],
              "imports": json.loads(result.stdout), "helps": [], "old_imports": []}
    for old in ["src", "src.cli", "src.models.schema", "src.loader.edge_planner", "src.utils.metrics", "src.orchestrator.docker_service"]:
        reject = f'''import importlib
try: importlib.import_module({old!r})
except ModuleNotFoundError as e: assert e.name == 'src', e
else: raise AssertionError('old namespace imported')
'''
        command = [str(python), "-I", "-c", reject]
        child = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
        assert child.returncode == 0, child.stdout + child.stderr
        record["old_imports"].append({"module": old, "command": command, "exit_code": child.returncode})
    for component in COMPONENTS:
        command = [str(python), "-I", "-m", "neo4j_graph_loader." + component, "--help"]
        child = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
        assert child.returncode == 0 and "usage:" in child.stdout.lower(), (command, child.stdout, child.stderr)
        record["helps"].append({"command": command, "exit_code": child.returncode,
                               "stdout_sha256": sha(child.stdout.encode()), "stderr": child.stderr})
    record["entry_parity"] = []
    for arguments, expected in [(["--help"], 0), (["--version"], 0), ([], 2), (["unknown-command"], 2), (["start"], 2)]:
        observations = []
        for prefix in ([str(python.parent / "graph-loader")], [str(python), "-I", "-m", "neo4j_graph_loader"],
                       [str(python), "-I", "-m", "neo4j_graph_loader.cli"]):
            command = [*prefix, *arguments]
            child = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
            assert child.returncode == expected, (command, child.stdout, child.stderr)
            observations.append((child.stdout, child.stderr))
        assert len(set(observations)) == 1, arguments
        record["entry_parity"].append({"arguments": arguments, "exit_code": expected,
                                      "stdout_sha256": sha(observations[0][0].encode()),
                                      "stderr_sha256": sha(observations[0][1].encode())})
    record["public_examples"] = []
    for fixture in ("e04_imports.py", "e04_public_example.py", "e05_resources.py", "e05_runtime.py"):
        content = (ROOT / "tests/fixtures" / fixture).read_text()
        command = [str(python), "-I", "-c", content]
        child = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
        assert child.returncode == 0, child.stdout + child.stderr
        record["public_examples"].append({"fixture": fixture, "code_sha256": sha(content.encode()),
                                          "command": command, "exit_code": child.returncode,
                                          "result": json.loads(child.stdout)})
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("sdist", type=Path)
    parser.add_argument("--python", type=Path, help="already populated clean noneditable environment")
    parser.add_argument("--cwd", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = inspect(args.wheel, args.sdist)
    if args.python:
        assert args.cwd and args.cwd.resolve() != ROOT
        result["clean_installed_probe"] = probe(args.python, result["runtime_modules"], args.cwd)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Validated {len(result['runtime_modules'])} runtime modules; version {result['version']}")
