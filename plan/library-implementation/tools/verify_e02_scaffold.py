#!/usr/bin/env python3
"""Inspect E02's intentionally incomplete artifacts and probe an isolated import.

This is not the future installed-loader regression runner. It accepts only the
two-module scaffold and makes no loader/API/CLI/service-support claim.
"""
import argparse
import ast
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib
import venv
import zipfile

from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[3]
MODULES = {"neo4j_graph_loader/__init__.py", "neo4j_graph_loader/_version.py"}
SDIST_INPUTS = {"pyproject.toml", "PACKAGE_README.md", "LICENSE", "MANIFEST.in", "PKG-INFO", "setup.cfg"}
EGG_FILES = {"PKG-INFO", "SOURCES.txt", "dependency_links.txt", "top_level.txt", "requires.txt"}


def inspect(directory):
    wheel, = directory.glob("*.whl")
    sdist, = directory.glob("*.tar.gz")
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    version_tree = ast.parse((ROOT / "src/neo4j_graph_loader/_version.py").read_text())
    version = next(ast.literal_eval(n.value) for n in version_tree.body if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets))
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert {n for n in names if ".dist-info/" not in n} == MODULES, names
        metadata = BytesParser().parsebytes(archive.read(next(n for n in names if n.endswith("/METADATA"))))
        assert metadata["Name"] == config["name"]
        assert metadata["Version"] == version
        assert metadata["Requires-Python"] == config["requires-python"]
        assert metadata["License-Expression"] == "Apache-2.0"
        assert metadata.get_all("License-File") == ["LICENSE"]
        assert archive.read(next(n for n in names if n.endswith("/licenses/LICENSE"))) == (ROOT / "LICENSE").read_bytes()
        description = metadata.get_payload(decode=True).decode("utf-8")
        assert description.rstrip("\n") == (ROOT / "PACKAGE_README.md").read_text().rstrip("\n")
        assert not any("entry_points" in n for n in names)
        assert not any("Operating System" in c for c in metadata.get_all("Classifier", []))
        # Setuptools normalizes the order of comma-separated version bounds.
        assert {Requirement(r) for r in metadata.get_all("Requires-Dist", [])} == {
            Requirement(r) for r in config.get("dependencies", [])}
    with tarfile.open(sdist) as archive:
        allowed = SDIST_INPUTS | {"src/" + n for n in MODULES} | {
            "src/neo4j_graph_loader.egg-info/" + n for n in EGG_FILES}
        snames = []
        for member in archive.getmembers():
            assert member.isdir() or member.isfile(), member.name
            relative = member.name.partition("/")[2]
            if member.isfile():
                assert relative in allowed, member.name
                snames.append(relative)
        assert ({"src/" + n for n in MODULES} | (SDIST_INPUTS - {"setup.cfg"})) <= set(snames)
    probe_code = """
import importlib.metadata as m, logging, socket, subprocess, threading
before = logging.getLogger().level, list(logging.getLogger().handlers)
def forbidden(*a, **k): raise AssertionError('import side effect')
socket.socket = forbidden
subprocess.Popen = forbidden
threading.Thread.start = forbidden
import neo4j_graph_loader as p
assert p.__version__ == m.version('neo4j-graph-loader') == EXPECTED
assert before == (logging.getLogger().level, logging.getLogger().handlers)
assert '/site-packages/' in p.__file__
assert p.__all__ == ['__version__']
assert not m.distribution('neo4j-graph-loader').entry_points
print(p.__version__, p.__file__)
""".replace("EXPECTED", repr(version))
    with tempfile.TemporaryDirectory(prefix="graph-loader-e02-import-") as temporary:
        temporary = Path(temporary)
        venv.EnvBuilder(with_pip=True).create(temporary / "env")
        python = temporary / "env/bin/python"
        subprocess.run([str(python), "-m", "pip", "install", "--no-deps", str(wheel.resolve())],
                       cwd=temporary, check=True, capture_output=True)
        probe = subprocess.run([str(python), "-I", "-c", probe_code], cwd=temporary,
                               env={k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME"}},
                               check=True, capture_output=True, text=True)
    return {"version": version, "wheel_inventory": names, "sdist_inventory": sorted(snames),
            "clean_import_probe": probe.stdout.strip(), "commands_exit_code": 0, "development_only": True,
            "artifacts": [{"path": str(p.resolve()), "size": p.stat().st_size,
                           "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in (wheel, sdist)]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = inspect(args.directory)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print("E02 scaffold inventory, metadata, license and isolated import: passed")
