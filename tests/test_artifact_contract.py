"""E15.F2.S11: the artifact contract is explicit and finite (approved resources, approved extras, optional modules).

Builds the real wheel and sdist from the checkout (``python -m build --no-isolation``: needs ``build`` and ``setuptools``;
skipped without them) and runs the strict verifier of ``plan/library-implementation/tools/verify_e03_artifacts.py`` on them
and on copies with one planted extra, then checks the same finite lists in ``scripts/verify_installed.py``.
"""
from __future__ import annotations

import importlib.util
import io
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
TOOL = REPO / "plan" / "library-implementation" / "tools" / "verify_e03_artifacts.py"
pytest.importorskip("packaging")
BUILD = pytest.mark.skipif(not (importlib.util.find_spec("build") and importlib.util.find_spec("setuptools")),
                           reason="needs build and setuptools (python -m build --no-isolation)")


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def verifier():
    return load(TOOL, "verify_e03_artifacts_under_test")


@pytest.fixture(scope="module")
def artifacts(tmp_path_factory):
    out = tmp_path_factory.mktemp("dist")
    source = tmp_path_factory.mktemp("tree")
    for name in ("pyproject.toml", "MANIFEST.in", "PACKAGE_README.md", "LICENSE"):
        shutil.copy2(REPO / name, source / name)
    shutil.copytree(REPO / "src", source / "src", ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
    result = subprocess.run([sys.executable, "-m", "build", "--no-isolation", "--outdir", str(out), str(source)], capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    return source, next(out.glob("*.whl")), next(out.glob("*.tar.gz"))


def rewrite_wheel(wheel: Path, target: Path, edit) -> Path:
    with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as out:
        members = {info.filename: source.read(info.filename) for info in source.infolist()}
        edit(members)
        for name, data in members.items():
            out.writestr(name, data)
    return target


def rewrite_sdist(sdist: Path, target: Path, name: str, data: bytes) -> Path:
    with tarfile.open(sdist) as source, tarfile.open(target, "w:gz") as out:
        for member in source.getmembers():
            out.addfile(member, source.extractfile(member) if member.isfile() else None)
        root = sdist.name.removesuffix(".tar.gz")
        info = tarfile.TarInfo(f"{root}/{name}")
        info.size = len(data)
        out.addfile(info, io.BytesIO(data))
    return target


@BUILD
def test_the_real_artifacts_satisfy_the_contract(verifier, artifacts) -> None:
    source, wheel, sdist = artifacts
    verifier.ROOT = source
    record = verifier.inspect(wheel, sdist)
    assert set(record["resources"]) == {"neo4j_graph_loader/" + name for name in verifier.RESOURCE_FILES}
    assert any(name.startswith("neo4j_graph_loader/monitoring/prometheus/alerts/") for name in record["resources"])
    with zipfile.ZipFile(wheel) as archive:
        metadata = archive.read(next(n for n in archive.namelist() if n.endswith("/METADATA"))).decode()
    assert sorted(re.findall(r"^Provides-Extra: (.+)$", metadata, re.M)) == ["postgres", "sql"]
    assert 'SQLAlchemy<3,>=2.0.30; extra == "sql"' in metadata and "psycopg[binary]" in metadata


@BUILD
def test_a_planted_extra_file_in_the_wheel_fails(verifier, artifacts, tmp_path) -> None:
    source, wheel, sdist = artifacts
    verifier.ROOT = source
    for planted in ("neo4j_graph_loader/monitoring/prometheus/alerts/planted.yml", "neo4j_graph_loader/data/planted.txt", "neo4j_graph_loader/planted.py"):
        bad = rewrite_wheel(wheel, tmp_path / f"bad-{Path(planted).name}.whl", lambda members, p=planted: members.update({p: b"x: 1\n"}))
        with pytest.raises(AssertionError):
            verifier.inspect(bad, sdist)


@BUILD
def test_a_planted_extra_file_in_the_sdist_fails(verifier, artifacts, tmp_path) -> None:
    source, wheel, sdist = artifacts
    verifier.ROOT = source
    bad = rewrite_sdist(sdist, tmp_path / "bad.tar.gz", "src/neo4j_graph_loader/monitoring/prometheus/alerts/planted.yml", b"x: 1\n")
    with pytest.raises(AssertionError):
        verifier.inspect(wheel, bad)


@BUILD
@pytest.mark.parametrize("edit_name", ["unlisted-extra", "missing-approved-extra", "extra-requirement-changed", "base-dependency-added"])
def test_an_unlisted_missing_or_altered_extra_fails(verifier, artifacts, tmp_path, edit_name) -> None:
    source, wheel, sdist = artifacts
    verifier.ROOT = source

    def edit(members):
        key = next(n for n in members if n.endswith("/METADATA"))
        text = members[key].decode()
        if edit_name == "unlisted-extra":
            text = text.replace("Provides-Extra: sql\n", "Provides-Extra: sql\nProvides-Extra: redis\n")
        elif edit_name == "missing-approved-extra":
            text = text.replace("Provides-Extra: postgres\n", "")
        elif edit_name == "extra-requirement-changed":
            text = text.replace("SQLAlchemy<3,>=2.0.30; extra", "SQLAlchemy<3,>=1.0; extra", 1)
        else:
            text = text.replace("Requires-Dist: prometheus-client", "Requires-Dist: SQLAlchemy>=2\nRequires-Dist: prometheus-client", 1)
        assert text != members[key].decode()
        members[key] = text.encode()
    bad = rewrite_wheel(wheel, tmp_path / "bad.whl", edit)
    with pytest.raises(AssertionError):
        verifier.inspect(bad, sdist)


@BUILD
def test_an_unlisted_resource_or_extra_in_the_checkout_fails_before_any_archive_is_read(verifier, artifacts, tmp_path) -> None:
    source, wheel, sdist = artifacts
    copy = tmp_path / "tree"
    shutil.copytree(source, copy)
    verifier.ROOT = copy
    (copy / "src" / "neo4j_graph_loader" / "monitoring" / "planted.json").write_text("{}")
    with pytest.raises(AssertionError, match="checkout resources differ"):
        verifier.inspect(wheel, sdist)
    (copy / "src" / "neo4j_graph_loader" / "monitoring" / "planted.json").unlink()
    pyproject = copy / "pyproject.toml"
    pyproject.write_text(pyproject.read_text().replace('postgres = [', 'redis = ["redis>=5"]\npostgres = [', 1))
    with pytest.raises(AssertionError, match="extras differ"):
        verifier.inspect(wheel, sdist)


def test_the_approved_lists_are_finite_and_equal_the_checkout() -> None:
    import tomllib
    verifier = load(TOOL, "verify_e03_lists")
    config = tomllib.loads((REPO / "pyproject.toml").read_text())["project"]
    assert config["optional-dependencies"] == verifier.APPROVED_EXTRAS
    assert sorted(verifier.APPROVED_EXTRAS) == ["postgres", "sql"] and set(verifier.OPTIONAL_DISTRIBUTIONS.values()) <= set(verifier.APPROVED_EXTRAS)
    present = {p.relative_to(REPO / "src" / "neo4j_graph_loader").as_posix() for p in (REPO / "src" / "neo4j_graph_loader").rglob("*")
               if p.is_file() and p.suffix not in {".py", ".pyc"} and "__pycache__" not in p.parts and ".egg-info" not in str(p)}
    assert present == verifier.RESOURCE_FILES and len(verifier.RESOURCE_FILES) == 19
    text = (REPO / "scripts" / "verify_installed.py").read_text()
    assert "APPROVED_EXTRAS = ('postgres', 'sql')" in text and "metadata.get_all('Provides-Extra') or []) != sorted(APPROVED_EXTRAS)" in text


def test_every_optional_module_is_the_only_one_importing_its_dependency() -> None:
    """A module outside OPTIONAL_MODULES must not import SQLAlchemy or psycopg at import time (a base install imports every other one)."""
    verifier = load(TOOL, "verify_e03_optional")
    package = REPO / "src" / "neo4j_graph_loader"
    offenders = []
    for path in package.rglob("*.py"):
        relative = "neo4j_graph_loader/" + path.relative_to(package).as_posix()
        top_level = [line for line in path.read_text().splitlines() if re.match(r"(import|from) (sqlalchemy|psycopg)\b", line)]
        if top_level and relative not in verifier.OPTIONAL_MODULES:
            offenders.append(relative)
    assert not offenders, offenders


def test_the_installed_runner_approves_exactly_the_monitoring_resources_of_the_strict_verifier() -> None:
    verifier = load(TOOL, "verify_e03_resources")
    runner = load(REPO / "scripts" / "verify_installed.py", "verify_installed_resources")
    expected = {"neo4j_graph_loader/" + name for name in verifier.RESOURCE_FILES if name.startswith("monitoring/")}
    assert set(runner.MONITORING_RESOURCES) == expected and len(expected) == 17
    for name in expected:
        runner.safe_relative(name, payload=True)
    for planted in ("neo4j_graph_loader/monitoring/prometheus/alerts/planted.yml", "neo4j_graph_loader/data/planted.json", "neo4j_graph_loader/x.txt"):
        with pytest.raises(ValueError, match="unexpected application payload"):
            runner.safe_relative(planted, payload=True)


@pytest.mark.parametrize("extras, accepted", [(("postgres", "sql"), True), ((), False), (("sql",), False), (("postgres", "sql", "redis"), False)])
def test_the_installed_runner_wheel_check_approves_exactly_the_two_extras(tmp_path, extras, accepted) -> None:
    import hashlib
    runner = load(REPO / "scripts" / "verify_installed.py", "verify_installed_extras")
    version, dist = "0.1.0a1.dev0", "neo4j_graph_loader-0.1.0a1.dev0.dist-info/"
    metadata = "Name: neo4j-graph-loader\nVersion: 0.1.0a1.dev0\n" + "".join(f"Provides-Extra: {name}\n" for name in extras)
    payload = {"neo4j_graph_loader/__init__.py": b"# fixture\n"}
    members = {dist + name: b"" for name in ("WHEEL", "top_level.txt", "RECORD", "licenses/LICENSE", "entry_points.txt")} | {dist + "METADATA": metadata.encode()}
    wheel = tmp_path / "w.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, data in (payload | members).items():
            archive.writestr(name, data)
    data = {"package_version": version, "package_payload": {n: hashlib.sha256(v).hexdigest() for n, v in payload.items()},
            "distribution_files": {n: hashlib.sha256(v).hexdigest() for n, v in members.items()}}
    if accepted:
        runner.validate_wheel(wheel, data)
    else:
        with pytest.raises(ValueError, match="unexpected wheel identity or extras"):
            runner.validate_wheel(wheel, data)
