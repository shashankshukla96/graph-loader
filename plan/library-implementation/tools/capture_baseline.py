#!/usr/bin/env python3
"""Capture allowlisted source/environment identities without reading secrets."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
TREES = {"src", "tests", "examples", "config", "scripts", "monitoring", "docs"}
SUFFIXES = {".py", ".json", ".yaml", ".yml", ".toml", ".txt", ".sh", ".ps1", ".xml", ".md", ".prom", ".csv"}
FILES = {"requirements.txt", "requirements-dev.txt", "pyproject.toml", "Makefile",
         "Dockerfile", ".dockerignore", ".gitignore", ".env.example", "LICENSE", "README.md",
         "requirements-build.txt", "PACKAGE_README.md", "MANIFEST.in", "docs/DISTRIBUTION_DEVELOPMENT.md",
         "plan/library-implementation/tools/capture_baseline.py",
         "plan/library-implementation/tools/run_dlq_proof.py",
         "plan/library-implementation/tools/verify_e03_artifacts.py",
         "plan/library-implementation/tools/run_e04_fault.py",
         "plan/library-implementation/tools/run_e06_service_proof.py"}
UNTRACKED_INPUTS = [
    "tests/test_library_preflight.py", "tests/test_dlq_proof_harness.py", "tests/fixtures",
    "src/neo4j_graph_loader", "PACKAGE_README.md", "MANIFEST.in", "requirements-build.txt",
    "scripts/sync_requirements.py", "tests/test_distribution_foundation.py",
    "docs/DISTRIBUTION_DEVELOPMENT.md", "tests/test_namespace_migration.py",
    "docs/LIBRARY_API.md", "tests/test_public_api.py", "tests/test_public_lifecycle.py",
    "tests/test_planner_transport.py", "tests/test_public_cancellation.py",
    "tests/test_public_cleanup.py", "tests/test_installed_cli.py", "tests/test_cli_resources.py", "tests/test_runtime_paths.py", "docs/CLI_RUNTIME.md",
    "plan/library-implementation/tools/run_e06_service_proof.py",
    "scripts/build_candidate_images.py", "tests/test_candidate_images.py", "tests/docker_support.py", "tests/test_image_selection.py",
    "tests/test_docker_topology.py", "docs/DOCKER_EXECUTION.md",
    "tests/test_planner_imports.py", "tests/fixtures/e04_faults.py",
    "plan/library-implementation/tools/run_e04_fault.py",
    "plan/library-implementation/tools/capture_baseline.py",
    "plan/library-implementation/tools/run_dlq_proof.py",
    "plan/library-implementation/tools/verify_e03_artifacts.py",
]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def allowed(relative):
    path = PurePosixPath(relative)
    if relative in {"src/neo4j_graph_loader/data/graph_schema.yaml", "src/neo4j_graph_loader/data/social_schema.yaml"}:
        return True
    if relative != ".env.example" and any(part.startswith(".env") or part in {"__pycache__", "data", "datasets", "reports", "logs"}
           for part in path.parts):
        return False
    return (relative in FILES or relative.startswith("Dockerfile.")
            or (len(path.parts) == 1 and relative.startswith("docker-compose") and path.suffix in {".yaml", ".yml"})
            or (path.parts[0] in TREES and path.suffix in SUFFIXES))


def capture():
    tracked = set(git("ls-files", "-z").decode().rstrip("\0").split("\0"))
    # Untracked inclusion is deliberately restricted to named prerequisite/E02/E03/E04 inputs.
    untracked = set(git("ls-files", "--others", "--exclude-standard", "-z", "--",
                        *UNTRACKED_INPUTS).decode().rstrip("\0").split("\0")) - {""}
    rows = []
    for relative in sorted(tracked | untracked):
        if not allowed(relative):
            continue
        path = ROOT / relative
        if path.is_symlink():
            raise ValueError(f"Refusing symlink input: {relative}")
        if not path.is_file():
            rows.append({"path": relative, "missing": True, "tracked": relative in tracked})
            continue
        content = path.read_bytes()
        rows.append({"path": relative, "size": len(content), "sha256": digest(content),
                     "tracked": relative in tracked, "executable": bool(path.stat().st_mode & 0o111)})
    fixture = "tests/fixtures/multi_topic_arrival.py"
    assert any(r["path"] == fixture for r in rows)
    source = {"head": git("rev-parse", "HEAD").decode().strip(), "files": rows}
    source_id = digest(canonical(source))
    with tempfile.TemporaryDirectory(prefix="graph-loader-manifest-proof-") as temporary:
        copy = Path(temporary) / "multi_topic_arrival.py"
        copy.write_bytes((ROOT / fixture).read_bytes() + b"\n# Temporary identity negative control\n")
        changed = [dict(r) for r in rows]
        row = next(r for r in changed if r["path"] == fixture)
        row.update(size=copy.stat().st_size, sha256=digest(copy.read_bytes()))
        changed_id = digest(canonical({"head": source["head"], "files": changed}))
    assert changed_id != source_id
    env = {"python": sys.version, "implementation": platform.python_implementation(),
           "executable": sys.executable, "platform": platform.platform(), "machine": platform.machine(),
           "dependencies": sorted({(d.metadata["Name"], d.version) for d in importlib.metadata.distributions()}),
           "configuration": {name: os.environ.get(name) for name in (
               "PYTEST_ADDOPTS", "PYTHONPATH", "PYTHONHASHSEED", "GRAPH_LOADER_METRICS_ENABLED",
               "GRAPH_LOADER_MULTI_TOPIC_PAIRS", "GRAPH_LOADER_STRESS_SCALE")}}
    result = {"source": source, "source_sha256": source_id, "environment": env,
              "environment_sha256": digest(canonical(env)),
              "staged_binary_diff_sha256": digest(git("diff", "--cached", "--binary", "--no-ext-diff")),
              "unstaged_binary_diff_sha256": digest(git("diff", "--binary", "--no-ext-diff")),
              "mutation_proof": {"temporary_copy_of": fixture, "changed_source_sha256": changed_id,
                                 "real_fixture_sha256_after": digest((ROOT / fixture).read_bytes())},
              "policy": {"tracked_trees": sorted(TREES), "suffixes": sorted(SUFFIXES),
                         "root_files": sorted(FILES), "untracked": UNTRACKED_INPUTS,
                         "excluded": [".env* except public tracked .env.example", "ignored inputs", "datasets", "reports", "logs", "plan/evidence (no self hashing)"]}}
    result["baseline_sha256"] = digest(canonical({"source": source_id, "environment": result["environment_sha256"]}))
    return result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: capture_baseline.py OUTPUT.json")
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    result = capture()
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("source_sha256", "environment_sha256", "baseline_sha256")}))
    print(f"Included {len(result['source']['files'])} files; temporary fixture mutation changed identity.")
