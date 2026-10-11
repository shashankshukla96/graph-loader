"""`make monitoring-up` reads .env, but exported variables win (only unset ones come from .env).

The test runs the recipe's own .env-loading line from the Makefile in a scratch directory with a
scratch .env, so it never reads the project .env and starts nothing.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

MAKEFILE = Path(__file__).resolve().parents[1] / "Makefile"


def _env_loading_line() -> str:
    lines = MAKEFILE.read_text(encoding="utf-8").splitlines()
    start = lines.index("monitoring-up:")
    first = lines[start + 1].strip()
    assert first.startswith("@") and first.endswith("\\"), first
    # Make semantics: drop the echo-suppressing @, the line continuation, and unescape $$.
    return first[1:-1].strip().rstrip(";").replace("$$", "$")


def _run(tmp_path: Path, env: dict[str, str], dotenv: str | None) -> dict[str, str]:
    if dotenv is not None:
        (tmp_path / ".env").write_text(dotenv, encoding="utf-8")
    script = (
        _env_loading_line()
        + '; printf "%s|%s|%s|%s\\n" "${NEO4J_URI-<unset>}" "${KAFKA_BOOTSTRAP_SERVERS-<unset>}" '
        '"${GRAPH_LOADER_DATA_DIR-<unset>}" "${EMPTY_EXPORTED-<unset>}"'
    )
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    result = subprocess.run(
        ["sh", "-c", script], cwd=tmp_path, env={**base, **env}, capture_output=True, text=True, check=True,
    )
    uri, kafka, data_dir, empty = result.stdout.strip().split("|")
    return {"uri": uri, "kafka": kafka, "data_dir": data_dir, "empty": empty}


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_exported_values_win_and_dotenv_only_fills_unset_variables(tmp_path: Path) -> None:
    dotenv = (
        "NEO4J_URI=bolt://remote.example:7687\n"
        "KAFKA_BOOTSTRAP_SERVERS=remote.example:9092\n"
        'GRAPH_LOADER_DATA_DIR="/srv/from dotenv"\n'
        "EMPTY_EXPORTED=from-dotenv\n"
    )
    seen = _run(
        tmp_path,
        {"NEO4J_URI": "bolt://localhost:7687", "EMPTY_EXPORTED": ""},
        dotenv,
    )
    assert seen["uri"] == "bolt://localhost:7687"          # exported wins
    assert seen["empty"] == ""                             # exported-but-empty still wins
    assert seen["kafka"] == "remote.example:9092"          # unset -> filled from .env
    assert seen["data_dir"] == "/srv/from dotenv"          # quoting keeps shell-source semantics


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_without_a_dotenv_the_environment_is_untouched(tmp_path: Path) -> None:
    seen = _run(tmp_path, {"NEO4J_URI": "bolt://localhost:7687"}, None)
    assert seen == {"uri": "bolt://localhost:7687", "kafka": "<unset>", "data_dir": "<unset>", "empty": "<unset>"}


_UNFILTERED_SNAPSHOT = '__gl_exported="$(export -p)"; set -a; [ -f .env ] && . ./.env; set +a; eval "$__gl_exported"'
_READONLY_ENV = {"SHELLOPTS": "braceexpand:hashall:interactive-comments", "NEO4J_URI": "bolt://localhost:7687"}


def _run_line(tmp_path: Path, line: str, env: dict[str, str]) -> subprocess.CompletedProcess:
    (tmp_path / ".env").write_text(
        "NEO4J_URI=bolt://remote.example:7687\nKAFKA_BOOTSTRAP_SERVERS=remote.example:9092\n", encoding="utf-8",
    )
    script = line + '; printf "%s|%s\\n" "${NEO4J_URI-<unset>}" "${KAFKA_BOOTSTRAP_SERVERS-<unset>}"'
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    return subprocess.run(["sh", "-c", script], cwd=tmp_path, env={**base, **env}, capture_output=True, text=True)


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
def test_an_exported_readonly_shell_variable_does_not_break_the_snapshot(tmp_path: Path) -> None:
    """bash makes an inherited SHELLOPTS readonly; re-applying it from `export -p` fails. The recipe
    leaves BASHOPTS/SHELLOPTS out of the snapshot, so the exported values still win."""
    result = _run_line(tmp_path, _env_loading_line(), _READONLY_ENV)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "bolt://localhost:7687|remote.example:9092"
    assert "readonly" not in result.stderr


@pytest.mark.skipif(
    shutil.which("sh") is None or shutil.which("bash") is None
    or os.path.realpath(shutil.which("sh")) != os.path.realpath(shutil.which("bash")),
    reason="the readonly SHELLOPTS failure is bash-specific",
)
def test_control_the_unfiltered_snapshot_is_what_breaks(tmp_path: Path) -> None:
    result = _run_line(tmp_path, _UNFILTERED_SNAPSHOT, _READONLY_ENV)
    assert "readonly" in result.stderr or result.stdout.strip() != "bolt://localhost:7687|remote.example:9092"
