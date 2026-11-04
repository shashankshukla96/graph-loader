"""The ``status-locator.json`` of an attempt directory (E15.F1.S3): which store holds a run's status, and where.

Every attempt directory gets one, whichever store is active, so a reader that starts from the run directory finds
the data. It never contains credentials: a database URL is stored without its user information and without
password-like query parameters (the password comes from the reader's own environment, ``PGPASSWORD``, ``.pgpass``
or a service file).
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

from neo4j_graph_loader.status import model

LOCATOR_NAME = "status-locator.json"
STORES = ("json", "sql")
_SECRET_QUERY_KEYS = ("password", "passwd", "pwd", "sslpassword", "secret", "token", "api_key", "apikey")


class LocatorError(ValueError):
    """The locator is missing or not a valid locator."""


def sanitize_db_url(url: str) -> str:
    """``url`` without user information (user and password) and without password-like query parameters."""
    parts = urlsplit(url)
    host = parts.netloc.rsplit("@", 1)[-1]
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
                       if key.lower() not in _SECRET_QUERY_KEYS])
    # Built by hand, not with urlunsplit: before CPython 3.12.4 urlunsplit dropped the "//" of an empty netloc, which turned
    # sqlite:////abs/path.db into sqlite://abs/path.db (found on the host's 3.12.3).
    return f"{parts.scheme}://{host}{parts.path}" + (f"?{query}" if query else "")


def atomic_write(path: Path, data: bytes, *, fsync: bool = True) -> None:
    """Write ``data`` to ``path`` so that a reader sees the old file or the new one, never a part of either."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def build_locator(*, store: str, execution_id: str, run_id: str | None, created_at: str,
                  db_url: str | None = None, schema: str | None = None) -> dict:
    if store not in STORES:
        raise ValueError(f"status store must be one of {STORES}, got {store!r}")
    locator: dict = {"schema_version": model.SCHEMA_VERSION, "kind": "status-locator", "store": store,
                     "execution_id": execution_id, "run_id": run_id, "created_at": created_at}
    if store == "json":
        locator["files"] = {"status": "status.json", "events": "events.<first seq>.jsonl"}
    else:
        locator["database"] = {"url": sanitize_db_url(db_url) if db_url else None, "schema": schema}
    return locator


def write_locator(directory: Path | str, locator: dict) -> Path:
    path = Path(directory) / LOCATOR_NAME
    atomic_write(path, (json.dumps(locator, indent=1) + "\n").encode())
    return path


def read_locator(path: Path | str) -> dict:
    """The locator at ``path`` (a file, or an attempt directory holding one)."""
    path = Path(path)
    if path.is_dir():
        path = path / LOCATOR_NAME
    try:
        locator = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LocatorError(f"no status locator at {path}") from exc
    except (OSError, ValueError) as exc:
        raise LocatorError(f"unreadable status locator {path}: {exc}") from exc
    if not isinstance(locator, dict) or locator.get("kind") != "status-locator" or locator.get("store") not in STORES \
            or not model.is_id(locator.get("execution_id")):
        raise LocatorError(f"{path} is not a status locator")
    return locator
