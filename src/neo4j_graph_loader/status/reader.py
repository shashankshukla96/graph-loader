"""Reading run status (E15.F1.S3): ``read_status`` and ``iter_events``, for either store.

``location`` is an attempt directory, a ``status-locator.json``, or a base directory holding attempt directories (the
newest one, or the one named by ``execution_id``); ``None`` means the default run-directory base (the OS temporary
directory). The locator names the store; this module reads the JSON store, and the SQL reader registers itself with
:func:`register_store_reader` when its optional dependencies are present.

Documents are validated against the PUBLISHED (open) JSON Schemas, so a later additive version's documents are
accepted. Events are tolerant of a torn final line (a line without its newline is not an event yet), and history that
rotation deleted is reported as a ``gap`` event, never silently skipped.
"""
from __future__ import annotations

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Callable, Iterator

from neo4j_graph_loader.status import json_store, locator as locator_module, model, schemas, validate as validate_module
from neo4j_graph_loader.status.types import StatusDocument, StatusEvent

logger = logging.getLogger(__name__)

ATTEMPT_PREFIX = "graph-loader-"


class StatusNotFound(LookupError):
    """No status was found at the location (or for the execution id)."""


class StatusStoreError(RuntimeError):
    """The location's store cannot be read here (for example the SQL reader's optional dependency is missing)."""


class StatusDataError(ValueError):
    """A status file is damaged in a way a torn tail does not explain."""


_STORE_READERS: dict[str, dict[str, Callable]] = {}


def register_store_reader(store: str, *, read_status: Callable, iter_events: Callable) -> None:
    """Install the readers of another store (``sql``); called by that store's module on import."""
    _STORE_READERS[store] = {"read_status": read_status, "iter_events": iter_events}


def resolve(location: Path | str | None = None, *, execution_id: str | None = None) -> tuple[Path, dict]:
    """``(attempt directory, locator)`` for a location, see the module docstring."""
    path = Path(location) if location is not None else Path(tempfile.gettempdir())
    if path.is_file():
        path = path.parent
    if not path.is_dir():
        raise StatusNotFound(f"no such run directory: {path}")
    candidates = [path] if (path / locator_module.LOCATOR_NAME).exists() else sorted(
        child for child in path.glob(f"{ATTEMPT_PREFIX}*") if (child / locator_module.LOCATOR_NAME).exists())
    found = []
    for directory in candidates:
        try:
            found.append((directory, locator_module.read_locator(directory)))
        except locator_module.LocatorError as exc:
            logger.debug("stage=status reason=skipping %s: %s", directory, exc)
    if execution_id is not None:
        found = [item for item in found if item[1]["execution_id"] == execution_id]
    if not found:
        raise StatusNotFound(f"no run status found under {path}" + (f" for execution {execution_id}" if execution_id else ""))
    return max(found, key=lambda item: (item[1].get("created_at", ""), str(item[0])))


def _validated(document: dict, schema: dict, what: str, validate: bool):
    if validate:
        try:
            validate_module.validate(document, schema)
        except validate_module.ValidationError as exc:
            raise StatusDataError(f"{what} does not match its schema: {exc}") from exc
    return document


def read_status(location: Path | str | None = None, *, execution_id: str | None = None, validate: bool = True) -> StatusDocument:
    """The run status snapshot of an execution (see the module docstring for ``location``)."""
    directory, locator = resolve(location, execution_id=execution_id)
    store = locator["store"]
    if store != "json":
        reader = _STORE_READERS.get(store)
        if reader is None:
            raise StatusStoreError(f"the status of {directory} is in the {store!r} store, which cannot be read here "
                                   "(install the neo4j-graph-loader[sql] or [postgres] extra)")
        return reader["read_status"](directory, locator, validate=validate)
    path = directory / json_store.STATUS_NAME
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StatusNotFound(f"{directory} has a locator but no {json_store.STATUS_NAME} yet") from exc
    except (OSError, ValueError) as exc:
        raise StatusDataError(f"unreadable {path}: {exc}") from exc
    return _validated(document, schemas.STATUS_SCHEMA, str(path), validate)


def _gap(execution_id: str, first: int, last: int, reason: str) -> StatusEvent:
    return {"schema_version": model.SCHEMA_VERSION, "execution_id": execution_id, "seq": first,
            "ts": model.timestamp(time.time()), "type": "gap", "from_seq": first, "to_seq": last,
            "count": last - first + 1, "reason": reason}


def iter_events(location: Path | str | None = None, *, execution_id: str | None = None, follow: bool = False,
                start_seq: int = 1, poll_interval: float = 0.2, stop: Callable[[], bool] | None = None,
                idle_timeout: float | None = None, validate: bool = True) -> Iterator[StatusEvent]:
    """The events of an execution in ``seq`` order, from ``start_seq``.

    Without ``follow`` the events written so far are yielded and the iterator ends. With ``follow`` it keeps
    waiting for new ones, across segment rotation, and ends after yielding ``execution.finished``, when ``stop()``
    returns true, or after ``idle_timeout`` seconds without a new event (``None``: wait for ever). Rotated-away
    history comes out as one ``gap`` event covering the missing range.
    """
    directory, locator = resolve(location, execution_id=execution_id)
    store = locator["store"]
    if store != "json":
        reader = _STORE_READERS.get(store)
        if reader is None:
            raise StatusStoreError(f"the events of {directory} are in the {store!r} store, which cannot be read here "
                                   "(install the neo4j-graph-loader[sql] or [postgres] extra)")
        yield from reader["iter_events"](directory, locator, follow=follow, start_seq=start_seq, poll_interval=poll_interval,
                                         stop=stop, idle_timeout=idle_timeout, validate=validate)
        return
    yield from _iter_json_events(directory, locator["execution_id"], follow, start_seq, poll_interval, stop, idle_timeout, validate)


def _iter_json_events(directory: Path, execution_id: str, follow: bool, start_seq: int, poll_interval: float,
                      stop: Callable[[], bool] | None, idle_timeout: float | None, validate: bool) -> Iterator[StatusEvent]:
    next_seq = max(1, start_seq)
    offsets: dict[Path, int] = {}
    last_progress = time.monotonic()
    while True:
        progressed = False
        segments = json_store.list_segments(directory)
        if segments and segments[0][0] > next_seq:
            first = segments[0][0]
            yield _gap(execution_id, next_seq, first - 1, "history-rotated")
            next_seq, progressed = first, True
        for index, (_first, path) in enumerate(segments):
            following = segments[index + 1][0] if index + 1 < len(segments) else None
            if following is not None and following <= next_seq:
                continue                                              # this whole segment is behind us
            try:
                with open(path, "rb") as handle:
                    handle.seek(offsets.get(path, 0))
                    chunk = handle.read()
            except FileNotFoundError:
                continue                                              # rotated away between the listing and the read
            consumed = chunk.rfind(b"\n") + 1                         # a torn final line is not an event yet
            for raw in chunk[:consumed].splitlines():
                if not raw.strip():
                    continue
                try:
                    event = json.loads(raw)
                except ValueError as exc:
                    raise StatusDataError(f"damaged event line in {path}: {exc}") from exc
                seq = event.get("seq") if isinstance(event, dict) else None
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise StatusDataError(f"event without a seq in {path}")
                if seq < next_seq:
                    continue
                if seq > next_seq:
                    yield _gap(execution_id, next_seq, seq - 1, "missing")
                _validated(event, schemas.EVENT_SCHEMA, f"event {seq}", validate)
                yield event
                next_seq, progressed = seq + 1, True
                if follow and event.get("type") == "execution.finished":
                    return
            offsets[path] = offsets.get(path, 0) + consumed
        if not follow:
            return
        if progressed:
            last_progress = time.monotonic()
        elif idle_timeout is not None and time.monotonic() - last_progress >= idle_timeout:
            return
        if stop is not None and stop():
            return
        time.sleep(poll_interval)
