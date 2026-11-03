"""Log format configuration shared by the CLI and every role (E15.F1.S7).

``GRAPH_LOADER_LOG_FORMAT`` / ``--log-format`` selects ``text`` (the default: exactly the lines every earlier version wrote,
``LEVEL: message``) or ``json`` (one JSON object per line on stderr, the designated log stream). The choice is read
before argument parsing, by :func:`configure_logging`, which every entrypoint calls first.

In JSON mode each line carries ``ts``, ``level``, ``logger``, ``execution_id``, ``run_id``, ``kind``, ``name``, ``replica``,
``incarnation``, ``stage`` and ``message`` plus the structured fields of the event. Identity comes from the environment the
CLI sets for every role (``GRAPH_LOADER_EXECUTION_ID``, ``GRAPH_LOADER_INCARNATION``, ``GRAPH_LOADER_RUN_ID``) and from
:func:`set_log_context`. ``stage`` and the fields are attached AT EMISSION by :func:`structured` for the documented stages
(``runtime``, ``metrics-ready``, ``node-label-ready``, ``node-write``, ``edge-write``, ``edge-plan-rejections``,
``bulk-substage*``, ``outcome``, the write-outage and retry-consumer stages); every other message carries ``message`` only.
Nothing parses ``key=value`` text. Text mode ignores the structured fields, so text output stays byte-identical.

JSON mode also routes uncaught exceptions (main thread and other threads), ``argparse`` errors and warnings through the same
handler, and redacts credentials (URL passwords, ``password=`` pairs) from the message, the fields and tracebacks.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

LOG_FORMAT_ENV = "GRAPH_LOADER_LOG_FORMAT"
FORMATS = ("text", "json")
TEXT_FORMAT = "%(levelname)s: %(message)s"

_context_lock = threading.Lock()
_context: dict[str, Any] = {"kind": None, "name": None, "replica": None, "run_id": None, "execution_id": None, "incarnation": None, "runtime": None}
_configured = False
_ORIGINAL_ARGPARSE_ERROR = argparse.ArgumentParser.error


def requested_format(argv: list[str] | None = None) -> str:
    """The log format asked for by ``--log-format`` in ``argv`` (anywhere in it) or the environment; ``text`` otherwise."""
    value = None
    arguments = list(sys.argv[1:] if argv is None else argv)
    for index, argument in enumerate(arguments):
        if argument == "--log-format" and index + 1 < len(arguments):
            value = arguments[index + 1]
        elif argument.startswith("--log-format="):
            value = argument.split("=", 1)[1]
    value = (value if value is not None else os.environ.get(LOG_FORMAT_ENV, "")).strip().lower()
    return value if value in FORMATS else "text"


def strip_log_format(argv: list[str]) -> list[str]:
    """``argv`` without ``--log-format X`` (it is consumed before parsing, so any subcommand accepts it)."""
    result, skip = [], False
    for index, argument in enumerate(argv):
        if skip:
            skip = False
        elif argument == "--log-format":
            skip = True
        elif not argument.startswith("--log-format="):
            result.append(argument)
    return result


def json_enabled() -> bool:
    return os.environ.get(LOG_FORMAT_ENV, "").strip().lower() == "json"


def log_context() -> dict[str, Any]:
    with _context_lock:
        return dict(_context)


def set_log_context(**fields: Any) -> None:
    """Identity of this process for every following JSON line and metric sample (``kind``, ``name``, ``replica``, ``run_id``,
    ``execution_id``, ``incarnation``, ``runtime``); the environment supplies what is not set here."""
    with _context_lock:
        _context.update({key: value for key, value in fields.items() if key in _context})


def structured(stage: str, **fields: Any) -> dict:
    """``extra=`` for a log call: the event's ``stage`` and structured fields (ignored by the text formatter)."""
    return {"gl_event": {"stage": stage, "fields": fields}}


class StageLogger(logging.LoggerAdapter):
    """A logger whose every record carries one ``stage`` (for modules whose lines all belong to one documented stage)."""

    def __init__(self, logger: logging.Logger, stage: str):
        super().__init__(logger, {})
        self._stage = stage

    def process(self, msg, kwargs):
        extra = dict(kwargs.get("extra") or {})
        extra.setdefault("gl_event", {"stage": self._stage, "fields": {}})
        kwargs["extra"] = extra
        return msg, kwargs


def _redact(text: str) -> str:
    from neo4j_graph_loader.status.tracker import redact
    return redact(text)


def _scrub(value: Any) -> Any:
    if isinstance(value, str):
        return _redact(value)
    if isinstance(value, dict):
        return {str(key): _scrub(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub(item) for item in value]
    return value


def _timestamp(created: float) -> str:
    moment = datetime.fromtimestamp(created, timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


class JsonFormatter(logging.Formatter):
    """One JSON object per record (see the module docstring)."""

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "gl_event", None) or {}
        with _context_lock:
            context = dict(_context)
        entry: dict[str, Any] = {
            "ts": _timestamp(record.created), "level": record.levelname.lower(), "logger": record.name,
            "execution_id": context["execution_id"] or os.environ.get("GRAPH_LOADER_EXECUTION_ID"), "run_id": context["run_id"] or os.environ.get("GRAPH_LOADER_RUN_ID"),
            "kind": context["kind"], "name": context["name"], "replica": context["replica"],
            "incarnation": context["incarnation"] or os.environ.get("GRAPH_LOADER_INCARNATION"), "stage": event.get("stage"),
            "message": _redact(record.getMessage()),
        }
        for key, value in (event.get("fields") or {}).items():
            entry.setdefault(key, _scrub(value))
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = _redact("".join(traceback.format_exception(*record.exc_info)).rstrip())
        return json.dumps(entry, separators=(",", ":"), default=str)


def _json_error(parser: argparse.ArgumentParser, message: str) -> None:
    logging.getLogger("argparse").error("%s: error: %s", parser.prog, message, extra=structured("argument-error", prog=parser.prog))
    raise SystemExit(2)


def _excepthook(kind, value, trace) -> None:
    if issubclass(kind, KeyboardInterrupt):
        sys.__excepthook__(kind, value, trace)
        return
    logging.getLogger("uncaught").critical("uncaught exception: %s: %s", kind.__name__, value, exc_info=(kind, value, trace),
                                           extra=structured("uncaught-exception", exception_type=kind.__name__))


def _thread_excepthook(args) -> None:
    if args.exc_type is SystemExit:
        return
    logging.getLogger("uncaught").critical(
        "uncaught exception in thread %s: %s: %s", getattr(args.thread, "name", "?"), args.exc_type.__name__, args.exc_value,
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        extra=structured("uncaught-exception", exception_type=args.exc_type.__name__, thread=getattr(args.thread, "name", None)))


def configure_logging(kind: str | None = None, name: str | None = None, replica: int | str | None = None, *,
                      argv: list[str] | None = None, stream=None, level: int = logging.INFO, force: bool = False,
                      text_configures: bool = True) -> str:
    """Configure the root logger for this process; call it first in every entrypoint. Returns the format in force.

    ``text``: ``logging.basicConfig(level=INFO, format="LEVEL: message")``, exactly as every entrypoint did before
    (nothing at all for an entrypoint that passes ``text_configures=False`` because it never configured logging).
    ``json``: one JSON object per line on ``stream`` (default stderr), plus uncaught-exception and argparse-error routing.
    """
    global _configured
    chosen = requested_format(argv)
    if chosen == "json":
        os.environ[LOG_FORMAT_ENV] = "json"                       # the roles and children this process starts inherit the choice
    set_log_context(kind=kind, name=name, replica=replica)
    if chosen == "text":
        if not text_configures:
            return "text"                                          # an entrypoint that never configured text logging keeps it so
        logging.basicConfig(level=level, format=TEXT_FORMAT, **({"stream": stream} if stream is not None else {}), force=force)
        return "text"
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
    logging.captureWarnings(True)
    from neo4j_graph_loader.utils import kafka_logging
    kafka_logging.install()                                        # (this package's own entrypoint processes only)
    sys.excepthook = _excepthook
    threading.excepthook = _thread_excepthook
    argparse.ArgumentParser.error = _json_error                    # (process-wide, JSON mode only)
    _configured = True
    return "json"


def run_entrypoint(main, *args):
    """``main(*args)`` for an entrypoint's ``raise SystemExit(...)``: in JSON mode a ``SystemExit`` carrying a message (which the
    interpreter would print raw on stderr) becomes one error log line and exit status 1; text mode re-raises untouched."""
    try:
        return main(*args)
    except SystemExit as exc:
        if json_enabled() and isinstance(exc.code, str):
            logging.getLogger("exit").error(exc.code, extra=structured("exit"))
            return 1
        raise


def reset_for_tests() -> None:
    """Undo what JSON mode changed process-wide (tests only): environment, hooks, argparse, handlers, warnings capture."""
    global _configured
    os.environ.pop(LOG_FORMAT_ENV, None)
    from neo4j_graph_loader.utils import kafka_logging
    kafka_logging.uninstall()
    sys.excepthook = sys.__excepthook__
    threading.excepthook = threading.__excepthook__
    argparse.ArgumentParser.error = _ORIGINAL_ARGPARSE_ERROR
    logging.captureWarnings(False)
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler.formatter, JsonFormatter):
            root.removeHandler(handler)
    with _context_lock:
        _context.update({key: None for key in _context})
    _configured = False


def emit_json_line(stream, message: str, *, level: str = "info", logger: str = "stderr", stage: str | None = None, **fields: Any) -> None:
    """One JSON log line written straight to ``stream`` (for code that runs before logging exists, such as an early signal
    handler, or that must use a raw file descriptor)."""
    entry = {"ts": _timestamp(time.time()), "level": level, "logger": logger,
             "execution_id": os.environ.get("GRAPH_LOADER_EXECUTION_ID"), "run_id": os.environ.get("GRAPH_LOADER_RUN_ID"),
             "kind": _context["kind"], "name": _context["name"], "replica": _context["replica"],
             "incarnation": os.environ.get("GRAPH_LOADER_INCARNATION"), "stage": stage, "message": _redact(message), **fields}
    stream.write(json.dumps(entry, separators=(",", ":"), default=str) + "\n")


__all__ = ["LOG_FORMAT_ENV", "JsonFormatter", "StageLogger", "configure_logging", "emit_json_line", "json_enabled",
           "requested_format", "reset_for_tests", "run_entrypoint", "set_log_context", "strip_log_format", "structured"]

