"""Tiny health endpoint for the process runtime: ``/healthz`` (liveness) and ``/readyz`` (readiness).

Off unless ``--health-addr`` is given. It binds the address it is given (loopback in the host shape; ``0.0.0.0`` is the
operator's choice for a pod so probes can reach it) and serves two paths, nothing else, with no request body handling.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable


def parse_address(text: str, *, default_host: str = "127.0.0.1") -> tuple[str, int]:
    """``HOST:PORT`` or ``:PORT`` or ``PORT`` -> (host, port); port 0 asks the OS for one."""
    host, sep, port = text.rpartition(":")
    if not sep:
        host, port = "", text
    try:
        number = int(port)
    except ValueError:
        raise ValueError(f"invalid address {text!r}: expected HOST:PORT") from None
    if not 0 <= number <= 65535:
        raise ValueError(f"invalid port in address {text!r}")
    return (host or default_host), number


class HealthServer:
    """``alive()`` -> bool and ``ready()`` -> (bool, detail dict) are evaluated on every request."""

    def __init__(self, address: str, alive: Callable[[], bool], ready: Callable[[], tuple[bool, dict]]):
        host, port = parse_address(address)

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):          # no access log
                pass

            def _reply(self, ok: bool, body: dict, status: int | None = None) -> None:
                payload = (json.dumps(body, sort_keys=True) + "\n").encode()
                self.send_response(status or (200 if ok else 503))
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                try:
                    if self.path == "/healthz":
                        ok = bool(alive())
                        self._reply(ok, {"status": "ok" if ok else "unhealthy"})
                    elif self.path == "/readyz":
                        ok, detail = ready()
                        self._reply(bool(ok), {"status": "ready" if ok else "not ready", **detail})
                    else:
                        self._reply(False, {"status": "not found"}, 404)
                except Exception as exc:           # a failing probe is "unhealthy", never a crashed server
                    self._reply(False, {"status": "error", "error": type(exc).__name__})

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self.host, self.port = host, self._server.server_port
        self._thread = threading.Thread(target=self._server.serve_forever, name="health-server", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
