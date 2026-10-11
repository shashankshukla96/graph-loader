"""Planned bulk relationship writer: dispatcher-granted chunks, one transaction each.

A writer process connects to the run's planned dispatcher and loops: ``NEXT``
grants one chunk (its resources are now exclusively held), the writer fetches
that chunk's Kafka message by ``(partition, offset)`` -- usually already
prefetched from the previous grant's hint while the last transaction ran --
validates it, runs ONE MERGE transaction with the validated single-statement
query (no APOC lock step and no row reordering: concurrently running chunks
are node-disjoint), and reports ``DONE`` only after the commit returned.

Validation is fail-closed: the message header must name this plan and chunk,
the record count must equal the planned rows, every record must pass the
relationship schema (the same normalization as the per-type loader), every
endpoint must map to one of the chunk's resources, and the statement must
resolve exactly one source and one target node per row.  Any failure sends
``FAIL`` and exits nonzero without writing; the dispatcher then fails the run.
"""
from __future__ import annotations

import argparse
from concurrent.futures import Future
from dataclasses import dataclass
import logging
import math
import os
from queue import Queue
import re
import signal
import socket
from threading import Lock, Thread
import time
from typing import Callable, Mapping, NoReturn

from src.loader.edge_plan import (
    DEFAULT_MAX_MESSAGE_BYTES, EdgePlan, EdgePlanError, PlannedChunk, decode_chunk_message, read_plan_file,
)
from src.models.schema import EdgeConfig
from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)

DEFAULT_NEXT_WAIT_MS = 1000
_TOKEN = re.compile(r"[A-Za-z0-9_.-]{1,64}")


# Reason the orchestrator sends with ABORT for an operator/signal stop; a
# writer that hears it between chunks exits 0 (any other abort is a failure).
STOP_REASON = "stop requested"
_STOP_REPLY = f"ERR failed aborted by orchestrator: {STOP_REASON}"


class DispatcherStopRequested(RuntimeError):
    """The orchestrator aborted the run because of an operator/signal stop."""


class DispatcherError(RuntimeError):
    """The dispatcher is unreachable, failed, or replied unexpectedly."""


def parse_dispatcher_address(address: str) -> tuple[str, int]:
    if not isinstance(address, str) or ":" not in address:
        raise ValueError("dispatcher address must be host:port")
    host, _, port_text = address.rpartition(":")
    if not host.strip() or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
        raise ValueError("dispatcher address must be host:port")
    return host.strip("[]"), int(port_text)


class DispatcherClient:
    """One persistent, strictly request/response dispatcher connection.

    ``writer_id`` selects the writer role; without it the client is a
    control connection (orchestrator).  Not thread-safe.
    """

    def __init__(self, address: str, *, digest: str, writer_id: str | None = None,
                 connect_timeout_seconds: float = 60.0, op_timeout_seconds: float = 30.0,
                 connect: Callable[..., socket.socket] = socket.create_connection,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> None:
        self._host, self._port = parse_dispatcher_address(address)
        self._address = address
        if writer_id is not None and not _TOKEN.fullmatch(writer_id):
            raise ValueError("writer id must be 1-64 letters, digits, dot, underscore, or hyphen")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("plan digest must be a sha256 hex digest")
        self._digest, self._writer_id = digest, writer_id
        self._connect_timeout, self._op_timeout = connect_timeout_seconds, op_timeout_seconds
        self._connect, self._sleep, self._clock = connect, sleep, clock
        self._socket: socket.socket | None = None
        self._reader = None
        self._broken = False
        self.plan_id: str | None = None

    @property
    def broken(self) -> bool:
        return self._broken

    def connect(self) -> str:
        """Connect (retrying until the connect deadline) and say HELLO; return the plan id."""
        if self._socket is not None:
            return self.plan_id or ""
        if self._broken:
            raise DispatcherError(f"dispatcher connection to {self._address} is broken")
        hello = (
            f"HELLO WRITER {self._writer_id} {self._digest}" if self._writer_id is not None
            else f"HELLO CONTROL {self._digest}"
        )
        deadline = self._clock() + self._connect_timeout
        while True:
            # A published Docker port accepts TCP before the dispatcher
            # listens and then resets it, so a connection that closes before
            # the HELLO reply is retried like a refused connect.
            sock = reader = None
            try:
                sock = self._connect((self._host, self._port), timeout=self._op_timeout)
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
                reader = sock.makefile("rb")
                sock.settimeout(self._op_timeout)
                sock.sendall(hello.encode("ascii") + b"\n")
                raw = reader.readline()
                if not raw.endswith(b"\n"):
                    raise ConnectionError("connection closed before the HELLO reply")
                break
            except (OSError, ValueError) as exc:
                for item in (reader, sock):
                    if item is not None:
                        try:
                            item.close()
                        except OSError:
                            pass
                if self._clock() >= deadline:
                    self._broken = True
                    raise DispatcherError(f"dispatcher {self._address} unreachable: {exc}") from exc
                self._sleep(0.2)
        self._socket, self._reader = sock, reader
        reply = raw.decode("ascii", "replace").strip()
        head, _, plan_id = reply.partition(" ")
        if head != "OK" or not plan_id:
            self._fail(f"HELLO rejected: {reply}")
        self.plan_id = plan_id
        return plan_id

    def next(self, wait_ms: int) -> tuple[int, int | None] | str:
        """Return ``(chunk, hint)``, ``"WAIT"`` or ``"END"``."""
        reply = self._request(f"NEXT {int(wait_ms)}", extra_timeout=wait_ms / 1000)
        if reply in ("WAIT", "END"):
            return reply
        parts = reply.split()
        if len(parts) == 3 and parts[0] == "OK" and parts[1].isdigit() and (parts[2] == "-" or parts[2].isdigit()):
            return int(parts[1]), None if parts[2] == "-" else int(parts[2])
        if reply.startswith(_STOP_REPLY):
            self._broken = True
            self.close()
            raise DispatcherStopRequested(reply)
        self._fail(f"unexpected NEXT reply: {reply}")

    def done(self, chunk_id: int, rows: int, write_ms: float) -> None:
        self._expect_ok(self._request(f"DONE {int(chunk_id)} {int(rows)} {write_ms:.3f}"), "DONE")

    def fail(self, chunk_id: int | None, reason: str) -> None:
        text = " ".join(str(reason).encode("ascii", "replace").decode("ascii").split())[:400] or "-"
        self._expect_ok(self._request(f"FAIL {'-' if chunk_id is None else int(chunk_id)} {text}"), "FAIL")

    def ready(self, labels) -> None:
        labels = sorted(set(labels))
        if labels:
            self._expect_ok(self._request("READY " + " ".join(labels)), "READY")

    def abort(self, reason: str) -> None:
        text = " ".join(str(reason).encode("ascii", "replace").decode("ascii").split())[:400] or "-"
        self._expect_ok(self._request(f"ABORT {text}"), "ABORT")

    def status(self) -> dict:
        return self._json("STATUS")

    def summary(self) -> dict:
        return self._json("SUMMARY")

    def _json(self, command: str) -> dict:
        import json
        reply = self._request(command)
        head, _, body = reply.partition(" ")
        if head != "OK":
            self._fail(f"unexpected {command} reply: {reply[:200]}")
        try:
            payload = json.loads(body)
        except ValueError:
            self._fail(f"unexpected {command} reply: {reply[:200]}")
        if not isinstance(payload, dict):
            self._fail(f"unexpected {command} reply: {reply[:200]}")
        return payload

    def close(self) -> None:
        sock, reader = self._socket, self._reader
        self._socket = self._reader = None
        for item in (reader, sock):
            if item is not None:
                try:
                    item.close()
                except OSError:
                    pass

    def _expect_ok(self, reply: str, command: str) -> None:
        if reply != "OK":
            self._fail(f"unexpected {command} reply: {reply}")

    def _fail(self, reason: str) -> NoReturn:
        self._broken = True
        self.close()
        raise DispatcherError(f"dispatcher {self._address}: {reason}")

    def _request(self, line: str, *, extra_timeout: float = 0.0) -> str:
        if self._socket is None:
            if self._broken:
                raise DispatcherError(f"dispatcher connection to {self._address} is broken")
            self.connect()
        assert self._socket is not None and self._reader is not None
        try:
            self._socket.settimeout(self._op_timeout + extra_timeout)
            self._socket.sendall(line.encode("ascii") + b"\n")
            raw = self._reader.readline()
        except (OSError, ValueError) as exc:
            self._fail(f"request failed: {exc}")
        if not raw.endswith(b"\n"):
            self._fail("connection closed")
        return raw.decode("ascii").strip()


# -- record validation ----------------------------------------------------------


_FAIL = object()


def _fast_value(property_type: str):
    """Return ``value -> normalized value | _FAIL`` for one schema scalar type.

    Accepts exactly what :func:`~src.loader.record_validation.normalize_property_value`
    accepts and returns the identical result; anything else yields ``_FAIL``
    so the caller re-runs the reference normalization for its exact error.
    """
    if property_type == "string":
        return lambda value: value if type(value) is str else _FAIL
    if property_type == "integer":
        return lambda value: value if type(value) is int else _FAIL

    if property_type == "float":
        isfinite = math.isfinite

        def to_float(value):
            kind = type(value)
            if kind is float:
                return value if isfinite(value) else _FAIL
            if kind is int:
                try:
                    converted = float(value)
                except OverflowError:
                    return _FAIL
                return converted if isfinite(converted) else _FAIL
            return _FAIL
        return to_float

    from src.loader.record_validation import normalize_property_value
    from src.models.schema import PropertyConfig

    config = PropertyConfig(type=property_type)

    def temporal(value):
        try:
            return normalize_property_value(value, config, entity_description="", property_name="",
                                            error_factory=ValueError)
        except ValueError:
            return _FAIL
    return temporal


def compile_record_normalizer(edge_config: EdgeConfig) -> Callable[[object], tuple[object, object, dict]]:
    """Return ``raw -> (source_key, target_key, properties)`` equal to ``normalize_edge_record``.

    Records in the exact canonical shape (three sections, one key per
    endpoint, declared properties only) take a direct path with the
    identical per-type checks for every schema type; anything else,
    including every invalid record, goes through
    :func:`~src.loader.edge_records.normalize_edge_record` itself, so results
    and errors are the reference ones.
    """
    from src.loader.edge_records import normalize_edge_record

    edge_config.single_pair  # v7-S7: a multi-pair parent raises MultiPairEdgeError; pass a pair view
    source_property, target_property = edge_config.source_key_property, edge_config.target_key_property
    declared = edge_config.properties
    declared_names = frozenset(declared)
    source_config, target_config = edge_config.source_key_config, edge_config.target_key_config
    fast = source_config is not None and target_config is not None
    if fast:
        source_value, target_value = _fast_value(source_config.type), _fast_value(target_config.type)
        fields = tuple((name, config.required, _fast_value(config.type)) for name, config in declared.items())
    fail = _FAIL

    def normalize(raw: object) -> tuple[object, object, dict]:
        if fast and type(raw) is dict and len(raw) == 3:
            source, target, properties = raw.get("source"), raw.get("target"), raw.get("properties")
            if (
                type(source) is dict and type(target) is dict and type(properties) is dict
                and len(source) == 1 and len(target) == 1
            ):
                source_key, target_key = source.get(source_property), target.get(target_property)
                if source_key is not None and target_key is not None and properties.keys() <= declared_names:
                    source_key, target_key = source_value(source_key), target_value(target_key)
                    if source_key is not fail and target_key is not fail:
                        normalized = {}
                        for name, required, convert in fields:
                            value = properties.get(name)
                            if value is None:
                                if required:
                                    break
                                continue
                            value = convert(value)
                            if value is fail:
                                break
                            normalized[name] = value
                        else:
                            return source_key, target_key, normalized
        record = normalize_edge_record(raw, edge_config)
        return record.source_key, record.target_key, record.properties

    return normalize


def writer_for(writers: Mapping, chunk: PlannedChunk):
    """The writer of ``chunk``: keyed by type, or by ``(type, pair)`` for a type of several label pairs."""
    writer = writers.get(chunk.edge_type)
    return writer if writer is not None else writers[(chunk.edge_type, chunk.pair)]


def build_planned_writers(plan: EdgePlan, edges: Mapping[str, EdgeConfig], make) -> dict:
    """``make(edge_config)`` for every planned type; one per label pair (from its view) for a type
    of several label pairs (v7-S7), keyed ``(type, pair)``."""
    writers: dict = {}
    for name, item in plan.types.items():
        if item.is_multi_pair:
            for index in range(len(item.pairs)):
                writers[(name, index)] = make(edges[name].pair_view(index))
        else:
            writers[name] = make(edges[name])
    return writers


class ChunkDecoder:
    """Decode and fully validate one chunk message against the plan."""

    def __init__(self, plan: EdgePlan, edge_configs: Mapping[str, EdgeConfig]) -> None:
        self._plan = plan
        self._normalizers = {}
        for name, item in plan.types.items():
            config = edge_configs.get(name)
            if config is None:
                raise EdgePlanError(f"planned edge type {name} is not in the schema")
            if item.is_multi_pair or config.is_multi_pair:
                # v7-S7: one normaliser per label pair, keyed (type, pair), from the pair's view.
                if [pair.labels for pair in item.pairs] != [pair.labels for pair in config.endpoint_pairs]:
                    raise EdgePlanError(f"planned edge type {name} label pairs differ from the schema")
                for index in range(len(item.pairs)):
                    self._normalizers[(name, index)] = compile_record_normalizer(config.pair_view(index))
                continue
            if (config.nodes.source, config.nodes.target) != (item.source_label, item.target_label):
                raise EdgePlanError(f"planned edge type {name} endpoint labels differ from the schema")
            self._normalizers[name] = compile_record_normalizer(config)

    def decode(self, chunk: PlannedChunk, value: bytes | None) -> list[dict]:
        header, records = decode_chunk_message(value)
        if header.get("run") != self._plan.plan_id or header.get("chunk") != chunk.key:
            raise EdgePlanError(f"message at {chunk.partition}:{chunk.offset} is not chunk {chunk.id} of this plan")
        if header.get("type") != chunk.edge_type or len(records) != chunk.rows:
            raise EdgePlanError(f"chunk {chunk.id} message holds {len(records)} {header.get('type')} records; "
                                f"plan has {chunk.rows} {chunk.edge_type}")
        normalize = self._normalizers.get(chunk.edge_type) or self._normalizers[(chunk.edge_type, chunk.pair)]
        source_label, target_label = self._plan.labels_of(chunk)
        resource = self._plan.resources.resource
        allowed = frozenset(chunk.resources)
        rows = []
        for raw in records:
            source_key, target_key, properties = normalize(raw)
            if resource(source_label, source_key) not in allowed or resource(target_label, target_key) not in allowed:
                raise EdgePlanError(f"chunk {chunk.id} has a record outside its planned resources")
            rows.append({"source_key": source_key, "target_key": target_key, "properties": properties})
        return rows


# -- Kafka fetching with prefetch -------------------------------------------------


def planned_consumer_config(bootstrap_servers: str, group_id: str,
                            max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES) -> dict:
    """Consumer settings for random-access chunk fetches (no group, no commits)."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
        # Fetch exactly one chunk per request: every chunk is its own record
        # batch, and a batch larger than the per-partition limit is still
        # returned whole, so a tiny limit stops read-ahead of neighbours
        # (the fetcher also unassigns as soon as the chunk arrived).
        "max.partition.fetch.bytes": 1024,
        "fetch.max.bytes": max_message_bytes + (1 << 20),
        "receive.message.max.bytes": max(max_message_bytes + (2 << 20), 100_000_000),
        "queued.min.messages": 1,
        "queued.max.messages.kbytes": 1024,
        "fetch.wait.max.ms": 10,
        # Once the one-message queue threshold is met librdkafka postpones
        # the partition's next fetch by this backoff (default 1000 ms), which
        # a re-assign for the next chunk would otherwise inherit.
        "fetch.queue.backoff.ms": 0,
        "fetch.error.backoff.ms": 20,
    }


@dataclass
class FetchedChunk:
    rows: list[dict]
    fetch_ms: float


class ChunkFetcher:
    """Fetch and decode chunk messages on one background thread.

    ``get(chunk)`` returns the decoded rows, waiting for an earlier
    ``prefetch(chunk)`` if one is in flight.  At most ``cache_limit``
    unclaimed prefetches are kept.
    """

    def __init__(self, consumer, topic: str, decoder: ChunkDecoder, *, fetch_timeout_seconds: float = 60.0,
                 poll_seconds: float = 0.2, cache_limit: int = 2, clock: Callable[[], float] = time.monotonic) -> None:
        self._consumer, self._topic, self._decoder = consumer, topic, decoder
        self._timeout, self._poll, self._clock = fetch_timeout_seconds, poll_seconds, clock
        self._cache_limit = cache_limit
        self._lock = Lock()
        self._futures: dict[int, Future] = {}
        self._requests: Queue = Queue()
        self._thread = Thread(target=self._run, name="planned-chunk-fetcher", daemon=True)
        self._thread.start()
        self.fetches = 0
        self.prefetch_hits = 0

    def _request(self, chunk: PlannedChunk) -> tuple[Future, bool]:
        with self._lock:
            future = self._futures.get(chunk.id)
            if future is not None:
                return future, True
            future = self._futures[chunk.id] = Future()
        self._requests.put((chunk, future))
        return future, False

    def prefetch(self, chunk: PlannedChunk) -> None:
        with self._lock:
            if chunk.id in self._futures:
                return
            while len(self._futures) >= self._cache_limit:
                oldest = next(iter(self._futures))
                self._futures.pop(oldest).cancel()
        self._request(chunk)

    def get(self, chunk: PlannedChunk) -> tuple[FetchedChunk, bool]:
        """Return ``(fetched chunk, was_prefetched)``; fetch errors propagate."""
        future, prefetched = self._request(chunk)
        try:
            result = future.result(timeout=self._timeout + 5)
        finally:
            with self._lock:
                if self._futures.get(chunk.id) is future:
                    del self._futures[chunk.id]
        if prefetched:
            self.prefetch_hits += 1
        return result, prefetched

    def close(self) -> None:
        self._requests.put(None)
        self._thread.join(timeout=10)

    def _run(self) -> None:
        while True:
            item = self._requests.get()
            if item is None:
                return
            chunk, future = item
            if not future.set_running_or_notify_cancel():
                continue
            started = self._clock()
            try:
                value = self._fetch(chunk)
                rows = self._decoder.decode(chunk, value)
                future.set_result(FetchedChunk(rows, (self._clock() - started) * 1000))
            except BaseException as exc:
                future.set_exception(exc)

    def _fetch(self, chunk: PlannedChunk) -> bytes:
        from confluent_kafka import TopicPartition

        self.fetches += 1
        self._consumer.assign([TopicPartition(self._topic, chunk.partition, chunk.offset)])
        try:
            return self._poll_chunk(chunk)
        finally:
            # Stop the partition fetcher at once: nothing after this chunk is needed.
            self._consumer.unassign()

    def _poll_chunk(self, chunk: PlannedChunk) -> bytes:
        deadline = self._clock() + self._timeout
        while True:
            message = self._consumer.poll(self._poll)
            if message is None:
                if self._clock() >= deadline:
                    raise EdgePlanError(
                        f"chunk {chunk.id} message {self._topic}:{chunk.partition}:{chunk.offset} not fetched "
                        f"within {self._timeout:.0f}s"
                    )
                continue
            if message.error():
                raise EdgePlanError(f"chunk {chunk.id} fetch failed: {message.error()}")
            if message.topic() != self._topic or message.partition() != chunk.partition:
                continue
            if message.offset() < chunk.offset:
                continue
            if message.offset() > chunk.offset:
                raise EdgePlanError(
                    f"chunk {chunk.id} message {chunk.partition}:{chunk.offset} is missing from Kafka"
                )
            return message.value()


# -- the writer loop ------------------------------------------------------------------


class _WriterShutdownRequested(Exception):
    """Raised only by the shutdown check in :meth:`PlannedEdgeWriterWorker.run`.

    P6-S5 Story 4: a distinct type (not a message-text sniff) lets
    ``run()``'s own except block tell "asked to stop, drained cleanly" apart
    from every real chunk-write failure, so a graceful shutdown reports
    ``DONE``/no-FAIL and exits 0 instead of being indistinguishable from a
    genuine failure.
    """


class PlannedEdgeWriterWorker:
    """Pull granted chunks until the dispatcher reports END."""

    def __init__(self, plan: EdgePlan, client: DispatcherClient, fetcher: ChunkFetcher,
                 writers: Mapping[str, object], *, writer_id: str, wait_ms: int = DEFAULT_NEXT_WAIT_MS,
                 prefetch: bool = True, event_logger: logging.Logger = logger,
                 shutdown_requested: Callable[[], bool] = lambda: False,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None) -> None:
        self._plan, self._client, self._fetcher, self._writers = plan, client, fetcher, writers
        self._writer_id, self._wait_ms, self._prefetch = writer_id, wait_ms, prefetch
        self._logger, self._shutdown = event_logger, shutdown_requested
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self.chunks = 0
        self.rows = 0

    def run(self) -> int:
        chunk_id: int | None = None
        try:
            self._client.connect()
            waited = 0.0
            while True:
                if self._shutdown():
                    raise _WriterShutdownRequested("writer shutdown requested before the plan completed")
                asked = time.perf_counter()
                reply = self._client.next(self._wait_ms)
                waited += time.perf_counter() - asked
                if reply == "WAIT":
                    continue
                if reply == "END":
                    break
                chunk_id, hint = reply
                chunk = self._plan.chunks[chunk_id]
                fetch_started = time.perf_counter()
                fetched, prefetched = self._fetcher.get(chunk)
                fetch_wait_ms = (time.perf_counter() - fetch_started) * 1000
                if self._prefetch and hint is not None:
                    self._fetcher.prefetch(self._plan.chunks[hint])
                write_started = time.perf_counter()
                attempts = writer_for(self._writers, chunk).write_rows(
                    fetched.rows, metadata={"graph_loader_plan": self._plan.plan_id, "chunk": chunk.id},
                )
                write_ms = (time.perf_counter() - write_started) * 1000
                self._client.done(chunk.id, len(fetched.rows), write_ms)
                self.chunks += 1
                self.rows += len(fetched.rows)
                # Per-chunk, not per-row (P6-S2 Story 6). `name` here is
                # the *chunk's* edge type, chosen per-call - a planned
                # writer serves every type in the plan across its
                # lifetime, unlike node/edge loaders whose `name` is fixed
                # at process start. Routed through safe_emit: this sits
                # inside the same broad `try:` that surrounds the whole
                # loop body, so a metrics exception here must never be
                # mistaken for a chunk write failure.
                metrics_module.safe_emit(lambda: self._metrics.write_duration.labels(
                    kind="planned-writer", name=chunk.edge_type, stage="edge-write"
                ).observe(write_ms / 1000.0))
                metrics_module.safe_emit(lambda: self._metrics.records_consumed.labels(
                    kind="planned-writer", name=chunk.edge_type, label_or_type=chunk.edge_type,
                ).inc(len(fetched.rows)))
                self._logger.info(
                    "stage=edge-write edge=%s topic=%s planned=1 writer=%s chunk=%s records=%s "
                    "write_ms=%.3f lease_wait_ms=%.3f fetch_wait_ms=%.3f fetch_ms=%.3f prefetched=%d attempts=%s",
                    chunk.edge_type, self._plan.topic, self._writer_id, chunk.id, len(fetched.rows), write_ms,
                    waited * 1000, fetch_wait_ms, fetched.fetch_ms, int(prefetched), attempts,
                )
                chunk_id, waited = None, 0.0
        except _WriterShutdownRequested:
            # P6-S5 Story 4: a shutdown is only ever observed here, before
            # requesting a new chunk (chunk_id is always None at this
            # point - reset after every successfully completed chunk, and
            # never set for one that hasn't been granted yet), so there is
            # never a chunk to report FAIL for. A clean, requested stop is
            # not a failure: no `self._client.fail(...)` call, exit 0.
            self._logger.info(
                "stage=planned-writer writer=%s reason=shutdown requested chunk=-",
                self._writer_id,
            )
            return 0
        except DispatcherStopRequested:
            # The orchestrator aborted for an operator/signal stop.  NEXT is
            # only asked between chunks (chunk_id is None), so nothing is
            # half-applied: a clean stop, exit 0, no FAIL.
            self._logger.info(
                "stage=planned-writer writer=%s reason=orchestrator stop requested chunk=-", self._writer_id,
            )
            return 0
        except Exception as exc:
            self._logger.error(
                "stage=planned-writer writer=%s chunk=%s reason=%s: %s",
                self._writer_id, "-" if chunk_id is None else chunk_id, type(exc).__name__, exc,
            )
            try:
                if not self._client.broken:
                    self._client.fail(chunk_id, f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            return 1
        self._logger.info(
            "stage=planned-writer-complete writer=%s chunks=%s rows=%s fetches=%s prefetch_hits=%s",
            self._writer_id, self.chunks, self.rows, self._fetcher.fetches, self._fetcher.prefetch_hits,
        )
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one planned bulk relationship writer")
    parser.add_argument("--config", default="config/graph_schema.yaml")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--dispatcher", required=True, help="dispatcher host:port")
    parser.add_argument("--writer-id", required=True)
    parser.add_argument("--wait-ms", type=int, default=DEFAULT_NEXT_WAIT_MS)
    parser.add_argument("--no-prefetch", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    from confluent_kafka import Consumer

    from src.cli import get_neo4j_credentials
    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.edge_loader import MissingRelationshipEndpointError
    from src.loader.write_timing_capture import configure_write_timing_capture
    from src.orchestrator.schema_initializer import get_neo4j_driver
    from src.utils.schema_loader import load_schema

    args = build_parser().parse_args(argv)
    configure_write_timing_capture(logger, "edge-write")
    if not 0 <= args.wait_ms <= 60_000:
        raise SystemExit("--wait-ms must be 0-60000")
    stop = {"requested": False}
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.__setitem__("requested", True))
    driver = consumer = fetcher = client = None
    process_metrics = metrics_module.process_metrics()
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="planned-writer", name="-")
    metrics_module.initialize_zero_series(process_metrics, kind="planned-writer", name="-")
    try:
        schema = load_schema(args.config)
        plan, digest = read_plan_file(args.plan)
        decoder = ChunkDecoder(plan, {edge.type: edge for edge in schema.edges})
        consumer = Consumer(planned_consumer_config(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            f"graph-loader-planned-{plan.plan_id}-{args.writer_id}",
            int(plan.settings.get("max_message_bytes", DEFAULT_MAX_MESSAGE_BYTES)),
        ))
        fetcher = ChunkFetcher(consumer, plan.topic, decoder)
        driver = get_neo4j_driver(*get_neo4j_credentials())
        edges = {edge.type: edge for edge in schema.edges}
        # P5-S1 Story 4 / P5-Q1 fencing amendment: planned mode has no
        # bucket-lease grant (no long-lived restartable TCP coordinator in
        # this slice's scope), so it gets a flat per-attempt T rather than
        # a per-grant deadline -- ApocLockedEdgeWriter.write_rows uses
        # self._tx_timeout_seconds directly whenever deadline=None.
        tx_timeout_seconds = schema.loading.bucket_lease.tx_timeout_ms / 1000
        writers = build_planned_writers(plan, edges, lambda config: ApocLockedEdgeWriter(
            driver, config, explicit_locks=False, sort_rows=False,
            missing_endpoint_error_factory=MissingRelationshipEndpointError,
            metrics=process_metrics, kind="planned-writer",
            tx_timeout_seconds=tx_timeout_seconds,
        ))
        client = DispatcherClient(args.dispatcher, digest=digest, writer_id=args.writer_id)
        logger.info(
            "stage=planned-writer-start writer=%s plan_id=%s chunks=%s dispatcher=%s prefetch=%s",
            args.writer_id, plan.plan_id, len(plan.chunks), args.dispatcher, not args.no_prefetch,
        )
        worker = PlannedEdgeWriterWorker(
            plan, client, fetcher, writers, writer_id=args.writer_id, wait_ms=args.wait_ms,
            prefetch=not args.no_prefetch, shutdown_requested=lambda: stop["requested"],
            metrics=process_metrics,
        )
        return worker.run()
    except Exception as exc:
        logger.error("stage=planned-writer writer=%s reason=unable to start: %s", args.writer_id, exc)
        return 1
    finally:
        for closer in (
            (lambda: client.close()) if client is not None else None,
            (lambda: fetcher.close()) if fetcher is not None else None,
            (lambda: consumer.close()) if consumer is not None else None,
            (lambda: driver.close()) if driver is not None else None,
        ):
            if closer is not None:
                try:
                    closer()
                except Exception as exc:  # pragma: no cover - best effort
                    logger.warning("stage=planned-writer shutdown step failed: %s", exc)
        metrics_module.write_metrics_snapshot(
            process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
        )
        metrics_module.stop_metrics_server(metrics_server)
        # F2: last statement of the shutdown path: CPython resets every Python-level handler to
        # SIG_DFL at interpreter exit (only SIG_IGN survives), so a repeated SIGTERM from a second
        # container stop would otherwise exit 143 during teardown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
