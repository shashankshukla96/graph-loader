"""Routing of librdkafka's own log lines into Python logging, for JSON log mode only (E15.F1.S7).

In text mode nothing here runs and librdkafka keeps writing to stderr in its own format, exactly as before. In JSON mode
:func:`install` replaces ``confluent_kafka.Consumer`` and ``Producer`` by thin subclasses and wraps the ``__init__`` of
``admin.AdminClient`` (a Python class that refers to itself by its module-global name, so it cannot be subclassed in place);
each constructor adds the ``logger`` setting, so EVERY client factory in the code base (module-level and deferred imports alike)
logs through the ``librdkafka`` logger and therefore through the JSON handler.

librdkafka hands log records to the application only when the client is polled (verified on confluent-kafka 2.5.3 and
the current release: a client that is never polled never logs). Every client factory in the code base and how its log
callbacks are drained (a drift guard in ``tests/test_json_logs.py`` fails when a factory is added without a line here):

``owner``: the client subscribes or is assigned, or a producer passes delivery callbacks; its owner's own ``poll`` /
``flush`` loop serves the log records too, and this module never polls it (a ``poll`` would consume records or move
callbacks to another thread). ``drain``: the client never subscribes or assigns (metadata-only consumers, admin clients,
producers without callbacks); ONE daemon thread calls ``poll(0)`` on it every 0.5 s while it lives, and ``close()`` /
``flush()`` drain it once more. A one-shot client that lives under half a second may log nothing.

* ``loader/node_loader.py``: consumer ``owner`` (the load loop), producer ``owner`` (control events, callbacks, ``flush``).
* ``loader/edge_loader.py``: consumer and coordination consumer ``owner``; producer ``owner``.
* ``loader/planned_writer.py``: consumer ``owner`` (the chunk loop polls it).
* ``loader/retry_consumer.py``: consumer ``owner``; the backlog check's consumer factory: ``drain`` (metadata only).
* ``loader/edge_planner.py``: scan consumers ``owner``; plan producer ``owner`` (callbacks); admin clients ``drain``.
* ``loader/dead_letter.py``, ``dead_letter_replay.py``, ``parked_pair_index.py``: producers ``owner`` (callbacks, ``flush``),
  consumers ``owner`` (assigned and polled), admin clients ``drain``.
* ``orchestrator/bulk_monitor.py``: watermark consumer ``drain``; control consumer ``owner``; admin client ``drain``.
* ``orchestrator/relationship_bulk_monitor.py``: watermark consumer ``drain`` (metadata only: it is never subscribed); control
  and coordination consumers ``owner``; admin client ``drain``.
* ``orchestrator/planned_edges.py``: plan-topic consumer and admin client ``drain``.
* ``orchestrator/clock_runner.py``: producer ``owner`` (``flush``). ``orchestrator/auto_settings.py``: ``drain``.
* ``cli.py``: its consumers (watermark and lag reads) ``drain``; admin clients ``drain``.
* ``status/lag.py`` (``status --refresh-lag``): consumer and admin client ``drain``; ``_docker_preflight.py``: admin ``drain``.
"""
from __future__ import annotations

import logging
import sys
import threading
import weakref

DRAIN_INTERVAL_SECONDS = 0.5
logger = logging.getLogger("librdkafka")
_installed = False
_originals: dict[str, object] = {}
_registry: "weakref.WeakSet" = weakref.WeakSet()
_registry_lock = threading.Lock()
_thread: threading.Thread | None = None


def _with_logger(config):
    if isinstance(config, dict) and "logger" not in config:
        return {**config, "logger": logger}
    return config


def _drain_loop() -> None:
    import time
    while True:
        time.sleep(DRAIN_INTERVAL_SECONDS)
        with _registry_lock:
            clients = list(_registry)
        for client in clients:
            if getattr(client, "_gl_owner_polls", False):
                continue
            try:
                client.poll(0)
            except Exception:                                    # noqa: BLE001 - closed or being closed: nothing to drain
                continue


def _register(client) -> None:
    global _thread
    with _registry_lock:
        _registry.add(client)
        if _thread is None:
            _thread = threading.Thread(target=_drain_loop, name="librdkafka-log-drain", daemon=True)
            _thread.start()


def drain(client) -> None:
    """Serve the client's pending log records once (a no-op for a client its owner polls)."""
    try:
        if not getattr(client, "_gl_owner_polls", False):
            client.poll(0)
    except Exception:                                            # noqa: BLE001
        pass


def _build(base_consumer, base_producer):
    class Consumer(base_consumer):
        def __init__(self, config, *args, **kwargs):
            self._gl_owner_polls = False
            super().__init__(_with_logger(config), *args, **kwargs)
            _register(self)

        def subscribe(self, *args, **kwargs):
            self._gl_owner_polls = True                          # the owner's loop polls it from now on
            return super().subscribe(*args, **kwargs)

        def assign(self, *args, **kwargs):
            self._gl_owner_polls = True
            return super().assign(*args, **kwargs)

        def close(self, *args, **kwargs):
            drain(self)
            return super().close(*args, **kwargs)

    class Producer(base_producer):
        def __init__(self, config, *args, **kwargs):
            self._gl_owner_polls = False
            super().__init__(_with_logger(config), *args, **kwargs)
            _register(self)

        def produce(self, *args, **kwargs):
            # produce(topic, value, key, partition, on_delivery, ...): a callback means the owner serves the queue
            if kwargs.get("on_delivery") is not None or kwargs.get("callback") is not None or (len(args) > 4 and args[4] is not None):
                self._gl_owner_polls = True
            return super().produce(*args, **kwargs)

        def flush(self, *args, **kwargs):
            result = super().flush(*args, **kwargs)
            drain(self)
            return result

    return Consumer, Producer


def _patch_admin(admin_class) -> None:
    """``AdminClient`` is a Python class whose ``__init__`` calls ``super(AdminClient, self)`` through the module global, so
    it cannot be replaced by a subclass: its ``__init__`` is wrapped in place instead."""
    original = admin_class.__init__

    def __init__(self, config, *args, **kwargs):
        self._gl_owner_polls = False
        original(self, _with_logger(config), *args, **kwargs)
        _register(self)

    _originals["AdminClient.__init__"] = original
    admin_class.__init__ = __init__


def _rebind_loaded_modules(originals: dict) -> None:
    """Modules of this package that imported ``Consumer`` or ``Producer`` by name before :func:`install` ran (the entrypoint
    modules do, at import) get the wrapper instead, so install order does not matter."""
    for name, module in list(sys.modules.items()):
        if module is None or not name.startswith("neo4j_graph_loader."):
            continue
        for attribute in ("Consumer", "Producer"):
            if getattr(module, attribute, None) is originals[attribute]:
                setattr(module, attribute, getattr(sys.modules["confluent_kafka"], attribute))


def install() -> bool:
    """Make every client created from now on log through ``librdkafka``. Idempotent; returns whether it is in force.

    Called ONLY by :func:`neo4j_graph_loader.utils.logging_config.configure_logging` in JSON mode, i.e. inside this package's
    own entrypoint processes (the CLI, the roles, the planner workers). It is never called by ``import neo4j_graph_loader`` or by
    the library API: patching a third-party module is a process-global change a library must not make."""
    global _installed
    if _installed:
        return True
    import confluent_kafka
    import confluent_kafka.admin as admin
    _originals.update(Consumer=confluent_kafka.Consumer, Producer=confluent_kafka.Producer)
    confluent_kafka.Consumer, confluent_kafka.Producer = _build(confluent_kafka.Consumer, confluent_kafka.Producer)
    _patch_admin(admin.AdminClient)
    _rebind_loaded_modules(_originals)
    _installed = True
    return True


def uninstall() -> None:
    """Restore the original classes (tests)."""
    global _installed
    if not _installed:
        return
    import confluent_kafka
    import confluent_kafka.admin as admin
    wrappers = {"Consumer": confluent_kafka.Consumer, "Producer": confluent_kafka.Producer}
    confluent_kafka.Consumer, confluent_kafka.Producer = _originals["Consumer"], _originals["Producer"]
    for name, module in list(sys.modules.items()):
        if module is not None and name.startswith("neo4j_graph_loader."):
            for attribute, wrapper in wrappers.items():
                if getattr(module, attribute, None) is wrapper:
                    setattr(module, attribute, _originals[attribute])
    admin.AdminClient.__init__ = _originals["AdminClient.__init__"]
    _installed = False
