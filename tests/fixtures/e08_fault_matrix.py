"""Opt-in E08 fault preparation and aggregate ownership; legacy controls unchanged."""
import multiprocessing as mp
import os
from pathlib import Path
import socket
import threading
import time

from neo4j_graph_loader.loader.edge_planner import _planner_worker_main
from neo4j_graph_loader.loader._planner_transport import FrameStream, WaitCancelled, WaitControl, WaitExpired
from tests.fixtures.e04_faults import _not_reading, _partial_reply, _public_nonreading_worker, _public_partial_worker


def mark_ready(worker):
    (Path(worker['spill_dir']).parent/f"child-boot-ready-{worker['worker_index']}").touch()


def ready_worker(*args):
    mark_ready(args[1])
    return _planner_worker_main(*args)


def ready_partial_worker(*args):
    mark_ready(args[1])
    return _public_partial_worker(*args)


def ready_nonreading_worker(*args):
    mark_ready(args[1])
    return _public_nonreading_worker(*args)


def private_fault(scenario, mode, *, workers, context):
    context = mp.get_context(context)
    owned = []
    signal = threading.Event()
    timer = None
    cleanup_seconds = None
    try:
        for _ in range(workers):
            parent, child = socket.socketpair()
            parent.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            child.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            ready = context.Event()
            process = context.Process(target=_partial_reply if scenario == 'private-recv' else _not_reading,
                                      args=(child, ready))
            owned.append((parent, child, process, ready))
            process.start()
            child.close()
        preparation = time.monotonic()
        deadline = preparation + 60
        for _, _, _, ready in owned:
            assert ready.wait(max(0., deadline-time.monotonic())), 'fault-peer preparation deadline exceeded'
        preparation_seconds = time.monotonic()-preparation
        started = time.monotonic()
        if mode == 'cancel':
            timer = threading.Timer(.15, signal.set)
            timer.start()
        control = WaitControl(deadline=started+.15 if mode == 'timeout' else None, cancel_event=signal)
        stream = FrameStream(owned[0][0], check=control.check)
        try:
            if scenario == 'private-recv':
                stream.recv()
            else:
                stream.send(('command', b'x'*(512 << 10)))
        except (WaitExpired, WaitCancelled) as error:
            assert isinstance(error, WaitExpired if mode == 'timeout' else WaitCancelled)
            observed = time.monotonic()-started
            assert .1 <= observed < .75, observed
        else:
            raise AssertionError('fault did not interrupt the private adapter')
    finally:
        # Signal every peer first; joins share one10-second budget.
        cleanup_started = time.monotonic()
        deadline = cleanup_started+10
        for parent, child, process, _ in owned:
            parent.close()
            child.close()
            if process.pid is not None and process.is_alive():
                process.terminate()
        for _, _, process, _ in owned:
            if process.pid is not None:
                process.join(max(0., min(deadline, cleanup_started+3)-time.monotonic()))
        for _, _, process, _ in owned:
            if process.pid is not None and process.is_alive():
                process.kill()
        for _, _, process, _ in owned:
            if process.pid is not None:
                process.join(max(0., deadline-time.monotonic()))
        cleanup_seconds = time.monotonic()-cleanup_started
        if timer:
            timer.cancel()
            timer.join(1)
        assert cleanup_seconds < 10
        assert all(not process.is_alive() for _, _, process, _ in owned)
        assert all(parent.fileno() == child.fileno() == -1 for parent, child, _, _ in owned)
        assert timer is None or not timer.is_alive()
    return {'scenario': scenario, 'mode': mode, 'context': context.get_start_method(),
            'client_pid': os.getpid(),
            'requested_workers': workers, 'actual_workers': len(owned),
            'fault_channels_exercised': 1, 'blocked_neighbors': workers-1,
            'observed_seconds': observed, 'excluded_boot_wait_seconds': preparation_seconds,
            'cleanup_seconds': cleanup_seconds, 'child_pids': [p.pid for _, _, p, _ in owned],
            'children_dead': True, 'endpoints_closed': True, 'fixture_timer_dead': True}
