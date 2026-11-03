"""D14-5: a stream schema without relationship types under ``--runtime process`` is supervised in the foreground, not detached.

Docker mode returns 0 and leaves the node containers running (``test_cli_shutdown.test_stream_without_edges_leaves_its_detached_
node_loaders_running`` keeps that contract). Process-mode children belong to the command, so returning would end them silently with
exit 0: the command stays up, exits 0 on a stop request and 1 when a node loader dies."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def alive(pid: int) -> bool:
    import psutil
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def start(tmp_path, count=2):
    process = subprocess.Popen([sys.executable, str(FIXTURES / "e14_node_only_standin.py"), str(tmp_path), str(count)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    info = json.loads(process.stdout.readline())
    return process, info


def wait_gone(pids, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if not any(alive(p) for p in pids):
            return True
        time.sleep(0.05)
    return False


def test_the_command_stays_up_and_a_stop_request_ends_the_loaders_in_order_with_exit_zero(tmp_path) -> None:
    process, info = start(tmp_path)
    try:
        time.sleep(1.5)                                                    # a detached run would have returned by now
        assert process.poll() is None and all(alive(p) for p in info["children"])
        process.send_signal(signal.SIGTERM)
        assert process.wait(30) == 0, process.stderr.read()
        assert wait_gone(info["children"], 5)
        assert all((tmp_path / f"node-{i}").read_text() == "term\n" for i in range(2)), "the loaders were stopped, not killed"
    finally:
        if process.poll() is None:
            process.kill()


def test_a_dying_node_loader_stops_the_rest_and_the_command_exits_one(tmp_path) -> None:
    process, info = start(tmp_path, count=3)
    try:
        os.kill(info["children"][0], signal.SIGKILL)
        assert process.wait(30) == 1, process.stderr.read()
        assert wait_gone(info["children"], 5)
        assert "node loader exited while the stream run was active" in process.stderr.read()
    finally:
        if process.poll() is None:
            process.kill()


def test_kill_minus_nine_of_the_command_stops_the_loaders_through_the_lifeline(tmp_path) -> None:
    process, info = start(tmp_path)
    try:
        killed_at = time.monotonic()
        os.kill(process.pid, signal.SIGKILL)
        process.wait(10)
        assert wait_gone(info["children"], 15), "the lifeline must end the loaders of a command killed with -9"
        assert time.monotonic() - killed_at < 15
    finally:
        for pid in info["children"]:
            if alive(pid):
                os.kill(pid, signal.SIGKILL)
