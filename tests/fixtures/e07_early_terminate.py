"""Real children terminated immediately after creation must retain startup evidence."""
import multiprocessing
import os
import signal
import subprocess
import sys
import time

import pytest


def _sleep():
    while True:
        time.sleep(1)


@pytest.mark.parametrize('method', ['popen', 'env', 'posix', 'fork', 'mp-spawn', 'mp-fork'])
def test_immediate_termination_retains_startup_receipt(method):
    command = [sys.executable, '-I', '-c', 'import time;time.sleep(60)']
    if method in {'popen', 'env'}:
        process = subprocess.Popen((['/usr/bin/env'] if method == 'env' else []) + command)
        try:
            process.terminate()
            assert process.wait(timeout=5) < 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    elif method in {'posix', 'fork'}:
        if method == 'posix':
            pid = os.posix_spawn(sys.executable, command, os.environ)
        else:
            pid = os.fork()
            if pid == 0:
                _sleep()
                os._exit(0)
        os.kill(pid, signal.SIGTERM)
        assert os.waitpid(pid, 0)[1] == signal.SIGTERM
    else:
        process = multiprocessing.get_context(method.removeprefix('mp-')).Process(target=_sleep)
        try:
            process.start()
            process.terminate()
            process.join(5)
            assert not process.is_alive() and process.exitcode < 0
        finally:
            if process.is_alive():
                process.kill()
                process.join(5)
