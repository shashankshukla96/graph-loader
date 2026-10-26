"""Caught interruptions cannot erase a failed installed startup gate."""
from pathlib import Path
import signal
import subprocess
import sys
import sysconfig
import time

import pytest


class StartupInterrupted(BaseException):
    pass


@pytest.mark.parametrize('interruption', [KeyboardInterrupt, SystemExit, StartupInterrupted])
def test_caught_interruption_is_a_durable_failure(interruption):
    guard = Path(sysconfig.get_paths()['purelib'])/'sitecustomize.py'
    saved = guard.read_text()
    guard.write_text(saved.replace('from __future__ import annotations',
                                   'from __future__ import annotations\nimport time;time.sleep(3.5)'))

    def interrupt(*_args):
        raise interruption()

    previous = signal.signal(signal.SIGALRM, interrupt)
    started = time.monotonic()
    signal.setitimer(signal.ITIMER_REAL, .2)
    try:
        with pytest.raises(interruption):
            subprocess.Popen([sys.executable, '-I', '-c', 'import neo4j_graph_loader'])
        assert time.monotonic()-started < .6
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        guard.write_text(saved)
    # Require the genuine delayed hook to finish. The durable violation, rather
    # than a missing eventual receipt, must prevent runner success.
    time.sleep(4)
