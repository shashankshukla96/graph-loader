"""A caught timeout and eventual receipt must still fail the installed lane."""
from pathlib import Path
import subprocess
import sys
import sysconfig
import time

import psutil


def test_caught_startup_timeout_is_a_durable_failure():
    guard = Path(sysconfig.get_paths()['purelib'])/'sitecustomize.py'
    saved = guard.read_text()
    guard.write_text(saved.replace('from __future__ import annotations',
                                   'from __future__ import annotations\nimport time;time.sleep(3.5)'))
    try:
        try:
            subprocess.Popen([sys.executable, '-I', '-c', 'import neo4j_graph_loader'])
        except RuntimeError:
            pass
        else:
            raise AssertionError('missing startup receipt did not interrupt creation')
        # Let the actual guard finish and emit its valid receipt. Absence alone
        # must not be the only durable reason this lane fails.
        deadline = time.monotonic()+5
        while time.monotonic() < deadline:
            children = [p for p in psutil.Process().children() if p.status() != psutil.STATUS_ZOMBIE]
            if not children:
                break
            time.sleep(.025)
        assert not [p for p in psutil.Process().children() if p.status() != psutil.STATUS_ZOMBIE]
    finally:
        guard.write_text(saved)
