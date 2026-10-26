"""Application clock patches cannot unbound the instrumentation's receipt wait."""
from pathlib import Path
import subprocess
import sys
import sysconfig
import time


def test_missing_hook_with_frozen_application_clock_still_times_out(monkeypatch):
    guard = Path(sysconfig.get_paths()['purelib'])/'sitecustomize.py'
    saved = guard.read_bytes()
    guard.write_text('# intentionally missing child startup hook\n')
    clock = time.monotonic
    monkeypatch.setattr(time, 'monotonic', lambda: 0)
    monkeypatch.setattr(time, 'monotonic_ns', lambda: 0)
    monkeypatch.setattr(time, 'sleep', lambda _: None)
    started = clock()
    try:
        try:
            subprocess.Popen([sys.executable, '-I', '-c', 'import neo4j_graph_loader'])
        except RuntimeError:
            pass
        else:
            raise AssertionError('missing startup hook was accepted')
        assert 2.5 < clock()-started < 4.5
    finally:
        guard.write_bytes(saved)
