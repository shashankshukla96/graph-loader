"""A pre-exec fork receipt must not satisfy a fresh unguarded interpreter."""
import os
from pathlib import Path
import sys
import sysconfig


def test_fork_exec_requires_a_fresh_receipt():
    guard=Path(sysconfig.get_paths()['purelib'])/'sitecustomize.py';saved=guard.read_bytes()
    guard.write_text('# deliberate absent post-exec handshake\n')
    try:
        pid=os.fork()
        if pid==0:
            os.execv(sys.executable,[sys.executable,'-I','-c','import neo4j_graph_loader'])
        assert os.waitpid(pid,0)[1]==0
    finally:
        guard.write_bytes(saved)
