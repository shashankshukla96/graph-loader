"""Actual uninstrumented Python child: the runner must fail closed."""
import subprocess
import sys
import sysconfig
from pathlib import Path


def test_missing_child_provenance():
    site=Path(sysconfig.get_paths()['purelib'])
    guard=site/'sitecustomize.py';saved=guard.read_bytes()
    # Deliberately remove the owned instrumentation, not application payload.
    guard.write_text('# deliberate missing child handshake\n')
    try:
        child=subprocess.run([sys.executable,'-I','-c',
            'import neo4j_graph_loader;print(neo4j_graph_loader.__file__)'],
            capture_output=True,text=True,timeout=20)
    finally:
        guard.write_bytes(saved)
    assert child.returncode==0,child.stdout+child.stderr
    assert 'site-packages/neo4j_graph_loader' in child.stdout
