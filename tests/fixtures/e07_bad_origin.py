"""Deliberate counterfeit child import: the runner must reject this run."""
from pathlib import Path
import subprocess
import sys


def test_wrong_origin_is_rejected(tmp_path):
    package=tmp_path/'neo4j_graph_loader';package.mkdir()
    (package/'__init__.py').write_text('# counterfeit module, not application code\n')
    child=subprocess.run([sys.executable,'-I','-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import neo4j_graph_loader',str(tmp_path)],
        capture_output=True,text=True,timeout=20)
    assert child.returncode==86,child.stdout+child.stderr
    assert 'outside target site-packages' in child.stderr
    assert Path(tmp_path).is_dir()
