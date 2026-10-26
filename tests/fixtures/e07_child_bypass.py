"""Actual alternate-launch negative controls. The runner must reject the lane."""
import multiprocessing
import os
from pathlib import Path
import shlex
import subprocess
import sys


def _counterfeit(path):
    sys.path.insert(0,path)
    for name in list(sys.modules):
        if name=='neo4j_graph_loader' or name.startswith('neo4j_graph_loader.'):
            del sys.modules[name]
    import neo4j_graph_loader
    assert neo4j_graph_loader.__file__.startswith(path)


def _fake(tmp_path):
    package=tmp_path/'neo4j_graph_loader';package.mkdir()
    (package/'__init__.py').write_text('# deliberate counterfeit\n')
    return str(tmp_path)


def test_env_wrapper_missing_guard(tmp_path):
    path=_fake(tmp_path)
    child=subprocess.run(['/usr/bin/env',sys.executable,'-S','-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import neo4j_graph_loader;print(neo4j_graph_loader.__file__)',path],
        capture_output=True,text=True,timeout=10)
    assert child.returncode==0,child.stderr
    assert child.stdout.strip()==str(Path(path)/'neo4j_graph_loader/__init__.py')


def test_posix_spawn_missing_guard(tmp_path):
    path=_fake(tmp_path)
    command=[sys.executable,'-S','-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import neo4j_graph_loader;assert neo4j_graph_loader.__file__.startswith(sys.argv[1])',path]
    pid=os.posix_spawn(sys.executable,command,os.environ)
    assert os.waitpid(pid,0)[1]==0


def test_multiprocessing_spawn_missing_guard(tmp_path):
    path=_fake(tmp_path);executable=tmp_path/'python-without-site'
    executable.write_text('#!/bin/sh\nexec '+shlex.quote(sys.executable)+' -S "$@"\n')
    executable.chmod(0o700)
    multiprocessing.set_executable(str(executable))
    child=multiprocessing.get_context('spawn').Process(target=_counterfeit,args=(path,))
    try:
        child.start();child.join(10)
        assert child.exitcode==0
    finally:
        multiprocessing.set_executable(sys.executable)
        if child.is_alive():
            child.kill();child.join(5)
