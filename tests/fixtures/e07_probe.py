"""Maintained mechanism probe; collected explicitly by the installed runner."""
import importlib
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import sysconfig

import pytest


def _origin(queue):
    import neo4j_graph_loader.planning as module
    queue.put((str(Path(module.__file__).resolve()), sys.prefix))


def test_installed_parent():
    import neo4j_graph_loader.planning as module
    assert Path(module.__file__).resolve().is_relative_to(Path(sysconfig.get_paths()['purelib']).resolve())
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module('src')


@pytest.mark.parametrize('method', [m for m in ('spawn','fork') if m in multiprocessing.get_all_start_methods()])
def test_installed_actual_multiprocessing(method):
    context = multiprocessing.get_context(method)
    queue = context.Queue()
    process = context.Process(target=_origin, args=(queue,))
    try:
        process.start()
        origin, prefix = queue.get(timeout=15)
        process.join(15)
        assert process.exitcode == 0
        assert prefix == sys.prefix
        assert Path(origin).is_relative_to(Path(sysconfig.get_paths()['purelib']).resolve())
    finally:
        if process.is_alive():
            process.kill(); process.join(5)
        queue.close(); queue.join_thread()


def test_installed_minimal_environment_child():
    child = subprocess.run([sys.executable, '-I', '-c',
        'import neo4j_graph_loader.planning as p;print(p.__file__)'],
        env={'PATH': os.defpath}, capture_output=True, text=True, timeout=20)
    assert child.returncode == 0, child.stdout+child.stderr
    assert Path(child.stdout.strip()).resolve().is_relative_to(Path(sysconfig.get_paths()['purelib']).resolve())


def test_installed_env_wrapper():
    child=subprocess.run(['/usr/bin/env',sys.executable,'-I','-c',
        'import neo4j_graph_loader;print(neo4j_graph_loader.__file__)'],capture_output=True,text=True,timeout=20)
    assert child.returncode==0,child.stderr
    assert Path(child.stdout.strip()).resolve().is_relative_to(Path(sysconfig.get_paths()['purelib']).resolve())


def test_installed_posix_spawn():
    command=[sys.executable,'-I','-c','import neo4j_graph_loader.planning']
    pid=os.posix_spawn(sys.executable,command,os.environ)
    assert os.waitpid(pid,0)[1]==0


def test_installed_fork_exec():
    pid=os.fork()
    if pid==0:
        os.execv(sys.executable,[sys.executable,'-I','-c','import neo4j_graph_loader.planning'])
    assert os.waitpid(pid,0)[1]==0


def test_installed_console_child():
    child=subprocess.run([str(Path(sys.executable).parent/'graph-loader'),'--version'],capture_output=True,text=True,timeout=20)
    assert child.returncode==0,child.stderr
    assert child.stdout.strip()=='graph-loader 0.1.0a1'
