"""Installed-runner site hook. Copied into the owned test environment only."""
from __future__ import annotations

import hashlib
import importlib.abc
import importlib.machinery
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import sysconfig
import time

try:
    import psutil
except BaseException:
    os.write(2, b'installed guard requires its declared psutil test tool\n')
    os._exit(86)


def _start():
    config = json.loads(Path(__file__).with_name('_e07_guard_config.json').read_text())
    site = Path(sysconfig.get_paths()['purelib']).resolve()
    payload = config['payload']
    log = config['provenance_log']
    # Tests patch the shared time module to drive application clocks. This
    # instrumentation must retain its own real, bounded startup clock.
    tick = time.monotonic_ns
    pause = time.sleep
    open_log = open

    def record(kind, **values):
        row = dict(kind=kind, pid=os.getpid(), ppid=os.getppid(),
                   created=psutil.Process().create_time(), tick=tick(), **values)
        # One append/write per record, including forked processes; no shared lock.
        raw = (json.dumps(row, sort_keys=True) + '\n').encode()
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            if os.write(fd, raw) != len(raw):
                raise RuntimeError('short provenance write')
        finally:
            os.close(fd)

    def fatal(message):
        record('violation', message=message)
        os.write(2, ('installed provenance rejected: ' + message + '\n').encode())
        os._exit(86)

    def check(name, origin):
        path = Path(origin).resolve()
        if not path.is_relative_to(site):
            fatal('application origin outside target site-packages: ' + name)
        key = path.relative_to(site).as_posix()
        if key not in payload or hashlib.sha256(path.read_bytes()).hexdigest() != payload[key]:
            fatal('application bytes differ from target wheel: ' + name)
        record('module', module=name, origin=str(path), sha256=payload[key])

    def audit_process():
        if Path(sys.prefix).resolve() != Path(config['prefix']).resolve():
            fatal('wrong target interpreter environment')
        for key, digest in payload.items():
            path = site / key
            if not path.is_file() or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                fatal('missing or changed installed payload: ' + key)
        for name, module in list(sys.modules.items()):
            if name == 'neo4j_graph_loader' or name.startswith('neo4j_graph_loader.'):
                origin = getattr(module, '__file__', None)
                if origin:
                    check(name, origin)
        record('process', prefix=sys.prefix, site=str(site), argv=sys.argv)

    class Finder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == 'neo4j_graph_loader' or fullname.startswith('neo4j_graph_loader.'):
                spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
                if spec is None or not spec.origin:
                    fatal('missing application module: ' + fullname)
                check(fullname, spec.origin)
                return spec
            return None

    sys.meta_path.insert(0, Finder())
    original = subprocess.Popen

    def validate_launch(executable, command):
        executable=os.fsdecode(executable)
        command=[os.fsdecode(x) for x in command]
        if executable=='/usr/bin/env' or executable=='env':
            if len(command)<2:
                fatal('env launcher needs an explicit target Python')
            return validate_launch(command[1],command[1:])
        candidate=Path(shutil.which(executable) or executable).absolute()
        allowed_bin=Path(sys.prefix)/'bin'
        if candidate.parent.resolve()!=allowed_bin.resolve() or not (
            candidate.name.startswith('python') or candidate.name=='graph-loader'
        ):
            fatal('unsupported child launcher; invoke target Python or graph-loader directly')
        if '-S' in command:
            fatal('Python -S disables required installed child provenance')

    def child_record(pid, method, command, launched):
        try:
            created = psutil.Process(pid).create_time()
        except psutil.NoSuchProcess:
            created = None
        record('python_child', child_pid=pid, child_created=created,
               method=method, command=[os.fsdecode(x) for x in command], launch_tick=launched)
        try:
            deadline = tick() + 3_000_000_000
            while True:
                with open_log(log, 'rb') as stream:
                    raw = stream.read()
                # Another process may currently be appending its final line.
                complete = raw[:raw.rfind(b'\n') + 1]
                receipt = None
                for line in complete.splitlines():
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError('provenance record is not an object')
                    if (row.get('kind') == 'process' and row.get('pid') == pid
                            and row.get('ppid') == os.getpid()
                            and row.get('tick', -1) >= launched
                            and row.get('prefix') == sys.prefix and row.get('site') == str(site)
                            and type(row.get('created')) in (int, float)
                            and 0 < row['created'] < float('inf')
                            and (created is None or row['created'] == created)):
                        receipt = row
                if receipt is not None:
                    if created is None:
                        # The child can finish before the parent's OS query.
                        # Retain the audited child identity with all matching
                        # parent, launch-anchor and environment evidence.
                        record('python_child', child_pid=pid, child_created=receipt['created'],
                               method=method, command=[os.fsdecode(x) for x in command],
                               launch_tick=launched, identity_from_fresh_receipt=True)
                    return
                if tick() >= deadline:
                    raise RuntimeError('missing fresh child startup receipt')
                pause(.005)
        except BaseException as exc:
            # Even a caught interruption must leave a permanent failed gate.
            record('violation', message='child startup receipt failed: ' + type(exc).__name__,
                   child_pid=pid, child_created=created)
            if not isinstance(exc, Exception):
                raise
            raise RuntimeError('installed child startup provenance failed') from exc

    class GuardedPopen(original):
        def __init__(self, args, *a, **kw):
            command = [args] if isinstance(args, str) else list(args)
            if kw.get('shell'):
                fatal('shell child launch is unsupported; invoke target Python directly')
            validate_launch(kw.get('executable') or command[0],command)
            launched = tick()
            super().__init__(args, *a, **kw)
            # Every subprocess is accountable, including env/shell/renamed
            # launchers. An unsupported non-Python process cannot silently pass.
            child_record(self.pid, 'subprocess', command, launched)

    subprocess.Popen = GuardedPopen
    for name in ('posix_spawn', 'posix_spawnp'):
        original_spawn = getattr(os, name, None)
        if original_spawn:
            def spawn(path, argv, env, *a, _original=original_spawn, **kw):
                validate_launch(path,argv)
                launched = tick()
                pid = _original(path, argv, env, *a, **kw)
                child_record(pid, 'posix_spawn', argv, launched)
                return pid
            setattr(os, name, spawn)
    if hasattr(os, 'fork'):
        original_fork = os.fork
        def fork():
            launched = tick()
            pid = original_fork()
            if pid:
                child_record(pid, 'fork', [], launched)
            return pid
        os.fork = fork
    # multiprocessing spawn bypasses subprocess.Popen and os.posix_spawn.
    import multiprocessing.util
    original_mp_spawn = multiprocessing.util.spawnv_passfds
    def mp_spawn(path, args, passfds):
        validate_launch(path,args)
        launched = tick()
        pid = original_mp_spawn(path, args, passfds)
        child_record(pid, 'multiprocessing_spawn', args, launched)
        return pid
    multiprocessing.util.spawnv_passfds = mp_spawn
    def audit(event, args):
        if event == 'os.exec':
            validate_launch(args[0],args[1])
            record('exec', command=[os.fsdecode(x) for x in args[1]])
        elif event == 'os.forkpty':
            fatal('os.forkpty is unsupported; use an instrumented Python child')
        elif event == 'os.system':
            fatal('os.system is unsupported; use an instrumented Python child')
    sys.addaudithook(audit)
    def after_fork():
        try:
            audit_process()
        except BaseException:
            os._exit(86)
    if hasattr(os, 'register_at_fork'):
        os.register_at_fork(after_in_child=after_fork)
    audit_process()


try:
    _start()
except BaseException as exc:
    # Python normally swallows sitecustomize exceptions. Fail closed instead.
    os.write(2, ('installed guard initialization failed: ' + type(exc).__name__ + '\n').encode())
    os._exit(86)
