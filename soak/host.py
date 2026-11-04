"""E14.F3.S10: the soak's fleet as host processes (``--runtime process``), bare or under a real systemd user unit.

Two shapes, one fleet view. The CLI is ``graph-loader start --mode stream --runtime process`` from the venv that holds the wheel under
test (no container image, ``DOCKER_HOST`` at a missing socket); the owned Neo4j and Kafka stay containers, reached on loopback.

* ``process``: the harness starts the command and, after any exit, starts it again (``Supervisor``) - the harness is the only
  launcher.
* ``service``: the shipped ``examples/systemd/graph-loader.service`` (paths, a CPU affinity, a memory bound and a larger start-limit
  burst substituted) is linked as a user unit. systemd is the only launcher: after a failure it restarts the unit itself
  (``Restart=on-failure``); after a clean stop (exit 0) the harness issues the explicit ``systemctl --user start`` and nothing else.

What a fleet is made of is read from the run's registry (``registry.jsonl`` in the attempt directory) through the installed package;
per-process facts come from ``/proc``; per-process metrics ports from the file_sd ``metrics/targets.json``.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import zipfile

from tests.installed_service.live_cli import ENV_KEEP, RunningCli

NO_DOCKER = 'unix:///nonexistent/e14-soak-no-docker.sock'
REASON = re.compile(r'(stage=\S+.*(failed|exited|stopping|abandon|lost).*|ERROR.*)', re.IGNORECASE)
EXAMPLE_UNIT = Path(__file__).resolve().parents[1] / 'examples' / 'systemd' / 'graph-loader.service'
START_LIMIT_BURST = 30
LIFELINE_ALLOWANCE_S = 30.0       # how long after the command died its children may still be running before they count as orphans


# ---- the wheel under test --------------------------------------------------------------------------------------------------

def sha256_file(path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def site_packages(venv_bin) -> Path:
    out = subprocess.run([str(Path(venv_bin) / 'python'), '-I', '-c', 'import sysconfig;print(sysconfig.get_paths()["purelib"])'],
                         capture_output=True, text=True, timeout=60, check=True).stdout.strip()
    return Path(out)


def verify_wheel(venv_bin, wheel_path, *, expected_sha256=None) -> dict:
    """The wheel file's hash (checked against ``expected_sha256`` when given) and, file by file, the installed package against the
    wheel's own RECORD: every hashed file must exist in the venv with the recorded digest. Raises on any mismatch."""
    wheel = Path(wheel_path)
    sha = sha256_file(wheel)
    if expected_sha256 and sha != expected_sha256:
        raise RuntimeError(f'wheel sha256 {sha} differs from the expected {expected_sha256}')
    root = site_packages(venv_bin)
    checked, bad = 0, []
    with zipfile.ZipFile(wheel) as archive:
        record = next(name for name in archive.namelist() if name.endswith('.dist-info/RECORD'))
        for line in archive.read(record).decode().splitlines():
            parts = line.split(',')
            if len(parts) < 3 or not parts[1]:
                continue
            name, digest = parts[0], parts[1]
            algorithm, _, value = digest.partition('=')
            installed = root / name
            if not installed.is_file():
                bad.append(f'{name}: missing')
                continue
            actual = hashlib.new(algorithm, installed.read_bytes()).digest()
            import base64
            if base64.urlsafe_b64encode(actual).rstrip(b'=').decode() != value:
                bad.append(f'{name}: digest differs')
            checked += 1
    if bad:
        raise RuntimeError(f'the installed package differs from the wheel: {bad[:5]}')
    return {'wheel': wheel.name, 'wheel_sha256': sha, 'expected_sha256': expected_sha256, 'installed_files_checked': checked,
            'site_packages': str(root)}


# ---- the registry, /proc, signals ------------------------------------------------------------------------------------------

def _registry():
    from neo4j_graph_loader.orchestrator import run_registry
    return run_registry


def registry_states(attempt: Path) -> list:
    """Latest state of every recorded process (children and externals) of an attempt, oldest first."""
    rr = _registry()
    return list(rr.fold(rr.RunRegistry.read(attempt / 'registry.jsonl')).values())


def is_alive(state) -> bool:
    rr = _registry()
    if state.get('phase') == 'exit' or 'pid' not in state:
        return False
    current = rr.process_identity(int(state['pid']))
    return current is not None and (current.get('start_time'), current.get('boot_id')) == (state.get('start_time'), state.get('boot_id'))


def alive_children(attempt: Path) -> list:
    return [s for s in registry_states(attempt) if not s.get('external') and is_alive(s)]


def signal_state(state, signum) -> bool:
    """Signal a recorded process through a pidfd, after its identity was verified; False if it is gone."""
    try:
        fd = os.pidfd_open(int(state['pid']))
    except OSError:
        return False
    try:
        if not is_alive(state):
            return False
        signal.pidfd_send_signal(fd, signum)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def proc_status(pid) -> dict:
    out = {}
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            key, _, value = line.partition(':')
            out[key] = value.strip()
    except OSError:
        pass
    return out


def rss_bytes(pid) -> int:
    value = proc_status(pid).get('VmRSS', '')
    return int(value.split()[0]) * 1024 if value else 0


def cpu_ns(pid) -> int:
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks * (10 ** 9 // os.sysconf('SC_CLK_TCK'))
    except (OSError, IndexError, ValueError):
        return 0


def child_env(stack, extra=None) -> dict:
    env = {key: os.environ[key] for key in ENV_KEEP if key in os.environ}
    env.update(DOCKER_HOST=NO_DOCKER, NEO4J_URI=stack.neo_uri, NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=stack.password,
               NEO4J_DATABASE=stack.database, KAFKA_BOOTSTRAP_SERVERS=stack.bootstrap)
    env.update(extra or {})
    return env


# ---- the fleet view --------------------------------------------------------------------------------------------------------

class HostFleet:
    """One incarnation of the fleet. Subclasses start (or attach to) the command and say how it is stopped."""

    shape = ''

    def __init__(self, ctx, incarnation: int, schema_path: Path, *, expected: dict):
        self.ctx, self.incarnation, self.expected = ctx, incarnation, expected
        self.name = f'fleet{incarnation:03d}'
        self.schema_path = Path(schema_path)
        self.started_at = time.time()
        self.cpuset_seen = {}
        self.finished = None
        self.attempt = None
        self.cli = None
        ctx.log_dir.mkdir(parents=True, exist_ok=True)

    # ---- attempt directory -------------------------------------------------------------------------------------------
    def locate_attempt(self, timeout=60):
        """The attempt directory this incarnation created: the one nobody has claimed yet."""
        base = self.ctx.run_dir / 'runs'
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if base.is_dir():
                for path in sorted(base.iterdir(), key=lambda p: p.stat().st_mtime):
                    if (path / 'registry.jsonl').is_file() and str(path) not in self.ctx.claimed_attempts:
                        self.ctx.claimed_attempts.add(str(path))
                        self.attempt = path
                        return path
            time.sleep(0.3)
        raise RuntimeError('the command never created its attempt directory')

    # ---- processes ---------------------------------------------------------------------------------------------------
    def states(self):
        return alive_children(self.attempt) if self.attempt is not None else []

    def live(self):
        out = {}
        for state in self.states():
            component = (state.get('labels') or {}).get('component')
            out.setdefault(component, []).append((state['id'], state['name']))
        return out

    def counts(self):
        return {k: len(v) for k, v in self.live().items()}

    def ready(self):
        counts = self.counts()
        return all(counts.get(k, 0) == n for k, n in self.expected.items()) and self.readyz()

    def readyz(self):
        port = getattr(self.ctx, 'health_port', None)
        if not port:
            return True
        import urllib.request
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/readyz', timeout=3) as response:
                return response.status == 200
        except Exception:
            return False

    def wait_ready(self, timeout=240):
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            if not self.cli.running():
                raise RuntimeError('the fleet command exited before the fleet was ready: ' + self.cli.text()[-1500:])
            if self.ready():
                return time.monotonic() - began
            time.sleep(1)
        raise TimeoutError(f'fleet not ready after {timeout}s: have {self.counts()}, want {self.expected}')

    def all_running(self):
        return True

    def metrics_ports(self) -> dict:
        """{process name: metrics port} from the run's file_sd targets."""
        path = self.attempt / 'metrics' / 'targets.json' if self.attempt is not None else None
        if path is None or not path.is_file():
            return {}
        try:
            groups = json.loads(path.read_text())
        except (OSError, ValueError):
            return {}
        out = {}
        for group in groups:
            exporter = (group.get('labels') or {}).get('exporter')
            for target in group.get('targets') or []:
                out[exporter] = int(target.rpartition(':')[2])
        return out

    def _affinity(self, pid):
        return proc_status(pid).get('Cpus_allowed_list', '')

    def record_new(self):
        for state in self.states():
            if state['id'] in self.cpuset_seen:
                continue
            self.cpuset_seen[state['id']] = {'name': state['name'], 'component': (state.get('labels') or {}).get('component'),
                                             'cpuset': self._affinity(state['pid']), 'memory_limit': 0, 'incarnation': self.incarnation}

    def inspect_all(self):
        ports = self.metrics_ports()
        rows = []
        for state in self.states():
            component = (state.get('labels') or {}).get('component')
            rows.append({'id': state['id'], 'name': state['name'], 'component': component, 'state': 'running', 'oom_killed': None,
                         'exit_code': None, 'restarts': 0, 'started': state.get('start_time'), 'metrics_port': ports.get(state['name']),
                         'pid': int(state['pid'])})
            self.cpuset_seen[state['id']] = {'name': state['name'], 'component': component, 'cpuset': self._affinity(state['pid']),
                                             'memory_limit': 0, 'incarnation': self.incarnation}
        return rows

    def kill_container(self, cid):
        """SIGKILL one registered child (the process analogue of killing a container)."""
        state = next((s for s in self.states() if s['id'] == cid), None)
        if state is None or not signal_state(state, signal.SIGKILL):
            raise RuntimeError(f'child {cid} is not alive')
        return {'signal': 'SIGKILL', 'pid': int(state['pid']), 'name': state['name']}

    def reason_lines(self, limit=6):
        lines = [line.strip() for line in self.cli.text().splitlines() if REASON.search(line)]
        return lines[-limit:]

    def exit_receipts(self):
        """{name: exit code} of every child the registry shows as exited."""
        return {s['name']: s.get('code') for s in registry_states(self.attempt) if not s.get('external') and s.get('phase') == 'exit'} \
            if self.attempt is not None else {}

    def lifeline_check(self, timeout=30):
        """Seconds until no recorded process of this attempt is alive; None if some outlive ``timeout``."""
        if self.attempt is None:
            return 0.0
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            if not [s for s in registry_states(self.attempt) if is_alive(s)]:
                return time.monotonic() - began
            time.sleep(0.05)
        return None

    def finish(self):
        """The incarnation is over: nothing it started may still run. Anything that does is reported (and then removed so the run can
        go on); there is no container-style recovery to perform, the lifeline and the manager are what clean up."""
        if self.finished is not None:
            return self.finished
        code = self.cli.process.poll()
        if code is None:
            code = self.cli.terminate(timeout=120)
        orphans = []
        gone_in = 0.0
        if self.attempt is not None:
            # The command is gone; its lifeline (and, under systemd, the manager) stops the children within seconds. Whatever is still
            # alive after the allowance is an orphan: reported, then removed so the run can go on.
            gone_in = self.lifeline_check(timeout=LIFELINE_ALLOWANCE_S)
            for state in registry_states(self.attempt):
                if is_alive(state):
                    orphans.append(state['name'])
                    signal_state(state, signal.SIGKILL)
        receipts = self.exit_receipts()
        self.finished = {'exit_code': code, 'lifeline_seconds': None if gone_in is None else round(gone_in, 3), 'child_exit_codes': receipts, 'leftovers': {'removed': orphans, 'failed': []},
                         'unobserved': [], 'refused': {}, 'observer_errors': [], 'closed_early': False, 'problems': [],
                         'attempt': str(self.attempt) if self.attempt is not None else None}
        return self.finished


class ProcessFleet(HostFleet):
    """The harness starts the command itself."""

    shape = 'process'

    def __init__(self, ctx, incarnation, schema_path, *, expected, extra_args=()):
        super().__init__(ctx, incarnation, schema_path, expected=expected)
        env = child_env(ctx.stack, {'GRAPH_LOADER_METRICS_ENABLED': '1', 'GRAPH_LOADER_METRICS_PORT': str(ctx.cli_metrics_port)})
        args = ['-c', ctx.cpuset, str(Path(ctx.config['venv_bin']) / 'graph-loader'), 'start', '--mode', 'stream', '--config',
                str(self.schema_path), '--runtime', 'process', '--run-dir', str(ctx.run_dir / 'runs'), '--run-id', ctx.run_id,
                '--stream-group-member-wait-seconds', str(ctx.member_wait), '--health-addr', f'127.0.0.1:{ctx.health_port}', *extra_args]
        self.cli = RunningCli('taskset', args, env=env, cwd=ctx.run_dir, log_dir=ctx.log_dir, name=self.name, deadline=ctx.cli_deadline,
                              redact=[ctx.stack.password])
        self.locate_attempt()

    def sigterm(self, timeout=90):
        return self.cli.terminate(timeout=timeout)

    def sigkill_cli(self):
        """``kill -9`` of the command only. Its children must stop through the lifeline."""
        os.kill(self.cli.process.pid, signal.SIGKILL)
        self.cli.process.wait(timeout=10)
        self.cli._finish()
        return time.monotonic()


# ---- the systemd shape -----------------------------------------------------------------------------------------------------

def sysenv():
    runtime = f'/run/user/{os.getuid()}'
    return {**os.environ, 'XDG_RUNTIME_DIR': runtime, 'DBUS_SESSION_BUS_ADDRESS': f'unix:path={runtime}/bus'}


def ctl(*args, check=True, timeout=240):
    done = subprocess.run(['systemctl', '--user', *args], env=sysenv(), capture_output=True, text=True, timeout=timeout)
    if check and done.returncode != 0:
        raise RuntimeError(f'systemctl --user {" ".join(args)} -> {done.returncode}: {done.stderr or done.stdout}')
    return done


class Unit:
    """The shipped example unit, substituted and linked under an owned name; the only thing the harness may do to it is start, stop,
    kill and read it."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.name = f'e14-{ctx.stack.owner[:8]}-soak.service'
        self.dir = ctx.run_dir / 'unit'
        self.dir.mkdir(exist_ok=True)
        self.link = Path.home() / '.config' / 'systemd' / 'user' / self.name
        self.env_file = self.dir / 'graph-loader.env'
        fd = os.open(self.env_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        stack = ctx.stack
        extra = {'GRAPH_LOADER_METRICS_ENABLED': '1', 'GRAPH_LOADER_METRICS_PORT': str(ctx.cli_metrics_port), 'NEO4J_URI': stack.neo_uri,
                 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': stack.password, 'NEO4J_DATABASE': stack.database,
                 'KAFKA_BOOTSTRAP_SERVERS': stack.bootstrap}
        with os.fdopen(fd, 'w') as out:
            out.write(''.join(f'{key}={value}\n' for key, value in extra.items()))
        text = EXAMPLE_UNIT.read_text()
        subs = [('%h/.config/graph-loader/graph-loader.env', str(self.env_file)),
                ('%h/graph-loader/venv/bin/graph-loader', str(Path(ctx.config['venv_bin']) / 'graph-loader')),
                ('%h/graph-loader/schema.yaml', str(ctx.run_dir / 'schema.yaml')),
                ('%h/graph-loader/runs', str(ctx.run_dir / 'runs')),
                ('--runtime process \\\n    --run-dir', f'--runtime process --run-id {ctx.run_id} --stream-group-member-wait-seconds '
                                                        f'{ctx.member_wait:g} \\\n    --run-dir'),
                ('127.0.0.1:8081', f'127.0.0.1:{ctx.health_port}'),
                ('StartLimitBurst=5', f'StartLimitBurst={START_LIMIT_BURST}'),
                # only the directive: the header comments of the example name KillMode=mixed too
                ('\nKillMode=mixed\nKillSignal', f'\nKillMode=mixed\nCPUAffinity={ctx.cpuset}\nMemoryMax={ctx.cfg.service_memory_max}\nMemorySwapMax=0\nKillSignal')]
        for old, new in subs:
            if old not in text:
                raise RuntimeError(f'the example unit no longer contains {old!r}')
            text = text.replace(old, new)
        if '%h' in text:
            raise RuntimeError('an unsubstituted path is left in the unit')
        self.path = self.dir / self.name
        self.path.write_text(text)
        (ctx.run_dir / 'runs').mkdir(exist_ok=True)
        ctl('link', str(self.path))
        ctl('daemon-reload')

    def show(self, *props) -> dict:
        out = ctl('show', self.name, *[f'-p{p}' for p in props]).stdout
        return dict(line.split('=', 1) for line in out.splitlines() if '=' in line)

    def state(self):
        return self.show('ActiveState', 'SubState', 'MainPID', 'Result', 'NRestarts', 'ExecMainCode', 'ExecMainStatus', 'ControlGroup')

    def start(self):
        ctl('start', self.name)

    def stop(self, timeout=180):
        ctl('stop', self.name, timeout=timeout)

    def kill_main(self, sig='KILL'):
        ctl('kill', '-s', sig, '--kill-whom=main', self.name)

    def journal(self, since=None, tail=None):
        args = ['journalctl', '--user', '--user-unit', self.name, '--no-pager', '-o', 'cat']
        if since is not None:
            args += ['--since', f'@{int(since)}']
        if tail:
            args += ['-n', str(tail)]
        return subprocess.run(args, env=sysenv(), capture_output=True, text=True, timeout=60).stdout

    def journal_append(self, path: Path, cursor_file: Path):
        """Append what the unit logged since the last call to ``path`` (journald's own cursor file keeps the position)."""
        done = subprocess.run(['journalctl', '--user', '--user-unit', self.name, '--no-pager', '-o', 'cat', f'--cursor-file={cursor_file}'],
                              env=sysenv(), capture_output=True, text=True, timeout=120)
        if done.stdout:
            with path.open('a') as handle:
                handle.write(done.stdout)
        return len(done.stdout)

    def cgroup_procs(self):
        group = self.show('ControlGroup')['ControlGroup']
        if not group:
            return set()
        procs = Path('/sys/fs/cgroup') / group.lstrip('/') / 'cgroup.procs'
        return {int(p) for p in procs.read_text().split()} if procs.is_file() else set()

    def memory_peak(self):
        group = self.show('ControlGroup')['ControlGroup']
        try:
            return int((Path('/sys/fs/cgroup') / group.lstrip('/') / 'memory.peak').read_text())
        except (OSError, ValueError):
            return 0

    def cleanup(self):
        ctl('stop', self.name, check=False, timeout=240)
        if self.cgroup_procs():
            ctl('kill', '-s', 'KILL', self.name, check=False)
        ctl('reset-failed', self.name, check=False)
        if self.link.is_symlink() and str(self.link.resolve()).startswith(str(self.dir)):
            self.link.unlink()
        ctl('daemon-reload', check=False)
        return {'unit_removed': not self.link.exists(), 'cgroup_left': sorted(self.cgroup_procs())}


class _MainProcess:
    """``Popen``-shaped view of the unit's main process for the supervisor and the faults."""

    def __init__(self, unit, pid, started_at):
        self.unit, self.pid, self.returncode, self.started_at = unit, pid, None, started_at
        rr = _registry()
        self.identity = rr.process_identity(pid)

    def _alive(self):
        rr = _registry()
        current = rr.process_identity(self.pid)
        return current is not None and current == self.identity

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        if self._alive():
            return None
        self.returncode = self._exit_code()
        return self.returncode

    def wait(self, timeout=None):
        end = time.monotonic() + (timeout if timeout is not None else 1e9)
        while self.poll() is None:
            if time.monotonic() > end:
                raise subprocess.TimeoutExpired('main', timeout)
            time.sleep(0.2)
        return self.returncode

    def _exit_code(self):
        """The main process's exit code (negative for a signal) as systemd recorded it for this process."""
        try:
            props = self.unit.show('ExecMainPID', 'ExecMainCode', 'ExecMainStatus')
            if int(props['ExecMainPID']) == self.pid:
                code, status = int(props['ExecMainCode']), int(props['ExecMainStatus'])
                return status if code == 1 else -status
        except (RuntimeError, KeyError, ValueError):
            pass
        match = None            # systemd restarted the unit already: its own journal line, from this process's lifetime only
        for match in re.finditer(r'Main process exited, code=(\w+), status=(\d+)/', self.unit.journal(since=self.started_at - 1)):
            pass
        if match is None:
            return 0
        return int(match.group(2)) if match.group(1) == 'exited' else -int(match.group(2))


class ServiceCli:
    """What ``Fleet`` and the faults read from a ``RunningCli``, for the unit's main process."""

    redactions = 0

    def __init__(self, unit, pid, started_at):
        self.unit, self.started_at = unit, started_at
        self.process = _MainProcess(unit, pid, started_at)
        self.stop_seconds = None

    def running(self):
        return self.process.poll() is None

    def text(self):
        return self.unit.journal(since=self.started_at - 1)

    def terminate(self, timeout=180):
        """``systemctl --user stop``: the operator's ordered stop. Returns the main process's exit code."""
        began = time.monotonic()
        if self.process.poll() is None:
            self.unit.stop(timeout=timeout)
        code = self.process.wait(30)
        self.stop_seconds = time.monotonic() - began
        return code

    def kill(self):
        try:
            self.unit.kill_main('KILL')
        except RuntimeError:
            pass

    def _finish(self):
        pass


class ServiceFleet(HostFleet):
    """systemd owns the command: this incarnation starts the unit only when it is inactive, otherwise it waits for systemd's own restart."""

    shape = 'service'

    def __init__(self, ctx, incarnation, schema_path, *, expected, extra_args=()):
        super().__init__(ctx, incarnation, schema_path, expected=expected)
        if extra_args:
            raise RuntimeError('a unit has a fixed command line')
        unit = ctx.unit
        previous = ctx.last_main_pid
        self.explicit_start = False
        deadline = time.monotonic() + 420
        while True:
            state = unit.state()
            pid = int(state['MainPID'])
            if state['ActiveState'] == 'active' and pid and pid != previous:
                break
            if state['ActiveState'] == 'inactive':
                unit.start()                       # after a clean stop (exit 0) systemd does not restart: the only action allowed
                self.explicit_start = True
            elif state['ActiveState'] == 'failed':
                raise RuntimeError(f'the unit failed and systemd gave up: {state}')
            if time.monotonic() > deadline:
                raise RuntimeError(f'no new main process of the unit within 420 s: {state}')
            time.sleep(0.5)
        self.started_at = time.time()
        ctx.last_main_pid = pid
        self.cli = ServiceCli(unit, pid, self.started_at)
        self.locate_attempt()

    def sigterm(self, timeout=180):
        return self.cli.terminate(timeout=timeout)

    def sigkill_cli(self):
        """``systemctl --user kill -s KILL --kill-whom=main``: systemd then removes the rest and restarts the unit."""
        self.ctx.unit.kill_main('KILL')
        self.cli.process.wait(30)
        return time.monotonic()
