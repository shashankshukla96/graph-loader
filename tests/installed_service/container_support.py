"""Shared plumbing of the E14.F3.S8 single-container cells: the CLI image running ``start --runtime process``.

One owned container runs the CLI image from the candidate manifest (``service_config['images']['cli']``) on the owned Neo4j/Kafka
network. No Docker socket is mounted and ``DOCKER_HOST`` is not set, so the CLI cannot reach any daemon; every role is a child
process inside the container and ``tini`` is PID 1. The run directory is a host bind mount owned by the invoking user (the container
runs as that user), so the run's ``registry.jsonl`` is read from the host while every *process* observation (pids, states, signals,
cgroup counters) is made **inside** the container with ``exec``: a registered pid belongs to the container's pid namespace.

Everything that does not need Docker (process-table parsing, the zombie and OOM-report checks, the container specification) is a plain
function so ``tests/test_single_container_support.py`` can exercise it offline. Nothing in this module is a test.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import docker

from neo4j_graph_loader.orchestrator.run_registry import fold
from tests.installed_service import ownership
from tests.installed_service.live_stack import ROLE_LABEL, free_port, owned_name
from tests.installed_service.process_support import (
    SAMPLE_SECONDS, STOP_BELOW_AVAILABLE_KB, available_kb, records, wait_for,
)

HEALTH_PORT = 8081                        # inside the container
WORK = '/work'                            # the bind-mounted run directory
GRACE = 60                                # ``docker stop -t`` seconds used by the cells
FLEET = 6                                 # lease server + 2 node loaders + 3 edge loaders (the stream fixture)
CHILD_MODULE = 'neo4j_graph_loader._child'
OOM_REPORT = 'cgroup OOM kill observed; victim unknown'
DOCKER_SOCKET_PATHS = ('/var/run/docker.sock', '/run/docker.sock')


# -- scripts run inside the container (the CLI image has python, not procps) -----------------------------------------------------
PS_SCRIPT = r'''
import json, os
rows = []
for name in os.listdir("/proc"):
    if not name.isdigit():
        continue
    try:
        stat = open(f"/proc/{name}/stat").read()
        cmd = open(f"/proc/{name}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
    except OSError:
        continue
    head, _, tail = stat.rpartition(")")
    fields = tail.split()
    rows.append({"pid": int(name), "comm": head.split("(", 1)[1], "state": fields[0], "ppid": int(fields[1]), "cmdline": cmd})
print(json.dumps(rows))
'''

ORPHAN_SCRIPT = r'''
import os, sys, time
count = int(sys.argv[1])
for _ in range(count):
    first = os.fork()
    if first == 0:
        if os.fork() == 0:          # the grandchild: orphaned the moment its parent leaves, so PID 1 inherits and must reap it
            time.sleep(0.2)
            os._exit(0)
        os._exit(0)
    os.waitpid(first, 0)
print(count)
'''

KILL_SCRIPT = r'''
import os, signal, sys
os.kill(int(sys.argv[1]), signal.SIGKILL)
print("killed", sys.argv[1])
'''

# Induce a cgroup OOM kill whose victim is a chosen role: the kernel kills the highest oom_score in the cgroup, so the victim's
# oom_score_adj is raised to 1000 (an owner may always raise its own processes) and this allocator fills the container's memory
# limit until memory.events shows one more oom_kill. With a second pid, that process is SIGKILLed by hand a moment before the
# limit is reached, so the manual kill and the OOM kill land together.
OOM_SCRIPT = r'''
import json, os, signal, sys, time
victim, manual = int(sys.argv[1]), int(sys.argv[2])
def events():
    return {key: int(value) for key, value in (line.split() for line in open("/sys/fs/cgroup/memory.events"))}
def number(path):
    text = open(path).read().strip()
    return None if text == "max" else int(text)
limit = number("/sys/fs/cgroup/memory.max")
assert limit, "the container has no memory limit"
with open(f"/proc/{victim}/oom_score_adj", "w") as handle:
    handle.write("1000")
start = events()
before, invoked = start["oom_kill"], start["oom"]
# The allocation runs in a CHILD: it is the largest process of the cgroup, so a second OOM event that lands before the dying victim's memory
# is released kills it next (seen on the host: two events in the same second). The parent is tiny, outlives that, and reports what the
# kernel did, including whether the allocator itself was taken.
child = os.fork()
if child == 0:
    held, began = [], time.time()
    # `oom` counts the moment the kernel's OOM killer is invoked, `oom_kill` the moment a victim is dead: stop allocating at the FIRST of them.
    while time.time() - began < 120 and events()["oom"] == invoked and events()["oom_kill"] == before:
        near = number("/sys/fs/cgroup/memory.current") > limit - (64 << 20)
        held.append(b"x" * ((16 << 10) if near else (4 << 20)))
        time.sleep(0.0005 if near else 0.002)
    os._exit(0)
_, status = os.waitpid(child, 0)
allocator_killed = os.WIFSIGNALED(status)
hand_killed = False
if manual:      # the hand kill lands within a millisecond of the OOM event: concurrent, and before the run reacts to the lost role
    try:
        os.kill(manual, signal.SIGKILL)
        hand_killed = True
    except ProcessLookupError:
        pass
settle = time.time() + 10
while events()["oom_kill"] == before and time.time() < settle:
    time.sleep(0.05)
after = events()["oom_kill"]      # no more waiting: the failed run ends the container, and this session with it
print(json.dumps({"oom_kill_before": before, "oom_kill_after": after, "hand_killed": hand_killed, "allocator_killed": allocator_killed, "limit": limit}))
'''


MEMORY_EVENTS_SCRIPT = r'''
import json
print(json.dumps({key: int(value) for key, value in (line.split() for line in open("/sys/fs/cgroup/memory.events"))}))
'''


# -- pure helpers (offline-testable) -------------------------------------------------------------------------------------------
def parse_processes(text):
    """The JSON line ``PS_SCRIPT`` prints -> list of process dicts."""
    return json.loads(text.strip().splitlines()[-1])


def dockerfile_entrypoint(path):
    """The exec-form ENTRYPOINT list of a Dockerfile (the last one wins, as in Docker)."""
    found = re.findall(r'^ENTRYPOINT\s+(\[.*\])\s*$', Path(path).read_text(), re.M)
    return json.loads(found[-1]) if found else None


def zombies(procs):
    return [p for p in procs if p['state'] == 'Z']


def fleet_processes(procs):
    """The role children: ``python -I -u -m neo4j_graph_loader._child <role> ...`` (planner workers and the CLI are not these)."""
    return [p for p in procs if CHILD_MODULE in p['cmdline'] and p['state'] != 'Z']


def pid1(procs):
    return next(p for p in procs if p['pid'] == 1)


def oom_lines(text):
    """Every output line that talks about an OOM kill."""
    return [line for line in text.splitlines() if re.search(r'\boom\b', line, re.I)]


def misattributed(text, child_names, child_pids=()):
    """OOM lines that name a child or a pid: the process runtime must report a cgroup OOM at group level only."""
    bad = []
    for line in oom_lines(text):
        if any(name in line for name in child_names) or any(re.search(rf'\b{pid}\b', line) for pid in child_pids):
            bad.append(line)
    return bad


def container_spec(*, image, name, owner, role, network, command, env, host_dir, health_port, uid, gid, mem_mb=None,
                   restart=None, grace=GRACE, platform=None, entrypoint=None):
    """The keyword arguments of ``containers.create`` for one owned single-container run. No socket mount, no privileges."""
    spec = {
        'image': image, 'name': name, 'command': list(command), 'environment': dict(env), 'user': f'{uid}:{gid}', 'working_dir': WORK,
        'volumes': {str(host_dir): {'bind': WORK, 'mode': 'rw'}}, 'ports': {f'{HEALTH_PORT}/tcp': ('127.0.0.1', health_port)},
        'network': network, 'labels': {ownership.OWNER_LABEL: owner, ROLE_LABEL: role}, 'stop_timeout': grace,
    }
    if mem_mb:
        spec.update(mem_limit=f'{mem_mb}m', memswap_limit=f'{mem_mb}m')      # no swap: an over-limit allocation is an OOM kill
    if restart:
        spec['restart_policy'] = dict(restart)
    if platform:
        spec['platform'] = platform
    if entrypoint:
        spec['entrypoint'] = list(entrypoint)
    return spec


def cli_command(*, extra=()):
    return ['start', '--mode', 'stream', '--config', f'{WORK}/schema.yaml', '--runtime', 'process', '--run-dir', f'{WORK}/runs',
            '--health-addr', f'0.0.0.0:{HEALTH_PORT}', *extra]


def live_children(attempt, **where):
    """Registry states of the children without an exit receipt, filtered by label/field."""
    rows = []
    for state in fold(records(attempt)).values():
        if state.get('external') or state.get('phase') == 'exit':
            continue
        labels = state.get('labels') or {}
        if all(labels.get(k, state.get(k)) == v for k, v in where.items()):
            rows.append(state)
    return rows


def unreceipted(attempt):
    """Children the registry started that have no exit receipt (an abrupt end of the CLI leaves them)."""
    return live_children(attempt)


def all_receipted(attempt):
    started = [s for s in fold(records(attempt)).values() if not s.get('external')]
    return bool(started) and all(s.get('phase') == 'exit' for s in started)


# -- the owned container --------------------------------------------------------------------------------------------------------
class SingleContainer:
    """One owned container running the CLI image; created, observed and removed by recorded id and verified owner label."""

    def __init__(self, stack, config, client, tag, schema_text, *, image=None, mem_mb=None, restart=None, platform=None, extra=(),
                 entrypoint=None):
        self.stack, self.config, self.client, self.tag = stack, config, client, tag
        self.image = image or config['images']['cli']['id']
        self.name = owned_name(stack.owner, f's8-{tag}', stack.prefix)
        self.dir = Path(config['run_dir']) / f's8-{tag}'
        self.dir.mkdir()
        (self.dir / 'runs').mkdir()
        (self.dir / 'schema.yaml').write_text(schema_text)
        self.health_port = free_port()
        env = {'HOME': '/tmp', 'NEO4J_URI': stack.container_neo_uri, 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': stack.password,
               'NEO4J_DATABASE': stack.database, 'KAFKA_BOOTSTRAP_SERVERS': stack.container_bootstrap, 'GRAPH_LOADER_METRICS_ENABLED': '0'}
        self.spec = container_spec(image=self.image, name=self.name, owner=stack.owner, role=f's8-{tag}', network=stack.network.name,
                                   command=[] if entrypoint else cli_command(extra=extra), env=env, host_dir=self.dir, health_port=self.health_port,
                                   uid=os.getuid(), gid=os.getgid(), mem_mb=mem_mb, restart=restart, platform=platform,
                                   entrypoint=entrypoint)
        self.mem_mb, self.container = mem_mb, None
        self.started_at = None

    # lifecycle
    def create(self):
        self.stack.registry.write('intent', resource='container', name=self.name)
        # docker-py's high-level create() has no stop_timeout argument (the low-level API does): the cells pass the grace to every
        # `container.stop(timeout=...)` themselves, so the container-level default is not needed.
        self.container = self.client.containers.create(**{k: v for k, v in self.spec.items() if k != 'stop_timeout'})
        self.stack.registry.write('created', resource='container', name=self.name, id=self.container.id)
        GUARDED.append(self)
        return self

    def start(self):
        self.started_at = int(time.time())
        self.stack.registry.write('control', action='start', id=self.container.id)
        self.container.start()
        return self

    def state(self):
        self.container.reload()
        return self.container.attrs['State']

    def running(self):
        return self.state()['Status'] == 'running'

    def text(self, tail=200):
        return self.container.logs(tail=tail).decode(errors='replace')

    def all_text(self):
        return self.container.logs().decode(errors='replace')

    def stop(self, grace=GRACE):
        """``docker stop -t grace``; returns (exit code, seconds)."""
        self.stack.registry.write('control', action='stop', id=self.container.id, deadline=grace)
        began = time.monotonic()
        self.container.stop(timeout=grace)
        seconds = time.monotonic() - began
        return self.state()['ExitCode'], seconds

    def kill(self):
        """``docker kill`` (SIGKILL to PID 1); returns the exit code Docker reports."""
        self.stack.registry.write('control', action='kill', id=self.container.id, signal='SIGKILL')
        self.container.kill(signal='SIGKILL')
        code = self.container.wait(timeout=GRACE)['StatusCode']
        return code

    def wait_exit(self, timeout):
        return self.container.wait(timeout=timeout)['StatusCode']

    def die_codes(self):
        """Exit codes of every ``die`` event of this container since it was first started (the restart policy hides all but the last)."""
        events = self.client.events(since=self.started_at, until=int(time.time()) + 1, decode=True,
                                    filters={'container': self.container.id, 'event': 'die'})
        return [int(e['Actor']['Attributes']['exitCode']) for e in events]

    def cleanup(self):
        if self.container is None:
            return
        try:
            self.container.reload()
        except docker.errors.NotFound:
            return
        finally:
            if self in GUARDED:
                GUARDED.remove(self)
        self.stack.remove_owned(self.container)           # verifies the owner label, then force-removes with its anonymous volumes
        try:
            self.client.containers.get(self.container.id)
        except docker.errors.NotFound:
            return
        raise RuntimeError(f'owned container {self.name} survived its removal')

    # observation inside the container
    def run_python(self, script, *args):
        result = self.container.exec_run(['python', '-I', '-c', script, *map(str, args)])
        text = result.output.decode(errors='replace')
        return result.exit_code, text

    def processes(self):
        code, text = self.run_python(PS_SCRIPT)
        if code != 0:
            raise AssertionError(f'process listing failed: {code} {text[-500:]}')
        return parse_processes(text)

    def exists_in_container(self, path):
        return self.container.exec_run(['python', '-I', '-c', f'import os,sys; sys.exit(0 if os.path.exists({path!r}) else 1)']).exit_code == 0

    def memory_events(self):
        code, text = self.run_python(MEMORY_EVENTS_SCRIPT)
        assert code == 0, text[-300:]
        return json.loads(text.strip().splitlines()[-1])

    def kill_inside(self, pid):
        code, text = self.run_python(KILL_SCRIPT, pid)
        assert code == 0, f'could not kill pid {pid} inside the container: {text[-300:]}'

    def induce_oom(self, victim_pid, manual_pid=0):
        """Returns the observer's JSON. Exit 137 means the failed run ended the container (and this exec session) first: that is reported as
        ``observer_lost`` rather than an error, because the run's own output carries the oom_kill counters (see the callers)."""
        code, text = self.run_python(OOM_SCRIPT, victim_pid, manual_pid)
        if code == 137:
            return {'observer_lost': True}
        assert code == 0, f'the OOM observer exited {code}: {text[-500:]}'
        return json.loads(text.strip().splitlines()[-1])

    # observation on the host
    def attempts(self):
        found = [p for p in (self.dir / 'runs').iterdir() if (p / 'registry.jsonl').is_file()] if (self.dir / 'runs').is_dir() else []
        return sorted(found, key=lambda p: (p / 'registry.jsonl').stat().st_mtime)      # attempt names are random: oldest first by activity

    def attempt(self, *, index=-1, wait=60):
        end = time.monotonic() + wait
        while time.monotonic() < end:
            found = self.attempts()
            if found:
                return found[index]
            time.sleep(0.3)
        raise AssertionError('the container never created an attempt directory')

    def ready(self, timeout=240):
        def probe():
            try:
                with urllib.request.urlopen(f'http://127.0.0.1:{self.health_port}/readyz', timeout=3) as response:
                    return response.status == 200
            except (urllib.error.URLError, OSError):
                return False
        return wait_for('/readyz 200', probe, timeout, step=1)

    def inspect_summary(self):
        """Allowlisted facts about how the container was configured (never the environment: it carries the scratch password)."""
        self.container.reload()
        attrs = self.container.attrs
        host = attrs['HostConfig']
        return {'binds': host.get('Binds'), 'privileged': host.get('Privileged'), 'pid_mode': host.get('PidMode'),
                'network_mode': host.get('NetworkMode'), 'restart_policy': host.get('RestartPolicy'), 'memory': host.get('Memory'),
                'memory_swap': host.get('MemorySwap'), 'init': host.get('Init'), 'mounts': [(m['Type'], m.get('Source'), m['Destination'])
                                                                                              for m in attrs.get('Mounts', [])],
                'has_docker_host_env': any(e.startswith('DOCKER_HOST=') for e in attrs['Config'].get('Env') or []),
                'entrypoint': attrs['Config'].get('Entrypoint'), 'image': attrs['Image']}


class View:
    """What ``mid_load`` / ``wait_for`` need from a CLI: a liveness test and the recent log."""

    def __init__(self, box):
        self.box = box

    def running(self):
        return self.box.running()

    def text(self):
        return self.box.text()[-2500:]


# -- host memory guard for container cells --------------------------------------------------------------------------------------
GUARDED = []                                                  # containers that are live right now (stopped the ordinary way on a breach)


class ContainerMemoryWatch(threading.Thread):
    """Samples MemAvailable; on a breach stops every live guarded container with ``docker stop`` (the CLI's ordered stop)."""

    def __init__(self):
        super().__init__(name='container-memory-watch', daemon=True)
        self.done = threading.Event()
        self.minimum = available_kb()
        self.samples, self.breached = 1, False

    def run(self):
        while not self.done.wait(SAMPLE_SECONDS):
            value = available_kb()
            self.samples += 1
            self.minimum = min(self.minimum, value)
            if value < STOP_BELOW_AVAILABLE_KB and not self.breached:
                self.breached = True
                for box in list(GUARDED):
                    try:
                        box.container.stop(timeout=GRACE)
                    except Exception:
                        pass
