"""Bounded launch of the installed ``graph-loader`` CLI and observation of its app containers.

The wheel ships no creation-observation adapter, so containers the CLI creates
are noticed from the Docker event stream (network attachment to the owned
network, or an explicitly supplied ``attempt`` label) and inspected on first sight. There is
no durable intent before the daemon create; a container that exists only
between events is recorded with its event attributes alone.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

import docker

from . import ownership

ENV_KEEP = ('PATH', 'HOME', 'TMPDIR', 'LANG', 'LC_ALL')
LABEL_ALLOW = ('app', 'component', 'attempt', 'graph_loader.kind', 'graph_loader.metrics_enabled',
               'graph_loader.metrics_port', 'io.graph-loader.component', ownership.OWNER_LABEL)
TAIL_BYTES = 16 * 1024
MAX_EVENTS = 5000


class CliResult:
    def __init__(self, exit_code, timed_out, elapsed, stdout, stderr, tail, redactions):
        self.exit_code, self.timed_out, self.elapsed = exit_code, timed_out, elapsed
        self.stdout, self.stderr, self.tail, self.redactions = stdout, stderr, tail, redactions


def cli_environment(stack, images, docker_host, extra=None):
    """Explicit environment: addresses, image IDs and credentials; nothing ambient."""
    env = {key: os.environ[key] for key in ENV_KEEP if key in os.environ}
    env.update(
        DOCKER_HOST=docker_host,
        NEO4J_URI=stack.neo_uri, NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=stack.password, NEO4J_DATABASE=stack.database,
        KAFKA_BOOTSTRAP_SERVERS=stack.bootstrap,
        LOADER_NEO4J_URI=stack.container_neo_uri, LOADER_KAFKA_BOOTSTRAP_SERVERS=stack.container_bootstrap,
        GRAPH_LOADER_METRICS_ENABLED='0',
        **{f'GRAPH_LOADER_{role.upper()}_IMAGE': row['id'] for role, row in images.items()})
    env.update(extra or {})
    return env


def run_cli(executable, args, *, env, cwd, log_dir, name, timeout, redact=()):
    """Run the installed CLI under its own hard deadline; output is capped and redacted."""
    out_path, err_path = Path(log_dir) / f'{name}.out', Path(log_dir) / f'{name}.err'
    started = time.monotonic()
    timed_out = False
    with open(out_path, 'xb') as out, open(err_path, 'xb') as err:
        process = subprocess.Popen([str(executable), *args], cwd=cwd, env=env, stdout=out, stderr=err,
                                   start_new_session=True)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
    secrets_ = [value.encode() for value in redact if value]
    texts = []
    redactions = 0
    for path in (out_path, err_path):
        data = path.read_bytes()
        for secret in secrets_:
            redactions += data.count(secret)
            data = data.replace(secret, b'[redacted]')
        path.write_bytes(data)
        texts.append(data)
    return CliResult(process.returncode, timed_out, time.monotonic() - started, out_path, err_path,
                     (texts[1][-TAIL_BYTES:] or texts[0][-TAIL_BYTES:]).decode(errors='replace'), redactions)


def project(attrs):
    """Allowlisted view of an inspected container; never environment or command."""
    config, host = attrs.get('Config', {}), attrs.get('HostConfig', {})
    return {
        'id': attrs['Id'], 'name': attrs['Name'].lstrip('/'), 'image_id': attrs['Image'], 'image_ref': config.get('Image'),
        'created': attrs['Created'],
        'labels': {k: v for k, v in (config.get('Labels') or {}).items() if k in LABEL_ALLOW},
        'mounts': sorted((m['Type'], m.get('Source', ''), m['Destination'], bool(m.get('RW'))) for m in attrs.get('Mounts', [])),
        'networks': sorted(n['NetworkID'] for n in attrs.get('NetworkSettings', {}).get('Networks', {}).values()),
        'privileged': bool(host.get('Privileged')), 'network_mode': host.get('NetworkMode'),
        'auto_remove': bool(host.get('AutoRemove')), 'pid_mode': host.get('PidMode') or '',
        'published': sorted((host.get('PortBindings') or {}).keys()),
    }


class AppObserver:
    """Records containers the CLI creates on the owned network.

    Completeness rests on ``barrier()``: an owned volume is created and removed
    after the CLI has finished, and the observer must see both events. Events on
    one stream are ordered, so everything the CLI did before the barrier has been
    handled when the barrier's events arrive. ``stop()`` reports whether the
    reader thread really ended.
    """

    join_timeout = 10

    def __init__(self, client, registry, network_id, attempt=None, *, owner, candidate_images=(), barrier_tag=''):
        self.client, self.registry = client, registry
        self.barrier_tag = barrier_tag
        self.network_id, self.attempt, self.owner = network_id, attempt, owner
        self.refused = {}
        self.protected = frozenset()
        self.stopped = None
        self._volume_events = set()
        self._barriers = 0
        self.candidate_images = frozenset(candidate_images)
        self.created_from_candidates = {}
        self.containers = {}
        self.events = []
        self.errors = []
        self._stream = None
        self._thread = None
        self._stop = threading.Event()
        self.ended = set()
        self.closed_early = False
        self._lock = threading.Lock()

    def start(self):
        if not ownership.labelled(self.client.api.inspect_network(self.network_id).get('Labels'), self.owner):
            raise RuntimeError('the owned network does not carry the run owner label')
        # Anything that exists before the run is never ours to adopt or remove.
        self.protected = frozenset(row['Id'] for row in self.client.api.containers(all=True))
        self._stream = self.client.events(since=time.time(), decode=True,
                                          filters={'type': ['container', 'network', 'volume']})
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        try:
            for event in self._stream:
                if self._stop.is_set():
                    return
                self._handle(event)
            self.closed_early = not self._stop.is_set()
        except Exception as error:
            if not self._stop.is_set():
                self.errors.append(type(error).__name__)

    def _handle(self, event):
        actor = event.get('Actor', {})
        attrs = actor.get('Attributes', {})
        kind, action = event.get('Type'), event.get('Action', '').split(':')[0]
        if kind == 'volume':
            with self._lock:
                self._volume_events.add((action, actor.get('ID')))
                if len(self.events) < MAX_EVENTS:
                    self.events.append({'type': kind, 'action': action, 'id': actor.get('ID'), 'time': event.get('timeNano')})
            return
        if kind == 'container' and action == 'create' and attrs.get('image') in self.candidate_images:
            with self._lock:
                self.created_from_candidates[actor.get('ID')] = attrs.get('name')
        if kind == 'container' and action in ('destroy', 'die'):
            with self._lock:
                self.ended.add(actor.get('ID'))
        if kind == 'network':
            if actor.get('ID') != self.network_id or action not in ('connect', 'disconnect'):
                return
            container_id = attrs.get('container')
        else:
            container_id = actor.get('ID')
            if container_id not in self.containers and (self.attempt is None or attrs.get('attempt') != self.attempt):
                return
        if not container_id or container_id in self.refused:
            return
        with self._lock:
            if len(self.events) < MAX_EVENTS:
                self.events.append({'type': kind, 'action': action, 'id': container_id, 'time': event.get('timeNano'),
                                    **({'exit_code': attrs['exitCode']} if 'exitCode' in attrs else {})})
            row = self.containers.setdefault(container_id, {'id': container_id, 'inspected': None,
                                                            'event_name': None, 'event_image': None})
            if kind == 'container':
                row['event_name'], row['event_image'] = attrs.get('name'), attrs.get('image')
            if row['inspected'] is None:
                try:
                    row['inspected'] = project(self.client.api.inspect_container(container_id))
                except docker.errors.NotFound:
                    row['inspected'] = False
                except Exception as error:
                    self.errors.append(type(error).__name__)
                info = row['inspected'] or {}
                label = (info.get('labels') or {}).get(ownership.OWNER_LABEL)
                reason = None
                if container_id in self.protected:
                    reason = 'existed before the run'
                elif label is not None and label != self.owner:
                    reason = 'owner label ' + label
                elif info and label is None and info['image_id'] not in self.candidate_images:
                    reason = 'unlabelled and not created from a candidate image'
                if reason is not None:
                    # Attached to our network but not provably ours: never adopted, never removed here.
                    del self.containers[container_id]
                    self.refused[container_id] = reason
                    self.registry.write('refused', resource='container', id=container_id, reason=reason)
                    return
                self.registry.write('observed', resource='container', id=container_id,
                                    inspected=bool(row['inspected']), name=(row['inspected'] or {}).get('name') or row['event_name'])

    def _await(self, key, timeout):
        end = time.monotonic() + timeout
        while True:
            with self._lock:
                if key in self._volume_events:
                    return
            if not self._thread.is_alive():
                raise RuntimeError('the event stream ended before the barrier was seen')
            if time.monotonic() > end:
                raise TimeoutError('the event stream did not deliver the barrier in time')
            time.sleep(0.02)

    def barrier(self, timeout=30):
        """Create and remove one owned volume; both events must arrive through the stream before this returns."""
        with self._lock:
            self._barriers += 1
            name = f'e09-{self.owner}-barrier-{self.barrier_tag}{self._barriers}'
        self.registry.write('intent', resource='volume', name=name)
        made = self.client.api.create_volume(name=name, labels={ownership.OWNER_LABEL: self.owner})
        if not ownership.labelled((made or {}).get('Labels'), self.owner):
            # Docker returns an existing volume of that name instead of failing: it is not ours to remove.
            self.registry.write('refused', resource='volume', name=name, reason='existing volume without the run owner label')
            raise RuntimeError('the barrier volume already exists without the run owner label')
        try:
            self.registry.write('created', resource='volume', name=name, id=name)
            self._await(('create', name), timeout)
        finally:
            self._remove_barrier_volume(name)
        self._await(('destroy', name), timeout)

    def _remove_barrier_volume(self, name):
        try:
            labels = self.client.api.inspect_volume(name).get('Labels')
        except docker.errors.NotFound:
            return
        if not ownership.labelled(labels, self.owner):
            self.registry.write('refused', resource='volume', name=name, reason='owner label changed before removal')
            raise RuntimeError('the barrier volume no longer carries the run owner label')
        self.client.api.remove_volume(name, force=True)
        self.registry.write('removed', resource='volume', id=name)

    def stop(self):
        self._stop.set()
        try:
            self._stream.close()
        except Exception:
            pass
        self._thread.join(timeout=self.join_timeout)
        self.stopped = not self._thread.is_alive()
        return self.stopped

    def drain(self, timeout=30):
        """Wait until every observed container has a die/destroy event or no longer exists."""
        end = time.monotonic() + timeout
        while True:
            with self._lock:
                pending = [cid for cid in self.containers if cid not in self.ended]
            alive = []
            for cid in pending:
                try:
                    if self.client.api.inspect_container(cid)['State']['Running']:
                        alive.append(cid)
                except docker.errors.NotFound:
                    pass
            if not alive:
                return
            if time.monotonic() > end:
                raise TimeoutError('observed application containers are still running')
            time.sleep(0.5)

    def unobserved_candidates(self):
        """Containers created from a candidate image in the window that were never recorded."""
        with self._lock:
            return sorted(name or cid for cid, name in self.created_from_candidates.items() if cid not in self.containers)

    def snapshot(self):
        with self._lock:
            return {'containers': json.loads(json.dumps(self.containers)), 'events': list(self.events),
                    'errors': list(self.errors), 'closed_early': self.closed_early, 'stopped': self.stopped,
                    'refused': dict(self.refused),
                    'created_from_candidates': dict(self.created_from_candidates)}

    def kill_observed(self, container_id, *, timeout=30):
        """SIGKILL one adopted application container after a fresh ownership check and read its terminal state back."""
        with self._lock:
            adopted = container_id in self.containers
        if not adopted or container_id in self.protected or container_id in self.refused:
            raise RuntimeError('refusing to terminate a container this observer did not adopt')
        attrs = self.client.api.inspect_container(container_id)
        if not ownership.container_owned(attrs, self.owner, {self.network_id}, self.candidate_images):
            raise RuntimeError('refusing to terminate a container that is not provably ours')
        self.registry.write('control', action='kill', id=container_id, signal='SIGKILL', deadline=timeout)
        self.client.api.kill(container_id, signal='SIGKILL')
        try:
            exit_code = self.client.api.wait(container_id, timeout=timeout)['StatusCode']
        except docker.errors.NotFound:
            return {'exit_code': None, 'status': 'removed', 'oom_killed': None}
        try:
            state = self.client.api.inspect_container(container_id)['State']
        except docker.errors.NotFound:
            # The CLI may already have removed the failed phase's containers.
            return {'exit_code': exit_code, 'status': 'removed', 'oom_killed': None}
        return {'exit_code': exit_code, 'status': state['Status'], 'oom_killed': state['OOMKilled']}

    def reap(self):
        """Force-remove still-present observed containers attached to the owned network."""
        removed, failed = [], []
        for container_id in list(self.containers):
            try:
                attrs = self.client.api.inspect_container(container_id)
            except docker.errors.NotFound:
                continue
            if container_id in self.protected or not ownership.container_owned(attrs, self.owner, {self.network_id},
                                                                              self.candidate_images):
                failed.append((container_id, 'not provably ours: owner label, network, image or pre-existence'))
                continue
            try:
                self.client.api.remove_container(container_id, force=True, v=True)
                self.registry.write('removed', resource='container', id=container_id)
                removed.append(container_id)
            except docker.errors.NotFound:
                pass
            except Exception as error:
                failed.append((container_id, type(error).__name__))
        return {'removed': removed, 'failed': failed}


class RunningCli:
    """A long-running installed ``graph-loader`` (streaming) with a hard deadline, SIGTERM stop and bounded, redacted output."""

    def __init__(self, executable, args, *, env, cwd, log_dir, name, deadline, redact=()):
        self.out_path, self.err_path = Path(log_dir) / f'{name}.out', Path(log_dir) / f'{name}.err'
        self.redact, self.deadline_at = [v.encode() for v in redact if v], time.monotonic() + deadline
        self.started = time.monotonic()
        self._out, self._err = open(self.out_path, 'xb'), open(self.err_path, 'xb')
        self.process = subprocess.Popen([str(executable), *args], cwd=cwd, env=env, stdout=self._out, stderr=self._err,
                                        start_new_session=True)
        self.stop_seconds = None

    def running(self):
        if time.monotonic() > self.deadline_at and self.process.poll() is None:
            self.kill()
            raise TimeoutError('the streaming CLI exceeded its overall deadline and was killed')
        return self.process.poll() is None

    def text(self):
        for stream in (self._out, self._err):
            if not stream.closed:
                stream.flush()
        return self.out_path.read_bytes().decode(errors='replace') + self.err_path.read_bytes().decode(errors='replace')

    def terminate(self, timeout=60):
        """SIGTERM the CLI (as an operator would) and wait; returns its exit code (None if it had to be killed)."""
        began = time.monotonic()
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            code = None
        self.stop_seconds = time.monotonic() - began
        self._finish()
        return code

    def kill(self):
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        self._finish()

    def _finish(self):
        for stream in (self._out, self._err):
            try:
                stream.close()
            except OSError:
                pass
        self.redactions = 0
        for path in (self.out_path, self.err_path):
            data = path.read_bytes()
            for secret in self.redact:
                self.redactions += data.count(secret)
                data = data.replace(secret, b'[redacted]')
            path.write_bytes(data)
