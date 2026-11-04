#!/usr/bin/env python3
"""E19.F4.S9 proof harness (private): the D19-8 restricted-network topology on the Linux host, built and removed by recorded id.

    python3 e19_proof_harness.py WORKDIR TOKEN snapshot OUT.json
    python3 e19_proof_harness.py WORKDIR TOKEN compare BEFORE.json AFTER.json
    python3 e19_proof_harness.py WORKDIR TOKEN seed        # the only phase with public access
    python3 e19_proof_harness.py WORKDIR TOKEN up          # isolated topology: internal network, registry, index, Debian repo, dind, runner
    python3 e19_proof_harness.py WORKDIR TOKEN verify      # preflight, buildx build into the inner store, ownership controls
    python3 e19_proof_harness.py WORKDIR TOKEN isolation   # S10: denied-public / allowed-internal pairs in four places and the non-isolated control
    python3 e19_proof_harness.py WORKDIR TOKEN down        # removal by recorded id

Everything is stdlib and runs on the host that owns the Docker daemon (the files are copied to WORKDIR first). Rules the code enforces:
names ``e19-<token>-*``, the owner label on every container, network and volume, ids recorded in ``state.json`` the moment they exist, removal only
of recorded ids (no force, no prune, no wildcard), images removed only under the ownership rules in ``remove_images``, no published host port,
a host-memory guard (stop, clean up and report below ``MIN_AVAILABLE_MB``) and a disk and memory budget. Credentials live in 0600 files and
are passed on stdin or through ``--env-file``; they are never logged or put on a command line.
"""
from __future__ import annotations

import argparse
import json
import lzma
import os
import re
import secrets
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from hashlib import sha256
from pathlib import Path

OWNER_LABEL = 'io.graph-loader.e19-owner'
MIN_AVAILABLE_MB = 2048            # the host keeps at least this much memory available at all times (orchestrator, S9)
MEMORY_BUDGET_MB = 5120            # dind and everything inside it, plus the small services and the runner
DISK_BUDGET_MB = 10240
CPU_BUDGET = 4.0
LISTEN_DENY = {5432, 7474, 7687, 8080, 9092, 9101}
USER_RESOURCES = {'containers': {'postgres', 'graph-loader-neo4j', 'graph-loader-kafka', 'graph-loader-kafka-ui'},
                  'networks': {'graph-loader-net', 'ws_default'}}

# Everything public is pulled by digest (the digests were read from the registries on 2026-11-04).
PINS = {
    'python': ('python', 'sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30'),                 # python:3.12-slim
    'dind': ('docker', 'sha256:0b6d18a4a222e71c88be6034926b4f6cde2fc33c2218643d03ccfddc65995470'),                   # docker:29.9.0-dind
    'registry': ('registry', 'sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373'),             # registry:2.8.3
    'pypiserver': ('pypiserver/pypiserver', 'sha256:286ce50e4a53b3fbc880d944d6a5e010ca6ccb1fe2ecad669b9f00a349107b20'),   # v2.3.2
    'nginx': ('nginx', 'sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10'),                   # 1.27-alpine
    'httpd': ('httpd', 'sha256:070da7661f0be8a05430b2c125c0cde9e53176b4048c813024021969a0e3893c'),                   # 2.4-alpine (htpasswd only)
    'neo4j': ('neo4j', 'sha256:c07aabc5a52c7097bcdc3085cc22152b3804f86752aecac4d668f0ab177afe15'),                   # 5.26-enterprise
    'kafka': ('confluentinc/cp-kafka', 'sha256:1fa3cdb3ef77b75bc56e7040bbdfa2bd6950b6ee6363381b2493f7a83e07a0b3'),   # 7.7.0
}
# What the inner daemon gets from the owned registry: (pin, path in the registry)
REGISTRY_CONTENT = {'python': 'e19/python:3.12-slim', 'neo4j': 'e19/neo4j:5.26-enterprise', 'kafka': 'e19/cp-kafka:7.7.0'}
LIMITS = {   # role: (memory MB, cpus)
    'dns': (48, 0.1), 'registry': (128, 0.3), 'pypiserver': (128, 0.2), 'pypi-proxy': (64, 0.1), 'debian': (64, 0.1),
    'dind': (3180, 2.6), 'runner': (1500, 0.5)}
DEBIAN_MIRROR_SOURCE = 'http://deb.debian.org/debian'
OWNED_NAMES = ('registry', 'pypi', 'debian', 'registry.e19.internal', 'pypi.e19.internal', 'debian.e19.internal')
INTERNAL_DOMAIN = 'e19.internal'     # fully qualified aliases of the owned services: the inner daemon's embedded DNS does not forward single-label names
PUBLIC_NAMES = ('registry-1.docker.io', 'auth.docker.io', 'index.docker.io', 'production.cloudflare.docker.com', 'docker.io', 'ghcr.io', 'pypi.org',
                'files.pythonhosted.org', 'deb.debian.org')
SEED_ROLE_LIMIT = (768, 1.0)


class HarnessError(RuntimeError):
    pass


class GuardTripped(HarnessError):
    pass


def sh(args, *, input=None, check=True, env=None, timeout=900, cwd=None) -> subprocess.CompletedProcess:
    done = subprocess.run([str(a) for a in args], input=input, capture_output=True, text=isinstance(input, str) or input is None,
                          env=env, timeout=timeout, cwd=cwd)
    if check and done.returncode:
        err = done.stderr if isinstance(done.stderr, str) else done.stderr.decode(errors='replace')
        raise HarnessError(f'{args[0]} {args[1] if len(args) > 1 else ""} failed ({done.returncode}): {err.strip()[-500:]}')
    return done


def mem_available_mb() -> int:
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) // 1024
    raise HarnessError('MemAvailable not found')


def disk_free_gb(path='/') -> float:
    usage = shutil.disk_usage(path)
    return usage.free / 2**30


class Harness:
    def __init__(self, work: Path, token: str, docker: str = 'docker', *, memory_probe=mem_available_mb):
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{2,30}', token):
            raise HarnessError('the token must be lowercase letters, digits and dashes')
        self.work, self.token, self.docker, self.memory_probe = Path(work), token, docker, memory_probe
        self.prefix = f'e19-{token}'
        self.work.mkdir(parents=True, exist_ok=True)
        self.state_path = self.work / 'state.json'
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {
            'token': token, 'containers': {}, 'networks': {}, 'volumes': [], 'images': {}, 'created': []}
        (self.work / 'logs').mkdir(exist_ok=True)
        self.log_path = self.work / 'logs' / 'harness.jsonl'
        self._watch_stop = threading.Event()
        self.tripped = None
        self.min_available_mb = None

    # -- records ------------------------------------------------------------------------------------------------------------------------------
    def save(self) -> None:
        tmp = self.state_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(self.state, indent=1))
        os.replace(tmp, self.state_path)

    def log(self, event: str, **fields) -> None:
        entry = {'t': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'event': event, **fields}
        with open(self.log_path, 'a') as handle:
            handle.write(json.dumps(entry) + '\n')
        print(json.dumps(entry), flush=True)

    def d(self, *args, **kw) -> subprocess.CompletedProcess:
        return sh([self.docker, *args], **kw)

    # -- guards ---------------------------------------------------------------------------------------------------------------------------------
    def guard(self, where: str = '') -> int:
        available = self.memory_probe()
        self.min_available_mb = available if self.min_available_mb is None else min(self.min_available_mb, available)
        if available < MIN_AVAILABLE_MB:
            self.tripped = f'host memory available {available} MB is below {MIN_AVAILABLE_MB} MB ({where})'
            raise GuardTripped(self.tripped)
        return available

    def watch(self):
        """Poll the memory guard in the background during long waits; ``self.tripped`` is set when it fires."""
        def loop():
            while not self._watch_stop.wait(5):
                try:
                    self.guard('background')
                except GuardTripped:
                    return
        self._watch_stop.clear()
        threading.Thread(target=loop, daemon=True).start()

    def unwatch(self):
        self._watch_stop.set()

    def wait_for(self, what: str, probe, timeout: float = 120, interval: float = 2):
        end = time.time() + timeout
        last = None
        while time.time() < end:
            if self.tripped:
                raise GuardTripped(self.tripped)
            try:
                result = probe()
                if result:
                    return result
            except (HarnessError, subprocess.SubprocessError, OSError) as exc:
                last = exc
            time.sleep(interval)
        raise HarnessError(f'timeout waiting for {what}' + (f' ({str(last)[:200]})' if last else ''))

    def budget(self) -> dict:
        """Declared limits of the owned containers against the budget, and the measured disk use of the owned data."""
        declared = {name: LIMITS[role] for name, role in ((n, n.removeprefix(self.prefix + '-')) for n in self.state['containers']) if role in LIMITS}
        memory = sum(m for m, _ in declared.values())
        cpus = round(sum(c for _, c in declared.values()), 2)
        used = 0
        for path in (self.work,):
            out = sh(['du', '-sk', path], check=False).stdout.split()
            used += int(out[0]) // 1024 if out else 0
        df = sh([self.docker, 'system', 'df', '-v', '--format', 'json'], check=False)
        volumes_mb = 0
        try:
            for volume in json.loads(df.stdout).get('Volumes', []):
                if volume.get('Name', '').startswith(self.prefix + '-'):
                    volumes_mb += _size_mb(volume.get('Size', '0B'))
        except (ValueError, AttributeError):
            pass
        images_mb = 0
        for record in self.state['images'].values():
            if record.get('new') and record.get('id'):
                size = self.d('image', 'inspect', '--format', '{{.Size}}', record['id'], check=False)
                images_mb += int(size.stdout.strip() or 0) // 2**20 if size.returncode == 0 else 0
        return {'memory_limits_mb': memory, 'memory_budget_mb': MEMORY_BUDGET_MB, 'cpus': cpus, 'cpu_budget': CPU_BUDGET,
                'work_dir_mb': used, 'owned_volumes_mb': volumes_mb, 'new_images_on_shared_daemon_mb': images_mb, 'disk_budget_mb': DISK_BUDGET_MB,
                'min_host_memory_available_mb': self.min_available_mb, 'host_memory_floor_mb': MIN_AVAILABLE_MB,
                'ok': memory <= MEMORY_BUDGET_MB and cpus <= CPU_BUDGET and used + volumes_mb + images_mb <= DISK_BUDGET_MB}

    # -- recorded creation -----------------------------------------------------------------------------------------------------------------------
    def label_args(self):
        return ['--label', f'{OWNER_LABEL}={self.token}']

    def create_network(self, name: str, *, internal: bool) -> str:
        full = f'{self.prefix}-{name}'
        args = ['network', 'create', '--driver', 'bridge', *self.label_args(), *(['--internal'] if internal else []), full]
        nid = self.d(*args).stdout.strip()
        self.state['networks'][full] = nid
        self.save()
        self.log('network', name=full, id=nid, internal=internal)
        return full

    def create_volume(self, name: str) -> str:
        full = f'{self.prefix}-{name}'
        self.d('volume', 'create', *self.label_args(), full)
        self.state['volumes'].append(full)
        self.save()
        self.log('volume', name=full)
        return full

    def create_container(self, role: str, image: str, command=(), *, network=None, aliases=(), env=None, env_file=None, mounts=(), memory=None,
                         cpus=None, privileged=False, user=None, dns=None, dns_search=None, read_only=False, entrypoint=None, init=False, tty=False, stdin=False) -> str:
        name = f'{self.prefix}-{role}'
        mem, cpu = (memory, cpus) if memory else LIMITS[role]
        args = ['create', '--name', name, *self.label_args(), '--memory', f'{mem}m', '--memory-swap', f'{mem}m', '--cpus', str(cpu), '--pids-limit', '2048']
        if privileged:
            args.append('--privileged')
        else:
            args += ['--security-opt', 'no-new-privileges']
        if network:
            args += ['--network', network]
            for alias in aliases:
                args += ['--network-alias', alias]
        if dns:
            args += ['--dns', dns]
        if dns_search:
            args += ['--dns-search', dns_search]
        if user:
            args += ['--user', user]
        if read_only:
            args.append('--read-only')
        if init:
            args.append('--init')
        if stdin:
            args.append('-i')
        if entrypoint is not None:
            args += ['--entrypoint', entrypoint]
        for key, value in (env or {}).items():
            args += ['-e', f'{key}={value}']
        if env_file:
            args += ['--env-file', str(env_file)]
        for mount in mounts:
            args += ['-v', mount]
        if any(a in ('-p', '--publish', '-P', '--publish-all') for a in args):
            raise HarnessError('no published host ports')
        cid = self.d(*args, image, *command).stdout.strip()
        self.state['containers'][name] = cid
        self.record_anonymous_volumes(name, cid)
        self.save()
        self.log('container', name=name, id=cid, memory_mb=mem, cpus=cpu, privileged=privileged)
        return name

    def record_anonymous_volumes(self, name: str, cid: str) -> None:
        """An image's VOLUME line creates an anonymous volume with the container: record its name at creation, remove it with the container."""
        mounts = self.d('inspect', '--format', '{{json .Mounts}}', cid, check=False)
        try:
            found = [m['Name'] for m in json.loads(mounts.stdout or '[]') if m.get('Type') == 'volume' and re.fullmatch(r'[0-9a-f]{64}', m.get('Name', ''))]
        except (ValueError, TypeError):
            found = []
        for volume in found:
            self.state.setdefault('anonymous_volumes', {})[volume] = name
            self.log('volume', name=volume, anonymous=True, container=name)

    def start(self, name: str) -> None:
        name = name if name.startswith(self.prefix + '-') else f'{self.prefix}-{name}'
        self.guard(f'before starting {name}')
        self.d('start', self.state['containers'][name])

    def run_once(self, role: str, image: str, command=(), *, input=None, mounts=(), network=None, user=None, env=None, entrypoint=None, timeout=1800,
                 memory=None, cpus=None) -> str:
        """Create, run to completion and remove (by recorded id) a short-lived container; returns its stdout."""
        name = self.create_container(role, image, command, network=network, mounts=mounts, user=user, env=env, entrypoint=entrypoint,
                                     memory=memory or SEED_ROLE_LIMIT[0], cpus=cpus or SEED_ROLE_LIMIT[1], stdin=input is not None)
        cid = self.state['containers'][name]
        try:
            self.guard(f'before running {name}')
            done = self.d('start', '-a', *(['-i'] if input is not None else []), cid, input=input, check=False, timeout=timeout)
            if done.returncode:
                raise HarnessError(f'{name} exited {done.returncode}: {(done.stdout + done.stderr)[-600:]}')
            return done.stdout
        finally:
            self.remove_container(name)

    def exec(self, name: str, *cmd, input=None, check=True, env_file=None, env=None, timeout=900, workdir=None) -> subprocess.CompletedProcess:
        args = ['exec', *(['-i'] if input is not None else []), *(['--env-file', str(env_file)] if env_file else []), *(['-w', workdir] if workdir else [])]
        for key, value in (env or {}).items():
            args += ['-e', f'{key}={value}']
        name = name if name.startswith(self.prefix + '-') else f'{self.prefix}-{name}'
        return self.d(*args, self.state['containers'][name], *cmd, input=input, check=check, timeout=timeout)

    # -- removal by recorded id -----------------------------------------------------------------------------------------------------------------
    def remove_container(self, name: str) -> None:
        cid = self.state['containers'].get(name)
        if not cid:
            return
        self.d('stop', '-t', '30', cid, check=False)
        done = self.d('rm', '-v', cid, check=False)       # -v: only the container's own anonymous volumes (an image's VOLUME line); named volumes stay
        if done.returncode and 'No such container' not in done.stderr:
            raise HarnessError(f'could not remove {name} ({cid[:12]}): {done.stderr.strip()[:200]}')
        del self.state['containers'][name]
        for volume, owner in list(self.state.get('anonymous_volumes', {}).items()):
            if owner == name:
                if self.d('volume', 'inspect', volume, check=False).returncode == 0:
                    self.d('volume', 'rm', volume)          # recorded at creation; `rm -v` normally took it already
                    self.log('removed', kind='anonymous-volume', name=volume, container=name, by='explicit name')
                del self.state['anonymous_volumes'][volume]
        self.save()
        self.log('removed', kind='container', name=name, id=cid)

    def owned(self, kind: str, ident: str) -> bool:
        fmt = '{{index .Config.Labels "%s"}}' % OWNER_LABEL if kind == 'container' else '{{index .Labels "%s"}}' % OWNER_LABEL
        done = self.d(kind, 'inspect', '--format', fmt, ident, check=False)
        return done.returncode != 0 or done.stdout.strip() == self.token       # absent counts as owned-nothing-to-do

    def remove_image_refs(self, record: dict) -> list[str]:
        """The ownership rules: owned tags are removed by tag; the image id only when it was newly created by this run, carries no other tag or
        digest reference and no container uses it. Never forced. Returns what was done, in words."""
        done_log = []
        for tag in list(record.get('owned_tags', [])):
            if self.d('image', 'inspect', tag, check=False).returncode == 0:
                self.d('rmi', tag)
                done_log.append(f'untagged {tag}')
            record['owned_tags'].remove(tag)
        image_id = record.get('id')
        if image_id and self.d('image', 'inspect', image_id, check=False).returncode == 0:
            info = json.loads(self.d('image', 'inspect', image_id).stdout)[0]
            digest_ref = record.get('digest_ref')
            # with the containerd image store a digest pull also lists ``name@sha256:...`` among the tags: that reference is this run's own
            others = [t for t in info.get('RepoTags') or [] if t != digest_ref]
            users = self.d('ps', '-a', '-q', '--filter', f'ancestor={image_id}').stdout.split()
            digests = [r for r in info.get('RepoDigests') or [] if r != digest_ref]
            if record.get('new') and not others and not digests and not users:
                if digest_ref and digest_ref in ((info.get('RepoDigests') or []) + (info.get('RepoTags') or [])):
                    self.d('rmi', digest_ref)
                    done_log.append(f'removed {digest_ref}')
                elif self.d('image', 'inspect', image_id, check=False).returncode == 0:
                    self.d('rmi', image_id)
                    done_log.append(f'removed image id {image_id[:19]}')
            else:
                reason = 'preexisting' if not record.get('new') else ('other tags ' + ','.join(others) if others else 'other references or containers')
                done_log.append(f'kept image id {image_id[:19]} ({reason})')
        return done_log

    def remove_images(self) -> None:
        for ref, record in list(self.state['images'].items()):
            for line in self.remove_image_refs(record):
                self.log('image', ref=ref, action=line)
            del self.state['images'][ref]
            self.save()

    def down(self, keep_images: bool = False) -> None:
        for name in reversed(list(self.state['containers'])):
            self.remove_container(name)
        for name, nid in list(self.state['networks'].items()):
            if not self.owned('network', nid):
                raise HarnessError(f'network {name} is not owned by this token; refusing to remove it')
            self.d('network', 'rm', nid)
            del self.state['networks'][name]
            self.save()
            self.log('removed', kind='network', name=name, id=nid)
        for name in list(self.state['volumes']):
            if not self.owned('volume', name):
                raise HarnessError(f'volume {name} is not owned by this token; refusing to remove it')
            self.d('volume', 'rm', name)
            self.state['volumes'].remove(name)
            self.save()
            self.log('removed', kind='volume', name=name)
        if not keep_images:
            self.remove_images()


def _size_mb(text: str) -> int:
    m = re.fullmatch(r'([0-9.]+)\s*([kKMGT]?)B', text.strip())
    if not m:
        return 0
    return int(float(m.group(1)) * {'': 1 / 2**20, 'k': 1 / 1024, 'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024}[m.group(2)])


# -- snapshot -------------------------------------------------------------------------------------------------------------------------------------
def snapshot(docker: str = 'docker') -> dict:
    def rows(*args):
        return [line for line in sh([docker, *args]).stdout.splitlines() if line]
    ports = sorted(int(m.group(1)) for m in re.finditer(r':(\d+)\s', sh(['ss', '-ltnH'], check=False).stdout + ' '))
    free = sh(['free', '-m']).stdout.splitlines()[1].split()
    df = sh(['df', '-BM', '/']).stdout.splitlines()[1].split()
    return {
        'time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'containers': rows('ps', '-a', '--no-trunc', '--format', '{{.ID}} {{.Names}} {{.Image}} {{.State}}'),
        'networks': rows('network', 'ls', '--no-trunc', '--format', '{{.ID}} {{.Name}} {{.Driver}}'),
        'volumes': rows('volume', 'ls', '--format', '{{.Name}}'),
        'images': rows('images', '--no-trunc', '--format', '{{.ID}} {{.Repository}}:{{.Tag}}'),
        'digests': rows('images', '--no-trunc', '--digests', '--format', '{{.ID}} {{.Repository}} {{.Digest}}'),
        'listening_ports': sorted(set(ports)),
        'home': sorted(os.listdir(Path.home())),
        'docker_df': rows('system', 'df', '--format', '{{.Type}} total={{.TotalCount}} size={{.Size}} reclaimable={{.Reclaimable}}'),
        'memory_mb': {'total': int(free[1]), 'available': int(free[6])},
        'disk_root': {'size': df[1], 'used': df[2], 'available': df[3]},
    }


def compare(before: dict, after: dict, token: str | None = None) -> dict:
    """Foreign-resource differences: everything that is not an owned (``e19-<token>``) name, per list."""
    def foreign(items):
        return [i for i in items if not (token and f'e19-{token}' in i)]
    diff = {}
    for key in ('containers', 'networks', 'volumes', 'images', 'digests', 'listening_ports', 'home'):
        b, a = foreign(map(str, before[key])), foreign(map(str, after[key]))
        if key == 'containers':        # the state column may change legitimately only for the user's own restarts; compare id/name/image
            b, a = [' '.join(x.split()[:3]) for x in b], [' '.join(x.split()[:3]) for x in a]
        removed, added = sorted(set(b) - set(a)), sorted(set(a) - set(b))
        if removed or added:
            diff[key] = {'removed': removed, 'added': added}
    return diff


# -- seeding (the only phase with public access) ---------------------------------------------------------------------------------------------------
def pin_ref(key: str) -> str:
    name, digest = PINS[key]
    return f'{name}@{digest}'


def fetch(url: str, dest: Path, *, expect_sha256: str | None = None) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()
            break
        except OSError:
            if attempt == 3:
                raise
            time.sleep(3 * (attempt + 1))
    digest = sha256(data).hexdigest()
    if expect_sha256 and digest != expect_sha256:
        raise HarnessError(f'sha256 mismatch for {url}')
    dest.write_bytes(data)
    return digest


def seed_debian(root: Path, suites=('trixie', 'trixie-updates'), arch: str = 'amd64', source: str = DEBIAN_MIRROR_SOURCE) -> dict:
    """A partial Debian archive in the archive layout: the real signed ``InRelease`` of each suite, the ``main`` Packages index (with by-hash copies)
    and the ``tini`` package, every file verified against the hashes the signed index lists. Public access: seeding only."""
    receipt = {'source': source, 'suites': {}, 'packages': {}}
    base = root / 'debian'
    for suite in suites:
        dist = base / 'dists' / suite
        fetch(f'{source}/dists/{suite}/InRelease', dist / 'InRelease')
        release = (dist / 'InRelease').read_text()
        hashes = {}
        in_block = False
        for line in release.splitlines():
            if line.startswith('SHA256:'):
                in_block = True
            elif in_block and line.startswith(' '):
                digest, size, path = line.split()
                hashes[path] = (digest, int(size))
            else:
                in_block = False
        wanted = [f'main/binary-{arch}/Packages.xz', f'main/binary-{arch}/Packages.gz', f'main/binary-{arch}/Release']
        fetched = []
        for rel in wanted:
            if rel not in hashes:
                continue
            digest, size = hashes[rel]
            target = dist / rel
            fetch(f'{source}/dists/{suite}/{rel}', target, expect_sha256=digest)
            by_hash = target.parent / 'by-hash' / 'SHA256' / digest
            by_hash.parent.mkdir(parents=True, exist_ok=True)
            if not by_hash.exists():
                os.link(target, by_hash)
            fetched.append(rel)
        receipt['suites'][suite] = {'files': fetched, 'release_sha256': sha256((dist / 'InRelease').read_bytes()).hexdigest()}
    packages = lzma.decompress((base / 'dists' / suites[0] / f'main/binary-{arch}/Packages.xz').read_bytes()).decode()
    for stanza in packages.split('\n\n'):
        fields = dict(line.split(': ', 1) for line in stanza.splitlines() if ': ' in line and not line.startswith(' '))
        if fields.get('Package') == 'tini':
            filename, digest = fields['Filename'], fields['SHA256']
            fetch(f'{source}/{filename}', base / filename, expect_sha256=digest)
            receipt['packages']['tini'] = {'version': fields['Version'], 'filename': filename, 'sha256': digest, 'depends': fields.get('Depends')}
    if 'tini' not in receipt['packages']:
        raise HarnessError('tini is not in the Packages index')
    return receipt


def make_tls(tls: Path, names: list[str]) -> None:
    """A throwaway CA (two days) and a server certificate for ``names``; the CA key stays in ``tls/`` and is never mounted."""
    tls.mkdir(parents=True, exist_ok=True)
    server = tls / 'server'
    server.mkdir(exist_ok=True)
    run = lambda *a: sh(['openssl', *a])      # noqa: E731
    run('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', tls / 'ca.key', '-out', tls / 'ca.pem', '-subj', '/CN=e19 throwaway CA', '-days', '2')
    run('req', '-newkey', 'rsa:2048', '-nodes', '-keyout', server / 'server.key', '-out', tls / 'server.csr', '-subj', '/CN=e19 services')
    (tls / 'ext.cnf').write_text('subjectAltName=' + ','.join(f'DNS:{n}' for n in names) + '\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n')
    run('x509', '-req', '-in', tls / 'server.csr', '-CA', tls / 'ca.pem', '-CAkey', tls / 'ca.key', '-CAcreateserial', '-out', server / 'server.pem',
        '-days', '2', '-extfile', tls / 'ext.cnf')
    for path in (tls / 'ca.key', server / 'server.key'):
        path.chmod(0o600)
    (server / 'server.pem').chmod(0o644)
    (server / 'server.key').chmod(0o644)       # read by the unprivileged service users inside their containers; the directory is 0700 on the host
    server.chmod(0o755)


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as handle:
        handle.write(value)


DNS_SERVER = r'''"""Owned resolver: A records for the owned service names (through Docker's embedded DNS) and for the public names listed in /dns/public.json
(the real addresses, recorded during seeding, as a corporate resolver would answer them); NXDOMAIN for everything else."""
import json
import os
import socket
import struct
import sys
import time

NAMES = {n.lower() for n in sys.argv[1:]}
PUBLIC = json.load(open('/dns/public.json')) if os.path.exists('/dns/public.json') else {}
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0', 53))
print('resolver for', sorted(NAMES), 'and', len(PUBLIC), 'public names', flush=True)
while True:
    data, peer = sock.recvfrom(512)
    try:
        i, labels = 12, []
        while data[i]:
            labels.append(data[i + 1:i + 1 + data[i]].decode().lower())
            i += data[i] + 1
        name, qtype = '.'.join(labels), struct.unpack('!H', data[i + 1:i + 3])[0]
        question = data[12:i + 5]
        header = data[:2]
        known = name in NAMES or name in PUBLIC
        if known and qtype in (1, 28):
            answers = b''
            count = 0
            if qtype == 1:
                for ip in (PUBLIC.get(name) or [socket.gethostbyname(name)]):
                    answers += b'\xc0\x0c' + struct.pack('!HHIH', 1, 1, 30, 4) + socket.inet_aton(ip)
                    count += 1
            reply = header + struct.pack('!HHHHH', 0x8180, 1, count, 0, 0) + question + answers
            outcome = 'public' if name in PUBLIC else 'owned'
        else:
            reply = header + struct.pack('!HHHHH', 0x8183, 1, 0, 0, 0) + question
            outcome = 'nxdomain'
        print(f'{time.strftime("%H:%M:%S")} {peer[0]} {name} type={qtype} {outcome}', flush=True)
        sock.sendto(reply, peer)
    except Exception as exc:
        print('bad query', exc, flush=True)
'''

RUNNER_INSTALL = (
    'set -eu; mkdir -p /usr/local/lib/docker/cli-plugins /work; mv /tmp/in/docker /usr/local/bin/docker; '
    'mv /tmp/in/docker-buildx /usr/local/lib/docker/cli-plugins/docker-buildx; chmod 755 /usr/local/bin/docker /usr/local/lib/docker/cli-plugins/docker-buildx; '
    'mv /tmp/in/e19-ca.crt /usr/local/share/ca-certificates/e19-ca.crt; update-ca-certificates >/dev/null; mv /tmp/in/wheelhouse /wheelhouse; '
    'pip install --no-cache-dir --no-index --find-links /wheelhouse neo4j_graph_loader; rm -rf /root/.cache /tmp/in')


def seed(h: Harness, args) -> None:
    """Pull the pinned images, prepare every file the isolated topology needs and build the runner image. The only phase with public access."""
    h.log('phase', name='seed', public_access=True)
    avail = h.guard('seed')
    h.log('guard', memory_available_mb=avail, minimum=MIN_AVAILABLE_MB)
    work = h.work
    for sub in ('auth', 'tls', 'conf', 'dns', 'pypi/packages', 'debian', 'wheelhouse', 'runner', 'secrets', 'out'):
        (work / sub).mkdir(parents=True, exist_ok=True)
    (work / 'secrets').chmod(0o700)
    (work / 'auth').chmod(0o755)
    # 1. images by digest; the image id is recorded as new or pre-existing before the pull
    before = set(h.d('images', '-q', '--no-trunc').stdout.split())
    for key in PINS:
        ref = pin_ref(key)
        h.d('pull', '-q', ref, timeout=1800)
        image_id = h.d('image', 'inspect', '--format', '{{.Id}}', ref).stdout.strip()
        size = int(h.d('image', 'inspect', '--format', '{{.Size}}', ref).stdout.strip())
        existing = image_id in before
        h.state['images'][ref] = {'id': image_id, 'new': not existing, 'owned_tags': [], 'digest_ref': ref, 'key': key}
        h.save()
        before.add(image_id)
        h.log('pull', key=key, ref=ref, id=image_id, size_mb=size // 2**20, newly_created=not existing)
    # 2. credentials, certificate authority
    registry_user, pypi_user = 'e19reg', 'e19pip'
    for name in ('registry', 'pypi', 'neo4j'):
        write_secret(work / 'secrets' / f'{name}.password', secrets.token_urlsafe(24))
    (work / 'secrets' / 'users.json').write_text(json.dumps({'registry': registry_user, 'pypi': pypi_user}))
    pw = lambda n: (work / 'secrets' / f'{n}.password').read_text()      # noqa: E731
    make_tls(work / 'tls', ['registry', 'pypi', 'debian', *(f'{n}.{INTERNAL_DOMAIN}' for n in ('registry', 'pypi', 'debian'))])
    shutil.copy(work / 'tls' / 'ca.pem', work / 'runner' / 'ca.pem')
    # 3. htpasswd files (bcrypt for the registry, apr1 for the index) from the pinned httpd image; the password goes on stdin
    for who, user, flag, target in (('registry', registry_user, '-niB', 'registry.htpasswd'), ('pypi', pypi_user, '-nim', 'pypi.htpasswd')):
        out = h.run_once('htpasswd', pin_ref('httpd'), [flag, user], input=pw(who) + '\n', entrypoint='htpasswd', network='none')
        (work / 'auth' / target).write_text(out)
        (work / 'auth' / target).chmod(0o644)
    # 4. nginx configurations, the resolver script
    (work / 'conf' / 'pypi.conf').write_text(
        'events {}\nhttp {\n  server {\n    listen 8443 ssl;\n    server_name pypi;\n    ssl_certificate /tls/server.pem;\n    ssl_certificate_key /tls/server.key;\n'
        '    client_max_body_size 100m;\n    location / {\n      proxy_pass http://pypiserver:8080;\n      proxy_set_header Host $http_host;\n'
        '      proxy_set_header Authorization $http_authorization;\n    }\n  }\n}\n')
    (work / 'conf' / 'debian.conf').write_text(
        'events {}\nhttp {\n  server {\n    listen 80;\n    server_name debian;\n    root /srv;\n    autoindex off;\n  }\n}\n')
    (work / 'dns' / 'dns_server.py').write_text(DNS_SERVER)
    public = {}
    for name in PUBLIC_NAMES:
        public[name] = sorted({info[4][0] for info in socket.getaddrinfo(name, 443, socket.AF_INET, socket.SOCK_STREAM)})
    (work / 'dns' / 'public.json').write_text(json.dumps(public, indent=1))
    h.log('public-dns-map', names=public)
    # 5. the Debian repository (tini only), the wheelhouse
    receipt = seed_debian(work)
    h.log('debian-mirror', **receipt)
    lock = Path(args.lock)
    shutil.copy(lock, work / 'inputs-lock.txt')
    wheelhouse = work / 'wheelhouse'
    h.run_once('wheelhouse', pin_ref('python'), ['pip', 'download', '--no-deps', '--only-binary=:all:', '--require-hashes', '--disable-pip-version-check',
                                                 '--no-cache-dir', '-r', '/lock/lock.txt', '-d', '/out'],
               mounts=[f'{work}/inputs-lock.txt:/lock/lock.txt:ro', f'{wheelhouse}:/out'], user=f'{os.getuid()}:{os.getgid()}',
               env={'HOME': '/tmp', 'PIP_DISABLE_PIP_VERSION_CHECK': '1'})
    package_wheel = Path(args.package_wheel)
    shutil.copy(package_wheel, wheelhouse / package_wheel.name)
    shutil.copytree(wheelhouse, work / 'runner' / 'wheelhouse', dirs_exist_ok=True)
    for wheel in wheelhouse.glob('*.whl'):
        shutil.copy(wheel, work / 'pypi' / 'packages' / wheel.name)
    h.log('wheelhouse', wheels=sorted(p.name for p in wheelhouse.glob('*.whl')), package_wheel_sha256=sha256(package_wheel.read_bytes()).hexdigest())
    # 6. the runner image, made before isolation WITHOUT BuildKit (a build would leave cache records in the shared daemon that this harness may not
    #    prune): the CLI and the buildx plugin are copied out of the pinned dind image, the package is installed from the wheelhouse inside an owned
    #    container, and the container is committed.
    tag = f'{h.prefix}/runner:1'
    tools = work / 'runner' / 'tools'
    tools.mkdir(exist_ok=True)
    source = h.create_container('tools', pin_ref('dind'), ['true'], network='none', memory=64, cpus=0.1)
    for src, dst in (('/usr/local/bin/docker', 'docker'), ('/usr/local/libexec/docker/cli-plugins/docker-buildx', 'docker-buildx')):
        h.d('cp', f'{h.state["containers"][source]}:{src}', str(tools / dst))
    h.remove_container(source)
    builder = h.create_container('runner-build', pin_ref('python'), ['sh', '-c', RUNNER_INSTALL], network='none', memory=512, cpus=1.0)
    bid = h.state['containers'][builder]
    staging = work / 'runner' / 'in'
    staging.mkdir(exist_ok=True)
    shutil.copy(tools / 'docker', staging / 'docker')
    shutil.copy(tools / 'docker-buildx', staging / 'docker-buildx')
    shutil.copy(work / 'runner' / 'ca.pem', staging / 'e19-ca.crt')
    shutil.copytree(work / 'runner' / 'wheelhouse', staging / 'wheelhouse', dirs_exist_ok=True)
    h.d('cp', f'{staging}/.', f'{bid}:/tmp/in')
    h.d('start', '-a', bid, timeout=900)
    h.state['images'][tag] = {'id': None, 'new': True, 'owned_tags': [tag], 'digest_ref': None, 'key': 'runner'}
    h.save()
    h.d('commit', '--change', 'ENV DOCKER_HOST=unix:///run/dind/docker.sock', '--change', 'WORKDIR /work', '--change', 'CMD ["sleep","infinity"]', bid, tag)
    rid = h.d('image', 'inspect', '--format', '{{.Id}}', tag).stdout.strip()
    h.state['images'][tag]['id'] = rid
    h.save()
    h.remove_container(builder)
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(work / 'runner' / 'wheelhouse', ignore_errors=True)
    h.log('runner-image', tag=tag, id=rid, method='docker commit (no BuildKit cache on the shared daemon)')
    h.log('phase-end', name='seed', public_access=False, memory_available_mb=h.guard('seed end'), budget=h.budget())


# -- the isolated topology ------------------------------------------------------------------------------------------------------------------------
def inner(h: Harness, *cmd, **kw):
    """Run the Docker CLI of the dind container against the inner daemon (the CLI inside dind, local socket)."""
    return h.exec('dind', 'docker', '-H', 'unix:///run/dind/docker.sock', *cmd, **kw)


def runner(h: Harness, *cmd, **kw):
    return h.exec('runner', *cmd, **kw)


def up(h: Harness, args) -> None:
    h.log('phase', name='up', public_access=False)
    work = h.work
    h.guard('up')
    h.watch()
    try:
        net = h.create_network('net', internal=True)
        vol = {n: h.create_volume(n) for n in ('registry-data', 'dind-lib', 'dind-sock', 'work')}
        tls_mount = f'{work}/tls/server:/tls:ro'
        # name service first: its address is the daemon's --dns
        h.create_container('dns', pin_ref('python'), ['python', '-u', '/dns/dns_server.py', *OWNED_NAMES], network=net, aliases=['dns'],
                           mounts=[f'{work}/dns:/dns:ro'], read_only=True)
        h.start('dns')
        dns_ip = h.d('inspect', '--format', '{{(index .NetworkSettings.Networks "%s").IPAddress}}' % net, h.state['containers'][f'{h.prefix}-dns']).stdout.strip()
        reg_env = {'REGISTRY_HTTP_ADDR': '0.0.0.0:5000', 'REGISTRY_HTTP_TLS_CERTIFICATE': '/tls/server.pem', 'REGISTRY_HTTP_TLS_KEY': '/tls/server.key',
                   'REGISTRY_AUTH': 'htpasswd', 'REGISTRY_AUTH_HTPASSWD_REALM': 'e19', 'REGISTRY_AUTH_HTPASSWD_PATH': '/auth/registry.htpasswd'}
        h.create_container('registry', pin_ref('registry'), network=net, aliases=['registry', f'registry.{INTERNAL_DOMAIN}'], env=reg_env,
                           mounts=[tls_mount, f'{work}/auth/registry.htpasswd:/auth/registry.htpasswd:ro', f'{vol["registry-data"]}:/var/lib/registry'])
        h.create_container('pypiserver', pin_ref('pypiserver'), ['run', '-p', '8080', '-a', 'list,download', '-P', '/auth/pypi.htpasswd', '--disable-fallback',
                                                                 '/packages'], network=net, aliases=['pypiserver'],
                           mounts=[f'{work}/pypi/packages:/packages:ro', f'{work}/auth/pypi.htpasswd:/auth/pypi.htpasswd:ro'])
        h.create_container('pypi-proxy', pin_ref('nginx'), ['nginx', '-g', 'daemon off;', '-c', '/etc/nginx/pypi.conf'], network=net, aliases=['pypi', f'pypi.{INTERNAL_DOMAIN}'],
                           mounts=[tls_mount, f'{work}/conf/pypi.conf:/etc/nginx/pypi.conf:ro'])
        h.create_container('debian', pin_ref('nginx'), ['nginx', '-g', 'daemon off;', '-c', '/etc/nginx/debian.conf'], network=net, aliases=['debian', f'debian.{INTERNAL_DOMAIN}'],
                           mounts=[f'{work}/debian:/srv/debian:ro', f'{work}/conf/debian.conf:/etc/nginx/debian.conf:ro'])
        for role in ('registry', 'pypiserver', 'pypi-proxy', 'debian'):
            h.start(f'{h.prefix}-{role}')
        # the inner daemon: its own store and socket volumes, its own arguments, the owned resolver
        h.create_container('dind', pin_ref('dind'), ['--host=unix:///run/dind/docker.sock', '--insecure-registry=registry:5000', f'--dns={dns_ip}'],
                           network=net, aliases=['dind'], privileged=True, env={'DOCKER_TLS_CERTDIR': ''}, dns=dns_ip, dns_search='.',
                           mounts=[f'{vol["dind-lib"]}:/var/lib/docker', f'{vol["dind-sock"]}:/run/dind', f'{vol["work"]}:/work'])
        h.start(f'{h.prefix}-dind')
        h.wait_for('the inner daemon', lambda: inner(h, 'info', '--format', '{{.ServerVersion}}', check=False).returncode == 0, timeout=120)
        info = inner(h, 'info', '--format', '{{.ServerVersion}} {{.Driver}} {{.DockerRootDir}}').stdout.strip()
        h.log('inner-daemon', info=info, dns=dns_ip, insecure_registry='registry:5000', published_ports=0)
        # the registry content: the shared daemon exports each image, the inner daemon loads and pushes it (only the inner daemon has the insecure flag)
        h.wait_for('the registry', lambda: inner(h, 'info', check=False).returncode == 0, timeout=30)
        reg_user = json.loads((work / 'secrets' / 'users.json').read_text())['registry']
        reg_password = (work / 'secrets' / 'registry.password').read_text()
        inner(h, 'login', 'registry:5000', '-u', reg_user, '--password-stdin', input=reg_password)
        h.log('registry-login', ok=True)
        for key, path in REGISTRY_CONTENT.items():
            ref = pin_ref(key)
            image_id = h.state['images'][ref]['id']
            save = subprocess.Popen([h.docker, 'save', image_id], stdout=subprocess.PIPE)
            load = subprocess.run([h.docker, 'exec', '-i', h.state['containers'][f'{h.prefix}-dind'], 'docker', '-H', 'unix:///run/dind/docker.sock', 'load'],
                                  stdin=save.stdout, capture_output=True, text=True, timeout=1800)
            save.stdout.close()
            if save.wait() or load.returncode:
                raise HarnessError(f'loading {key} into the inner daemon failed: {load.stderr[-300:]}')
            target = f'registry:5000/{path}'
            inner(h, 'tag', image_id, target)
            pushed = inner(h, 'push', target, timeout=1800)
            digest = re.search(r'digest: (sha256:[0-9a-f]{64})', pushed.stdout + pushed.stderr)
            inner(h, 'rmi', target)
            h.log('registry-seed', key=key, source=ref, target=target, registry_digest=digest.group(1) if digest else None)
        # the runner container (image built before isolation). By default it is an owned container on the internal network with its own address; with
        # ``--runner-netns`` it shares the inner daemon's network namespace, which is what the operator's host is to its own Docker daemon: published
        # ports are reached on 127.0.0.1 (the docker runtime's dispatcher and lease control ports are published on the loopback of the CLI's host).
        runner_mounts = [f'{vol["dind-sock"]}:/run/dind', f'{vol["work"]}:/work', f'{work}/secrets:/secrets:ro']
        runner_env = {'DOCKER_HOST': 'unix:///run/dind/docker.sock', 'GRAPH_LOADER_IMAGE_REGISTRY': 'registry:5000/e19'}
        if getattr(args, 'runner_netns', False):
            h.create_container('runner', f'{h.prefix}/runner:1', ['sleep', 'infinity'], network=f'container:{h.state["containers"][h.prefix + "-dind"]}', init=True,
                               mounts=runner_mounts, env=runner_env)
        else:
            h.create_container('runner', f'{h.prefix}/runner:1', ['sleep', 'infinity'], network=net, aliases=['runner'], init=True, dns=dns_ip,
                               mounts=runner_mounts, env=runner_env)
        h.start(f'{h.prefix}-runner')
        # the large seed images are only needed until the registry holds them: release them by the ownership rules (never forced)
        for key in ('neo4j', 'httpd'):
            ref = pin_ref(key)
            for line in h.remove_image_refs(h.state['images'][ref]):
                h.log('image', ref=ref, action=line)
            del h.state['images'][ref]
            h.save()
        h.log('phase-end', name='up', memory_available_mb=h.guard('up end'), budget=h.budget())
    finally:
        h.unwatch()


def rx(h: Harness, *cmd, **kw):
    """Run in the runner with the scoped login config."""
    env = {'DOCKER_CONFIG': '/work/login', **kw.pop('env', {})}
    return h.exec('runner', *cmd, env=env, **kw)


def step(h: Harness, results: dict, name: str, fn):
    started = time.time()
    try:
        detail = fn()
        results[name] = {'ok': True, 'seconds': round(time.time() - started, 1), 'detail': detail}
    except (HarnessError, subprocess.SubprocessError, OSError, AssertionError) as exc:
        results[name] = {'ok': False, 'seconds': round(time.time() - started, 1), 'detail': str(exc)[-900:]}
    h.log('verify-step', name=name, ok=results[name]['ok'], seconds=results[name]['seconds'], detail=results[name]['detail'])
    h.guard(name)
    return results[name]['ok']


PREFLIGHT = r"""
import json, sys
from pathlib import Path
from types import SimpleNamespace
from neo4j_graph_loader.orchestrator.docker_service import DockerService
root = Path('/work/preflight')
for sub in ('rejections', 'plans', 'spill', 'timings'):
    (root / sub).mkdir(parents=True, exist_ok=True)
service = DockerService(runtime=SimpleNamespace(rejections=root / 'rejections', plans=root / 'plans', spill=root / 'spill', timings=root / 'timings',
                                                schema=root / 'schema.yaml', root=root))
print('endpoint', service.client.api.base_url, flush=True)
print('node image', service.prepare_images(['node'], local_only=True)['node'], flush=True)
service.preflight_topology(sys.argv[1])
print('preflight passed', flush=True)
"""


def stage_env(h: Harness) -> dict:
    """The users file and the two environment files (0600) that the runner and the inner Neo4j container read."""
    work = h.work
    users = json.loads((work / 'secrets' / 'users.json').read_text())
    neo_pw = (work / 'secrets' / 'neo4j.password').read_text()
    write_secret(work / 'secrets' / 'neo4j.env', f'NEO4J_AUTH=neo4j/{neo_pw}\n')
    write_secret(work / 'secrets' / 'runner.env', f'NEO4J_PASSWORD={neo_pw}\nLOADER_NEO4J_URI=bolt://neo4j:7687\nLOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092\n')
    return users


def stage_login(h, users):
    rx(h, 'sh', '-c', 'mkdir -p -m 700 /work/login && docker login registry:5000 -u "$U" --password-stdin < /secrets/registry.password',
       env={'U': users['registry']})
    rx(h, 'docker', 'pull', '-q', 'registry:5000/e19/python:3.12-slim')
    digest = rx(h, 'docker', 'buildx', 'imagetools', 'inspect', 'registry:5000/e19/python:3.12-slim', '--format', '{{.Manifest.Digest}}').stdout.strip()
    return {'registry_digest': digest}

def stage_build(h):
    rx(h, 'sh', '-c', 'umask 077; python - <<EOF\nimport json\nu=json.load(open("/secrets/users.json"))["pypi"]\n'
                      'p=open("/secrets/pypi.password").read()\nopen("/work/pip-index.txt","w").write(f"https://{u}:{p}@pypi:8443/simple/")\nEOF')
    started = time.time()
    rx(h, 'graph-loader', 'images', 'build', '--registry', 'registry:5000/e19', '--package-wheel',
              '/wheelhouse/neo4j_graph_loader-0.1.0a1-py3-none-any.whl', '--base-image', 'registry:5000/e19/python:3.12-slim',
              '--pip-index-secret', '/work/pip-index.txt', '--pip-ca-bundle', '/usr/local/share/ca-certificates/e19-ca.crt',
              '--debian-mirror', 'http://debian/debian', '--docker-config', '/work/login', '--output', '/work/build-out', timeout=1800)
    manifest = json.loads(rx(h, 'cat', '/work/build-out/images.json').stdout)
    inner_images = rx(h, 'docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}} {{.ID}}').stdout.splitlines()
    tags = [row['local_tag'] for row in manifest['images']]
    for tag in tags:
        assert any(line.startswith(tag + ' ') for line in inner_images), f'{tag} is not in the inner store'
        assert h.d('image', 'inspect', tag, check=False).returncode != 0, f'{tag} leaked into the shared daemon'
    return {'seconds': round(time.time() - started), 'images': [(r['component'], r['local_tag'], r['id']) for r in manifest['images']],
            'schema': manifest['schema'], 'base_image': manifest.get('base_image'), 'loaded_into_inner_store': True,
            'absent_from_shared_daemon': True}

def stage_services(h, publish=False):
    rx(h, 'docker', 'network', 'create', 'e19-load')
    rx(h, 'docker', 'pull', '-q', 'registry:5000/e19/neo4j:5.26-enterprise', timeout=900)
    rx(h, 'docker', 'pull', '-q', 'registry:5000/e19/cp-kafka:7.7.0', timeout=900)
    neo_ports = ['-p', '7687:7687'] if publish else []
    kafka_ports = ['-p', '9092:9092'] if publish else []
    rx(h, 'docker', 'run', '-d', '--name', 'neo4j', '--network', 'e19-load', '--memory', '2200m', '--env-file', '/secrets/neo4j.env', *neo_ports,
       '-e', 'NEO4J_ACCEPT_LICENSE_AGREEMENT=yes', '-e', 'NEO4J_server_memory_heap_initial__size=1G', '-e', 'NEO4J_server_memory_heap_max__size=1G',
       '-e', 'NEO4J_server_memory_pagecache_size=512M', 'registry:5000/e19/neo4j:5.26-enterprise')
    kafka_env = {'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller', 'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:29093',
                 'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,INTERNAL://0.0.0.0:29092',
                 'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://{"dind" if publish else "kafka"}:9092,INTERNAL://kafka:29092',
                 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT',
                 'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
                 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'true', 'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                 'KAFKA_HEAP_OPTS': '-Xms512m -Xmx1g'}
    flat = []
    for key, value in kafka_env.items():
        flat += ['-e', f'{key}={value}']
    rx(h, 'docker', 'run', '-d', '--name', 'kafka', '--network', 'e19-load', '--memory', '1500m', *kafka_ports, *flat, 'registry:5000/e19/cp-kafka:7.7.0')
    h.wait_for('kafka', lambda: rx(h, 'docker', 'exec', 'kafka', 'kafka-broker-api-versions', '--bootstrap-server', 'kafka:29092', check=False).returncode == 0,
               timeout=240, interval=5)
    h.wait_for('neo4j', lambda: 'Started.' in rx(h, 'docker', 'logs', 'neo4j', check=False).stdout + rx(h, 'docker', 'logs', 'neo4j', check=False).stderr,
               timeout=240, interval=5)
    return {'network': 'e19-load', 'neo4j_heap': '1G', 'pagecache': '512M', 'kafka_heap': '-Xms512m -Xmx1g'}


def verify(h: Harness, args) -> None:
    h.log('phase', name='verify', public_access=False)
    work = h.work
    results: dict = {}
    h.watch()
    try:
        users = stage_env(h)

        def identity():
            version = rx(h, 'docker', 'version', '--format', '{{.Server.Version}} {{.Client.Version}}').stdout.strip()
            ls = rx(h, 'docker', 'buildx', 'ls').stdout
            info = rx(h, 'docker', 'info', '--format', '{{.Name}} {{.Driver}}').stdout.strip()
            host = rx(h, 'sh', '-c', 'echo $DOCKER_HOST').stdout.strip()
            assert host == 'unix:///run/dind/docker.sock', host
            assert re.search(r'^default\*\s+docker\b', ls, re.M), ls
            shared = h.d('info', '--format', '{{.Name}}').stdout.strip()
            assert shared != info.split()[0], 'the runner talks to the shared daemon'
            return {'server_client': version, 'inner_daemon': info, 'buildx': ls.strip().splitlines()[:3], 'docker_host': host}

        def login_and_pull():
            return stage_login(h, users)

        def build():
            return stage_build(h)

        def services():
            return stage_services(h)


        def preflight():
            rx(h, 'sh', '-c', 'mkdir -p /work/preflight && graph-loader schema-example --name default > /work/preflight/schema.yaml')
            done = h.exec('runner', 'python', '-c', PREFLIGHT, 'e19-load', env_file=work / 'secrets' / 'runner.env',
                          env={'DOCKER_CONFIG': '/work/login', 'GRAPH_LOADER_IMAGE_REGISTRY': 'registry:5000/e19'}, check=False, timeout=300)
            out = (done.stdout + done.stderr).strip()
            assert done.returncode == 0 and 'preflight passed' in out, out[-600:]
            return {'output': out.splitlines()[-4:], 'preflight': 'real (not skipped)'}

        def controls():
            return ownership_controls(h)

        def footprint():
            stats = h.d('stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}} {{.CPUPerc}}', *[i for n, i in h.state['containers'].items()]).stdout.splitlines()
            inner_stats = rx(h, 'docker', 'stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}').stdout.splitlines()
            return {'owned_containers': stats, 'inner': inner_stats, 'budget': h.budget(), 'host_memory_available_mb': mem_available_mb()}

        for name, fn in (('identity and local socket', identity), ('scoped login and authenticated pull', login_and_pull),
                         ('images build into the inner store', build), ('inner load services', services), ('real preflight', preflight),
                         ('ownership controls', controls), ('footprint', footprint)):
            if not step(h, results, name, fn) and name in ('identity and local socket', 'images build into the inner store', 'inner load services'):
                break
    finally:
        h.unwatch()
        (work / 'logs' / 'verify.json').write_text(json.dumps(results, indent=1))
    if not all(r['ok'] for r in results.values()):
        raise HarnessError('verify failed: ' + ', '.join(k for k, v in results.items() if not v['ok']))


# -- S10: isolation controls --------------------------------------------------------------------------------------------------------------------
NETWORK_MARKERS = ('network is unreachable', 'no route to host', 'connection refused', 'i/o timeout', 'timed out', 'deadline exceeded')
DNS_MARKERS = ('no such host', 'server misbehaving', 'temporary failure in name resolution', 'name or service not known', 'could not resolve')
AUTH_MARKERS = ('unauthorized', 'authentication required', 'no basic auth credentials', 'pull access denied', 'access denied', 'denied:', 'forbidden')
MISSING_MARKERS = ('not found', 'manifest unknown', 'repository does not exist', 'name unknown')


def classify_text(text: str) -> str:
    """Classify the failure text of a pull. A transport failure wins even when the text also says "failed to authorize": that sentence is how a
    registry client reports that it could not reach the token endpoint. A DNS failure, an authentication error and a missing image are never a denial."""
    lowered = text.lower()
    if any(m in lowered for m in NETWORK_MARKERS):
        return 'network'
    if any(m in lowered for m in DNS_MARKERS):
        return 'dns'
    if any(m in lowered for m in AUTH_MARKERS):
        return 'auth'
    if any(m in lowered for m in MISSING_MARKERS):
        return 'not-found'
    return 'other'


def parse_probe(text: str) -> list[dict]:
    rows = []
    for match in re.finditer(r'PROBE (\{.*\})\s*$', text, re.M):
        try:
            rows.append(json.loads(match.group(1)))
        except ValueError:
            continue
    return rows


def judge(rows: list[dict], *, expect_denied: int = 0, expect_allowed: int = 0) -> dict:
    """Every denied check must have failed with a network error (the probe sets ok only then) and every allowed check must have succeeded; the numbers of
    checks are asserted so a probe that silently skipped something fails."""
    denied = [r for r in rows if r['kind'] == 'public-denied']
    allowed = [r for r in rows if r['kind'] == 'internal-allowed']
    reached = [r for r in rows if r['kind'] == 'public-reached']
    return {'denied': len(denied), 'denied_ok': sum(r['ok'] for r in denied), 'allowed': len(allowed), 'allowed_ok': sum(r['ok'] for r in allowed),
            'reached': len(reached), 'reached_ok': sum(r['ok'] for r in reached),
            'ok': len(denied) >= expect_denied and len(allowed) >= expect_allowed and all(r['ok'] for r in rows) and bool(rows)}


BUILD_DOCKERFILE = """FROM registry:5000/e19/python:3.12-slim
COPY probe.py /probe.py
COPY ca.pem /ca.pem
RUN --mount=type=secret,id=pip_index python /probe.py --place build-run --public denied --internal-http pypi,debian --pypi-url-file /run/secrets/pip_index \\
    --ca /ca.pem --apt-public --apt-mirror http://debian/debian --pip-public --pip-index /run/secrets/pip_index
"""
PROBE_FILE = Path(__file__).with_name('e19_isolation_probe.py')
CA_IN_RUNNER = '/usr/local/share/ca-certificates/e19-ca.crt'


def isolation(h: Harness, args) -> None:
    h.log('phase', name='isolation', public_access=False)
    work = h.work
    results: dict = {}
    probes: list[dict] = []
    h.watch()
    try:
        users = stage_env(h)
        results['docker_df_before'] = {'ok': True, 'detail': h.d('system', 'df', '--format', '{{.Type}} total={{.TotalCount}} size={{.Size}} reclaimable={{.Reclaimable}}').stdout.splitlines()}
        step(h, results, 'scoped login and base pull', lambda: stage_login(h, users))
        step(h, results, 'images build into the inner store', lambda: stage_build(h))
        step(h, results, 'inner load services', lambda: stage_services(h))
        # the probe and the build context go into the runner's work volume (the same absolute path in the inner daemon)
        probe_dir = work / 'probe'
        (probe_dir / 'build').mkdir(parents=True, exist_ok=True)
        shutil.copy(PROBE_FILE, probe_dir / 'probe.py')
        shutil.copy(PROBE_FILE, probe_dir / 'build' / 'probe.py')
        shutil.copy(work / 'runner' / 'ca.pem', probe_dir / 'build' / 'ca.pem')
        (probe_dir / 'build' / 'Dockerfile').write_text(BUILD_DOCKERFILE)
        h.d('cp', f'{probe_dir}/.', f"{h.state['containers'][h.prefix + '-runner']}:/work/probe")
        node = 'registry:5000/e19/graph-loader-node:0.1.0a1'

        def collect(place: str, text: str, **expect) -> dict:
            rows = parse_probe(text)
            probes.extend(rows)
            verdict = judge(rows, **expect)
            assert verdict['ok'], f'{place}: ' + json.dumps([r for r in rows if not r['ok']])[:700]
            return verdict

        def place_runner():
            done = h.exec('runner', 'python', '/work/probe/probe.py', '--place', 'runner', '--public', 'denied', '--internal-http', 'registry,pypi,debian',
                          '--secrets-dir', '/secrets', '--ca', CA_IN_RUNNER, '--pip-public', '--pip-index', '/work/pip-index.txt', check=False, timeout=600)
            return collect('runner', done.stdout, expect_denied=7, expect_allowed=4)

        def place_inner_pull():
            rows = []
            for label, ref in (('pull docker.io', 'docker.io/library/hello-world:latest'), ('pull ghcr.io', 'ghcr.io/neburyx/graph-loader-cli:0.1.0a1')):
                done = rx(h, 'docker', 'pull', ref, check=False, timeout=300)
                text = done.stdout + done.stderr
                cls = classify_text(text) if done.returncode else 'reached'
                rows.append({'place': 'inner-daemon-pull', 'kind': 'public-denied', 'name': label, 'ok': cls == 'network', 'class': cls, 'exit': done.returncode,
                             'message': text.strip().splitlines()[-1][:260] if text.strip() else ''})
            done = rx(h, 'docker', 'pull', 'registry:5000/e19/python:3.12-slim', check=False, timeout=300)
            rows.append({'place': 'inner-daemon-pull', 'kind': 'internal-allowed', 'name': 'pull owned registry (authenticated)', 'ok': done.returncode == 0,
                         'exit': done.returncode, 'message': (done.stdout + done.stderr).strip().splitlines()[-1][:160]})
            probes.extend(rows)
            verdict = judge(rows, expect_denied=2, expect_allowed=1)
            assert verdict['ok'], json.dumps([r for r in rows if not r['ok']])[:700]
            return verdict

        def place_build():
            done = rx(h, 'docker', 'build', '--progress=plain', '--no-cache', '--secret', 'id=pip_index,src=/work/pip-index.txt', '-t', 'e19-probe:1',
                      '/work/probe/build', check=False, timeout=900)
            text = done.stdout + done.stderr
            assert done.returncode == 0, text[-700:]
            return collect('build-run', text, expect_denied=7, expect_allowed=3)

        def place_role():
            done = rx(h, 'docker', 'run', '--rm', '--network', 'e19-load', '--entrypoint', 'python', '-v', '/work/probe:/probe:ro', node, '/probe/probe.py',
                      '--place', 'role-container', '--public', 'denied', '--internal-domain', INTERNAL_DOMAIN,
                      '--internal-tcp', f'neo4j:7687,kafka:29092,registry.{INTERNAL_DOMAIN}:5000,pypi.{INTERNAL_DOMAIN}:8443,debian.{INTERNAL_DOMAIN}:80',
                      '--internal-http', 'debian', check=False, timeout=300)
            return collect('role-container', done.stdout + done.stderr, expect_denied=6, expect_allowed=6)

        def control_not_isolated():
            net = h.create_network('open', internal=False)
            out = h.run_once('open-probe', pin_ref('python'), ['python', '/probe/probe.py', '--place', 'non-isolated-control', '--public', 'reached'],
                             mounts=[f'{probe_dir}:/probe:ro'], network=net, memory=64, cpus=0.2, timeout=300)
            rows = parse_probe(out)
            probes.extend(rows)
            verdict = judge(rows, expect_denied=0)
            assert verdict['reached'] >= 6 and verdict['ok'], json.dumps(rows)[:700]
            return verdict

        def classifier_controls():
            report = {}
            nologin = h.exec('runner', 'docker', 'pull', 'registry:5000/e19/python:3.12-slim', env={'DOCKER_CONFIG': '/tmp/e19-nologin'}, check=False, timeout=120)
            report['registry pull without a scoped login'] = {'class': classify_text(nologin.stdout + nologin.stderr), 'message': (nologin.stderr or nologin.stdout).strip().splitlines()[-1][:200]}
            missing = rx(h, 'docker', 'pull', 'registry:5000/e19/does-not-exist:1', check=False, timeout=120)
            report['missing image in the owned registry'] = {'class': classify_text(missing.stdout + missing.stderr), 'message': (missing.stderr or missing.stdout).strip().splitlines()[-1][:200]}
            lookup = h.exec('runner', 'python', '-c', 'import socket\ntry:\n    socket.getaddrinfo("e19-unresolvable.example", 443)\nexcept socket.gaierror as e:\n    print("gaierror", e)', check=False)
            report['name the resolver does not know'] = {'class': 'dns' if 'gaierror' in lookup.stdout else 'other', 'message': lookup.stdout.strip()[:160]}
            assert report['registry pull without a scoped login']['class'] == 'auth' and report['missing image in the owned registry']['class'] == 'not-found'
            assert report['name the resolver does not know']['class'] == 'dns'
            return report

        def resolver_log():
            log = h.d('logs', '--tail', '400', h.state['containers'][f'{h.prefix}-dns'], check=False)
            lines = (log.stdout + log.stderr).splitlines()
            public = [line for line in lines if line.endswith(' public')]
            names = sorted({line.split()[2] for line in public})
            assert names, 'the owned resolver answered no public name: the denials would be DNS-only'
            return {'public_names_answered': names, 'queries_public': len(public), 'queries_owned': sum(1 for line in lines if line.endswith(' owned')),
                    'queries_nxdomain': sum(1 for line in lines if line.endswith(' nxdomain'))}

        for name, fn in (('place 1: runner', place_runner), ('place 2: inner daemon pull', place_inner_pull), ('place 3: build RUN step', place_build),
                         ('place 4: role container', place_role), ('control: owned container on a normal bridge network', control_not_isolated),
                         ('classifier controls (auth, missing image, DNS only are not denials)', classifier_controls), ('owned resolver answered the public names', resolver_log)):
            step(h, results, name, fn)
        results['docker_df_after'] = {'ok': True, 'detail': h.d('system', 'df', '--format', '{{.Type}} total={{.TotalCount}} size={{.Size}} reclaimable={{.Reclaimable}}').stdout.splitlines()}
        results['footprint'] = {'ok': True, 'detail': {'budget': h.budget(), 'host_memory_available_mb': mem_available_mb()}}
    finally:
        h.unwatch()
        (work / 'logs' / 'isolation.json').write_text(json.dumps(results, indent=1))
        (work / 'logs' / 'isolation-probes.jsonl').write_text('\n'.join(json.dumps(r, sort_keys=True) for r in probes) + '\n')
    if not all(r['ok'] for r in results.values()):
        raise HarnessError('isolation failed: ' + ', '.join(k for k, v in results.items() if not v['ok']))


def ownership_controls(h: Harness) -> dict:
    """Live shared-id, foreign-tag, container-in-use and pre-existing-image controls with the same removal code as the cleanup."""
    ref = pin_ref('nginx')
    base = h.state['images'][ref]
    owned, sim = f'{h.prefix}/ctl:owned', f'{h.prefix}-foreign-sim/ctl:keep'
    report = {}
    h.d('tag', base['id'], owned)
    h.d('tag', base['id'], sim)
    record = {'id': base['id'], 'new': True, 'owned_tags': [owned], 'digest_ref': ref}
    log = h.remove_image_refs(record)
    still = h.d('image', 'inspect', base['id'], check=False).returncode == 0
    report['shared_id_with_unrecorded_tag'] = {'actions': log, 'image_kept': still, 'owned_tag_gone': h.d('image', 'inspect', owned, check=False).returncode != 0,
                                               'unrecorded_tag_kept': h.d('image', 'inspect', sim, check=False).returncode == 0}
    assert still and report['shared_id_with_unrecorded_tag']['owned_tag_gone'] and report['shared_id_with_unrecorded_tag']['unrecorded_tag_kept']
    # a container uses the image: no removal of the id (no force)
    h.d('tag', base['id'], owned)
    name = h.create_container('ctl', owned, ['true'], network='none', memory=32, cpus=0.1)
    h.d('rmi', sim)
    record = {'id': base['id'], 'new': True, 'owned_tags': [owned], 'digest_ref': ref}
    log = h.remove_image_refs(record)
    report['image_in_use_by_a_container'] = {'actions': log, 'image_kept': h.d('image', 'inspect', base['id'], check=False).returncode == 0}
    h.remove_container(name)
    assert report['image_in_use_by_a_container']['image_kept']
    # an image that existed before the run is never removed (the kafka image is the user's own)
    kafka = h.state['images'][pin_ref('kafka')]
    record = {'id': kafka['id'], 'new': kafka['new'], 'owned_tags': [], 'digest_ref': pin_ref('kafka')}
    log = h.remove_image_refs(record)
    report['preexisting_image'] = {'new': kafka['new'], 'actions': log, 'image_kept': h.d('image', 'inspect', kafka['id'], check=False).returncode == 0}
    assert report['preexisting_image']['image_kept'] and not kafka['new']
    # a tag that is not recorded is never removed even if its name looks owned
    h.d('tag', base['id'], sim)
    record = {'id': base['id'], 'new': True, 'owned_tags': [], 'digest_ref': ref}
    h.remove_image_refs(record)
    report['unrecorded_owned_looking_tag'] = {'kept': h.d('image', 'inspect', sim, check=False).returncode == 0}
    h.d('rmi', sim)                # this control's own tag, created above in this function
    assert report['unrecorded_owned_looking_tag']['kept']
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('work', type=Path)
    parser.add_argument('token')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('snapshot').add_argument('out', type=Path)
    cmp_ = commands.add_parser('compare')
    cmp_.add_argument('before', type=Path)
    cmp_.add_argument('after', type=Path)
    seed_ = commands.add_parser('seed')
    seed_.add_argument('--lock', required=True)
    seed_.add_argument('--package-wheel', required=True)
    for name in ('verify', 'isolation', 'status'):
        commands.add_parser(name)
    commands.add_parser('up').add_argument('--runner-netns', action='store_true')
    commands.add_parser('down').add_argument('--keep-images', action='store_true')
    commands.add_parser('controls')
    args = parser.parse_args(argv)
    h = Harness(args.work, args.token)
    try:
        if args.command == 'snapshot':
            args.out.write_text(json.dumps(snapshot(), indent=1))
            return 0
        if args.command == 'compare':
            diff = compare(json.loads(args.before.read_text()), json.loads(args.after.read_text()), args.token)
            print(json.dumps(diff, indent=1) if diff else 'foreign resources unchanged')
            return 1 if diff else 0
        if args.command == 'status':
            print(json.dumps({'state': h.state, 'budget': h.budget(), 'memory_available_mb': mem_available_mb()}, indent=1))
            return 0
        if args.command == 'down':
            h.down(keep_images=args.keep_images)
            h.log('down-complete', left=h.state)
            return 0
        if args.command == 'controls':
            print(json.dumps(ownership_controls(h), indent=1))
            return 0
        {'seed': seed, 'up': up, 'verify': verify, 'isolation': isolation}[args.command](h, args)
        return 0
    except GuardTripped as exc:
        h.log('guard-tripped', reason=str(exc))
        h.down()
        h.log('down-complete', left=h.state)
        return 3
    except (HarnessError, KeyError, subprocess.SubprocessError, OSError) as exc:
        h.log('error', command=args.command, message=str(exc)[:800])
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
