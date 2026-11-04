#!/usr/bin/env python3
"""Generate, check and exercise the hashed per-platform dependency locks of the loader images (developer tool, never part of the package).

    python scripts/lock_requirements.py generate --platform linux/amd64      # needs Docker; pip-tools runs inside the base image
    python scripts/lock_requirements.py check                                # offline; this is what ``make check-lock`` runs
    python scripts/lock_requirements.py wheelhouse --platform linux/arm64 --package-wheel dist/neo4j_graph_loader-X-py3-none-any.whl --output DIR

Locks are resolved from ``requirements.txt`` (the generated list of direct dependency ranges) inside the target base image on the target platform, with
``--only-binary=:all:``, so the closure is binary-only and matches what pip would install there. ``pip-tools`` exists only inside that throwaway
container; it is not a project dependency. Containers are created, started and removed by recorded id, never with force.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from neo4j_graph_loader.image_build import lockfile  # noqa: E402

REQUIREMENTS = ROOT / 'requirements.txt'
LOCK_DIR = ROOT / 'src' / 'neo4j_graph_loader' / 'image_build' / 'locks'      # package data: `graph-loader images build` reads them
BASE_IMAGE = 'python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30'
PIP_TOOLS = 'pip-tools==7.6.2'
OWNER_LABEL = 'io.graph-loader.lock-tool-owner'


def lock_path(platform: str) -> Path:
    return LOCK_DIR / f'py{lockfile.PYTHON.replace(".", "")}-{platform.replace("/", "-")}.txt'


def lock_paths() -> list[Path]:
    return [lock_path(platform) for platform in lockfile.PLATFORMS]


def parse_pip_compile(text: str) -> list[lockfile.Entry]:
    """The pinned entries of ``pip-compile --generate-hashes`` output (comments and annotations ignored)."""
    entries, name, version, hashes = [], None, None, []
    for raw in text.splitlines():
        line = raw.strip().removesuffix('\\').strip()
        if not line or line.startswith('#'):
            continue
        if line == '--only-binary :all:':
            continue                      # pip-compile echoes the pip argument the lock was resolved with
        hash_match = re.fullmatch(r'--hash=sha256:([0-9a-f]{64})', line)
        if hash_match:
            hashes.append(hash_match.group(1))
            continue
        pin = re.fullmatch(r'([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]*)', line)
        if not pin:
            raise lockfile.LockError('unexpected line in the resolver output: ' + line[:60])
        if name is not None:
            entries.append(lockfile.Entry(name, version, tuple(sorted(set(hashes)))))
        name, version, hashes = lockfile.canonical_name(pin.group(1)), pin.group(2), []
    if name is not None:
        entries.append(lockfile.Entry(name, version, tuple(sorted(set(hashes)))))
    if not entries or any(not entry.hashes for entry in entries):
        raise lockfile.LockError('the resolver output has no pins, or a pin without hashes')
    return entries


def restrict_to_downloaded(entries, listing: str) -> list[lockfile.Entry]:
    """Keep, per pin, only the hash of the wheel(s) actually downloaded on this platform, after checking each is a published hash of that release."""
    downloaded = {}
    for line in listing.splitlines():
        digest, _, filename = line.partition('  ')
        fields = lockfile.wheel_fields(filename.strip())
        downloaded.setdefault((lockfile.canonical_name(fields['name']), fields['version'].lower()), []).append(digest)
    result = []
    for entry in entries:
        mine = downloaded.pop((entry.name, entry.version.lower()), [])
        if not mine:
            raise lockfile.LockError(f'no wheel was downloaded for {entry.name}=={entry.version}')
        unpublished = [digest for digest in mine if digest not in entry.hashes]
        if unpublished:
            raise lockfile.LockError(f'a downloaded {entry.name} wheel is not among the published hashes of the release')
        result.append(lockfile.Entry(entry.name, entry.version, tuple(sorted(mine))))
    if downloaded:
        raise lockfile.LockError('wheels were downloaded that are not pinned: ' + ', '.join(sorted(name for name, _ in downloaded)))
    return result


# -- Docker (create / start / remove by recorded id; no force, no prune) -------------------------------------------------------------------------
class Container:
    def __init__(self, docker: str, token: str, label: str):
        self.docker, self.token, self.name, self.id = docker, token, f'e19-{token}-{label}', None

    def create(self, image: str, platform: str, mounts: list[str], command: list[str], env: dict) -> str:
        args = [self.docker, 'create', '--platform', platform, '--name', self.name, '--label', f'{OWNER_LABEL}={self.token}',
                '--user', f'{os.getuid()}:{os.getgid()}', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--memory', '1g', '--cpus', '2']
        for mount in mounts:
            args += ['-v', mount]
        for key, value in env.items():
            args += ['-e', f'{key}={value}']
        done = subprocess.run([*args, image, *command], capture_output=True, text=True, timeout=300)
        if done.returncode:
            raise RuntimeError('docker create failed: ' + done.stderr.strip()[:300])
        self.id = done.stdout.strip()
        print(f'container {self.name} id={self.id}', file=sys.stderr)
        return self.id

    def run(self, timeout: int = 900) -> str:
        done = subprocess.run([self.docker, 'start', '-a', self.id], capture_output=True, text=True, timeout=timeout)
        if done.returncode:
            raise RuntimeError(f'{self.name} failed ({done.returncode}): ' + (done.stdout + done.stderr).strip()[-600:])
        return done.stdout

    def remove(self) -> None:
        if self.id:
            subprocess.run([self.docker, 'rm', self.id], capture_output=True, text=True, timeout=120, check=True)
            print(f'removed {self.id}', file=sys.stderr)
            self.id = None


def container_env(work: str) -> dict:
    return {'HOME': '/tmp/lockhome', 'PIP_DISABLE_PIP_VERSION_CHECK': '1', 'PIP_NO_CACHE_DIR': '1', 'PYTHONDONTWRITEBYTECODE': '1',
            'PATH': '/tmp/lockhome/.local/bin:/usr/local/bin:/usr/bin:/bin'}


RESOLVE = r"""set -eu
mkdir -p /tmp/lockhome
pip install --quiet --user "$PIP_TOOLS"
python -c 'import sys; print(sys.version_info.major, sys.version_info.minor)' > /work/python.txt
pip --version | cut -d' ' -f2 > /work/pip.txt
pip-compile --version | cut -d' ' -f3 > /work/piptools.txt
pip-compile --quiet --no-header --no-annotate --generate-hashes --strip-extras --resolver=backtracking \
    --pip-args '--only-binary=:all:' --output-file /work/out.txt /work/requirements.in
# The resolver lists the hashes of every file of a release (all platforms and Python versions). The lock keeps only the wheel(s) pip picks on
# THIS platform, so a wheel built for another one is refused by hash as well as by tag.
grep -E '^[A-Za-z0-9]' /work/out.txt | sed 's/ *\\$//' > /work/pins.txt
mkdir /work/wheels
pip download --quiet --no-deps --only-binary=:all: -r /work/pins.txt -d /work/wheels
(cd /work/wheels && sha256sum *.whl) > /work/wheel-hashes.txt
"""


def generate(platform: str, base_image: str, output: Path, docker: str, token: str) -> Path:
    requirements = REQUIREMENTS.read_bytes()
    with tempfile.TemporaryDirectory(prefix='lock-work-', dir=os.environ.get('LOCK_WORK_PARENT')) as work:
        Path(work, 'requirements.in').write_bytes(requirements)
        container = Container(docker, token, 'lock-' + platform.split('/')[1])
        try:
            container.create(base_image, platform, [f'{work}:/work'], ['sh', '-c', RESOLVE], {**container_env(work), 'PIP_TOOLS': PIP_TOOLS})
            container.run()
        finally:
            container.remove()
        entries = restrict_to_downloaded(parse_pip_compile(Path(work, 'out.txt').read_text()), Path(work, 'wheel-hashes.txt').read_text())
        python = Path(work, 'python.txt').read_text().split()
        versions = {name: Path(work, f'{name}.txt').read_text().strip() for name in ('pip', 'piptools')}
    if f'{python[0]}.{python[1]}' != lockfile.PYTHON:
        raise RuntimeError(f'the base image runs CPython {python[0]}.{python[1]}, not {lockfile.PYTHON}')
    header = {'platform': platform, 'python': lockfile.PYTHON, 'base-image': base_image,
              'requirements-sha256': lockfile.sha256_bytes(requirements), 'generator': f'pip-tools {versions["piptools"]} with pip {versions["pip"]}'}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(lockfile.render(header, entries))
    return output


# -- offline check ---------------------------------------------------------------------------------------------------------------------------
def direct_requirements() -> list:
    from packaging.requirements import Requirement
    dependencies = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']['dependencies']
    return [Requirement(item) for item in dependencies]


def check(paths=None) -> list[str]:
    """Problems of the lock files against ``requirements.txt`` and ``pyproject.toml`` (empty when everything agrees). No network."""
    problems, locks = [], {}
    expected = lockfile.sha256_bytes(REQUIREMENTS.read_bytes())
    for path in paths or lock_paths():
        if not path.is_file():
            problems.append(f'{path.name}: missing')
            continue
        try:
            lock = lockfile.parse_lock(path.read_text(encoding='utf-8'))
        except lockfile.LockError as exc:
            problems.append(f'{path.name}: {exc}')
            continue
        locks[path] = lock
        if path != lock_path(lock.platform) and paths is None:
            problems.append(f'{path.name}: file name does not match platform {lock.platform}')
        if lock.header['requirements-sha256'] != expected:
            problems.append(f'{path.name}: resolved from a different requirements.txt; run make lock')
        versions = lock.versions()
        for requirement in direct_requirements():
            name = lockfile.canonical_name(requirement.name)
            if name not in versions:
                problems.append(f'{path.name}: {name} is not pinned')
            elif not requirement.specifier.contains(versions[name], prereleases=True):
                problems.append(f'{path.name}: {name}=={versions[name]} is outside {requirement.specifier}')
    by_platform = {lock.platform: lock for lock in locks.values()}
    if len(by_platform) == len(lockfile.PLATFORMS):
        first, second = (lock.versions() for lock in by_platform.values())
        for name in sorted(set(first) & set(second)):
            if first[name] != second[name]:
                problems.append(f'{name} has different versions on the two platforms: {first[name]} / {second[name]}')
    return problems


def wheelhouse(platform: str, base_image: str, lock_file: Path, package_wheel: Path, output: Path, docker: str, token: str) -> dict:
    """Prepare a wheelhouse inside the base image on the platform (the documented command), then verify it against the lock."""
    lock = lockfile.parse_lock(lock_file.read_text(encoding='utf-8'))
    if lock.platform != platform:
        raise RuntimeError(f'{lock_file.name} is the lock for {lock.platform}, not {platform}')
    output.mkdir(parents=True, exist_ok=False)
    command = ('mkdir -p /tmp/lockhome && pip download --quiet --no-deps --only-binary=:all: --require-hashes -r /lock/lock.txt -d /wheelhouse')
    container = Container(docker, token, 'wheelhouse-' + platform.split('/')[1])
    try:
        container.create(base_image, platform, [f'{lock_file.resolve()}:/lock/lock.txt:ro', f'{output.resolve()}:/wheelhouse'], ['sh', '-c', command],
                         container_env(''))
        container.run()
    finally:
        container.remove()
    (output / package_wheel.name).write_bytes(package_wheel.read_bytes())
    return lockfile.verify_wheelhouse(output, lock, package_wheel=package_wheel.name)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('generate', 'wheelhouse'):
        sub = commands.add_parser(name)
        sub.add_argument('--platform', choices=sorted(lockfile.PLATFORMS), required=True)
        sub.add_argument('--base-image', default=BASE_IMAGE)
        sub.add_argument('--docker', default='docker')
        sub.add_argument('--token', default='lock', help='owner token in container names and labels')
        if name == 'generate':
            sub.add_argument('--output', type=Path)
        else:
            sub.add_argument('--lock', type=Path)
            sub.add_argument('--package-wheel', type=Path, required=True)
            sub.add_argument('--output', type=Path, required=True)
    commands.add_parser('check')
    args = parser.parse_args(argv)
    if args.command == 'check':
        problems = check()
        for problem in problems:
            print('lock problem:', problem, file=sys.stderr)
        print('image dependency locks are consistent' if not problems else f'{len(problems)} lock problem(s)')
        return 1 if problems else 0
    if args.command == 'generate':
        path = generate(args.platform, args.base_image, args.output or lock_path(args.platform), args.docker, args.token)
        print(f'wrote {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}')
        return 0
    print(json.dumps(wheelhouse(args.platform, args.base_image, args.lock or lock_path(args.platform), args.package_wheel, args.output, args.docker,
                                args.token), indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
