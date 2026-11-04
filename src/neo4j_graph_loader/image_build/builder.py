"""Build the four loader images from the installed package with ``docker buildx`` (BuildKit).

The build context is generated in a fresh temporary directory from package data (the four Dockerfiles, ``install.py`` and the hashed per-platform
lock), the package wheel and, for an offline build, a wheelhouse. Nothing is read from a checkout. ``docker`` runs with a **sanitized
environment**: a fresh ``DOCKER_CONFIG`` that holds only the registry logins for the base image's registry and the target registry, no proxy
variable unless ``--proxy`` is given, and the daemon taken from the current Docker context (local sockets only). The Docker SDK cannot pass
BuildKit secrets, so it is never used for building or pushing. The index URL may carry credentials: it is passed only as a BuildKit secret
file and is never put in an argument list, the context, the manifest or a log.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import importlib.metadata
from importlib.resources import files
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from urllib.parse import urlsplit
import zipfile

from neo4j_graph_loader import _images
from neo4j_graph_loader._version import __version__
from neo4j_graph_loader.image_build import lockfile

logger = logging.getLogger(__name__)
RECIPE_VERSION = 1
MANIFEST_SCHEMA = 'graph-loader-image-manifest/2'
ROLES = _images.ROLES
DOCKERFILES = {'cli': 'Dockerfile', 'node': 'Dockerfile.node_loader', 'edge': 'Dockerfile.edge_loader', 'clock': 'Dockerfile.clock'}
DEFAULT_BASE = 'python:3.12-slim'
DISTRIBUTION = 'neo4j-graph-loader'
SUFFIX = re.compile(r'[a-z0-9][a-z0-9_.-]{0,63}')
LABELS = ('org.opencontainers.image.version', 'io.graph-loader.component', 'io.graph-loader.image-contract')
EXPECTED_ENTRYPOINTS = {
    'cli': ['tini', '--', 'python', '-m', 'neo4j_graph_loader.cli'],
    'node': ['python', '-m', 'neo4j_graph_loader.loader.node_loader'],
    'edge': ['python', '-m', 'neo4j_graph_loader.loader.edge_loader'],
    'clock': ['python', '-m', 'neo4j_graph_loader.orchestrator.clock_runner'],
}
PLATFORM_BY_ARCH = {'amd64': 'linux/amd64', 'x86_64': 'linux/amd64', 'arm64': 'linux/arm64', 'aarch64': 'linux/arm64'}


class BuildError(RuntimeError):
    """A refusal or a failed step; the message never contains a credential."""


@dataclass
class Settings:
    registry: str
    version: str = __version__
    package_wheel: Path | None = None
    base_image: str = DEFAULT_BASE
    index_url: str | None = None
    index_secret: Path | None = None
    wheelhouse: Path | None = None
    ca_bundle: Path | None = None
    proxy: str | None = None
    docker_config: Path | None = None
    debian_mirror: str | None = None
    tag_suffix: str | None = None
    push: bool = False
    dry_run: bool = False
    output: Path | None = None
    replace: bool = False
    bind_to_installed: bool = True          # the candidate builder builds a wheel from a checkout instead of the installed package
    roles: tuple = ROLES
    docker: str = 'docker'
    extra_build_args: dict = field(default_factory=dict)

    @property
    def source(self) -> str:
        return 'wheelhouse' if self.wheelhouse else 'index-secret' if self.index_secret else 'index-url'


# -- package data ------------------------------------------------------------------------------------------------------------------------------
def package_file(*parts: str):
    return files('neo4j_graph_loader').joinpath('image_build', *parts)


def package_bytes(*parts: str) -> bytes:
    return package_file(*parts).read_bytes()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


# -- Docker, with a sanitized environment ----------------------------------------------------------------------------------------------------
class Docker:
    """The docker CLI run with a fixed environment; ``run`` returns the completed process and never raises for a non-zero exit."""

    def __init__(self, executable: str, env: dict):
        self.executable, self.env = executable, env

    def run(self, *args: str, timeout: int = 3600, input_text: str | None = None) -> subprocess.CompletedProcess:
        try:
            return subprocess.run([self.executable, *args], env=self.env, capture_output=True, text=True, timeout=timeout, input=input_text)
        except FileNotFoundError:
            raise BuildError('the docker command is not installed or not on PATH') from None
        except subprocess.TimeoutExpired:
            raise BuildError(f'docker {args[0] if args else ""} timed out') from None


def user_environment() -> dict:
    """The part of the caller's environment docker needs (to find itself and credential helpers); no proxy, registry or pip variable."""
    return {key: os.environ[key] for key in ('PATH', 'HOME') if key in os.environ} | (
        {'DOCKER_HOST': os.environ['DOCKER_HOST']} if os.environ.get('DOCKER_HOST') else {}) | (
        {'DOCKER_CONFIG': os.environ['DOCKER_CONFIG']} if os.environ.get('DOCKER_CONFIG') else {})


def daemon_endpoint(user: Docker) -> str:
    """The local unix socket of the caller's current Docker context. A network daemon is refused."""
    if user.env.get('DOCKER_HOST'):
        host = user.env['DOCKER_HOST']
    else:
        done = user.run('context', 'inspect', '--format', '{{.Endpoints.docker.Host}}', timeout=60)
        if done.returncode:
            raise BuildError('cannot read the current Docker context; is docker installed and a daemon configured?')
        host = done.stdout.strip()
    parts = urlsplit(host)
    if parts.scheme != 'unix' or parts.netloc or not parts.path.startswith('/') or parts.query or '%' in parts.path or '..' in Path(parts.path).parts:
        raise BuildError('the build needs a local Docker daemon reached through a unix socket')
    return host


def buildx_plugin(user: Docker) -> Path:
    done = user.run('info', '--format', '{{json .ClientInfo.Plugins}}', timeout=60)
    try:
        plugins = json.loads(done.stdout) if done.returncode == 0 else []
        path = next(Path(plugin['Path']) for plugin in plugins if plugin.get('Name') == 'buildx')
    except (ValueError, StopIteration, KeyError, TypeError):
        raise BuildError('docker buildx (BuildKit) is required to build images; install the buildx plugin') from None
    if not path.is_file():
        raise BuildError('docker buildx (BuildKit) is required to build images; install the buildx plugin')
    return path


def registry_host(reference: str) -> str:
    """The registry host of an image reference (``docker.io`` when it names none)."""
    first = reference.split('/', 1)[0]
    return first if '/' in reference and ('.' in first or ':' in first or first == 'localhost') else 'docker.io'


def _auth_keys(host: str) -> set:
    if host in ('docker.io', 'index.docker.io', 'registry-1.docker.io'):
        return {'docker.io', 'index.docker.io', 'registry-1.docker.io', 'https://index.docker.io/v1/'}
    return {host, f'https://{host}', f'http://{host}'}


def scoped_config(source: dict, registries) -> dict:
    """A Docker ``config.json`` holding only the logins for ``registries``: their ``auths`` and ``credHelpers`` entries, and the global
    ``credsStore`` as a ``credHelpers`` entry for each of them. No other registry, proxy, plugin or context setting is carried over."""
    wanted, auths, helpers = set(), {}, {}
    for host in registries:
        wanted |= _auth_keys(host)
    for key, value in (source.get('auths') or {}).items():
        if key in wanted or urlsplit(key if '//' in key else '//' + key).netloc in wanted:
            auths[key] = value
    for host in registries:
        helper = (source.get('credHelpers') or {}).get(host) or next(
            (v for k, v in (source.get('credHelpers') or {}).items() if k in _auth_keys(host)), None) or source.get('credsStore')
        if helper:
            for key in (_auth_keys(host) if host in ('docker.io',) else {host}):
                helpers[key] = helper
    result = {}
    if auths:
        result['auths'] = auths
    if helpers:
        result['credHelpers'] = helpers
    return result


def read_config(directory: Path | None) -> dict:
    base = Path(directory) if directory else Path(os.environ.get('DOCKER_CONFIG') or Path.home() / '.docker')
    try:
        return json.loads((base / 'config.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


# -- validation and preparation ---------------------------------------------------------------------------------------------------------------
def validate_settings(settings: Settings) -> None:
    try:
        _images.registry_prefix(settings.registry)
    except _images.ImageSelectionError as exc:
        raise BuildError(str(exc).replace('GRAPH_LOADER_IMAGE_REGISTRY', '--registry')) from None
    if settings.version != __version__ and settings.bind_to_installed:
        raise BuildError(f'this package builds images for its own version {__version__}, not {settings.version}')
    if settings.tag_suffix is not None:
        if not SUFFIX.fullmatch(settings.tag_suffix):
            raise BuildError('the tag suffix must be 1-64 lowercase tag-safe characters')
        if settings.push:
            raise BuildError('a tag suffix marks a local development build; it cannot be pushed')
    sources = [bool(settings.wheelhouse), bool(settings.index_url), bool(settings.index_secret)]
    if sum(sources) != 1:
        raise BuildError('give exactly one of --wheelhouse, --pip-index-url and --pip-index-secret')
    if settings.index_url:
        parts = urlsplit(settings.index_url)
        if parts.scheme not in ('http', 'https') or not parts.hostname or any(c.isspace() for c in settings.index_url):
            raise BuildError('--pip-index-url must be an http(s) URL')
        if parts.username is not None or parts.password is not None:
            raise BuildError('a URL with credentials must be given with --pip-index-secret, not on the command line')
    if settings.index_secret and not Path(settings.index_secret).is_file():
        raise BuildError('--pip-index-secret must be a file that holds the index URL')
    if settings.proxy:
        parts = urlsplit(settings.proxy)
        if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username is not None:
            raise BuildError('--proxy must be an http(s) URL without credentials')
    if settings.debian_mirror:
        parts = urlsplit(settings.debian_mirror)
        if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username is not None or any(c.isspace() for c in settings.debian_mirror):
            raise BuildError('--debian-mirror must be an http(s) URL without credentials')
    if settings.ca_bundle and not Path(settings.ca_bundle).is_file():
        raise BuildError('--pip-ca-bundle must be a file')
    if settings.output is not None and Path(settings.output).exists():
        raise BuildError('--output must name a new directory')
    unknown = set(settings.roles) - set(ROLES)
    if unknown or not settings.roles:
        raise BuildError('unknown image role')


def installed_binding(wheel: Path, version: str) -> None:
    """The wheel must be the package that is installed: its file hashes for ``neo4j_graph_loader/`` equal the installed RECORD."""
    try:
        installed = importlib.metadata.distribution(DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        raise BuildError('the package is not installed from a wheel') from None
    direct = installed.read_text('direct_url.json')
    if direct and (json.loads(direct).get('dir_info') or {}).get('editable'):
        raise BuildError('an editable install cannot be built into images; install the released wheel')
    expected = {}
    for item in installed.files or ():
        if str(item).startswith('neo4j_graph_loader/') and item.hash and '__pycache__' not in str(item):
            expected[str(item)] = item.hash.value
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if f'neo4j_graph_loader-{version}.dist-info/METADATA' not in names:
            raise BuildError(f'{wheel.name} is not a wheel of {DISTRIBUTION} {version}')
        import base64
        actual = {n: base64.urlsafe_b64encode(hashlib.sha256(archive.read(n)).digest()).rstrip(b'=').decode()
                  for n in names if n.startswith('neo4j_graph_loader/') and not n.endswith('/')}
    if not expected or actual != expected:
        raise BuildError('the package wheel is not the installed package (file hashes differ)')


def locate_package_wheel(settings: Settings) -> Path:
    name = f'neo4j_graph_loader-{settings.version}-py3-none-any.whl'
    if settings.wheelhouse:
        in_house = Path(settings.wheelhouse) / name
        if settings.package_wheel:
            given = Path(settings.package_wheel)
            if in_house.is_file() and in_house.read_bytes() != given.read_bytes():
                raise BuildError('the wheelhouse holds a different package wheel than --package-wheel')
            if not in_house.is_file():
                raise BuildError(f'put {name} into the wheelhouse next to its dependencies')
        if not in_house.is_file():
            raise BuildError(f'the wheelhouse is missing the package wheel {name}')
        return in_house
    if not settings.package_wheel:
        raise BuildError('give --package-wheel (the wheel you installed) when installing dependencies from an index')
    path = Path(settings.package_wheel)
    if not path.is_file() or not path.name.endswith('.whl'):
        raise BuildError('--package-wheel must be a wheel file')
    return path


def host_platform(docker: Docker) -> str:
    done = docker.run('version', '--format', '{{.Server.Os}}/{{.Server.Arch}}', timeout=60)
    if done.returncode or '/' not in done.stdout:
        raise BuildError('cannot reach the Docker daemon; is it running and is the socket readable?')
    os_name, arch = done.stdout.strip().split('/', 1)
    platform = PLATFORM_BY_ARCH.get(arch)
    if os_name != 'linux' or platform is None:
        raise BuildError(f'images are built for linux/amd64 or linux/arm64; the daemon is {os_name}/{arch}')
    return platform


def resolve_base(docker: Docker, base: str) -> str:
    """The base image as ``name@sha256:digest`` (a reference that already has a digest is used as given)."""
    if re.search(r'@sha256:[0-9a-f]{64}$', base):
        return base
    done = docker.run('buildx', 'imagetools', 'inspect', base, '--format', '{{.Manifest.Digest}}', timeout=300)
    digest = done.stdout.strip()
    if done.returncode or not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise BuildError('cannot resolve the base image to a digest; check the name and the registry login')
    name = re.sub(r':[^:/@]+$', '', base)
    return f'{name}@{digest}'


def tag_for(settings: Settings, role: str) -> str:
    suffix = f'-{settings.tag_suffix}' if settings.tag_suffix else ''
    return f'{settings.registry}/graph-loader-{role}:{settings.version}{suffix}'


def build_context(destination: Path, settings: Settings, lock_name: str, wheel: Path) -> None:
    for role in settings.roles:
        (destination / DOCKERFILES[role]).write_bytes(package_bytes('dockerfiles', DOCKERFILES[role]))
    (destination / 'install.py').write_bytes(package_bytes('install.py'))
    (destination / 'lock.txt').write_bytes(package_bytes('locks', lock_name))
    shutil.copy(wheel, destination / wheel.name)
    (destination / 'pip-ca.pem').write_bytes(Path(settings.ca_bundle).read_bytes() if settings.ca_bundle else b'')
    if settings.wheelhouse:
        shutil.copytree(settings.wheelhouse, destination / 'wheelhouse')


# -- the build ---------------------------------------------------------------------------------------------------------------------------------
def build(settings: Settings, *, docker_factory=Docker) -> dict:
    """Validate, build (unless ``dry_run``), optionally push, and return the manifest (the plan, for a dry run)."""
    validate_settings(settings)
    user = docker_factory(settings.docker, user_environment())
    wheel = locate_package_wheel(settings)
    if settings.bind_to_installed:
        installed_binding(wheel, settings.version)
    wheel_sha = lockfile.sha256_file(wheel)
    endpoint = daemon_endpoint(user)
    plugin = buildx_plugin(user)
    with tempfile.TemporaryDirectory(prefix='graph-loader-images-') as work:
        work_dir = Path(work)
        config = work_dir / 'docker-config'
        (config / 'cli-plugins').mkdir(parents=True, mode=0o700)
        (config / 'cli-plugins' / 'docker-buildx').symlink_to(plugin)
        environment = {key: value for key, value in user_environment().items() if key in ('PATH', 'HOME')}
        environment |= {'DOCKER_CONFIG': str(config), 'DOCKER_HOST': endpoint, 'DOCKER_BUILDKIT': '1'}
        docker = docker_factory(settings.docker, environment)
        platform = host_platform(docker)
        lock_name = f'py{lockfile.PYTHON.replace(".", "")}-{platform.replace("/", "-")}.txt'
        lock_text = package_bytes('locks', lock_name).decode('utf-8')
        try:
            lock = lockfile.parse_lock(lock_text)
        except lockfile.LockError as exc:
            raise BuildError(f'the shipped dependency lock is invalid: {exc}') from None
        if settings.wheelhouse:
            try:
                lockfile.verify_wheelhouse(settings.wheelhouse, lock, package_wheel=wheel.name)
            except lockfile.LockError as exc:
                raise BuildError(f'the wheelhouse does not satisfy the lock: {exc}') from None
        tags = {role: tag_for(settings, role) for role in settings.roles}
        refused = [tag for tag in tags.values() if docker.run('image', 'inspect', tag, timeout=60).returncode == 0]
        if refused and not settings.replace and not settings.dry_run:
            raise BuildError('image tag already exists locally: ' + ', '.join(refused) + '; pass --replace to overwrite it')
        base_registry = registry_host(settings.base_image)
        target_registry = registry_host(settings.registry + '/x')
        plan = {
            'schema': MANIFEST_SCHEMA, 'version': settings.version, 'platform': platform, 'dry_run': settings.dry_run,
            'base_image': {'requested': settings.base_image}, 'dependency_source': settings.source,
            'package_wheel': {'file': wheel.name, 'sha256': wheel_sha}, 'lock': {'file': lock_name, 'sha256': sha256_bytes(lock_text.encode())},
            'dependency_sha256': sorted(d for entry in lock.entries for d in entry.hashes),
            'recipe': {'version': RECIPE_VERSION, 'dockerfiles': {DOCKERFILES[r]: sha256_bytes(package_bytes('dockerfiles', DOCKERFILES[r])) for r in settings.roles},
                       'install_helper_sha256': sha256_bytes(package_bytes('install.py'))},
            'images': [{'component': role, 'local_tag': tags[role], 'dockerfile': DOCKERFILES[role]} for role in settings.roles],
            'registries': sorted({base_registry, target_registry}), 'push': settings.push,
        }
        if settings.dry_run:
            return plan
        source_config = read_config(settings.docker_config)
        (config / 'config.json').write_text(json.dumps(scoped_config(source_config, {base_registry, target_registry})), encoding='utf-8')
        (config / 'config.json').chmod(0o600)
        resolved = resolve_base(docker, settings.base_image)
        context = work_dir / 'context'
        context.mkdir()
        build_context(context, settings, lock_name, wheel)
        manifest = {**plan, 'dry_run': False, 'published': False, 'base_image': {'requested': settings.base_image, 'resolved': resolved}, 'images': [],
                    'build_args': {}}
        rows = []
        for role in settings.roles:
            tag = tags[role]
            args = {'BASE_IMAGE': resolved, 'GRAPH_LOADER_VERSION': settings.version, 'PACKAGE_WHEEL': wheel.name, 'PACKAGE_WHEEL_SHA256': wheel_sha,
                    'DEPENDENCY_SOURCE': settings.source}
            if settings.index_url:
                args['INDEX_URL'] = settings.index_url
            if settings.debian_mirror and role == 'cli':
                args['DEBIAN_MIRROR'] = settings.debian_mirror
            args |= settings.extra_build_args
            head = ['buildx', 'build', '--load', '--progress=plain', '--provenance=false', '--sbom=false', '--file']
            command = [*head, str(context / DOCKERFILES[role]), '-t', tag]
            recorded = [*head, DOCKERFILES[role], '-t', tag]
            for key, value in args.items():
                command += ['--build-arg', f'{key}={value}']
                recorded += ['--build-arg', f'{key}=<url>' if key == 'INDEX_URL' else f'{key}={value}']
            if settings.proxy:
                for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
                    command += ['--build-arg', f'{key}={settings.proxy}']
                recorded += ['--build-arg', 'HTTP(S)_PROXY=<proxy>']
            if settings.index_secret:
                command += ['--secret', f'id=pip_index,src={Path(settings.index_secret).resolve()}']
                recorded += ['--secret', 'id=pip_index,src=<secret file>']
            done = docker.run(*command, str(context), timeout=3600)
            if done.returncode:
                raise BuildError(f'the {role} image build failed:\n' + redact((done.stdout + done.stderr)[-2500:], settings))
            rows.append(inspect_image(docker, role, tag, settings, recorded))
            manifest['build_args'] = {k: v for k, v in args.items() if k != 'INDEX_URL'}
        if 'cli' in settings.roles:
            manifest['debian_packages'] = debian_packages(docker, tags['cli'])
        if settings.push:
            for row in rows:
                row['registry_digest'] = push_image(docker, row['local_tag'])
        manifest['images'] = rows
        manifest['proxy'] = bool(settings.proxy)
        manifest.pop('registries', None)
        if settings.output is not None:
            write_manifest(Path(settings.output), manifest)
        return manifest


def redact(text: str, settings: Settings) -> str:
    """Keep the index URL, its credentials and any proxy out of a message."""
    secrets = []
    for value in (settings.index_url, settings.proxy):
        if value:
            secrets.append(value)
    if settings.index_secret and Path(settings.index_secret).is_file():
        url = Path(settings.index_secret).read_text(encoding='utf-8', errors='replace').strip()
        parts = urlsplit(url)
        secrets += [url, parts.netloc, parts.username or '', parts.password or '']
    for secret in sorted((s for s in secrets if len(s) >= 4), key=len, reverse=True):
        text = text.replace(secret, '***')
    return re.sub(r'(://)[^/@\s]*@', r'\1***@', text)


def inspect_image(docker: Docker, role: str, tag: str, settings: Settings, recorded: list) -> dict:
    done = docker.run('image', 'inspect', tag, timeout=120)
    try:
        info = json.loads(done.stdout)[0]
    except (ValueError, IndexError):
        raise BuildError(f'cannot inspect the built {role} image') from None
    labels = info.get('Config', {}).get('Labels') or {}
    expected = {LABELS[0]: settings.version, LABELS[1]: role, LABELS[2]: _images.CONTRACT}
    if any(labels.get(k) != v for k, v in expected.items()):
        raise BuildError(f'the built {role} image does not carry the three image-contract labels')
    if info.get('Config', {}).get('Entrypoint') != EXPECTED_ENTRYPOINTS[role]:
        raise BuildError(f'the built {role} image has an unexpected entrypoint')
    return {'component': role, 'id': info['Id'], 'local_tag': tag, 'proposed_registry_tag': tag, 'registry_digest': None,
            'platform': f'{info.get("Os")}/{info.get("Architecture")}', 'labels': {k: labels[k] for k in sorted(labels)},
            'entrypoint': info['Config']['Entrypoint'], 'command': ['docker', *recorded, '<generated context>'], 'exit_code': 0}


def debian_packages(docker: Docker, tag: str) -> dict:
    done = docker.run('run', '--rm', '--network', 'none', '--entrypoint', 'dpkg-query', tag, '-W', '-f', '${Package}=${Version}\n', 'tini', 'base-files', timeout=300)
    if done.returncode:
        raise BuildError('cannot read the Debian package versions of the CLI image')
    return dict(line.split('=', 1) for line in done.stdout.split())


def push_image(docker: Docker, tag: str) -> str:
    done = docker.run('push', tag, timeout=3600)
    match = re.search(r'digest: (sha256:[0-9a-f]{64})', done.stdout + done.stderr)
    if done.returncode or not match:
        raise BuildError(f'pushing {tag} failed: ' + (done.stderr or done.stdout).strip()[-300:])
    return match.group(1)


def write_manifest(output: Path, manifest: dict) -> None:
    output.mkdir(parents=False, exist_ok=False)
    (output / 'images.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    (output / 'image-overrides.sh').write_text('# References of the images built from this manifest; no credentials.\n' + ''.join(
        f"export GRAPH_LOADER_{row['component'].upper()}_IMAGE='{row['local_tag']}'\n" for row in manifest['images']), encoding='utf-8')
