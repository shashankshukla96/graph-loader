#!/usr/bin/env python3
"""E19.F2.S5 proof harness (private): mirror-aware Dockerfiles and secret transport, on the Mac's Docker Desktop.

    python e19_s5_proof.py WORKDIR TOKEN EXPERIMENT [EXPERIMENT ...]

Owned throwaway builds only: image tags ``e19-<token>/...``, a fresh empty DOCKER_CONFIG (so the user's contexts, builders and logins are never
read or changed; the daemon is reached through DOCKER_HOST), the implicit ``default`` docker-driver builder of that config, no prune. Local
owned servers (an https index with basic auth and a throwaway CA, a logging Debian mirror proxy, and logging sinks that stand in for a planted
fallback index and proxy) are started in this process and logged. Results go to WORKDIR/results/<experiment>.json.
"""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import e19_servers as srv  # noqa: E402

BASE = 'python@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30'
SENTINEL_USER, SENTINEL_PASSWORD = 'ci-user', 'Zq7Xk2Sentinel'
ROLES = {'cli': 'Dockerfile', 'node': 'Dockerfile.node_loader', 'edge': 'Dockerfile.edge_loader', 'clock': 'Dockerfile.clock'}
HOST = 'host.docker.internal'


class Harness:
    def __init__(self, work: Path, token: str):
        self.work, self.token = work, token
        for sub in ('ctx', 'logs', 'results', 'secrets', 'tls', 'save', 'cfg'):
            (work / sub).mkdir(parents=True, exist_ok=True)
        self.version = re.search(r"__version__\s*=\s*['\"]([^'\"]+)", (ROOT / 'src/neo4j_graph_loader/_version.py').read_text()).group(1)
        self.wheel = next((work / 'dist').glob('*.whl'))
        self.deps = work / 'deps'
        self.lock = ROOT / 'image-locks/py312-linux-arm64.txt'
        self.env = {k: v for k, v in os.environ.items() if not k.upper().endswith('_PROXY')}
        self.env.update(DOCKER_CONFIG=str(work / 'dockercfg'), DOCKER_HOST='unix:///Users/shashank/.docker/run/docker.sock')
        self.ca, self.cert, self.key = srv.make_ca_and_cert(work / 'tls', [HOST, 'localhost'])
        self.images: list[str] = []
        self.servers: list = []

    # -- servers ---------------------------------------------------------------------------------------------------------------------
    def index(self, name, credentials=f'{SENTINEL_USER}:{SENTINEL_PASSWORD}', directory=None):
        server = srv.index_server(directory or self.deps, tls=(self.cert, self.key), credentials=credentials, log_file=self.work / 'logs' / f'{name}.jsonl')
        self.servers.append(server)
        return server

    def sink(self, name):
        server = srv.logging_sink(self.work / 'logs' / f'{name}.jsonl')
        self.servers.append(server)
        return server

    def mirror(self, name):
        server = srv.mirror_server(log_file=self.work / 'logs' / f'{name}.jsonl')
        self.servers.append(server)
        return server

    def stop_servers(self):
        for server in self.servers:
            server.stop()
        self.servers = []

    # -- docker ----------------------------------------------------------------------------------------------------------------------
    def docker(self, *args, check=True, timeout=300, env=None, input_text=None):
        done = subprocess.run(['docker', *args], env=env or self.env, capture_output=True, text=True, timeout=timeout, input=input_text)
        if check and done.returncode:
            raise RuntimeError(f'docker {args[0]} failed: {done.stderr[-500:]}')
        return done

    def context(self, name, role, *, ca=True, wheelhouse=None, lock=None, wheel=None, dockerfile=None, extra=None):
        d = self.work / 'ctx' / name
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        wheel = wheel or self.wheel
        shutil.copy(dockerfile or ROOT / ROLES[role], d / 'Dockerfile')
        shutil.copy(ROOT / 'src/neo4j_graph_loader/image_build/install.py', d / 'install.py')
        shutil.copy(lock or self.lock, d / 'lock.txt')
        shutil.copy(wheel, d / wheel.name)
        (d / 'pip-ca.pem').write_bytes(self.ca.read_bytes() if ca else b'')
        if wheelhouse:
            shutil.copytree(wheelhouse, d / 'wheelhouse')
        for rel, data in (extra or {}).items():
            (d / rel).write_bytes(data)
        return d

    def build(self, name, role, ctx, *, base=BASE, source='index-secret', secret=None, index_url=None, mirror=None, flags=None, env=None,
              network=None, tag=None, timeout=1200):
        tag = tag or f'e19-{self.token}/graph-loader-{role}:{self.version}-{name}'
        wheel = next(p for p in Path(ctx).glob('*.whl'))
        args = ['buildx', 'build', '--load', '--progress=plain', '--no-cache', '--provenance=false', '--sbom=false', '-t', tag,
                '--build-arg', f'BASE_IMAGE={base}', '--build-arg', f'PACKAGE_WHEEL={wheel.name}',
                '--build-arg', f'PACKAGE_WHEEL_SHA256={srv_sha(wheel)}', '--build-arg', f'GRAPH_LOADER_VERSION={self.version}',
                '--build-arg', f'DEPENDENCY_SOURCE={source}']
        if index_url:
            args += ['--build-arg', f'INDEX_URL={index_url}']
        if mirror is not None:
            args += ['--build-arg', f'DEBIAN_MIRROR={mirror}']
        if flags:
            args += ['--build-arg', f'INSTALL_FLAGS={flags}']
        if network:
            args += ['--network', network]
        if secret is not None:
            path = self.work / 'secrets' / f'{name}.secret'
            path.write_text(secret)
            path.chmod(0o600)
            args += ['--secret', f'id=pip_index,src={path}']
        args += [str(ctx)]
        done = self.docker(*args, check=False, timeout=timeout, env=env)
        log = self.work / 'logs' / f'build-{name}.log'
        log.write_text(done.stdout + done.stderr)
        if done.returncode == 0:
            self.images.append(tag)
        return {'ok': done.returncode == 0, 'code': done.returncode, 'tag': tag, 'log': str(log), 'argv': ['docker', *args]}

    def save(self, name, results):
        (self.work / 'results' / f'{name}.json').write_text(json.dumps(results, indent=2, default=str) + '\n')
        return results

    # -- scanning ----------------------------------------------------------------------------------------------------------------------
    def scan_image(self, tag, needles):
        """Search the config, the history, the manifests and the bytes of every layer of a saved image for the needles."""
        archive = self.work / 'save' / (tag.replace('/', '_').replace(':', '_') + '.tar')
        self.docker('save', '-o', str(archive), tag, timeout=600)
        found, blobs, layer_files = [], 0, 0
        data = [n.encode() for n in needles]
        with tarfile.open(archive) as outer:
            for member in outer.getmembers():
                if not member.isfile():
                    continue
                raw = outer.extractfile(member).read()
                blobs += 1
                for needle in data:
                    if needle in raw:
                        found.append((member.name, 'raw'))
                try:
                    inner = tarfile.open(fileobj=io.BytesIO(raw), mode='r:*')
                except tarfile.TarError:
                    continue
                for entry in inner.getmembers():
                    if entry.isfile():
                        layer_files += 1
                        body = inner.extractfile(entry).read()
                        for needle in data:
                            if needle in body:
                                found.append((member.name + '!' + entry.name, 'layer file'))
        inspect = self.docker('image', 'inspect', tag).stdout
        history = self.docker('history', '--no-trunc', '--format', '{{.CreatedBy}}', tag).stdout
        for needle in needles:
            if needle in inspect:
                found.append(('image inspect (configuration)', needle[:3] + '…'))
            if needle in history:
                found.append(('image history', needle[:3] + '…'))
        archive.unlink()
        return {'blobs': blobs, 'layer_files': layer_files, 'found': found}

    def inspect(self, tag):
        return json.loads(self.docker('image', 'inspect', tag).stdout)[0]

    def freeze(self, tag):
        return sorted(self.docker('run', '--rm', '--entrypoint', 'python', tag, '-m', 'pip', 'list', '--format=freeze').stdout.split())

    def remove_images(self):
        removed = []
        for tag in reversed(self.images):
            if self.docker('image', 'rm', tag, check=False).returncode == 0:
                removed.append(tag)
        self.images = [t for t in self.images if t not in removed]
        return removed


def srv_sha(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def needles_for(url):
    from urllib.parse import urlsplit
    parts = urlsplit(url)
    return [url, parts.netloc.rsplit('@', 1)[0], parts.password, parts.username + ':' + parts.password]


def read_log(path, limit=0):
    text = Path(path).read_text()
    return text[-limit:] if limit else text


def jsonl(path):
    p = Path(path)
    return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []


# --------------------------------------------------------------------------------------------------------------------------------------
def e_default(h: Harness):
    """Default builds: the four images from the owned authenticated https index; labels, entrypoints, user; compare with the old builds."""
    index = h.index('index-default')
    url = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index.port}/simple/'
    result = {'new': {}, 'old': {}}
    for role in ROLES:
        ctx = h.context(f'd-{role}', role)
        built = h.build(f'd-{role}', role, ctx, secret=url + '\n', mirror=None)
        result['new'][role] = {k: built[k] for k in ('ok', 'tag')}
        if not built['ok']:
            result['new'][role]['log_tail'] = read_log(built['log'], 1500)
            continue
        info = h.inspect(built['tag'])
        result['new'][role].update(labels=info['Config'].get('Labels'), entrypoint=info['Config'].get('Entrypoint'), cmd=info['Config'].get('Cmd'), user=info['Config'].get('User'),
                                   workdir=info['Config'].get('WorkingDir'), env=info['Config'].get('Env'), freeze=h.freeze(built['tag']), id=info['Id'])
        result['new'][role]['from_digest'] = f'FROM docker.io/library/{BASE}' in read_log(built['log'])
    h.stop_servers()
    result['index_requests'] = len(jsonl(h.work / 'logs' / 'index-default.jsonl'))
    return h.save('default', result)


def e_old(h: Harness):
    """The 0.1.0a1-era Dockerfiles (git HEAD) built from the checkout, for the comparison."""
    d = h.work / 'ctx' / 'old'
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    archive = subprocess.run(['git', '-C', str(ROOT), 'archive', 'HEAD', '--', 'Dockerfile', 'Dockerfile.node_loader', 'Dockerfile.edge_loader',
                              'Dockerfile.clock', 'requirements.txt', 'pyproject.toml', 'PACKAGE_README.md', 'LICENSE', 'MANIFEST.in', 'src',
                              'config', '.env.example', '.dockerignore'], capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as handle:
        handle.extractall(d)
    result = {}
    for role, dockerfile in ROLES.items():
        tag = f'e19-{h.token}/old-graph-loader-{role}:{h.version}'
        args = ['buildx', 'build', '--load', '--progress=plain', '--provenance=false', '--sbom=false', '-f', str(d / dockerfile), '--build-arg',
                f'GRAPH_LOADER_VERSION={h.version}', '-t', tag, str(d)]
        done = h.docker(*args, check=False, timeout=1500)
        (h.work / 'logs' / f'build-old-{role}.log').write_text(done.stdout + done.stderr)
        if done.returncode:
            result[role] = {'ok': False, 'log_tail': (done.stdout + done.stderr)[-1200:]}
            continue
        h.images.append(tag)
        info = h.inspect(tag)
        listing = h.docker('run', '--rm', '--entrypoint', 'sh', tag, '-c', 'cd /app && find . -maxdepth 2 | sort').stdout.split()
        result[role] = {'ok': True, 'tag': tag, 'labels': info['Config'].get('Labels'), 'entrypoint': info['Config'].get('Entrypoint'), 'cmd': info['Config'].get('Cmd'),
                        'user': info['Config'].get('User'), 'workdir': info['Config'].get('WorkingDir'), 'env': info['Config'].get('Env'), 'freeze': h.freeze(tag),
                        'app_listing': listing}
    return h.save('old', result)


def e_credentials(h: Harness):
    """A credentialed URL as a BuildKit secret: absent from argv, context, logs, configuration, history, every layer and manifest; cmdline probe."""
    index = h.index('index-cred')
    url = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index.port}/simple/'
    needles = needles_for(url)
    ctx = h.context('cred', 'clock')
    built = h.build('cred', 'clock', ctx, secret=url + '\n', flags='--probe-cmdline')
    result = {'build_ok': built['ok'], 'tag': built['tag'], 'needles_checked': ['full URL', 'userinfo', 'password', 'user:password']}
    if not built['ok']:
        result['log_tail'] = read_log(built['log'], 2000)
        return h.save('credentials', result)
    log = read_log(built['log'])
    result['argv_contains_needle'] = any(n in ' '.join(built['argv']) for n in needles)
    result['context_contains_needle'] = any(n.encode() in p.read_bytes() for p in Path(ctx).rglob('*') if p.is_file() for n in needles)
    result['build_log_contains_needle'] = [n[:3] + '…' for n in needles if n in log]
    result['probe_line'] = [line.split(' ', 2)[-1] for line in log.splitlines() if 'probe:' in line][:2]
    result['config_check_line'] = [line.split(' ', 2)[-1] for line in log.splitlines() if 'pip configuration check' in line][:2]
    result['looking_in_line'] = [line.split(' ', 2)[-1] for line in log.splitlines() if 'Looking in indexes' in line][:1]
    result['scan'] = h.scan_image(built['tag'], needles)
    entries = jsonl(h.work / 'logs' / 'index-cred.jsonl')
    result['index'] = {'requests': len(entries), 'auth': sorted({e['auth'] for e in entries})}
    # the probe must be able to see a URL that is on a command line: the plain --index-url mode puts it on install.py's argv
    plain = h.build('cred-probe-control', 'clock', h.context('cred-probe-control', 'clock'), source='index-url', index_url=url, flags='--probe-cmdline')
    result['probe_control'] = {'build_ok': plain['ok'], 'log_tail': [l for l in read_log(plain['log']).splitlines() if 'command line' in l or 'probe' in l][-3:]}
    h.stop_servers()
    return h.save('credentials', result)


def e_planted(h: Harness):
    """Everything pip could be steered by is planted in the client and in the base image; only the intended index is contacted."""
    index = h.index('index-planted')
    fallback = h.sink('fallback-index')
    proxy = h.sink('fallback-proxy')
    fallback_url = f'http://{HOST}:{fallback.port}/simple/'      # plain http: a contact is logged even if the answer is an error
    proxy_url = f'http://{HOST}:{proxy.port}'
    planted = h.work / 'ctx' / 'planted-base'
    planted.mkdir(parents=True, exist_ok=True)
    (planted / 'Dockerfile').write_text(f'''FROM {BASE}
ENV PIP_INDEX_URL={fallback_url} PIP_EXTRA_INDEX_URL={fallback_url} PIP_TRUSTED_HOST={HOST} PIP_FIND_LINKS={fallback_url} PIP_NO_INDEX=0 \\
    HTTP_PROXY={proxy_url} HTTPS_PROXY={proxy_url} ALL_PROXY={proxy_url} http_proxy={proxy_url} https_proxy={proxy_url} NO_PROXY=
RUN printf '[global]\\nindex-url = {fallback_url}\\nextra-index-url = {fallback_url}\\ntrusted-host = {HOST}\\n' > /etc/pip.conf \\
    && printf '[global]\\nextra-index-url = {fallback_url}\\n' > /usr/local/pip.conf \\
    && mkdir -p /root/.config/pip /root/.pip && cp /etc/pip.conf /root/.config/pip/pip.conf && cp /etc/pip.conf /root/.pip/pip.conf
''')
    base_tag = f'e19-{h.token}/planted-base:1'
    h.docker("buildx", "build", "--load", "--provenance=false", "--sbom=false", "-t", base_tag, str(planted))
    h.images.append(base_tag)
    result = {'planted_base': base_tag}
    # pip's own view of that base image, with and without the helper's fresh environment
    raw = h.docker('run', '--rm', base_tag, 'sh', '-c', 'python -m pip config list').stdout
    result['planted_config_seen_by_plain_pip'] = sorted({line.split('=')[0].split('.')[0] + '.' + line.split('=')[0].split('.', 1)[-1] for line in raw.splitlines()})
    fresh = h.docker('run', '--rm', base_tag, 'env', '-i', 'PATH=/usr/local/bin:/usr/bin:/bin', 'PIP_CONFIG_FILE=/dev/null',
                     'PIP_INDEX_URL=https://index.invalid/simple/', 'python', '-I', '-m', 'pip', 'config', 'list').stdout
    result['pip_config_list_fresh_environment'] = fresh.splitlines()
    result['pip_version'] = h.docker('run', '--rm', base_tag, 'python', '-m', 'pip', '--version').stdout.strip()
    # planted client environment and planted Docker client proxy settings
    cfg = h.work / 'cfg' / 'planted-dockercfg'
    if cfg.exists():
        shutil.rmtree(cfg)
    shutil.copytree(h.work / 'dockercfg', cfg, symlinks=True)
    (cfg / 'config.json').write_text(json.dumps({'proxies': {'default': {'httpProxy': proxy_url, 'httpsProxy': proxy_url, 'allProxy': proxy_url}}}))
    env = {**h.env, 'DOCKER_CONFIG': str(cfg), 'PIP_INDEX_URL': fallback_url, 'PIP_EXTRA_INDEX_URL': fallback_url, 'HTTP_PROXY': proxy_url,
           'HTTPS_PROXY': proxy_url, 'ALL_PROXY': proxy_url, 'PIP_CONFIG_FILE': '/nonexistent'}
    url = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index.port}/simple/'
    built = h.build('planted', 'clock', h.context('planted', 'clock'), base=base_tag, secret=url + '\n', env=env, flags='--probe-cmdline')
    result.update(build_ok=built['ok'])
    log = read_log(built['log'])
    result['config_check_line'] = [line.split(' ', 2)[-1] for line in log.splitlines() if 'pip configuration check' in line][:2]
    if not built['ok']:
        result['log_tail'] = log[-2500:]
    result['intended_index'] = {'requests': len(jsonl(h.work / 'logs' / 'index-planted.jsonl')), 'auth': sorted({e['auth'] for e in jsonl(h.work / 'logs' / 'index-planted.jsonl')})}
    result['fallback_index_requests'] = len(jsonl(h.work / 'logs' / 'fallback-index.jsonl'))
    result['fallback_proxy_requests'] = len(jsonl(h.work / 'logs' / 'fallback-proxy.jsonl'))
    # positive control: plain pip in that same image does reach the planted fallback, so the sink can see a contact
    before = len(jsonl(h.work / 'logs' / 'fallback-index.jsonl'))
    h.docker('run', '--rm', base_tag, 'python', '-m', 'pip', 'download', '--no-deps', '--only-binary=:all:', '--retries', '0', '--timeout', '10', 'idna==3.20',
             '-d', '/tmp/x', check=False, timeout=120)
    result['positive_control'] = {'plain_pip_requests_to_fallback': len(jsonl(h.work / 'logs' / 'fallback-index.jsonl')) - before,
                                  'proxy_requests': len(jsonl(h.work / 'logs' / 'fallback-proxy.jsonl'))}
    h.stop_servers()
    return h.save('planted', result)


def e_tini(h: Harness):
    """tini: skip path with a base that carries it; apt path through an owned mirror; a dead mirror fails closed (public Debian is never used)."""
    result = {}
    # a base image that already carries tini 0.19.0
    base_dir = h.work / 'ctx' / 'base-tini'
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / 'Dockerfile').write_text(f'FROM {BASE}\nRUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*\n')
    base_tag = f'e19-{h.token}/base-with-tini:1'
    h.docker('buildx', 'build', '--load', '--provenance=false', '--sbom=false', '-t', base_tag, str(base_dir), timeout=900)
    h.images.append(base_tag)
    index = h.index('index-tini')
    url = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index.port}/simple/'
    dead = h.sink('dead-mirror')
    skip = h.build('tini-skip', 'cli', h.context('tini-skip', 'cli'), base=base_tag, secret=url + '\n', mirror=f'http://{HOST}:{dead.port}/debian')
    skip_log = read_log(skip['log'])
    result['skip'] = {'ok': skip['ok'], 'says_apt_skipped': bool(re.search(r'^#\d+ [0-9.]+ tini 0\.19\.0 is already in the base image; apt is skipped$', skip_log, re.M)), 'apt_get_ran': 'apt-get update' in skip_log.replace('RUN set -eu', '') and 'Get:1' in skip_log,
                      'dead_mirror_requests': len(jsonl(h.work / 'logs' / 'dead-mirror.jsonl'))}
    # the apt path with a mirror (a logging reverse proxy of the Debian archive)
    mirror = h.mirror('debian-mirror')
    apt = h.build('tini-apt', 'cli', h.context('tini-apt', 'cli'), secret=url + '\n', mirror=f'http://{HOST}:{mirror.port}/debian')
    apt_log = read_log(apt['log'])
    entries = jsonl(h.work / 'logs' / 'debian-mirror.jsonl')
    result['apt'] = {'ok': apt['ok'], 'mirror_requests': len(entries), 'mirror_paths': sorted({e['path'].split('/')[1] + '/' + '/'.join(e['path'].split('/')[2:4]) for e in entries})[:8],
                     'tini_deb_fetched': any('tini' in e['path'] for e in entries), 'apt_skipped': bool(re.search(r'^#\d+ [0-9.]+ tini 0\.19\.0 is already in the base image; apt is skipped$', apt_log, re.M))}
    if apt['ok']:
        info = h.inspect(apt['tag'])
        result['apt']['entrypoint'] = info['Config'].get('Entrypoint')
        result['apt']['tini_version'] = h.docker('run', '--rm', '--entrypoint', 'tini', apt['tag'], '--version').stdout.strip()
        result['apt']['debian_packages'] = h.docker('run', '--rm', '--entrypoint', 'dpkg-query', apt['tag'], '-W', '-f', '${Package}=${Version}\n', 'tini', 'base-files').stdout.split()
        result['apt']['sources_file'] = h.docker('run', '--rm', '--entrypoint', 'cat', apt['tag'], '/etc/apt/sources.list.d/debian.sources').stdout.splitlines()
    else:
        result['apt']['log_tail'] = apt_log[-2000:]
    # a mirror that does not serve Debian: the build fails, which also shows the public sources were replaced
    bad = h.build('tini-deadmirror', 'cli', h.context('tini-deadmirror', 'cli'), secret=url + '\n', mirror=f'http://{HOST}:{dead.port}/debian')
    result['dead_mirror'] = {'build_ok': bad['ok'], 'requests_to_dead_mirror': len(jsonl(h.work / 'logs' / 'dead-mirror.jsonl')) - result['skip']['dead_mirror_requests'],
                             'fails_in_apt': 'apt-get update' in read_log(bad['log']) or 'Failed to fetch' in read_log(bad['log'])}
    h.stop_servers()
    return h.save('tini', result)


def e_failclosed(h: Harness):
    """Fail-closed controls: no BuildKit (legacy builder), secret not supplied, wrong wheel digest, a tampered lock, wrong index content, offline wheelhouse."""
    result = {}
    index = h.index('index-fc')
    url = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index.port}/simple/'
    # legacy builder
    legacy_ctx = h.context('legacy', 'clock')
    legacy_env = {**h.env, 'DOCKER_BUILDKIT': '0'}
    done = subprocess.run(['docker', 'build', '-t', f'e19-{h.token}/legacy-attempt:1', str(legacy_ctx)], env=legacy_env, capture_output=True, text=True, timeout=300)
    result['legacy_builder'] = {'code': done.returncode, 'message': (done.stdout + done.stderr).strip().splitlines()[-1][:200] if (done.stdout + done.stderr).strip() else ''}
    # the secret is not supplied
    nosecret = h.build('nosecret', 'clock', h.context('nosecret', 'clock'), secret=None)
    result['secret_not_supplied'] = {'build_ok': nosecret['ok'], 'message': [l.split(' ', 2)[-1] for l in read_log(nosecret['log']).splitlines() if 'install failed' in l][:1]}
    # a secret with extra content
    junk = h.build('junk-secret', 'clock', h.context('junk', 'clock'), secret=url + '\nsecond line\n')
    result['secret_with_two_lines'] = {'build_ok': junk['ok'], 'message': [l.split(' ', 2)[-1] for l in read_log(junk['log']).splitlines() if 'install failed' in l][:1]}
    # wrong wheel digest
    ctx = h.context('badsha', 'clock')
    args_tag = f'e19-{h.token}/graph-loader-clock:{h.version}-badsha'
    wheel = next(ctx.glob('*.whl'))
    wheel.write_bytes(wheel.read_bytes() + b'\0')                 # same name, different bytes: the build arg digest (of the original) no longer matches
    command = ['buildx', 'build', '--load', '--progress=plain', '--no-cache', '--provenance=false', '--sbom=false', '-t', args_tag, '--build-arg', f'BASE_IMAGE={BASE}',
               '--build-arg', f'PACKAGE_WHEEL={wheel.name}', '--build-arg', f'PACKAGE_WHEEL_SHA256={srv_sha(h.wheel)}', '--build-arg', f'GRAPH_LOADER_VERSION={h.version}',
               '--secret', f'id=pip_index,src={h.work / "secrets" / "nosecret.secret"}', str(ctx)]
    (h.work / 'secrets' / 'nosecret.secret').write_text(url + '\n')
    done = h.docker(*command, check=False, timeout=600)
    (h.work / 'logs' / 'build-badsha.log').write_text(done.stdout + done.stderr)
    result['substituted_wheel'] = {'build_ok': done.returncode == 0, 'message': [l.split(' ', 2)[-1] for l in (done.stdout + done.stderr).splitlines() if 'install failed' in l][:1]}
    # tampered lock (one hash changed)
    tampered = h.work / 'ctx' / 'tampered.lock'
    text = h.lock.read_text()
    first = next(l for l in text.splitlines() if l.strip().startswith('--hash=sha256:'))
    tampered.write_text(text.replace(first.strip().split(':')[1][:8], '00000000', 1))
    bad_lock = h.build('badlock', 'clock', h.context('badlock', 'clock', lock=tampered), secret=url + '\n')
    result['tampered_lock'] = {'build_ok': bad_lock['ok'], 'message': [l.split(' ', 2)[-1] for l in read_log(bad_lock['log']).splitlines() if 'THESE PACKAGES DO NOT MATCH' in l or 'Hashes are required' in l or 'DO NOT MATCH' in l][:1]}
    # the index serves a different wheel than the lock names
    other = h.work / 'deps-tampered'
    if other.exists():
        shutil.rmtree(other)
    shutil.copytree(h.deps, other)
    victim = next(other.glob('idna*.whl'))
    victim.write_bytes(victim.read_bytes() + b'\0')
    index2 = h.index('index-fc-tampered', directory=other)
    url2 = f'https://{SENTINEL_USER}:{SENTINEL_PASSWORD}@{HOST}:{index2.port}/simple/'
    wrong = h.build('wrongindex', 'clock', h.context('wrongindex', 'clock'), secret=url2 + '\n')
    result['index_serves_other_bytes'] = {'build_ok': wrong['ok'], 'message': [l.split(' ', 2)[-1] for l in read_log(wrong['log']).splitlines() if 'DO NOT MATCH' in l][:1]}
    # offline wheelhouse install: no network at all in the build
    house = h.build('wheelhouse', 'clock', h.context('wheelhouse', 'clock', wheelhouse=h.deps), source='wheelhouse', network='none')
    result['wheelhouse_offline'] = {'build_ok': house['ok'], 'network': 'none', 'config_check_line': [l.split(' ', 2)[-1] for l in read_log(house['log']).splitlines() if 'pip configuration check' in l][:1]}
    if not house['ok']:
        result['wheelhouse_offline']['log_tail'] = read_log(house['log'])[-1500:]
    h.stop_servers()
    return h.save('failclosed', result)


EXPERIMENTS = {'default': e_default, 'old': e_old, 'credentials': e_credentials, 'planted': e_planted, 'tini': e_tini, 'failclosed': e_failclosed}


def main(argv):
    work, token, names = Path(argv[1]), argv[2], argv[3:]
    harness = Harness(work, token)
    try:
        for name in names:
            started = time.time()
            outcome = EXPERIMENTS[name](harness)
            print(f'== {name} ({time.time() - started:.0f}s)')
            print(json.dumps(outcome, indent=2, default=str)[:6000])
    finally:
        harness.stop_servers()
        (work / 'images.txt').write_text('\n'.join(sorted(set(harness.images + ((work / 'images.txt').read_text().split() if (work / 'images.txt').exists() else [])))) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
