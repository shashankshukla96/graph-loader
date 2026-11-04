#!/usr/bin/env python3
"""E19.F4.S11 proof cells (private): builds, registry-auth controls, loads and routes 1 and 2 inside the D19-8 isolation.

    python3 e19_cells.py WORKDIR TOKEN cells

Runs on the host after ``e19_proof_harness.py ... up`` (it imports that module and uses its recorded-id helpers). The runner container plays the
part of the operator's host: it holds the Docker CLI (against the inner daemon), the package, the scoped registry login and the secrets; Neo4j and
Kafka run in the inner daemon with their client ports published on the inner daemon's own network namespace (``dind:7687``, ``dind:9092``), which is
what a Docker host with published ports looks like to its CLI. Nothing is published on the real host.

Phases (each a recorded step, red or green):
  prep         login, base pull, load services with published inner ports, the cell B base image (tini from the owned Debian mirror)
  auth         build with no scoped login (stops at the private FROM), push without login (refused)
  cell A       ``images build --pip-index-secret --pip-ca-bundle --debian-mirror --push`` from the authenticated owned index
  cell B       ``images build --wheelhouse`` with a tini base image and no index, no mirror (the owned index and mirror logs show no new request)
  loads        per cell: a bulk load and a short stream load with GRAPH_LOADER_IMAGE_REGISTRY set, role images pulled from the owned registry
  secrets      the registry, index and Neo4j passwords and the index URL are absent from every output, manifest, image config, history and layer
  route 2      the cell A images copied by digest (and one by ``docker save``/``load``) to ``registry:5000/mirror`` and a load from there
  route 1      the process runtime installed into a fresh venv from the owned index and a load with ``--runtime process``
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e19_proof_harness as P  # noqa: E402

VERSION = '0.1.0a1'
ROLES = ('cli', 'node', 'edge', 'clock')
CITIES = 5
BUILD_ARGS_COMMON = ['--package-wheel', f'/wheelhouse/neo4j_graph_loader-{VERSION}-py3-none-any.whl', '--docker-config', '/work/login']
CA = '/usr/local/share/ca-certificates/e19-ca.crt'

SCHEMA = """loading:
  mode: bulk
  consumer_group_id: {cell}
nodes:
  - label: Person
    topic: {cell}-person
    key_property: person_id
    properties:
      person_id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
  - label: City
    topic: {cell}-city
    key_property: name
    properties:
      name: {{type: string, required: true, constraint: {{type: uniqueness}}}}
edges:
  - type: LIVES_IN
    topic: {cell}-lives-in
    nodes: {{source: Person, target: City}}
    source_key_property: person_id
    target_key_property: name
    properties:
      since: {{type: date}}
"""

PRODUCER = r'''
import json, sys
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
bootstrap, cell, kind, first, count = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
cities = [f"{cell}-city-{k}" for k in range(int(sys.argv[6]))]
topics = {"person": f"{cell}-person", "city": f"{cell}-city", "edge": f"{cell}-lives-in"}
admin = AdminClient({"bootstrap.servers": bootstrap})
for future in admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics.values()]).values():
    try:
        future.result(30)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
producer = Producer({"bootstrap.servers": bootstrap})
def send(topic, key, value):
    producer.produce(topic, key=str(key).encode(), value=json.dumps(value, separators=(",", ":")).encode())
if kind == "city":
    for name in cities:
        send(topics["city"], name, {"name": name})
elif kind == "person":
    for i in range(first, first + count):
        send(topics["person"], i, {"person_id": i, "name": f"P{i}"})
else:
    for i in range(first, first + count):
        send(topics["edge"], i, {"source": {"person_id": i}, "target": {"name": cities[i % len(cities)]}, "properties": {"since": "2020-05-01"}})
producer.flush(60)
print("produced", kind, count if kind != "city" else len(cities), flush=True)
'''

SCANNER = r'''
"""Reads {"needles": [...], "images": [...], "files": [...]} on stdin; scans `docker save` output (tar, nested gzip layers) and files for the needles.
Prints only counts: never a needle, never a match."""
import gzip, json, subprocess, sys, tarfile
request = json.load(sys.stdin)
needles = [n.encode() for n in request["needles"]]
keep = max(len(n) for n in needles) - 1
hits = [0] * len(needles)
stats = {"members": 0, "bytes": 0, "gzip_members": 0, "zstd_members": 0, "unscanned": 0}
def scan(stream):
    tail = b""
    while True:
        chunk = stream.read(1 << 20)
        if not chunk:
            break
        stats["bytes"] += len(chunk)
        data = tail + chunk
        for i, n in enumerate(needles):
            if n in data:
                hits[i] += 1
        tail = data[-keep:] if keep else b""
def scan_member(stream):
    head = stream.peek(4)[:4]
    if head[:2] == b"\x1f\x8b":
        stats["gzip_members"] += 1
        scan(gzip.GzipFile(fileobj=stream))
    elif head == b"\x28\xb5\x2f\xfd":
        stats["zstd_members"] += 1
        stats["unscanned"] += 1
    else:
        scan(stream)
for path in request.get("files", []):
    with open(path, "rb") as handle:
        scan(handle)
if request.get("images"):
    proc = subprocess.Popen(["docker", "save", *request["images"]], stdout=subprocess.PIPE)
    with tarfile.open(fileobj=proc.stdout, mode="r|") as archive:
        for member in archive:
            if member.isfile():
                stats["members"] += 1
                scan_member(archive.extractfile(member))
    if proc.wait():
        raise SystemExit("docker save failed")
print(json.dumps({"hits": hits, **stats}))
'''


def schema_text(cell: str) -> str:
    return SCHEMA.format(cell=cell)


def parse_count(text: str) -> int:
    numbers = re.findall(r'^\s*(\d+)\s*$', text, re.M)
    if not numbers:
        raise P.HarnessError('no count in cypher-shell output: ' + text[-200:])
    return int(numbers[-1])


class Stats(threading.Thread):
    """Samples the memory use of the dind container while a load runs; the peak goes into the evidence."""

    def __init__(self, h):
        super().__init__(daemon=True)
        self.h, self.stop_event, self.peak_mib, self.samples = h, threading.Event(), 0.0, 0

    def run(self):
        while not self.stop_event.wait(8):
            done = self.h.d('stats', '--no-stream', '--format', '{{.MemUsage}}', self.h.state['containers'][f'{self.h.prefix}-dind'], check=False)
            match = re.match(r'([0-9.]+)(MiB|GiB)', done.stdout.strip())
            if match:
                value = float(match.group(1)) * (1024 if match.group(2) == 'GiB' else 1)
                self.peak_mib, self.samples = max(self.peak_mib, value), self.samples + 1


class Cells:
    def __init__(self, h: P.Harness):
        self.h = h
        self.work = h.work
        self.results: dict = {}
        self.out: dict = {}
        self.users = P.stage_env(h)
        self.secrets = {name: (self.work / 'secrets' / f'{name}.password').read_text() for name in ('registry', 'pypi', 'neo4j')}
        self.transcripts: list[str] = []          # every captured output is checked for the secrets at the end
        self.logdir = self.work / 'logs' / 'cells'
        self.logdir.mkdir(parents=True, exist_ok=True)
        self.sampler = Stats(h)

    # -- helpers -----------------------------------------------------------------------------------------------------------------------------
    def rx(self, *cmd, **kw):
        return P.rx(self.h, *cmd, **kw)

    def keep(self, name: str, text: str) -> str:
        (self.logdir / f'{name}.log').write_text(text)
        self.transcripts.append(text)
        return text

    def count(self, query: str) -> int:
        """A Cypher count through ``cypher-shell`` inside the Neo4j container; the password is read from the container's own environment."""
        done = self.rx('docker', 'exec', '-e', f'Q={query}', 'neo4j', 'sh', '-c', 'cypher-shell -u neo4j -p "${NEO4J_AUTH#neo4j/}" --format plain "$Q"', check=False)
        return parse_count(done.stdout + done.stderr)

    def graph_counts(self, cell: str, low: int, high: int) -> dict:
        return {'persons': self.count(f'MATCH (p:Person) WHERE p.person_id >= {low} AND p.person_id < {high} RETURN count(p)'),
                'cities': self.count(f"MATCH (c:City) WHERE c.name STARTS WITH '{cell}-city-' RETURN count(c)"),
                'lives_in': self.count(f'MATCH (p:Person)-[r:LIVES_IN]->(:City) WHERE p.person_id >= {low} AND p.person_id < {high} RETURN count(r)')}

    def wait_counts(self, cell: str, low: int, high: int, expect: dict, timeout: float = 240) -> dict:
        last: dict = {}

        def probe():
            nonlocal last
            last = self.graph_counts(cell, low, high)
            return last == expect
        self.h.wait_for(f'{cell} graph {expect}', probe, timeout=timeout, interval=6)
        return last

    def wait_nodes(self, cell: str, low: int, high: int, persons: int, timeout: float = 300):
        """A stream relationship whose endpoint does not exist yet fails its loader, so edges are produced only after their nodes are in the graph."""
        self.h.wait_for(f'{cell}: {persons} persons', lambda: self.graph_counts(cell, low, high)['persons'] == persons, timeout=timeout, interval=5)

    def produce(self, cell: str, kind: str, first: int = 0, count: int = 0):
        done = self.h.exec('runner', 'python', '/work/cells/producer.py', 'dind:9092', cell, kind, str(first), str(count), str(CITIES), check=False, timeout=180)
        if done.returncode:
            raise P.HarnessError('producer failed: ' + (done.stderr or done.stdout)[-300:])

    def load_env(self, prefix: str) -> dict:
        return {'DOCKER_CONFIG': '/work/login', 'GRAPH_LOADER_IMAGE_REGISTRY': prefix, 'NEO4J_URI': 'bolt://localhost:7687', 'NEO4J_USERNAME': 'neo4j',
                'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092'}

    def start_cmd(self, cell: str, mode: str, binary: str = 'graph-loader', runtime: str = 'docker') -> list[str]:
        cmd = [binary, 'start', '--mode', mode, '--config', f'/work/cells/{cell}.yaml', '--run-dir', f'/work/runs/{cell}-{mode}', '--runtime', runtime]
        if runtime == 'docker':
            cmd += ['--network', 'e19-load']
        if mode == 'bulk':
            cmd += ['--plan-workers', '1', '--plan-writers', '1']          # small loads on a 4 GB inner daemon / a 640 MB runner
        return cmd

    def inner_role_containers(self) -> list[str]:
        out = self.rx('docker', 'ps', '--format', '{{.Names}}').stdout.split()
        return sorted(n for n in out if n not in ('neo4j', 'kafka'))

    # -- steps ---------------------------------------------------------------------------------------------------------------------------------
    def prep(self):
        h = self.h
        probe_dir = self.work / 'cells'
        probe_dir.mkdir(exist_ok=True)
        P.write_secret(self.work / 'secrets' / 'route1.env', f'NEO4J_PASSWORD={self.secrets["neo4j"]}\n')
        (probe_dir / 'producer.py').write_text(PRODUCER)
        (probe_dir / 'scanner.py').write_text(SCANNER)
        for cell in ('cell-a', 'cell-b', 'route2', 'route1'):
            (probe_dir / f'{cell}.yaml').write_text(schema_text(cell))
        (probe_dir / 'tini-base').mkdir(exist_ok=True)
        (probe_dir / 'tini-base' / 'Dockerfile').write_text(
            'FROM registry:5000/e19/python:3.12-slim\n'
            'RUN set -eu; . /etc/os-release; rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*; \\\n'
            '    printf \'Types: deb\\nURIs: http://debian/debian\\nSuites: %s %s-updates\\nComponents: main\\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\\n\' '
            '"${VERSION_CODENAME}" "${VERSION_CODENAME}" > /etc/apt/sources.list.d/debian.sources; \\\n'
            '    apt-get update; apt-get install -y --no-install-recommends tini; rm -rf /var/lib/apt/lists/*; tini --version\n')
        h.d('cp', f'{probe_dir}/.', f"{h.state['containers'][h.prefix + '-runner']}:/work/cells")
        P.stage_login(h, self.users)
        self.rx('sh', '-c', 'umask 077; python - <<EOF\nimport json\nu=json.load(open("/secrets/users.json"))["pypi"]\n'
                            'p=open("/secrets/pypi.password").read()\nopen("/work/pip-index.txt","w").write(f"https://{u}:{p}@pypi:8443/simple/")\nEOF')
        P.stage_services(h, publish=True)
        build = self.rx('docker', 'build', '--progress=plain', '-t', 'registry:5000/e19/python-tini:3.12-slim', '/work/cells/tini-base', timeout=900)
        self.keep('tini-base-build', build.stdout + build.stderr)
        pushed = self.rx('docker', 'push', 'registry:5000/e19/python-tini:3.12-slim', timeout=600)
        self.keep('tini-base-push', pushed.stdout + pushed.stderr)
        tini = self.rx('docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'tini', 'registry:5000/e19/python-tini:3.12-slim', '--version').stdout.strip()
        return {'cell_b_base': 'registry:5000/e19/python-tini:3.12-slim', 'tini': tini}

    def build_args(self, prefix: str, base: str, source: list[str], out: str, push: bool = True, config: str = '/work/login') -> list[str]:
        args = ['graph-loader', 'images', 'build', '--registry', prefix, '--base-image', base, *source, '--docker-config', config, '--output', out]
        return args + ['--push'] if push else args

    def logs_requests(self) -> dict:
        counts = {}
        for role in ('pypi-proxy', 'debian', 'pypiserver'):
            text = self.h.d('logs', self.h.state['containers'][f'{self.h.prefix}-{role}'], check=False)
            counts[role] = sum(1 for line in (text.stdout + text.stderr).splitlines() if ' GET ' in line or 'GET /' in line)
        return counts

    def auth(self):
        """Registry-auth controls. A build with no scoped login stops at the private base image (its first contact with the registry, the digest lookup),
        and the underlying request is classified as an authentication failure; a push without a login is refused; a push with a login succeeds."""
        self.rx('mkdir', '-p', '/work/nologin-cfg')
        nologin = self.rx(*self.build_args('registry:5000/auth-control', 'registry:5000/e19/python:3.12-slim',
                                           [*BUILD_ARGS_COMMON[:2], '--pip-index-secret', '/work/pip-index.txt', '--pip-ca-bundle', CA, '--debian-mirror', 'http://debian/debian'],
                                           '/work/out-auth-control', push=False, config='/work/nologin-cfg'), check=False, timeout=600)
        text = nologin.stdout + nologin.stderr
        self.keep('auth-build-without-login', text)
        assert nologin.returncode != 0 and 'registry login' in text, f'exit {nologin.returncode}: ' + text[-500:]
        lookup = self.rx('docker', 'buildx', 'imagetools', 'inspect', 'registry:5000/e19/python:3.12-slim', env={'DOCKER_CONFIG': '/work/nologin-cfg'}, check=False, timeout=120)
        lookup_text = lookup.stdout + lookup.stderr
        self.keep('auth-lookup-without-login', lookup_text)
        lookup_class = P.classify_text(lookup_text)
        assert lookup.returncode != 0 and lookup_class == 'auth', lookup_text[-400:]
        leftover = [r for r in self.rx('docker', 'image', 'ls', '--format', '{{.Repository}}').stdout.split() if r.startswith('registry:5000/auth-control/')]
        assert not leftover, f'the build without a login produced images: {leftover}'
        # a push without login is refused; the image to push is the base image tagged under the control name (inner store only)
        self.rx('docker', 'tag', 'registry:5000/e19/python:3.12-slim', 'registry:5000/auth-control/push-check:1')
        refused = self.rx('docker', 'push', 'registry:5000/auth-control/push-check:1', env={'DOCKER_CONFIG': '/work/nologin-cfg'}, check=False, timeout=300)
        refused_text = refused.stdout + refused.stderr
        self.keep('auth-push-without-login', refused_text)
        assert refused.returncode != 0 and P.classify_text(refused_text) == 'auth', refused_text[-400:]
        accepted = self.rx('docker', 'push', 'registry:5000/auth-control/push-check:1', timeout=300)
        self.keep('auth-push-with-login', accepted.stdout + accepted.stderr)
        self.rx('docker', 'rmi', 'registry:5000/auth-control/push-check:1')
        return {'build_without_login': {'exit': nologin.returncode, 'message': text.strip().splitlines()[-1][:240],
                                        'underlying_lookup': {'exit': lookup.returncode, 'class': lookup_class, 'message': lookup_text.strip().splitlines()[-1][:200]}},
                'push_without_login': {'exit': refused.returncode, 'class': P.classify_text(refused_text), 'message': refused_text.strip().splitlines()[-1][:200]},
                'push_with_login': {'exit': 0, 'digest': re.search(r'digest: (sha256:[0-9a-f]{64})', accepted.stdout + accepted.stderr).group(1)}}

    def build_cell(self, cell: str, prefix: str, base: str, source: list[str]):
        before = self.logs_requests()
        started = time.time()
        done = self.rx(*self.build_args(prefix, base, source, f'/work/out-{cell}'), timeout=2400)
        self.keep(f'{cell}-build', done.stdout + done.stderr)
        after = self.logs_requests()
        manifest = json.loads(self.rx('cat', f'/work/out-{cell}/images.json').stdout)
        self.out[f'{cell}_manifest'] = manifest
        rows = {r['component']: r for r in manifest['images']}
        assert set(rows) == set(ROLES) and all(r['registry_digest'] for r in rows.values()), 'every image has a registry digest'
        for role, row in rows.items():
            labels = row['labels']
            assert labels['io.graph-loader.component'] == role and labels['org.opencontainers.image.version'] == VERSION
        (self.logdir / f'{cell}-images.json').write_text(json.dumps(manifest, indent=1))
        return {'seconds': round(time.time() - started), 'prefix': prefix, 'base_image': manifest.get('base_image'), 'dependency_source': manifest.get('dependency_source'),
                'images': {r: {'id': rows[r]['id'][:19], 'registry_digest': rows[r]['registry_digest'], 'tag': rows[r]['local_tag']} for r in ROLES},
                'debian_packages': manifest.get('debian_packages'), 'requests_before': before, 'requests_after': after}

    def cell_a(self):
        result = self.build_cell('cell-a', 'registry:5000/cell-a', 'registry:5000/e19/python:3.12-slim',
                                 [*BUILD_ARGS_COMMON[:2], '--pip-index-secret', '/work/pip-index.txt', '--pip-ca-bundle', CA, '--debian-mirror', 'http://debian/debian'])
        grew = {k: result['requests_after'][k] - result['requests_before'][k] for k in result['requests_after']}
        assert grew['pypi-proxy'] > 0 and grew['debian'] > 0, f'cell A used the owned index and mirror: {grew}'
        result['new_requests'] = grew
        return result

    def cell_b(self):
        result = self.build_cell('cell-b', 'registry:5000/cell-b', 'registry:5000/e19/python-tini:3.12-slim', ['--wheelhouse', '/wheelhouse'])
        grew = {k: result['requests_after'][k] - result['requests_before'][k] for k in result['requests_after']}
        assert grew == {'pypi-proxy': 0, 'debian': 0, 'pypiserver': 0}, f'cell B must not touch the owned index or mirror: {grew}'
        result['new_requests'] = grew
        tini = self.rx('docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'tini', 'registry:5000/cell-b/graph-loader-cli:' + VERSION, '--version').stdout.strip()
        result['cli_image_tini'] = tini
        return result

    def drop_local(self, prefix: str):
        for role in ROLES:
            self.rx('docker', 'rmi', f'{prefix}/graph-loader-{role}:{VERSION}', check=False)
        assert not [r for r in self.rx('docker', 'image', 'ls', '--format', '{{.Repository}}').stdout.split() if r.startswith(prefix + '/')], 'local copies remain'

    def run_bulk(self, cell: str, prefix: str, low: int, runtime: str = 'docker', binary: str = 'graph-loader', persons: int = 100, env_file: str = 'runner.env') -> dict:
        self.produce(cell, 'city')
        self.produce(cell, 'person', low, persons)
        self.produce(cell, 'edge', low, persons)
        env = self.load_env(prefix)
        started = time.time()
        done = self.h.exec('runner', *self.start_cmd(cell, 'bulk', binary, runtime), env=env, env_file=self.work / 'secrets' / env_file, check=False, timeout=900)
        text = done.stdout + done.stderr
        self.keep(f'{cell}-bulk', text)
        assert done.returncode == 0, f'{cell} bulk exited {done.returncode}: ' + text[-700:]
        counts = self.graph_counts(cell, low, low + persons)
        assert counts == {'persons': persons, 'cities': CITIES, 'lives_in': persons}, counts
        images = sorted(set(re.findall(r'stage=image component=(\w+) version=\S+ id=(sha256:[0-9a-f]{12})', text)))
        return {'seconds': round(time.time() - started), 'exit': done.returncode, 'counts': counts, 'role_images_resolved': images, 'leftover_role_containers': self.inner_role_containers()}

    def run_stream(self, cell: str, prefix: str, low: int, first: int = 20, second: int = 10) -> dict:
        self.produce(cell, 'city')
        self.produce(cell, 'person', low, first)
        env = {**self.load_env(prefix)}
        logfile = self.logdir / f'{cell}-stream.log'
        cmd = ['docker', 'exec', '--env-file', str(self.work / 'secrets' / 'runner.env'), *sum((['-e', f'{k}={v}'] for k, v in env.items()), []),
               self.h.state['containers'][f'{self.h.prefix}-runner'], *self.start_cmd(cell, 'stream')]
        started = time.time()
        with open(logfile, 'w') as sink:
            proc = subprocess.Popen(cmd, stdout=sink, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                self.wait_nodes(cell, low, low + first + second, first)
                self.produce(cell, 'edge', low, first)
                seen_first = self.wait_counts(cell, low, low + first + second, {'persons': first, 'cities': CITIES, 'lives_in': first}, timeout=300)
                self.produce(cell, 'person', low + first, second)
                self.wait_nodes(cell, low, low + first + second, first + second)
                self.produce(cell, 'edge', low + first, second)
                seen_second = self.wait_counts(cell, low, low + first + second, {'persons': first + second, 'cities': CITIES, 'lives_in': first + second}, timeout=300)
            finally:
                self.h.exec('runner', 'python', '-c', STOP_SNIPPET, check=False)
                try:
                    code = proc.wait(timeout=240)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    code = proc.wait(timeout=60)
        self.transcripts.append(logfile.read_text())
        time.sleep(2)
        leftover = self.inner_role_containers()
        text = logfile.read_text()
        images = sorted(set(re.findall(r'stage=image component=(\w+) version=\S+ id=(sha256:[0-9a-f]{12})', text)))
        assert not leftover, f'role containers left behind: {leftover}'
        return {'seconds': round(time.time() - started), 'exit_after_sigterm': code, 'first_batch': seen_first, 'second_batch': seen_second,
                'role_images_resolved': images, 'leftover_role_containers': leftover}

    def secret_scan(self):
        needles = [self.secrets['registry'], self.secrets['pypi'], self.secrets['neo4j'], self.rx('cat', '/work/pip-index.txt').stdout.strip()]
        texts = {'harness transcripts': sum(t.count(n) for t in self.transcripts for n in needles)}
        images = [f'registry:5000/{cell}/graph-loader-{role}:{VERSION}' for cell in ('cell-a', 'cell-b') for role in ROLES]
        for image in images:
            self.rx('docker', 'pull', '-q', image, check=False)
        listing = []
        for image in images:
            history = self.rx('docker', 'history', '--no-trunc', '--format', '{{.CreatedBy}}', image).stdout
            inspect = self.rx('docker', 'image', 'inspect', image).stdout
            listing.append(sum(history.count(n) + inspect.count(n) for n in needles))
        texts['docker history and inspect'] = sum(listing)
        request = json.dumps({'needles': needles, 'images': images, 'files': ['/work/out-cell-a/images.json', '/work/out-cell-b/images.json', '/work/out-cell-a/image-overrides.sh',
                                                                              '/work/out-cell-b/image-overrides.sh']})
        done = self.h.exec('runner', 'python', '/work/cells/scanner.py', input=request, timeout=1800)
        scan = json.loads(done.stdout.strip().splitlines()[-1])
        for prefix in ('registry:5000/cell-a', 'registry:5000/cell-b'):
            self.drop_local(prefix)
        assert sum(scan['hits']) == 0 and scan['unscanned'] == 0 and sum(texts.values()) == 0, {'scan': scan, 'texts': texts}
        return {'images': len(images), 'scan': scan, 'texts': texts, 'needles': len(needles)}

    def route2(self, source_prefix: str):
        rows = {r['component']: r for r in self.out['cell-a_manifest']['images']}
        mirror = 'registry:5000/mirror'
        copied = {}
        for role in ROLES:
            digest = rows[role]['registry_digest']
            ref = f'{source_prefix}/graph-loader-{role}@{digest}'
            self.rx('docker', 'pull', '-q', ref, timeout=600)
            source = self.rx('docker', 'image', 'inspect', '--format', '{{.Id}}', ref).stdout.strip()
            if role == 'cli':
                self.rx('docker', 'save', '-o', '/work/route2-cli.tar', source)
                for old in self.rx('docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}}', '--filter', f'reference={source_prefix}/graph-loader-cli').stdout.split():
                    self.rx('docker', 'rmi', old, check=False)
                self.rx('docker', 'rmi', ref, check=False)
                self.rx('docker', 'load', '-i', '/work/route2-cli.tar')
                method = 'docker save / docker load'
            else:
                method = 'docker pull by digest'
            tag = f'{mirror}/graph-loader-{role}:{VERSION}'
            self.rx('docker', 'tag', source, tag)
            pushed = self.rx('docker', 'push', tag, timeout=600)
            new = re.search(r'digest: (sha256:[0-9a-f]{64})', pushed.stdout + pushed.stderr).group(1)
            copied[role] = {'method': method, 'source_digest': digest, 'destination_digest': new, 'digest_kept': new == digest}
            self.rx('docker', 'rmi', tag, check=False)
            self.rx('docker', 'rmi', ref, check=False)
        self.drop_local(mirror)
        for role in ROLES:
            self.rx('docker', 'rmi', f'{source_prefix}/graph-loader-{role}:{VERSION}', check=False)
        bulk = self.run_bulk('route2', mirror, 3000, persons=50)
        self.drop_local(mirror)
        return {'copied': copied, 'bulk': bulk}

    def route1(self):
        install = self.rx('sh', '-c', 'rm -rf /work/route1 && python -m venv /work/route1/venv && export PIP_INDEX_URL="$(cat /work/pip-index.txt)" '
                          f'PIP_CERT={CA} && /work/route1/venv/bin/python -m pip install --no-cache-dir "neo4j-graph-loader=={VERSION}" 2>&1 | tail -4', timeout=900)
        self.keep('route1-install', install.stdout + install.stderr)
        shown = self.rx('/work/route1/venv/bin/graph-loader', '--version').stdout.strip()
        packages = self.rx('/work/route1/venv/bin/python', '-m', 'pip', 'list', '--format', 'freeze').stdout.split()
        used = self.logs_requests()
        bulk = self.run_bulk('route1', 'registry:5000/unused', 4000, runtime='process', binary='/work/route1/venv/bin/graph-loader', persons=50, env_file='route1.env')
        return {'version': shown, 'installed_packages': len(packages), 'index_requests_total': used, 'bulk': bulk}

    # -- the run ---------------------------------------------------------------------------------------------------------------------------------
    def step(self, name: str, fn):
        ok = P.step(self.h, self.results, name, fn)
        if ok:
            self.out[name] = self.results[name]['detail']
        return ok

    def run(self):
        h = self.h
        h.log('phase', name='cells', public_access=False)
        h.watch()
        self.sampler.start()
        try:
            self.results['docker_df_before'] = {'ok': True, 'detail': h.d('system', 'df', '--format', '{{.Type}} total={{.TotalCount}} size={{.Size}} reclaimable={{.Reclaimable}}').stdout.splitlines()}
            if not self.step('prep: login, services with published inner ports, cell B base', self.prep):
                return
            self.step('registry-auth controls', self.auth)
            self.step('cell A: authenticated index, CA, owned Debian mirror, private base', self.cell_a)
            self.step('cell B: offline wheelhouse, tini base', self.cell_b)
            for cell, prefix, low in (('cell-a', 'registry:5000/cell-a', 1000), ('cell-b', 'registry:5000/cell-b', 2000)):
                self.drop_local(prefix)
                self.step(f'{cell}: bulk load from images pulled from the owned registry', lambda c=cell, p=prefix, lo=low: self.run_bulk(c, p, lo))
                self.step(f'{cell}: short stream load', lambda c=cell, p=prefix, lo=low: self.run_stream(c, p, lo + 100))
                self.drop_local(prefix)             # the registry keeps them; the inner store does not need them any more (disk budget)
            self.step('route 2: copy by digest, load from the copy', lambda: self.route2('registry:5000/cell-a'))
            self.step('route 1: process runtime from the owned index', self.route1)
            self.step('secrets absent from outputs, manifests, image configs, history and layers', self.secret_scan)
            self.results['docker_df_after'] = {'ok': True, 'detail': h.d('system', 'df', '--format', '{{.Type}} total={{.TotalCount}} size={{.Size}} reclaimable={{.Reclaimable}}').stdout.splitlines()}
            stats = self.rx('docker', 'stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}').stdout.splitlines()
            self.results['footprint'] = {'ok': True, 'detail': {'budget': h.budget(), 'dind_peak_mib_sampled': round(self.sampler.peak_mib), 'samples': self.sampler.samples,
                                                                  'inner_at_end': stats, 'host_memory_available_mb': P.mem_available_mb()}}
        finally:
            self.sampler.stop_event.set()
            h.unwatch()
            (self.work / 'logs' / 'cells.json').write_text(json.dumps(self.results, indent=1))
        if not all(r['ok'] for r in self.results.values()):
            raise P.HarnessError('cells failed: ' + ', '.join(k for k, v in self.results.items() if not v['ok']))


STOP_SNIPPET = r'''
import os, signal
me = os.getpid()
for pid in filter(str.isdigit, os.listdir("/proc")):
    if int(pid) == me:
        continue
    try:
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
    except OSError:
        continue
    if b"start" in cmd and b"stream" in cmd and any(part.endswith(b"graph-loader") for part in cmd):
        os.kill(int(pid), signal.SIGTERM)
        print("sent SIGTERM", pid)
'''


def main(argv=None) -> int:
    work, token, command = Path(argv[0] if argv else sys.argv[1]), (argv or sys.argv[1:])[1], (argv or sys.argv[1:])[2]
    h = P.Harness(work, token)
    if command != 'cells':
        raise SystemExit('usage: e19_cells.py WORKDIR TOKEN cells')
    try:
        Cells(h).run()
        return 0
    except P.GuardTripped as exc:
        h.log('guard-tripped', reason=str(exc))
        h.down()
        return 3
    except (P.HarnessError, KeyError, subprocess.SubprocessError, OSError, AssertionError) as exc:
        h.log('error', command='cells', message=str(exc)[:800])
        return 2


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
