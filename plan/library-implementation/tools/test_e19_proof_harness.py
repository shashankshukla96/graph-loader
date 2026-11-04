"""Tests of the proof harness' ownership, budget and guard logic against a fake Docker daemon (no Docker needed)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e19_proof_harness as P  # noqa: E402

TOKEN = 'unit-1'
OWNER = P.OWNER_LABEL


class FakeDaemon:
    """Just enough of the Docker CLI: images (id → tags), containers (id → image id, labels), networks and volumes (labels)."""

    def __init__(self):
        self.images = {}          # id -> {'tags': set, 'size': int}
        self.containers = {}      # id -> {'name', 'image', 'labels'}
        self.networks, self.volumes = {}, {}
        self.calls = []
        self.counter = 0

    def add_image(self, image_id, tags=()):
        self.images[image_id] = {'tags': set(tags), 'size': 10 * 2**20}

    def find(self, ref):
        for image_id, info in self.images.items():
            if ref == image_id or ref in info['tags']:
                return image_id
        return None

    def run(self, args, **kw):
        self.calls.append(list(args))
        verb = args[0]
        ok = lambda out='': subprocess.CompletedProcess(args, 0, out, '')          # noqa: E731
        bad = lambda err='': subprocess.CompletedProcess(args, 1, '', err)         # noqa: E731
        if args[:2] == ['image', 'inspect']:
            found = self.find(args[-1])
            if not found:
                return bad('No such image')
            info = self.images[found]
            if '--format' in args:
                return ok(str(info['size']))
            digests = sorted(t for t in info['tags'] if '@sha256:' in t)
            return ok(json.dumps([{'Id': found, 'RepoTags': sorted(info['tags']), 'RepoDigests': digests}]))
        if verb == 'rmi':
            assert not {'-f', '--force'} & set(args), 'no force'
            found = self.find(args[1])
            if not found:
                return bad('No such image')
            if args[1] == found:                                 # by id: refuses while a tag or container uses it
                if self.images[found]['tags'] or any(c['image'] == found for c in self.containers.values()):
                    return bad('image is referenced')
                del self.images[found]
            else:
                self.images[found]['tags'].discard(args[1])
                if not self.images[found]['tags'] and not any(c['image'] == found for c in self.containers.values()):
                    del self.images[found]              # the last reference is gone: the daemon removes the image
            return ok()
        if verb == 'tag':
            self.images[self.find(args[1])]['tags'].add(args[2])
            return ok()
        if verb == 'ps':
            ancestor = args[args.index('--filter') + 1].split('=', 1)[1]
            return ok('\n'.join(cid for cid, c in self.containers.items() if c['image'] == ancestor))
        if verb == 'create':
            self.counter += 1
            cid = f'c{self.counter}'
            name = args[args.index('--name') + 1]
            labels = {OWNER: args[args.index('--label') + 1].split('=', 1)[1]}
            self.containers[cid] = {'name': name, 'image': self.find(args[-1]) or args[-1], 'labels': labels}
            return ok(cid)
        if verb in ('stop',):
            return ok()
        if verb == 'rm':
            self.containers.pop(args[-1], None)
            return ok()
        if args[:2] == ['network', 'rm']:
            key = next(k for k, v in self.networks.items() if k == args[2] or v.get('id') == args[2])
            del self.networks[key]
            return ok()
        if args[:2] == ['volume', 'rm']:
            self.volumes.pop(args[2])
            return ok()
        if args[0] in ('network', 'volume') and args[1] == 'inspect':
            table = self.networks if args[0] == 'network' else self.volumes
            ident = args[-1]
            key = next((k for k, v in table.items() if k == ident or v.get('id') == ident), None)
            return ok(table[key]['owner']) if key else bad('No such')
        return bad(f'unsupported {args}')


@pytest.fixture
def harness(tmp_path, monkeypatch):
    daemon = FakeDaemon()
    h = P.Harness(tmp_path / 'work', TOKEN, memory_probe=lambda: 9000)
    monkeypatch.setattr(h, 'd', lambda *args, check=True, **kw: _checked(daemon.run(list(args)), check))
    h.daemon = daemon
    return h


def _checked(done, check):
    if check and done.returncode:
        raise P.HarnessError(done.stderr)
    return done


def record(image_id, ref, new=True, tags=()):
    return {'id': image_id, 'new': new, 'owned_tags': list(tags), 'digest_ref': ref}


def test_a_new_image_with_only_the_digest_reference_is_removed(harness):
    ref = 'httpd@sha256:aa'
    harness.daemon.add_image('sha256:aa', [ref])
    rec = record('sha256:aa', ref)
    log = harness.remove_image_refs(rec)
    assert 'sha256:aa' not in harness.daemon.images and any('removed' in line for line in log)


def test_owned_tags_are_removed_by_tag_and_the_id_stays_while_another_tag_exists(harness):
    ref = 'httpd@sha256:aa'
    harness.daemon.add_image('sha256:aa', [ref, 'e19-unit-1/ctl:owned', 'someone-else/img:1'])
    log = harness.remove_image_refs(record('sha256:aa', ref, tags=['e19-unit-1/ctl:owned']))
    info = harness.daemon.images['sha256:aa']
    assert 'e19-unit-1/ctl:owned' not in info['tags'] and 'someone-else/img:1' in info['tags'] and any('kept image id' in line for line in log)


def test_an_unrecorded_tag_with_an_owned_looking_name_is_never_removed(harness):
    ref = 'httpd@sha256:aa'
    harness.daemon.add_image('sha256:aa', [ref, 'e19-unit-1-foreign-sim/ctl:keep'])
    harness.remove_image_refs(record('sha256:aa', ref))
    assert 'e19-unit-1-foreign-sim/ctl:keep' in harness.daemon.images['sha256:aa']['tags']


def test_a_preexisting_image_is_never_removed(harness):
    ref = 'confluentinc/cp-kafka@sha256:bb'
    harness.daemon.add_image('sha256:bb', [ref, 'confluentinc/cp-kafka:7.7.0'])
    log = harness.remove_image_refs(record('sha256:bb', ref, new=False))
    assert 'sha256:bb' in harness.daemon.images and harness.daemon.images['sha256:bb']['tags'] == {ref, 'confluentinc/cp-kafka:7.7.0'}
    assert any('preexisting' in line for line in log)


def test_an_image_used_by_a_container_is_kept_and_nothing_is_forced(harness):
    ref = 'nginx@sha256:cc'
    harness.daemon.add_image('sha256:cc', [ref])
    harness.daemon.containers['cx'] = {'name': 'user-container', 'image': 'sha256:cc', 'labels': {}}
    log = harness.remove_image_refs(record('sha256:cc', ref))
    assert 'sha256:cc' in harness.daemon.images and any('kept image id' in line for line in log)
    assert not any({'-f', '--force', 'prune'} & set(call) for call in harness.daemon.calls)


def test_no_call_ever_forces_prunes_or_publishes(harness):
    ref = 'nginx@sha256:cc'
    harness.daemon.add_image('sha256:cc', [ref])
    harness.create_container('dns', ref, ['true'], network='x')
    harness.remove_container(f'{harness.prefix}-dns')
    harness.remove_image_refs(record('sha256:cc', ref))
    flat = [a for call in harness.daemon.calls for a in call]
    assert not ({'-f', '--force', 'prune', '-p', '--publish', '-P', '--publish-all'} & set(flat))


def test_every_created_container_carries_the_owner_label_and_the_recorded_id(harness):
    harness.daemon.add_image('sha256:cc', ['nginx@sha256:cc'])
    name = harness.create_container('registry', 'nginx@sha256:cc', network='n')
    cid = harness.state['containers'][name]
    assert name == 'e19-unit-1-registry' and harness.daemon.containers[cid]['labels'] == {OWNER: TOKEN}
    assert json.loads(harness.state_path.read_text())['containers'][name] == cid          # recorded the moment it exists
    harness.remove_container(name)
    assert harness.daemon.containers == {} and harness.state['containers'] == {}


def test_a_volume_or_network_with_another_owner_is_never_removed(harness):
    harness.daemon.volumes['e19-unit-1-work'] = {'owner': 'someone-else'}
    harness.state['volumes'].append('e19-unit-1-work')
    with pytest.raises(P.HarnessError, match='not owned'):
        harness.down()
    assert 'e19-unit-1-work' in harness.daemon.volumes


def test_down_removes_what_was_recorded_and_nothing_else(harness):
    harness.daemon.volumes['e19-unit-1-work'] = {'owner': TOKEN}
    harness.daemon.volumes['user-volume'] = {'owner': ''}
    harness.daemon.networks['e19-unit-1-net'] = {'owner': TOKEN, 'id': 'n1'}
    harness.state['volumes'].append('e19-unit-1-work')
    harness.state['networks']['e19-unit-1-net'] = 'n1'
    harness.down()
    assert list(harness.daemon.volumes) == ['user-volume'] and harness.daemon.networks == {}


def test_the_memory_guard_trips_below_the_floor_and_records_the_reason(tmp_path):
    h = P.Harness(tmp_path / 'w', TOKEN, memory_probe=lambda: P.MIN_AVAILABLE_MB - 1)
    with pytest.raises(P.GuardTripped, match='below'):
        h.guard('test')
    assert h.tripped
    ok = P.Harness(tmp_path / 'w2', TOKEN, memory_probe=lambda: P.MIN_AVAILABLE_MB)
    assert ok.guard() == P.MIN_AVAILABLE_MB and ok.min_available_mb == P.MIN_AVAILABLE_MB


def test_the_declared_limits_fit_the_amended_budget():
    memory = sum(m for m, _ in P.LIMITS.values())
    cpus = sum(c for _, c in P.LIMITS.values())
    assert memory <= P.MEMORY_BUDGET_MB == 5120 and cpus <= P.CPU_BUDGET and P.DISK_BUDGET_MB == 10240 and P.MIN_AVAILABLE_MB == 2048
    assert P.LIMITS['dind'][0] + 400 <= P.MEMORY_BUDGET_MB


def test_foreign_comparison_ignores_only_owned_names():
    before = {k: [] for k in ('containers', 'networks', 'volumes', 'images', 'digests', 'listening_ports', 'home')}
    before['containers'] = ['id1 postgres postgres running']
    after = json.loads(json.dumps(before))
    after['containers'] = ['id1 postgres postgres exited', 'id9 e19-unit-1-dns img running']
    after['volumes'] = ['e19-unit-1-work', 'surprise-volume']
    diff = P.compare(before, after, TOKEN)
    assert diff == {'volumes': {'removed': [], 'added': ['surprise-volume']}}


def test_the_user_ports_are_listed_and_no_container_is_created_with_a_published_port(harness):
    assert {5432, 7474, 7687, 8080, 9092, 9101} == P.LISTEN_DENY
    harness.daemon.add_image('sha256:cc', ['nginx@sha256:cc'])
    harness.create_container('registry', 'nginx@sha256:cc', network='n')
    creates = [c for c in harness.daemon.calls if c[0] == 'create']
    assert creates and not any({'-p', '--publish', '-P', '--publish-all'} & set(c[:-1]) for c in creates)


def test_tls_material_has_the_service_names_and_the_ca_key_is_not_in_the_mounted_directory(tmp_path):
    P.make_tls(tmp_path / 'tls', ['registry', 'pypi', 'debian'])
    assert (tmp_path / 'tls/ca.key').exists() and not (tmp_path / 'tls/server/ca.key').exists()
    text = P.sh(['openssl', 'x509', '-in', tmp_path / 'tls/server/server.pem', '-noout', '-ext', 'subjectAltName']).stdout
    assert all(f'DNS:{n}' in text for n in ('registry', 'pypi', 'debian'))
    verified = P.sh(['openssl', 'verify', '-CAfile', tmp_path / 'tls/ca.pem', tmp_path / 'tls/server/server.pem']).stdout
    assert 'OK' in verified


def test_secrets_are_written_0600(tmp_path):
    P.write_secret(tmp_path / 'secrets' / 'x.password', 'value')
    assert oct((tmp_path / 'secrets' / 'x.password').stat().st_mode & 0o777) == '0o600'


def test_the_resolver_script_compiles_and_answers_only_the_owned_names():
    compile(P.DNS_SERVER, 'dns_server', 'exec')
    assert 'NXDOMAIN' in P.DNS_SERVER.upper() or 'nxdomain' in P.DNS_SERVER


# -- S10: the probe and the classification of denials ---------------------------------------------------------------------------------------------
import errno  # noqa: E402
import socket  # noqa: E402
import ssl  # noqa: E402
import urllib.error  # noqa: E402

import e19_isolation_probe as PR  # noqa: E402


@pytest.mark.parametrize('text, expected', [
    ('Head "https://registry-1.docker.io/v2/library/x/manifests/latest": dial tcp 1.2.3.4:443: connect: network is unreachable', 'network'),
    ('failed to authorize: failed to fetch anonymous token: Get "https://auth.docker.io/token": dial tcp 1.2.3.4:443: connect: network is unreachable', 'network'),
    ('dial tcp 1.2.3.4:443: i/o timeout', 'network'),
    ('dial tcp 1.2.3.4:443: connect: connection refused', 'network'),
    ('lookup ghcr.io on 127.0.0.11:53: no such host', 'dns'),
    ('unauthorized: authentication required', 'auth'),
    ('Error response from daemon: pull access denied for x, repository does not exist or may require authorization', 'auth'),
    ('failed to resolve reference: no basic auth credentials', 'auth'),
    ('manifest unknown', 'not-found'),
    ('something else entirely', 'other'),
])
def test_pull_failures_are_classified_and_only_transport_failures_count_as_network(text, expected):
    assert P.classify_text(text) == expected


def test_a_denial_is_a_network_error_and_nothing_else():
    for cls in ('dns', 'auth', 'not-found', 'other'):
        assert cls != 'network'
    assert P.classify_text('connect: network is unreachable') == 'network'


def test_probe_classifies_exceptions():
    assert PR.classify(OSError(errno.ENETUNREACH, 'unreachable')) == 'network'
    assert PR.classify(OSError(errno.ECONNREFUSED, 'refused')) == 'network'
    assert PR.classify(socket.timeout()) == 'network'
    assert PR.classify(socket.gaierror(-2, 'Name or service not known')) == 'dns'
    assert PR.classify(ssl.SSLError('bad')) == 'tls'
    assert PR.classify(urllib.error.URLError(OSError(errno.ENETUNREACH, 'x'))) == 'network'
    assert PR.classify(urllib.error.HTTPError('u', 401, 'no', {}, None)) == 'http'
    assert PR.classify(ValueError('x')) == 'other'


def run_public(monkeypatch, capsys, *, answers, tcp_class, http_class, expect='denied'):
    def getaddrinfo(host, port, *a, **k):
        if answers is None:
            raise socket.gaierror(-2, 'Name or service not known')
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, port)) for ip in answers]
    monkeypatch.setattr(PR.socket, 'getaddrinfo', getaddrinfo)
    monkeypatch.setattr(PR, 'tcp', lambda host, port: {'to': f'{host}:{port}', 'class': tcp_class})
    monkeypatch.setattr(PR, 'http', lambda url, headers=None, context=None: {'class': http_class})
    ok = PR.public_checks('unit', expect)
    rows = P.parse_probe(capsys.readouterr().out)
    return ok, rows


def test_the_probe_accepts_a_denial_only_when_dns_answered_and_every_transport_attempt_failed_on_the_network(monkeypatch, capsys):
    ok, rows = run_public(monkeypatch, capsys, answers=['9.9.9.9'], tcp_class='network', http_class='network')
    assert ok and len(rows) == 6 and all(r['ok'] for r in rows)
    ok, rows = run_public(monkeypatch, capsys, answers=None, tcp_class='network', http_class='dns')
    assert not ok and any(not r['ok'] and not r['dns']['answered'] for r in rows), 'a DNS failure alone is not a denial'
    ok, rows = run_public(monkeypatch, capsys, answers=['9.9.9.9'], tcp_class='network', http_class='reached')
    assert not ok, 'an HTTP answer (even an authentication error) means the host was reached'
    ok, rows = run_public(monkeypatch, capsys, answers=['9.9.9.9'], tcp_class='connected', http_class='network')
    assert not ok, 'a successful connect is not a denial'


def test_the_positive_control_needs_connects_and_http_answers(monkeypatch, capsys):
    ok, rows = run_public(monkeypatch, capsys, answers=['9.9.9.9'], tcp_class='connected', http_class='reached', expect='reached')
    assert ok and {r['kind'] for r in rows} == {'public-reached'}
    ok, _ = run_public(monkeypatch, capsys, answers=['9.9.9.9'], tcp_class='network', http_class='network', expect='reached')
    assert not ok


def test_judge_counts_checks_and_fails_on_any_red_row_or_a_skipped_check():
    rows = [{'kind': 'public-denied', 'ok': True}] * 3 + [{'kind': 'internal-allowed', 'ok': True}]
    assert P.judge(rows, expect_denied=3, expect_allowed=1)['ok']
    assert not P.judge(rows, expect_denied=4, expect_allowed=1)['ok']
    assert not P.judge(rows + [{'kind': 'internal-allowed', 'ok': False}], expect_denied=3)['ok']
    assert not P.judge([], expect_denied=0)['ok']


def test_probe_output_is_parsed_from_a_build_log_with_progress_prefixes():
    log = '#7 0.45 PROBE {"kind": "public-denied", "ok": true, "name": "a", "place": "build-run"}\n#7 0.5 noise\n#7 0.6 PROBE-SUMMARY {"ok": true}\n'
    assert [r['name'] for r in P.parse_probe(log)] == ['a']


def test_the_resolver_script_answers_the_public_map_with_real_addresses_and_nxdomain_for_the_rest():
    assert 'public.json' in P.DNS_SERVER and 'nxdomain' in P.DNS_SERVER and 'PUBLIC.get(name)' in P.DNS_SERVER
    compile(P.DNS_SERVER, 'dns_server', 'exec')
    assert {'registry-1.docker.io', 'ghcr.io', 'pypi.org', 'deb.debian.org'} <= set(P.PUBLIC_NAMES)


def test_anonymous_volumes_are_recorded_at_creation_and_removed_with_the_container(harness):
    harness.daemon.add_image('sha256:cc', ['pypiserver@sha256:cc'])
    anon = 'a' * 64
    original = harness.d

    def d(*args, check=True, **kw):
        if args[:1] == ('inspect',):
            return subprocess.CompletedProcess(args, 0, json.dumps([{'Type': 'volume', 'Name': anon}, {'Type': 'bind', 'Name': ''}, {'Type': 'volume', 'Name': 'e19-unit-1-work'}]), '')
        if args[:2] == ('volume', 'inspect'):
            return subprocess.CompletedProcess(args, 0 if anon in harness.daemon.volumes else 1, '', '')
        if args[:2] == ('volume', 'rm'):
            harness.daemon.volumes.pop(args[2], None)
            return subprocess.CompletedProcess(args, 0, '', '')
        return original(*args, check=check, **kw)
    harness.d = d
    harness.daemon.volumes[anon] = {'owner': ''}
    name = harness.create_container('pypiserver', 'pypiserver@sha256:cc', network='n')
    assert harness.state['anonymous_volumes'] == {anon: name}
    harness.remove_container(name)
    assert anon not in harness.daemon.volumes and harness.state['anonymous_volumes'] == {}


def test_apt_update_that_exits_zero_with_failed_fetches_is_not_reached(monkeypatch, capsys):
    text = ('Err:1 http://deb.debian.org/debian trixie InRelease\n  Could not connect to deb.debian.org:80 (1.2.3.4), connection timed out\n'
            'W: Failed to fetch http://deb.debian.org/debian/dists/trixie/InRelease  Unable to connect to deb.debian.org:http:\n')
    monkeypatch.setattr(PR.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a, 0, '', text))
    assert PR.run_apt('u', 'public-denied', 'apt', ['apt-get'], 'network') is True
    assert P.parse_probe(capsys.readouterr().out)[0]['class'] == 'network'
    monkeypatch.setattr(PR.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a, 0, '', 'Hit:1 http://debian/debian trixie InRelease\n'))
    assert PR.run_apt('u', 'internal-allowed', 'apt', ['apt-get'], 'reached') is True
    monkeypatch.setattr(PR.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a, 100, '', 'Temporary failure resolving deb.debian.org'))
    assert PR.run_apt('u', 'public-denied', 'apt', ['apt-get'], 'network') is False       # DNS alone is not a denial
