"""E19.F2.S4: hashed per-platform dependency locks and the wheelhouse check.

The committed locks are checked offline against ``requirements.txt`` / ``pyproject.toml``; the generation and the wheelhouse preparation inside the
base image run in Docker and are exercised by the developer tool (``scripts/lock_requirements.py``), not here.
"""
import hashlib
from pathlib import Path
import tomllib
import zipfile

import pytest
from packaging.version import Version

from neo4j_graph_loader.image_build import lockfile as L
from scripts import lock_requirements as tool

ROOT = Path(__file__).resolve().parents[1]
PLATFORM_LOCKS = {platform: tool.lock_path(platform) for platform in L.PLATFORMS}
# The distributions the 0.1.0a1 amd64 images installed on 2026-11-01 (from its build log): a lock may move forward from these, never back.
A1_RESOLVED = {
    'pyyaml': '6.0.3', 'annotated-types': '0.8.0', 'certifi': '2026.10.23', 'charset-normalizer': '3.5.2', 'confluent-kafka': '2.16.1',
    'docker': '7.2.0', 'idna': '3.20', 'neo4j': '6.4.1', 'neo4j-rust-ext': '6.4.1.0', 'numpy': '2.5.4', 'prometheus-client': '0.26.0',
    'pydantic': '2.14.2', 'pydantic-core': '2.50.2', 'pytz': '2026.6', 'requests': '2.34.2', 'typing-extensions': '4.16.0',
    'typing-inspection': '0.4.4', 'urllib3': '2.8.0',
}


def downgrades(lock) -> list[str]:
    versions = lock.versions()
    return [f'{name}: {versions[name]} < {floor}' for name, floor in A1_RESOLVED.items() if name in versions and Version(versions[name]) < Version(floor)]


def header(platform='linux/amd64', **override):
    return {'platform': platform, 'python': '3.12', 'base-image': 'python:3.12-slim@sha256:' + 'a' * 64, 'requirements-sha256': 'b' * 64,
            'generator': 'pip-tools test', **override}


def make_wheel(directory: Path, filename: str, content: bytes = b'') -> str:
    path = directory / filename
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('x/__init__.py', content.decode() or '')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def synthetic(tmp_path, platform='linux/amd64'):
    """A wheelhouse of one pure-Python and one binary wheel for the platform, and the lock that names them."""
    machine = L.PLATFORMS[platform]
    house = tmp_path / 'wh'
    house.mkdir(parents=True)
    pure = make_wheel(house, 'purelib-1.2-py3-none-any.whl')
    binary_name = f'binlib-3.4.5-cp312-cp312-manylinux_2_28_{machine}.whl'
    binary = make_wheel(house, binary_name, b'# binary')
    make_wheel(house, 'neo4j_graph_loader-0.1.0a1-py3-none-any.whl', b'# package')
    lock = L.parse_lock(L.render(header(platform), [L.Entry('purelib', '1.2', (pure,)), L.Entry('binlib', '3.4.5', (binary,))]))
    return house, lock, binary_name


# -- lock text ------------------------------------------------------------------------------------------------------------------------------
def rendered():
    return L.render(header(), [L.Entry('beta', '2.0', ('b' * 64, 'a' * 64)), L.Entry('alpha', '1.0', ('c' * 64,))])


def test_a_lock_round_trips_and_is_in_canonical_order():
    lock = L.parse_lock(rendered())
    assert [entry.name for entry in lock.entries] == ['alpha', 'beta']
    assert lock.entries[1].hashes == ('a' * 64, 'b' * 64) and lock.platform == 'linux/amd64' and lock.machine == 'x86_64'
    assert L.render(lock.header, lock.entries) == rendered()


def test_the_lock_body_is_valid_pip_input():
    body = rendered().split('\n\n', 1)[1]
    assert body.startswith('alpha==1.0 \\\n    --hash=sha256:') and 'beta==2.0 \\\n    --hash=sha256:' + 'a' * 64 + ' \\\n    --hash=sha256:' + 'b' * 64 in body


@pytest.mark.parametrize('mutate,message', [
    (lambda t: t.replace('beta==2.0', 'beta==2.1'), 'edited by hand'),                      # the control: an edited pin
    (lambda t: t.replace('--hash=sha256:' + 'a' * 64, '--hash=sha256:' + 'd' * 64), 'edited by hand|canonical'),          # an edited hash
    (lambda t: t.replace('alpha==1.0', 'alpha>=1.0'), 'only exact pins'),
    (lambda t: t.replace('alpha==1.0', 'alpha @ https://example.com/a.whl'), 'only exact pins'),
    (lambda t: t.replace('# platform: linux/amd64', '# platform: linux/s390x'), 'platform must be one of'),
    (lambda t: t.replace('# python: 3.12', '# python: 3.13'), 'not 3.12'),
    (lambda t: t.replace('# requirements-sha256: ' + 'b' * 64 + '\n', ''), 'lacks requirements-sha256'),
    (lambda t: t.replace('    --hash=sha256:' + 'c' * 64, '    --hash=sha1:' + 'c' * 40), 'only exact pins|hash'),
    (lambda t: t.replace('alpha==1.0 \\\n    --hash=sha256:' + 'c' * 64 + '\n', 'alpha==1.0 \\\n'), 'needs one or more'),
    (lambda t: t + 'gamma==1 \\\n    --hash=sha256:' + 'e' * 64 + '\n', 'edited by hand'),                  # an added pin, digest not renewed
    (lambda t: t.replace('\nbeta==2.0', '\n\nbeta==2.0'), 'blank line'),
    (lambda t: t.split('\n\n', 1)[0] + '\n\n', 'no pins|non-empty|only exact|lacks'),
])
def test_control_a_damaged_or_edited_lock_is_refused(mutate, message):
    with pytest.raises(L.LockError, match=message):
        L.parse_lock(mutate(rendered()))


def test_control_an_unsorted_body_with_a_forged_digest_is_still_refused():
    body = 'beta==2.0 \\\n    --hash=sha256:' + 'a' * 64 + '\nalpha==1.0 \\\n    --hash=sha256:' + 'c' * 64 + '\n'
    forged = {**header(), 'content-sha256': L.sha256_bytes(body.encode())}
    text = ''.join(f'# {key}: {forged[key]}\n' for key in L.HEADER_KEYS) + '\n' + body
    with pytest.raises(L.LockError, match='canonical'):
        L.parse_lock(text)


@pytest.mark.parametrize('filename,machine,expected', [
    ('numpy-2.5.4-cp312-cp312-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'x86_64', True),
    ('numpy-2.5.4-cp312-cp312-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl', 'aarch64', False),
    ('x-1-py3-none-any.whl', 'aarch64', True), ('x-1-py2.py3-none-any.whl', 'x86_64', True),
    ('x-1-cp312-cp312-musllinux_1_2_x86_64.whl', 'x86_64', False), ('x-1-cp311-cp311-manylinux2014_x86_64.whl', 'x86_64', False),
    ('x-1-cp39-abi3-manylinux2014_aarch64.whl', 'aarch64', True), ('x-1-cp313-cp313-manylinux2014_x86_64.whl', 'x86_64', False),
    ('x-1-cp312-cp312-macosx_11_0_arm64.whl', 'aarch64', False), ('x-1-cp312-cp312-win_amd64.whl', 'x86_64', False)])
def test_wheel_tags_must_fit_cpython_312_glibc_linux_and_the_architecture(filename, machine, expected):
    assert L.wheel_fits(filename, machine) is expected


# -- wheelhouse ----------------------------------------------------------------------------------------------------------------------------
PACKAGE = 'neo4j_graph_loader-0.1.0a1-py3-none-any.whl'


@pytest.mark.parametrize('platform', sorted(L.PLATFORMS))
def test_a_wheelhouse_that_matches_the_lock_passes(tmp_path, platform):
    house, lock, binary = synthetic(tmp_path, platform)
    known = L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    assert set(known) == {'purelib-1.2-py3-none-any.whl', binary, PACKAGE} and all(len(d) == 64 for d in known.values())


def test_control_a_missing_wheel_is_refused_with_no_fallback(tmp_path):
    house, lock, binary = synthetic(tmp_path)
    (house / binary).unlink()
    with pytest.raises(L.LockError, match='missing binlib==3.4.5'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)


def test_control_a_wheel_of_the_other_architecture_is_refused_by_tag_and_by_hash(tmp_path):
    house, lock, binary = synthetic(tmp_path, 'linux/amd64')
    (house / binary).unlink()
    other = binary.replace('x86_64', 'aarch64')
    make_wheel(house, other, b'# arm build')
    with pytest.raises(L.LockError, match='not a CPython 3.12 wheel for linux/amd64'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    (house / other).unlink()
    make_wheel(house, binary, b'# arm build under the right name')           # right file name, content from elsewhere
    with pytest.raises(L.LockError, match='does not match any hash'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)


def test_control_a_forged_lock_hash_does_not_let_a_wrong_architecture_wheel_through(tmp_path):
    house, _, binary = synthetic(tmp_path, 'linux/amd64')
    (house / binary).unlink()
    other = binary.replace('x86_64', 'aarch64')
    digest = make_wheel(house, other, b'# arm build')
    pure = hashlib.sha256((house / 'purelib-1.2-py3-none-any.whl').read_bytes()).hexdigest()
    lock = L.parse_lock(L.render(header('linux/amd64'), [L.Entry('purelib', '1.2', (pure,)), L.Entry('binlib', '3.4.5', (digest,))]))
    with pytest.raises(L.LockError, match='not a CPython 3.12 wheel'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)


@pytest.mark.parametrize('name', ['other-9.9-py3-none-any.whl', 'purelib-1.3-py3-none-any.whl', 'thing-1.0.tar.gz', 'README.txt'])
def test_control_anything_the_lock_does_not_name_is_refused(tmp_path, name):
    house, lock, _ = synthetic(tmp_path)
    if name.endswith('.whl'):
        make_wheel(house, name)
    else:
        (house / name).write_bytes(b'x')
    with pytest.raises(L.LockError, match='does not name'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)


def test_control_changed_content_a_second_wheel_a_symlink_a_subdirectory_and_a_missing_package_wheel_are_refused(tmp_path):
    house, lock, binary = synthetic(tmp_path)
    path = house / 'purelib-1.2-py3-none-any.whl'
    path.write_bytes(path.read_bytes() + b'0')
    with pytest.raises(L.LockError, match='does not match any hash'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    house, lock, binary = synthetic(tmp_path / 'two')
    make_wheel(house, 'binlib-3.4.5-1-cp312-cp312-manylinux_2_17_x86_64.whl', b'# second build')
    with pytest.raises(L.LockError, match='more than one wheel'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    house, lock, binary = synthetic(tmp_path / 'link')
    (house / 'extra.whl').symlink_to(house / binary)
    with pytest.raises(L.LockError, match='plain files only'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    house, lock, binary = synthetic(tmp_path / 'dir')
    (house / 'sub').mkdir()
    with pytest.raises(L.LockError, match='plain files only'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    house, lock, binary = synthetic(tmp_path / 'nopkg')
    (house / PACKAGE).unlink()
    with pytest.raises(L.LockError, match='missing the package wheel'):
        L.verify_wheelhouse(house, lock, package_wheel=PACKAGE)
    with pytest.raises(L.LockError, match='not a directory'):
        L.verify_wheelhouse(tmp_path / 'absent', lock)


# -- the generation helpers ---------------------------------------------------------------------------------------------------------------
COMPILED = '''--only-binary :all:

alpha==1.0 \\
    --hash=sha256:%s \\
    --hash=sha256:%s
beta==2.0 \\
    --hash=sha256:%s
''' % ('1' * 64, '2' * 64, '3' * 64)


def test_the_resolver_output_is_parsed_and_restricted_to_the_wheels_downloaded_on_the_platform():
    entries = tool.parse_pip_compile(COMPILED)
    assert [(e.name, len(e.hashes)) for e in entries] == [('alpha', 2), ('beta', 1)]
    kept = tool.restrict_to_downloaded(entries, f'{"2" * 64}  alpha-1.0-cp312-cp312-manylinux_2_28_x86_64.whl\n{"3" * 64}  beta-2.0-py3-none-any.whl\n')
    assert [(e.name, e.hashes) for e in kept] == [('alpha', ('2' * 64,)), ('beta', ('3' * 64,))]


@pytest.mark.parametrize('listing,message', [
    (f'{"9" * 64}  alpha-1.0-py3-none-any.whl\n{"3" * 64}  beta-2.0-py3-none-any.whl\n', 'not among the published hashes'),
    (f'{"2" * 64}  alpha-1.0-py3-none-any.whl\n', 'no wheel was downloaded for beta'),
    (f'{"2" * 64}  alpha-1.0-py3-none-any.whl\n{"3" * 64}  beta-2.0-py3-none-any.whl\n{"4" * 64}  gamma-1-py3-none-any.whl\n', 'not pinned')])
def test_control_the_downloaded_wheels_must_match_the_published_hashes_and_the_pins(listing, message):
    with pytest.raises(L.LockError, match=message):
        tool.restrict_to_downloaded(tool.parse_pip_compile(COMPILED), listing)


@pytest.mark.parametrize('text', ['alpha>=1.0\n', 'alpha==1.0\n', '-e git+https://example.com/x#egg=x\n', 'alpha==1.0 ; python_version<"3"\n', ''])
def test_control_resolver_output_that_is_not_a_hashed_pin_is_refused(text):
    with pytest.raises(L.LockError):
        tool.parse_pip_compile(text)


# -- the committed locks -------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('platform', sorted(L.PLATFORMS))
def test_the_committed_lock_is_valid_and_resolved_from_this_requirements_file(platform):
    path = PLATFORM_LOCKS[platform]
    lock = L.parse_lock(path.read_text(encoding='utf-8'))
    assert lock.platform == platform and path.name == f'py312-{platform.replace("/", "-")}.txt'
    assert lock.header['requirements-sha256'] == hashlib.sha256((ROOT / 'requirements.txt').read_bytes()).hexdigest()
    assert all(len(entry.hashes) == 1 for entry in lock.entries), 'one wheel per pin on its platform (a second would be a second tag choice)'


def test_check_passes_on_the_committed_locks_and_the_make_target_exists():
    assert tool.check() == []
    makefile = (ROOT / 'Makefile').read_text(encoding='utf-8')
    assert '\nlock:' in makefile and '\ncheck-lock:' in makefile and 'lock_requirements.py check' in makefile


@pytest.mark.parametrize('platform', sorted(L.PLATFORMS))
def test_no_dependency_is_downgraded_relative_to_the_0_1_0a1_images(platform):
    lock = L.parse_lock(PLATFORM_LOCKS[platform].read_text(encoding='utf-8'))
    assert downgrades(lock) == []
    assert set(A1_RESOLVED) <= set(lock.versions()), 'every distribution the 0.1.0a1 images carried is still locked'


def test_control_lowering_a_pin_is_a_downgrade_and_fails_the_lock_check(tmp_path, monkeypatch):
    path = PLATFORM_LOCKS['linux/arm64']
    text = path.read_text(encoding='utf-8').replace('numpy==2.5.4', 'numpy==2.5.3')
    edited = tmp_path / path.name
    edited.write_text(text, encoding='utf-8')
    assert any('edited by hand' in problem for problem in tool.check([edited]))      # the digest catches it offline
    forged = L.render(L.parse_lock(path.read_text(encoding='utf-8')).header,
                      [L.Entry(e.name, '2.5.3' if e.name == 'numpy' else e.version, e.hashes) for e in L.parse_lock(path.read_text(encoding='utf-8')).entries])
    assert downgrades(L.parse_lock(forged)) == ['numpy: 2.5.3 < 2.5.4']                  # and a regenerated one is still a downgrade


def test_control_a_pin_outside_the_declared_range_and_a_stale_requirements_hash_are_reported(tmp_path, monkeypatch):
    lock = L.parse_lock(PLATFORM_LOCKS['linux/amd64'].read_text(encoding='utf-8') if PLATFORM_LOCKS['linux/amd64'].exists()
                        else PLATFORM_LOCKS['linux/arm64'].read_text(encoding='utf-8'))
    out_of_range = L.render(lock.header, [L.Entry(e.name, '3.0.0' if e.name == 'pydantic' else e.version, e.hashes) for e in lock.entries])
    path = tmp_path / tool.lock_path(lock.platform).name
    path.write_text(out_of_range, encoding='utf-8')
    assert any('pydantic==3.0.0 is outside' in problem for problem in tool.check([path]))
    stale = tmp_path / 'requirements.txt'
    stale.write_text((ROOT / 'requirements.txt').read_text(encoding='utf-8') + 'tabulate>=0.9,<1\n', encoding='utf-8')
    monkeypatch.setattr(tool, 'REQUIREMENTS', stale)
    assert any('different requirements.txt' in problem for problem in tool.check([PLATFORM_LOCKS[lock.platform]]))


def test_the_two_locks_agree_on_every_shared_version():
    if not all(path.exists() for path in PLATFORM_LOCKS.values()):
        pytest.fail('both platform locks must be committed')
    first, second = (L.parse_lock(path.read_text(encoding='utf-8')).versions() for path in PLATFORM_LOCKS.values())
    assert {n: v for n, v in first.items() if n in second} == {n: v for n, v in second.items() if n in first}
    assert set(first) == set(second), 'the closure is the same set of distributions on both platforms'


def test_the_lock_tool_adds_no_dependency_to_the_package():
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']
    assert project['dependencies'] == ['neo4j>=6.0,<7', 'confluent-kafka>=2.5,<3', 'pydantic>=2.7,<3', 'PyYAML>=6.0.1,<7', 'docker>=7.1.0,<8',
                                       'neo4j-rust-ext>=6.0,<7', 'numpy>=1.26,<3', 'prometheus-client>=0.20,<1']
    for name in ('pyproject.toml', 'requirements.txt', 'requirements-dev.txt', 'requirements-build.txt'):
        assert 'pip-tools' not in (ROOT / name).read_text(encoding='utf-8'), name
    for path in (ROOT / 'src' / 'neo4j_graph_loader' / 'image_build' / 'dockerfiles').iterdir():
        assert 'pip-tools' not in path.read_text(encoding='utf-8'), path.name
    assert 'pip-tools' in (ROOT / 'scripts' / 'lock_requirements.py').read_text(encoding='utf-8'), 'only the container script installs it'


# -- the container commands (a recording stand-in for the Docker CLI) -----------------------------------------------------------------------
def test_containers_are_created_started_and_removed_by_recorded_id_without_force(monkeypatch):
    calls = []

    class Done:
        def __init__(self, out=''):
            self.returncode, self.stdout, self.stderr = 0, out, ''

    def fake(args, **kwargs):
        calls.append(list(args))
        return Done('cid-123\n' if args[1] == 'create' else '')

    monkeypatch.setattr(tool.subprocess, 'run', fake)
    container = tool.Container('docker', 'tok', 'lock-arm64')
    container.create('python@sha256:' + 'a' * 64, 'linux/arm64', ['/w:/work'], ['sh', '-c', 'true'], {'HOME': '/tmp/home'})
    container.run()
    container.remove()
    verbs = [call[1] for call in calls]
    assert verbs == ['create', 'start', 'rm'] and calls[1][-1] == 'cid-123' and calls[2] == ['docker', 'rm', 'cid-123']
    flat = ' '.join(' '.join(call) for call in calls)
    assert '-f' not in flat.split() and '--force' not in flat and 'prune' not in flat and 'kill' not in flat
    create = calls[0]
    assert '--user' in create and '--cap-drop' in create and 'ALL' in create and 'no-new-privileges' in create
    assert 'io.graph-loader.lock-tool-owner=tok' in create and 'e19-tok-lock-arm64' in create
