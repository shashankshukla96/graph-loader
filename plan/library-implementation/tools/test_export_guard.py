"""Tests of the export guard (private; run with ``pytest plan/library-implementation/tools/test_export_guard.py``).

Every kind of violation the guard is meant to stop is planted and must fail with the file and the line (or, for commits and tags, the object), and
the product tree itself must pass with the reviewed allow-entries and nothing else.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import export_guard as G  # noqa: E402

ROOT = HERE.parents[2]
IDENTITY = G.public_set()['identity']
PLAIN = '100644'


def entry(path='src/a.py', text='x = 1\n', mode=PLAIN):
    return G.Entry(path, mode, text.encode() if isinstance(text, str) else text)


def scan(*entries, allow=None, secrets=None):
    return G.Guard(allow or G.Allow(), secrets or {}).scan(list(entries))


def rules(hits):
    return sorted({h.rule for h in hits})


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def line_allow(path, rule, line, reason='reviewed in the test: the line is a planted sample'):
    return G.Allow({'entries': [{'rule': rule, 'path': path, 'line_sha256': sha(line), 'reason': reason}]})


# -- clean text and product identifiers pass ---------------------------------------------------------------------------------------------------
def test_a_clean_file_passes():
    assert scan(entry()) == []


@pytest.mark.parametrize('text', [
    'PLAN_FORMAT = "graph-loader-edge-plan/1"\n', 'the orchestrator starts the loaders\n', 'stored plan/chunk/DLQ formats\n', 'plan/spill/timing overrides\n',
    'see plan.json\n', 'version 0.10.0.1 of 110.1.1.1 and 172.15.0.1 and 172.32.0.1 and 11.0.0.1\n', 'user@localhost\n',
    f'contact {IDENTITY["public_contact"]} or {IDENTITY["email"]}\n', 'password = os.environ["P"]\n', 'token_count = 3\n', 'url = "https://example.com/a"\n',
    '@pytest.fixture\n', 'pkg@1.2.3\n', 'a gpt model and GPT without a dash are fine\n'])
def test_product_identifiers_and_near_misses_pass(text):
    assert scan(entry(text=text)) == []


# -- content violations: file and line ---------------------------------------------------------------------------------------------------------
PLANTED = {
    'denylist-word': ['made with Claude', 'codex branch', 'by Anthropic', 'OpenAI client', 'model gpt-5', 'ask the subagent', 'the Cartographer', 'see .agents/ folder',
                      '.claude/settings', 'Co-Authored-By: someone'],
    'plan-pointer': ['see plan/v6/slice-4/stories.md', 'plan/library-implementation/epics/e19.md', 'see plan/notes.md'],
    'private-address': ['host 10.1.2.3', 'host 172.16.0.1', 'host 172.31.255.254', 'host 192.168.1.75'],
    'local-path': ['open /Users/someone/file', 'open /home/someone/file', 'open /HOME/someone/file'],
    'email-address': ['write to someone.else@corp.example.org', 'a+b@sub.domain.io'],
    'credential-private-key': ['-----BEGIN RSA PRIVATE KEY-----', '-----BEGIN PRIVATE KEY-----', '-----BEGIN OPENSSH PRIVATE KEY-----'],
    'credential-pem-header': ['-----BEGIN CERTIFICATE-----'],
    'credential-aws-key': ['key AKIAIOSFODNN7EXAMPLE'],
    'credential-github-token': ['ghp_' + 'a' * 36, 'gho_' + 'B' * 40, 'ghs_' + '1' * 36, 'github_pat_11ABC'],
    'credential-pypi-token': ['pypi-AgEIcHlwaS5vcmc'],
    'credential-slack-token': ['xoxb-123-456', 'xoxp-1', 'xoxa-2', 'xoxr-3'],
    'credential-url-userinfo': ['https://user:pass@host/x', 'postgresql://svc:pw@db/runs', 'ssh://git@host/repo'],
    'credential-literal': ['password = "hunter2"', "'NEO4J_PASSWORD': 'abc123'", 'secret_key: "s3cr3t"', 'api_token="xyz"'],
}


@pytest.mark.parametrize('rule,text', [(rule, text) for rule, texts in sorted(PLANTED.items()) for text in texts])
def test_each_planted_violation_fails_with_its_rule_path_and_line(rule, text):
    hits = scan(entry('src/pkg/mod.py', f'fine = 1\n\n{text}\nalso fine = 2\n'))
    assert rule in rules(hits), (rule, text, rules(hits))
    hit = next(h for h in hits if h.rule == rule)
    assert hit.path == 'src/pkg/mod.py' and hit.line == 3 and hit.render().startswith(rule) and 'src/pkg/mod.py:3' in hit.render()


@pytest.mark.parametrize('text', ['password = "hunter2"', 'key AKIAIOSFODNN7EXAMPLE', 'https://user:pass@host/x', 'ghp_' + 'z' * 36])
def test_a_credential_hit_never_shows_the_matched_text(text):
    rendered = ' '.join(h.render() for h in scan(entry(text=text + '\n')))
    for fragment in ('hunter2', 'AKIAIOSFODNN7EXAMPLE', 'user:pass', 'z' * 20):
        assert fragment not in rendered


def test_every_denylist_word_is_case_insensitive():
    for word in ('CLAUDE', 'Codex', 'ANTHROPIC', 'openai', 'GPT-4', 'SubAgent', 'CARTOGRAPHER', '.AGENTS', '.CLAUDE', 'CO-AUTHORED-BY'):
        assert 'denylist-word' in rules(scan(entry(text=f'x {word} y\n'))), word


def test_the_path_is_checked_for_the_denylist_too():
    assert 'denylist-path' in rules(scan(entry('src/claude_notes.py')))
    assert 'plan-pointer-path' in rules(scan(entry('docs/plan/v5/x.md')))
    assert 'private-address-path' in rules(scan(entry('tests/192.168.1.5.txt')))


def test_a_real_secret_value_is_found_by_its_hash_and_never_printed(tmp_path):
    secret_file = tmp_path / 'credentials'
    secret_file.write_text('# private\nPYPI_TOKEN=Zk39-private-value-0042\napi_secret: other-private-value-9\nNEO4J_URI=bolt://localhost:7687\nDEFAULT_PASSWORD=changeme-default\n')
    secrets = G.secret_hashes([secret_file], public_text='DEFAULT_PASSWORD=changeme-default\n')
    assert all(len(key) == 64 for key in secrets) and 'Zk39-private-value-0042' not in json.dumps(secrets)
    assert len(secrets) == 2, 'only sensitive keys count, and a value that the public .env.example documents is not a secret'
    hits = scan(entry('docs/a.md', 'fine\nuse Zk39-private-value-0042 here\n'), secrets=secrets)
    assert rules(hits) == ['secret-value'] and hits[0].line == 2 and 'Zk39' not in hits[0].render() and 'credentials' in hits[0].render()
    assert scan(entry('docs/a.md', 'use other-private-value-9\n'), secrets=secrets)
    assert scan(entry('docs/a.md', 'nothing like that\n'), secrets=secrets) == []
    assert scan(entry('docs/a.md', 'bolt://localhost:7687 and changeme-default\n'), secrets=secrets) == []


def test_docker_config_secrets_include_the_decoded_auth_value(tmp_path):
    import base64
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'auths': {'ghcr.io': {'auth': base64.b64encode(b'someone:Zr58-registry-password-7').decode()}}, 'credsStore': 'desktop',
                                  'currentContext': 'desktop-linux'}))
    secrets = G.secret_hashes([config])
    assert scan(entry('docs/a.md', 'pw Zr58-registry-password-7\n'), secrets=secrets)
    assert scan(entry('docs/a.md', 'desktop-linux and desktop\n'), secrets=secrets) == []


# -- entries and paths ---------------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('path', ['plan/x.md', 'soak/a.py', 'var/log.txt', '.claude/a.json', '.agents/a', 'newtop/a.py', 'README2.md', 'docs/PYPI_CHECKLIST_REVIEW.md',
                                  'docs/PYPI_LIBRARY_CHECKLIST.md', 'src/__pycache__/a.pyc', 'src/pkg.egg-info/PKG-INFO', 'tests/.DS_Store', 'LICENSE/sub'])
def test_a_path_outside_the_public_set_fails(path):
    assert 'path-not-allowlisted' in rules(scan(entry(path)))


@pytest.mark.parametrize('path', ['src/a.py', 'tests/x/y.py', 'docs/CLI.md', 'README.md', 'pyproject.toml', '.env.example', 'config/neo4j/server-logs.xml'])
def test_paths_of_the_public_set_pass(path):
    assert scan(entry(path)) == []


@pytest.mark.parametrize('mode,kind', [('120000', 'symlink'), ('160000', 'gitlink / submodule'), ('040000', 'directory entry'), ('100664', 'mode 100664')])
def test_only_regular_files_of_mode_100644_or_100755_pass(mode, kind):
    hits = scan(G.Entry('src/link', mode))
    assert 'entry-type' in rules(hits) and kind in ' '.join(h.detail for h in hits)
    assert scan(entry('scripts/run.sh', 'echo ok\n', mode='100755')) == []


def test_nested_git_metadata_fails():
    assert 'nested-git' in rules(scan(entry('src/vendor/.git/config', '[core]\n')))
    assert 'nested-git' in rules(scan(entry('src/vendor/.gitmodules', '[submodule "x"]\n')))
    assert 'nested-git' in rules(scan(G.Entry('src/vendor/.git/', '160000')))


@pytest.mark.parametrize('path', ['src/a\nb.py', 'src/a\x00b.py', 'src/a\\b.py', 'src/-rf.py', 'src/trail.', 'src/trail ', 'src/CON.txt', 'src/aux', 'src/é.py',
                                  'src//a.py', 'src/./a.py', 'src/../a.py', 'src/' + 'x' * 300])
def test_a_bad_filename_fails(path):
    assert 'bad-filename' in rules(scan(entry(path)))


def test_case_collisions_fail():
    assert 'path-case-collision' in rules(scan(entry('docs/A.md'), entry('docs/a.md')))


def test_non_utf8_text_fails_and_unicode_text_passes():
    assert 'non-utf8-text' in rules(scan(G.Entry('docs/a.md', PLAIN, b'caf\xe9 \xff\xfe')))
    assert scan(entry('docs/a.md', 'café — naïve ✓\n')) == []


def test_a_binary_file_needs_an_exact_path_and_sha256_and_archives_never_pass_unlisted():
    blob = b'\x89PNG\r\n\x1a\n\x00\x00data'
    assert 'binary-not-listed' in rules(scan(G.Entry('docs/img.png', PLAIN, blob)))
    allow = lambda path, data: G.Allow({'binaries': [{'path': path, 'sha256': hashlib.sha256(data).hexdigest(), 'reason': 'reviewed image used by the docs'}]})   # noqa: E731
    assert scan(G.Entry('docs/img.png', PLAIN, blob), allow=allow('docs/img.png', blob)) == []
    assert 'binary-not-listed' in rules(scan(G.Entry('docs/img.png', PLAIN, blob + b'x'), allow=allow('docs/img.png', blob))), 'changed bytes'
    assert 'binary-not-listed' in rules(scan(G.Entry('docs/other.png', PLAIN, blob), allow=allow('docs/img.png', blob))), 'another path'
    for archive in (b'PK\x03\x04rest', b'\x1f\x8b\x08rest', b'BZh9rest', b'7z\xbc\xaf\x27\x1c', b'Rar!\x1a\x07'):
        assert 'binary-not-listed' in rules(scan(G.Entry('docs/a.bin', PLAIN, archive))), archive[:4]


# -- allow-entries are exact ---------------------------------------------------------------------------------------------------------------------
LINE = 'host 172.17.0.9 is a Docker bridge address in a test'


def test_an_allow_entry_covers_exactly_one_rule_one_path_and_one_line_text():
    allow = line_allow('tests/a.py', 'private-address', LINE)
    assert scan(entry('tests/a.py', LINE + '\n'), allow=allow) == []
    other = line_allow('tests/a.py', 'private-address', LINE)
    assert 'private-address' in rules(scan(entry('tests/b.py', LINE + '\n'), allow=other)), 'another path'
    assert 'private-address' in rules(scan(entry('tests/a.py', LINE + ' changed\n'), allow=line_allow('tests/a.py', 'private-address', LINE))), 'another line'
    mixed = LINE + ' and /home/x/y'
    hits = scan(entry('tests/a.py', mixed + '\n'), allow=line_allow('tests/a.py', 'private-address', mixed))
    assert rules(hits) == ['local-path', 'stale-allow-entry'] or rules(hits) == ['local-path'], 'an entry for one rule does not allow another rule on the line'


@pytest.mark.parametrize('bad', [
    {'rule': 'private-address', 'path': 'tests/', 'line_sha256': 'a' * 64, 'reason': 'directory wide entry is refused'},
    {'rule': 'private-address', 'path': 'tests/*.py', 'line_sha256': 'a' * 64, 'reason': 'wildcard entries are refused'},
    {'rule': '*', 'path': 'tests/a.py', 'line_sha256': 'a' * 64, 'reason': 'all rules is refused'},
    {'rule': 'private-address', 'path': 'tests/a.py', 'line_sha256': 'xyz', 'reason': 'not a digest is refused'},
    {'rule': 'private-address', 'path': 'tests/a.py', 'line_sha256': 'a' * 64, 'reason': 'short'},
    {'rule': 'private-address', 'path': 'tests/a.py', 'line_sha256': 'a' * 64},
    {'rule': 'private-address', 'path': 'tests/a.py', 'line_sha256': 'a' * 64, 'reason': 'extra fields are refused', 'note': 'x'}])
def test_allow_entries_that_are_not_exact_are_refused_when_loaded(bad):
    with pytest.raises(G.GuardError):
        G.Allow({'entries': [bad]})


def test_an_allow_entry_that_matches_nothing_is_an_error():
    allow = line_allow('tests/a.py', 'private-address', LINE)
    assert rules(scan(entry('tests/a.py', 'clean\n'), allow=allow)) == ['stale-allow-entry']
    binary = G.Allow({'binaries': [{'path': 'docs/x.png', 'sha256': 'b' * 64, 'reason': 'a reviewed image that is gone now'}]})
    assert rules(scan(entry(), allow=binary)) == ['stale-binary-entry']


# -- the product tree -----------------------------------------------------------------------------------------------------------------------------
def test_the_product_tree_passes_with_the_reviewed_allow_entries(tmp_path):
    destination = tmp_path / 'tree'
    summary = G.materialize(ROOT, destination)
    assert summary['files'] > 400 and 'plan' in summary['excluded_top_level'] and 'soak' in summary['excluded_top_level']
    allow = G.Allow.from_file(None)
    hits = G.Guard(allow).scan(G.tree_entries(destination))
    assert hits == [], '\n'.join(h.render() for h in hits)
    assert len(allow.entries) > 0 and len(allow.used) == len(allow.entries)


def test_without_its_allow_entries_the_product_tree_fails_on_exactly_the_documented_categories(tmp_path):
    destination = tmp_path / 'tree'
    G.materialize(ROOT, destination)
    hits = G.Guard(G.Allow({})).scan(G.tree_entries(destination))
    assert rules(hits) == ['credential-literal', 'credential-url-userinfo', 'local-path', 'private-address']


def test_materialize_is_a_full_replacement_that_leaves_out_the_excluded_files(tmp_path):
    destination = tmp_path / 'tree'
    G.materialize(ROOT, destination)
    present = {p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file()}
    assert not any(p.startswith(('plan/', 'soak/', 'var/')) for p in present)
    assert 'docs/PYPI_LIBRARY_CHECKLIST.md' not in present and 'docs/PYPI_CHECKLIST_REVIEW.md' not in present
    assert {'README.md', 'pyproject.toml', 'src/neo4j_graph_loader/_version.py'} <= present
    with pytest.raises(G.GuardError):
        G.materialize(ROOT, destination)                     # an existing destination is refused
    with pytest.raises(G.GuardError):
        G.materialize(ROOT, ROOT / 'inside')                 # inside the worktree is refused


def test_a_materialized_symlink_or_nested_git_directory_is_seen_by_the_tree_scan(tmp_path):
    tree = tmp_path / 'tree'
    (tree / 'src').mkdir(parents=True)
    (tree / 'src' / 'real.py').write_text('x = 1\n')
    (tree / 'src' / 'link.py').symlink_to('real.py')
    (tree / 'src' / 'vendor' / '.git').mkdir(parents=True)
    (tree / 'src' / 'vendor' / '.git' / 'HEAD').write_text('ref: refs/heads/main\n')
    (tree / 'src' / 'exec.sh').write_text('echo\n')
    (tree / 'src' / 'exec.sh').chmod(0o755)
    hits = G.Guard().scan(G.tree_entries(tree))
    assert 'entry-type' in rules(hits) and 'nested-git' in rules(hits)
    assert {h.path for h in hits if h.rule == 'entry-type'} == {'src/link.py', 'src/vendor/.git/'}


# -- commits and tags -----------------------------------------------------------------------------------------------------------------------------
def run_git(repo, *args, env=None, input_text=None):
    base = {**os.environ, 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_SYSTEM': os.devnull, **(env or {})}
    done = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True, env=base, input=input_text)
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


def identity_env(name=IDENTITY['name'], email=IDENTITY['email'], committer_name=None, committer_email=None):
    return {'GIT_AUTHOR_NAME': name, 'GIT_AUTHOR_EMAIL': email, 'GIT_COMMITTER_NAME': committer_name or name, 'GIT_COMMITTER_EMAIL': committer_email or email,
            'GIT_AUTHOR_DATE': '2026-11-04T10:00:00+0000', 'GIT_COMMITTER_DATE': '2026-11-04T10:00:00+0000'}


@pytest.fixture
def repo(tmp_path):
    path = tmp_path / 'repo'
    path.mkdir()
    run_git(path, 'init', '-q', '-b', 'main')
    return path


def commit(repo, files, message='Add the loader', parent=None, env=None):
    for name, text in files.items():
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        run_git(repo, 'add', name)
    tree = run_git(repo, 'write-tree')
    args = ['commit-tree', tree, '-m', message] + (['-p', parent] if parent else [])
    return run_git(repo, *args, env=env or identity_env())


def check(repo, rev, parent_ok=(), validated=()):
    return G.check_commit(G.Guard(), repo, rev, set(parent_ok), set(validated))


def test_a_commit_by_the_public_identity_with_a_plain_message_passes(repo):
    first = commit(repo, {'README.md': 'hello\n'})
    assert check(repo, first) == []
    assert G.Guard().scan(G.git_entries(repo, first)) == []
    second = commit(repo, {'docs/a.md': 'more\n'}, 'Describe the second step', parent=first)
    assert check(repo, second, validated={first}) == []


def test_a_wrong_author_or_committer_fails(repo):
    assert 'commit-identity' in rules(check(repo, commit(repo, {'a.md': 'x\n'}, env=identity_env(name='Someone Else', email='else@example.org'))))
    assert 'commit-identity' in rules(check(repo, commit(repo, {'b.md': 'x\n'}, env=identity_env(email='shashankshukla96@gmail.com'))))
    only_committer = commit(repo, {'c.md': 'x\n'}, env=identity_env(committer_name='Machine', committer_email='machine@host.local'))
    hits = check(repo, only_committer)
    assert 'commit-identity' in rules(hits) and 'committer' in ' '.join(h.detail for h in hits)


@pytest.mark.parametrize('message', ['Add it\n\nCo-Authored-By: Someone <s@example.org>\n', 'Add it\n\nSigned-off-by: Someone <s@example.org>\n',
                                      'Add it\n\nReviewed-by: A B\n', 'Fix\n\nChange-Id: I1234\n'])
def test_a_commit_message_with_a_trailer_fails(repo, message):
    assert 'commit-trailer' in rules(check(repo, commit(repo, {'a.md': 'x\n'}, message)))


def test_a_commit_message_is_scanned_like_a_file(repo):
    hits = check(repo, commit(repo, {'a.md': 'x\n'}, 'Add things\n\nBuilt with Claude and checked in plan/v6/x.md\n'))
    assert {'denylist-word', 'plan-pointer'} <= set(rules(hits)) and all('message' in h.path for h in hits if h.rule in ('denylist-word', 'plan-pointer'))


def test_a_parent_must_be_validated_or_the_last_public_commit(repo):
    first = commit(repo, {'a.md': 'x\n'})
    second = commit(repo, {'b.md': 'y\n'}, parent=first)
    assert 'commit-parent' in rules(check(repo, second))
    assert check(repo, second, parent_ok={first}) == []
    assert check(repo, second, validated={first}) == []


def test_the_tree_of_a_commit_with_a_symlink_a_gitlink_or_a_planted_word_fails(repo):
    first = commit(repo, {'README.md': 'hello\n'})
    blob = run_git(repo, 'hash-object', '-w', '--stdin', input_text='real.py')
    run_git(repo, 'update-index', '--add', '--cacheinfo', f'120000,{blob},src/link.py')
    run_git(repo, 'update-index', '--add', '--cacheinfo', f'160000,{first},src/submodule')
    run_git(repo, 'update-index', '--add', '--cacheinfo', f'100644,{run_git(repo, "hash-object", "-w", "--stdin", input_text="made with Claude")},src/x.py')
    tree = run_git(repo, 'write-tree')
    planted = run_git(repo, 'commit-tree', tree, '-m', 'Add', '-p', first, env=identity_env())
    hits = G.Guard().scan(G.git_entries(repo, planted))
    assert {h.path for h in hits if h.rule == 'entry-type'} == {'src/link.py', 'src/submodule'}
    assert any(h.rule == 'denylist-word' and h.path == 'src/x.py' and h.line == 1 for h in hits)


def tag(repo, name, target, message='Release 0.1.0a2', env=None, lightweight=False):
    if lightweight:
        run_git(repo, 'tag', name, target)
    else:
        run_git(repo, 'tag', '-a', name, target, '-m', message, env=env or identity_env())


def test_an_annotated_tag_by_the_public_identity_on_a_validated_commit_passes(repo):
    first = commit(repo, {'a.md': 'x\n'})
    tag(repo, 'v0.1.0a2', first)
    assert G.check_tag(G.Guard(), repo, 'v0.1.0a2', {first}) == []


def test_tag_controls_a_wrong_tagger_a_forbidden_message_an_unvalidated_target_a_lightweight_tag(repo):
    first = commit(repo, {'a.md': 'x\n'})
    second = commit(repo, {'b.md': 'y\n'}, parent=first)
    tag(repo, 'wrong-tagger', first, env=identity_env(name='Someone', email='s@example.org'))
    tag(repo, 'bad-message', first, message='Release by Claude, see plan/v6/x.md')
    tag(repo, 'unvalidated', second)
    tag(repo, 'light', first, lightweight=True)
    tag(repo, 'partial-history', second)
    guard = G.Guard()
    assert 'tag-identity' in rules(G.check_tag(guard, repo, 'wrong-tagger', {first}))
    assert {'denylist-word', 'plan-pointer'} <= set(rules(G.check_tag(guard, repo, 'bad-message', {first})))
    assert 'tag-target' in rules(G.check_tag(guard, repo, 'unvalidated', {first}))
    assert rules(G.check_tag(guard, repo, 'light', {first})) == ['tag-not-annotated']
    assert 'tag-not-annotated' in rules(G.check_tag(guard, repo, 'does-not-exist', {first}))
    assert G.check_tag(guard, repo, 'partial-history', {first, second}) == []
    third = commit(repo, {'c.md': 'z\n'}, parent=second)
    tag(repo, 'ancestry', third)
    hits = G.check_tag(guard, repo, 'ancestry', {third})
    assert 'tag-ancestry' in rules(hits), 'a validated target with unvalidated ancestors'


def test_a_tag_that_peels_through_another_tag_is_checked_at_every_level(repo):
    first = commit(repo, {'a.md': 'x\n'})
    tag(repo, 'inner', first, env=identity_env(name='Someone', email='s@example.org'))
    run_git(repo, 'tag', '-a', 'outer', 'refs/tags/inner', '-m', 'Release 0.1.0a2', env=identity_env())
    assert 'tag-identity' in rules(G.check_tag(G.Guard(), repo, 'outer', {first}))


# -- the command line -----------------------------------------------------------------------------------------------------------------------------
def cli(*args):
    return subprocess.run([sys.executable, str(HERE / 'export_guard.py'), *args], capture_output=True, text=True)


def test_the_command_line_exit_codes_and_report(tmp_path):
    empty = tmp_path / 'empty.json'
    empty.write_text('{}')
    tree = tmp_path / 'tree'
    (tree / 'src').mkdir(parents=True)
    (tree / 'src' / 'a.py').write_text('x = 1\n')
    done = cli('--allow', str(empty), 'tree', str(tree))
    assert done.returncode == 0 and 'PASS' in done.stdout
    (tree / 'src' / 'b.py').write_text('ok = 1\n# written with claude\n')
    done = cli('--allow', str(empty), 'tree', str(tree))
    assert done.returncode == 1 and 'denylist-word' in done.stdout and 'src/b.py:2' in done.stdout and 'FAIL: 1' in done.stdout
    report = json.loads(cli('--allow', str(empty), '--json', 'tree', str(tree)).stdout)
    assert report['ok'] is False and report['hits'][0]['path'] == 'src/b.py' and report['hits'][0]['line'] == 2
    bad = tmp_path / 'allow.json'
    bad.write_text(json.dumps({'entries': [{'rule': '*', 'path': 'src/', 'line_sha256': 'x', 'reason': 'x'}]}))
    done = cli('--allow', str(bad), 'tree', str(tree))
    assert done.returncode == 2 and 'invalid allow-entry' in done.stderr
    stale = tmp_path / 'stale.json'
    stale.write_text(json.dumps({'entries': [{'rule': 'private-address', 'path': 'src/a.py', 'line_sha256': 'a' * 64, 'reason': 'a stale entry for the command-line test'}]}))
    done = cli('--allow', str(stale), 'tree', str(tree / 'src' / '..'))
    assert done.returncode == 1 and 'stale-allow-entry' in done.stdout


def test_a_documented_default_is_not_a_secret_but_a_real_value_still_is(tmp_path):
    secret_file = tmp_path / '.env'
    secret_file.write_text('NEO4J_PASSWORD=changeme\nGRAFANA_ADMIN_PASSWORD=Zr58-private-grafana-9\n')
    allow = G.Allow({'documented_defaults': [{'value': 'changeme', 'reason': 'a documented default of the example stack'}]})
    secrets = G.secret_hashes([secret_file])
    for item in allow.defaults:
        secrets.pop(G.sha256(item['value']), None)
    assert scan(entry('docs/a.md', 'the old default was changeme\n'), secrets=secrets, allow=allow) == []
    assert 'secret-value' in rules(scan(entry('docs/a.md', 'pw Zr58-private-grafana-9\n'), secrets=secrets, allow=allow))
    with pytest.raises(G.GuardError):
        G.Allow({'documented_defaults': [{'value': 'x', 'reason': 'short'}]})
