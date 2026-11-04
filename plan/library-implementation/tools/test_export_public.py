"""Tests of the public export tool (private). Every remote is a local bare repository standing in for ``Neburyx/graph-loader``: nothing here touches the
network, the private remote or the real public repository, and nothing is ever pushed by the tool (the tests push by hand only to make a second export possible)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import export_guard as G  # noqa: E402
import export_public as E  # noqa: E402

IDENTITY = G.public_set()['identity']
EMPTY_ALLOW = G.Allow({})


def run_git(cwd, *args, check=True, **kw):
    done = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True, env=E.git_env(kw.pop('env', None)), **kw)
    assert done.returncode == 0 or not check, done.stderr
    return done.stdout.strip()


def leftovers(root):
    return sorted(p.name for p in root.iterdir()) if root.exists() else []


def refs_and_objects(bare):
    return run_git(bare, 'for-each-ref'), run_git(bare, 'count-objects', '-v')


@pytest.fixture
def bare(tmp_path):
    path = tmp_path / 'public.git'
    run_git(tmp_path, 'init', '--bare', '-q', '-b', 'main', str(path))
    return path


@pytest.fixture
def worktree(tmp_path):
    root = tmp_path / 'private'
    files = {'README.md': 'The loader.\n', 'pyproject.toml': '[project]\nname = "x"\n', 'src/pkg/__init__.py': 'VALUE = 1\n', 'src/pkg/gone.py': 'OLD = 1\n',
             'docs/a.md': 'Docs.\n', '.env.example': 'NEO4J_PASSWORD=change-this-local-password\n', 'tests/test_a.py': 'def test_a():\n    assert True\n',
             'plan/notes.md': 'internal\n', 'soak/run.py': 'x = 1\n', 'var/out.txt': 'local\n'}
    for name, text in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    run_git(root, 'init', '-q', '-b', 'main')
    run_git(root, 'add', '-A')
    run_git(root, 'commit', '-q', '-m', 'private history', env={'GIT_AUTHOR_NAME': 'Dev', 'GIT_AUTHOR_EMAIL': 'dev@private.example', 'GIT_COMMITTER_NAME': 'Dev',
                                                                'GIT_COMMITTER_EMAIL': 'dev@private.example'})
    return root


@pytest.fixture
def roots(tmp_path):
    return tmp_path / 'quarantine'


def prepare(worktree, bare, roots, tmp_path, message='Add the loader', parent='none', **kw):
    return E.prepare(worktree, message, parent, roots, local_remote=bare, allow=EMPTY_ALLOW, secrets={}, mapping=tmp_path / 'mapping.jsonl', **kw)


def publish_by_hand(result, bare):
    """What the user's separate instruction would do; the tool itself never pushes."""
    run_git(result['clone'], 'push', '-q', str(bare), 'main')


# -- the dry export --------------------------------------------------------------------------------------------------------------------------------
def test_a_first_export_prints_the_file_list_the_diff_and_the_proposed_reference_and_pushes_nothing(worktree, bare, roots, tmp_path):
    before = refs_and_objects(bare)
    result = prepare(worktree, bare, roots, tmp_path)
    assert refs_and_objects(bare) == before == ('', before[1]), 'the public repository is untouched'
    names = Path(result['file_list']).read_text().splitlines()
    assert sorted(line.split('\t', 1)[1] for line in names) == ['.env.example', 'README.md', 'docs/a.md', 'pyproject.toml', 'src/pkg/__init__.py', 'src/pkg/gone.py',
                                                                'tests/test_a.py'] and all(line.startswith('A\t') for line in names)
    assert 'The loader.' in Path(result['diff']).read_text()
    assert result['proposed_reference'] == 'refs/heads/main' and result['parent'] is None and result['changes']['A'] == 7
    clone = Path(result['clone'])
    assert run_git(clone, 'show', '--format=%an <%ae>|%cn <%ce>|%s', '-s', 'HEAD') == f'{IDENTITY["name"]} <{IDENTITY["email"]}>|{IDENTITY["name"]} <{IDENTITY["email"]}>|Add the loader'
    assert not any((clone / part).exists() for part in ('plan', 'soak', 'var')), 'excluded paths never reach the public tree'
    assert run_git(clone, 'rev-list', '--count', 'HEAD') == '1'


def test_the_private_to_public_mapping_is_recorded(worktree, bare, roots, tmp_path):
    result = prepare(worktree, bare, roots, tmp_path)
    record = json.loads((tmp_path / 'mapping.jsonl').read_text().splitlines()[-1])
    assert record['private_head'] == run_git(worktree, 'rev-parse', 'HEAD') and record['private_source'] == 'worktree'
    assert record['public_commit'] == result['commit'] and record['public_parent'] is None and record['status'] == 'proposed'
    assert len(record['materialized_tree_sha256']) == 64 and record['public_tree'] == run_git(result['clone'], 'rev-parse', 'HEAD^{tree}')


def test_the_clone_cannot_push_and_keeps_its_identity_to_itself(worktree, bare, roots, tmp_path):
    result = prepare(worktree, bare, roots, tmp_path)
    clone = Path(result['clone'])
    assert run_git(clone, 'config', '--local', 'remote.origin.pushurl') == 'DISABLED-by-export-public-no-push'
    assert subprocess.run(['git', '-C', str(clone), 'push', 'origin', 'main'], capture_output=True, env=E.git_env()).returncode != 0
    assert run_git(clone, 'config', '--local', 'user.email') == IDENTITY['email'] and run_git(clone, 'config', '--local', 'commit.gpgsign') == 'false'


def test_the_global_git_configuration_is_neither_read_nor_changed(worktree, bare, roots, tmp_path, monkeypatch):
    home = tmp_path / 'home'
    home.mkdir()
    config = home / '.gitconfig'
    config.write_text('[user]\n\tname = Machine Local\n\temail = machine@host.local\n[commit]\n\tgpgsign = true\n')
    monkeypatch.setenv('HOME', str(home))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(home / 'xdg'))
    monkeypatch.delenv('GIT_CONFIG_GLOBAL', raising=False)
    before = config.read_bytes()
    result = prepare(worktree, bare, roots, tmp_path)
    assert config.read_bytes() == before and list(home.iterdir()) == [config]
    assert run_git(result['clone'], 'log', '-1', '--format=%ae|%ce', env={'HOME': str(home)}) == f'{IDENTITY["email"]}|{IDENTITY["email"]}'


def test_a_named_private_commit_is_exported_not_the_worktree(worktree, bare, roots, tmp_path):
    head = run_git(worktree, 'rev-parse', 'HEAD')
    (worktree / 'README.md').write_text('changed after the commit\n')
    (worktree / 'src/pkg/new.py').write_text('NEW = 1\n')
    result = prepare(worktree, bare, roots, tmp_path, commit=head)
    clone = Path(result['clone'])
    assert (clone / 'README.md').read_text() == 'The loader.\n' and not (clone / 'src/pkg/new.py').exists()
    assert json.loads((tmp_path / 'mapping.jsonl').read_text().splitlines()[-1])['private_source'] == head


# -- full replacement ------------------------------------------------------------------------------------------------------------------------------
def test_control_the_full_replacement_tree_deletes_files_absent_from_the_new_set(worktree, bare, roots, tmp_path):
    first = prepare(worktree, bare, roots, tmp_path)
    publish_by_hand(first, bare)
    E.discard(first['quarantine'])
    (worktree / 'src/pkg/gone.py').unlink()
    (worktree / 'docs/a.md').write_text('Docs, revised.\n')
    (worktree / 'docs/b.md').write_text('New page.\n')
    second = prepare(worktree, bare, roots, tmp_path, 'Update the loader', parent=first['commit'])
    clone = Path(second['clone'])
    assert not (clone / 'src/pkg/gone.py').exists(), 'a file absent from the new manifest is deleted'
    changes = dict(line.split('\t', 1)[::-1] for line in Path(second['file_list']).read_text().splitlines())
    assert changes == {'src/pkg/gone.py': 'D', 'docs/a.md': 'M', 'docs/b.md': 'A'}
    assert second['parent'] == first['commit'] and run_git(clone, 'rev-parse', 'HEAD~1') == first['commit'] and '-OLD = 1' in Path(second['diff']).read_text()


def test_an_export_identical_to_the_last_public_snapshot_is_refused(worktree, bare, roots, tmp_path):
    first = prepare(worktree, bare, roots, tmp_path)
    publish_by_hand(first, bare)
    E.discard(first['quarantine'])
    with pytest.raises(E.ExportError, match='nothing to export'):
        prepare(worktree, bare, roots, tmp_path, 'Same again', parent=first['commit'])
    assert leftovers(roots) == []


# -- rejection, then a corrected export ----------------------------------------------------------------------------------------------------------
def test_a_rejected_export_leaves_nothing_reachable_and_a_corrected_export_carries_none_of_it(worktree, bare, roots, tmp_path):
    planted = worktree / 'src/pkg/leak.py'
    planted.write_text('# generated with Claude\nVALUE = "private"\n')
    blob = run_git(worktree, 'hash-object', str(planted))
    before = refs_and_objects(bare)
    with pytest.raises(E.Rejected) as rejected:
        prepare(worktree, bare, roots, tmp_path)
    assert [h.rule for h in rejected.value.hits] == ['denylist-word'] and rejected.value.hits[0].path == 'src/pkg/leak.py' and rejected.value.hits[0].line == 1
    assert leftovers(roots) == [], 'the quarantine clone is deleted'
    assert refs_and_objects(bare) == before and not (tmp_path / 'mapping.jsonl').exists(), 'no reference, object or mapping record'
    planted.write_text('VALUE = "private"\n')
    corrected = prepare(worktree, bare, roots, tmp_path)
    clone = Path(corrected['clone'])
    assert run_git(clone, 'cat-file', '-t', blob, check=False) == '', 'the rejected blob is not in the corrected clone'
    reachable = run_git(clone, 'rev-list', '--objects', '--all')
    assert blob not in reachable and 'Claude' not in run_git(clone, 'log', '-p', '--all')


def test_the_guard_runs_before_anything_is_committed(worktree, bare, roots, tmp_path):
    (worktree / 'src/pkg/leak.py').write_text('x = 1  # plan/v6/notes.md\n')
    with pytest.raises(E.Rejected):
        prepare(worktree, bare, roots, tmp_path, keep_on_reject=True)
    [kept] = list(roots.iterdir())
    clone = kept / 'clone'
    assert subprocess.run(['git', '-C', str(clone), 'rev-parse', '--verify', 'HEAD'], capture_output=True, env=E.git_env()).returncode != 0, 'no commit exists'
    assert not (clone / 'src').exists() or not (clone / 'src/pkg/leak.py').exists()
    E.discard(kept)


@pytest.mark.parametrize('message', ['Made with Claude', 'Add it\n\nCo-Authored-By: A <a@example.org>', 'See plan/v6/stories.md', '  \n'])
def test_a_forbidden_or_empty_message_is_rejected_before_the_commit(worktree, bare, roots, tmp_path, message):
    with pytest.raises((E.Rejected, E.ExportError)):
        prepare(worktree, bare, roots, tmp_path, message)
    assert leftovers(roots) == [] and refs_and_objects(bare)[0] == ''


def test_the_second_guard_pass_on_the_new_commit_also_deletes_the_quarantine(worktree, bare, roots, tmp_path, monkeypatch):
    monkeypatch.setattr(G, 'check_commit', lambda *a, **k: [G.Hit('commit-identity', 'abc', 0, 'forced for the test')])
    with pytest.raises(E.Rejected) as rejected:
        prepare(worktree, bare, roots, tmp_path)
    assert rejected.value.hits[0].rule == 'commit-identity' and leftovers(roots) == [] and refs_and_objects(bare)[0] == ''


def test_a_real_secret_value_in_the_tree_rejects_the_export(worktree, bare, roots, tmp_path):
    (worktree / 'docs/a.md').write_text('use Zk39-private-value-0042 here\n')
    secret_file = tmp_path / 'secrets.env'
    secret_file.write_text('PYPI_TOKEN=Zk39-private-value-0042\n')
    with pytest.raises(E.Rejected) as rejected:
        E.prepare(worktree, 'Add', 'none', roots, local_remote=bare, allow=EMPTY_ALLOW, secrets=G.secret_hashes([secret_file]), mapping=tmp_path / 'm.jsonl')
    assert [h.rule for h in rejected.value.hits] == ['secret-value'] and 'Zk39' not in ' '.join(h.render() for h in rejected.value.hits)


# -- remotes and state -----------------------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('url', [
    'https://github.com/shashankshukla96/graph-loader', 'https://github.com/shashankshukla96/graph-loader.git', 'git@github.com:shashankshukla96/graph-loader.git',
    'ssh://git@github.com/ShashankShukla96/Graph-Loader.git', 'HTTPS://GITHUB.COM/shashankshukla96/graph-loader/', 'https://x:token@github.com/shashankshukla96/graph-loader.git',
    'github.com:shashankshukla96/graph-loader', 'https://example.org/mirror/shashankshukla96/graph-loader.git', '/srv/git/shashankshukla96/graph-loader.git'])
def test_control_the_private_repository_is_refused_in_every_spelling(url, worktree, roots, tmp_path):
    with pytest.raises(E.ExportError, match='private repository'):
        E.prepare(worktree, 'Add', 'none', roots, remote=url, allow=EMPTY_ALLOW, secrets={}, mapping=tmp_path / 'm.jsonl')
    assert not roots.exists() or leftovers(roots) == [], 'refused before any clone is made'


@pytest.mark.parametrize('url', ['https://github.com/Neburyx/other-repo.git', 'https://github.com/someone/graph-loader.git', 'https://gitlab.com/Neburyx/graph-loader.git',
                                 'file:///srv/x.git', 'relative/path', '', 'git@github.com:Neburyx/graph-loader-fork.git'])
def test_control_only_the_public_repository_is_accepted(url):
    with pytest.raises(E.ExportError, match='only public repository'):
        E.check_remote(url)


def test_the_public_repository_in_its_spellings_is_accepted_without_a_clone():
    for url in ('https://github.com/Neburyx/graph-loader.git', 'https://github.com/Neburyx/graph-loader', 'git@github.com:Neburyx/graph-loader.git',
                'https://github.com/neburyx/graph-loader/'):
        assert E.check_remote(url) == E.PUBLIC_REMOTE


def test_a_local_test_remote_must_be_a_bare_repository_and_never_the_private_one(tmp_path, worktree):
    with pytest.raises(E.ExportError, match='bare repository'):
        E.check_remote('', local_remote=tmp_path / 'absent')
    private = tmp_path / 'shashankshukla96' / 'graph-loader.git'
    private.mkdir(parents=True)
    run_git(private, 'init', '--bare', '-q')
    with pytest.raises(E.ExportError, match='private repository'):
        E.check_remote('', local_remote=private)


def test_the_remote_of_the_worktree_is_never_a_target(worktree, tmp_path):
    run_git(worktree, 'remote', 'add', 'origin', 'https://github.com/shashankshukla96/graph-loader.git')
    with pytest.raises(E.ExportError, match='private'):
        E.check_remote('https://github.com/shashankshukla96/graph-loader.git', worktree)


def test_the_expected_parent_must_match_the_state_of_the_public_repository(worktree, bare, roots, tmp_path):
    first = prepare(worktree, bare, roots, tmp_path)
    publish_by_hand(first, bare)
    E.discard(first['quarantine'])
    (worktree / 'docs/b.md').write_text('more\n')
    with pytest.raises(E.ExportError, match='not at the expected'):
        prepare(worktree, bare, roots, tmp_path, parent='none')
    with pytest.raises(E.ExportError, match='not at the expected'):
        prepare(worktree, bare, roots, tmp_path, parent='1' * 40)
    with pytest.raises(E.ExportError, match='40-hex'):
        prepare(worktree, bare, roots, tmp_path, parent='main')
    assert leftovers(roots) == []


def test_the_quarantine_must_be_outside_the_repositories(worktree, bare, tmp_path):
    for inside in (worktree / 'q', worktree / '.git' / 'q', worktree):
        with pytest.raises(E.ExportError, match='outside every repository'):
            E.prepare(worktree, 'Add', 'none', inside, local_remote=bare, allow=EMPTY_ALLOW, secrets={}, mapping=tmp_path / 'm.jsonl')


# -- tags ------------------------------------------------------------------------------------------------------------------------------------------
def test_an_annotated_tag_is_created_on_a_validated_commit_with_the_public_tagger(worktree, bare, roots, tmp_path):
    result = prepare(worktree, bare, roots, tmp_path)
    tagged = E.make_tag(result['quarantine'], 'v0.1.0a2', result['commit'], 'neo4j-graph-loader 0.1.0a2', EMPTY_ALLOW)
    clone = Path(result['clone'])
    assert tagged['target'] == result['commit'] and run_git(clone, 'cat-file', '-t', 'refs/tags/v0.1.0a2') == 'tag'
    assert run_git(clone, 'for-each-ref', '--format=%(taggername) <%(taggeremail)>', 'refs/tags/v0.1.0a2').replace('<<', '<').replace('>>', '>') == \
        f'{IDENTITY["name"]} <{IDENTITY["email"]}>'
    assert G.check_tag(G.Guard(EMPTY_ALLOW), clone, 'v0.1.0a2', {result['commit']}) == []
    assert refs_and_objects(bare)[0] == '', 'the tag exists only in the quarantine'


def test_control_a_tag_on_anything_but_a_validated_public_commit_is_refused(worktree, bare, roots, tmp_path):
    result = prepare(worktree, bare, roots, tmp_path)
    clone = Path(result['clone'])
    (clone / 'extra.md').write_text('not validated\n')
    run_git(clone, 'add', 'extra.md')
    run_git(clone, 'commit', '-q', '-m', 'Manual commit', env=E.identity_env(G.public_set()))
    manual = run_git(clone, 'rev-parse', 'HEAD')
    for target in (manual, 'HEAD', 'main', '0' * 40, 'refs/heads/nope'):
        with pytest.raises(E.ExportError, match='validated public commit'):
            E.make_tag(result['quarantine'], 'v0.1.0a2', target, 'Release', EMPTY_ALLOW)
    assert run_git(clone, 'tag') == ''


def test_control_a_forbidden_tag_message_and_a_bad_tag_name_are_refused(worktree, bare, roots, tmp_path):
    result = prepare(worktree, bare, roots, tmp_path)
    with pytest.raises(E.Rejected):
        E.make_tag(result['quarantine'], 'v0.1.0a2', result['commit'], 'Release prepared with Claude', EMPTY_ALLOW)
    with pytest.raises(E.ExportError, match='version'):
        E.make_tag(result['quarantine'], 'main', result['commit'], 'Release', EMPTY_ALLOW)
    with pytest.raises(E.ExportError, match='version'):
        E.make_tag(result['quarantine'], '-d', result['commit'], 'Release', EMPTY_ALLOW)
    assert run_git(result['clone'], 'tag') == ''


def test_the_tag_command_refuses_a_directory_that_is_not_a_quarantine(tmp_path):
    with pytest.raises(E.ExportError, match='not an export quarantine'):
        E.make_tag(tmp_path, 'v1', 'a' * 40, 'Release', EMPTY_ALLOW)
    with pytest.raises(E.ExportError, match='refusing to delete'):
        E.discard(tmp_path)
    assert tmp_path.exists()


# -- the tool never pushes -----------------------------------------------------------------------------------------------------------------------
def test_the_tool_has_no_push_code_path():
    source = (HERE / 'export_public.py').read_text()
    code = [line for line in source.splitlines() if not line.strip().startswith(('#', '"""')) and 'pushurl' not in line]
    assert not any(re.search(r"git\([^)]*'push'|\['git'[^]]*'push'|\"push\"", line) for line in code)
    assert "'remote', 'add'" not in source and "'remote', 'set-url'" not in source and "'config', '--global'" not in source and '--system' not in source


# -- the command line ---------------------------------------------------------------------------------------------------------------------------
def cli(*args):
    return subprocess.run([sys.executable, str(HERE / 'export_public.py'), *args], capture_output=True, text=True, env={**os.environ, 'GIT_CONFIG_GLOBAL': os.devnull})


def test_the_command_line_exit_codes(worktree, bare, roots, tmp_path):
    allow = tmp_path / 'allow.json'
    allow.write_text('{}')
    done = cli('prepare', '--source', str(worktree), '--message', 'Add the loader', '--expect-parent', 'none', '--quarantine-root', str(roots), '--local-remote', str(bare),
               '--allow', str(allow), '--mapping', str(tmp_path / 'm.jsonl'))
    assert done.returncode == 0 and 'proposed: refs/heads/main ->' in done.stdout and 'nothing is pushed' in done.stdout
    quarantine = Path(done.stdout.split('quarantine clone: ')[1].split(' ')[0]).parent
    assert cli('discard', '--quarantine', str(quarantine)).returncode == 0 and not quarantine.exists()
    done = cli('prepare', '--source', str(worktree), '--message', 'Add', '--expect-parent', 'none', '--quarantine-root', str(roots),
               '--remote', 'https://github.com/shashankshukla96/graph-loader.git', '--allow', str(allow))
    assert done.returncode == 2 and 'private repository' in done.stderr
    (worktree / 'src/pkg/leak.py').write_text('x = 1  # codex\n')
    done = cli('prepare', '--source', str(worktree), '--message', 'Add', '--expect-parent', 'none', '--quarantine-root', str(roots), '--local-remote', str(bare),
               '--allow', str(allow), '--mapping', str(tmp_path / 'm.jsonl'))
    assert done.returncode == 1 and 'REJECTED' in done.stdout and 'src/pkg/leak.py:1' in done.stdout and leftovers(roots) == []
