#!/usr/bin/env python3
"""Public export tool (private): prepares a clean snapshot of the product for ``Neburyx/graph-loader`` in a quarantine clone. It never pushes.

    export_public.py prepare  --source WORKTREE [--commit REV] --message TEXT --expect-parent {none|SHA} --quarantine-root DIR [--local-remote BARE_REPO]
    export_public.py tag      --quarantine DIR --name vX --target SHA --message TEXT
    export_public.py discard  --quarantine DIR

``prepare`` (D19-7): refuses any remote other than the public repository (and always the private one), clones it into a fresh directory outside both
repositories whose last approved public commit must be the one you name, materializes the allowlisted files of the worktree (or of a named private commit)
as a **full replacement tree** so files absent from the new set are deleted, runs the guard on that tree and on the message **before anything is committed**,
only then commits with the D19-1 identity and your message, runs the guard again on the new commit (tree and metadata), and prints the file list, the diff
against the last public snapshot and the proposed reference. A rejection deletes the quarantine clone, so rejected content is reachable from no reference
that could be proposed for a push. The mapping private source -> public commit is appended to a private file. Pushing is not part of this tool: it is a
separate step that needs the user's instruction each time. ``tag`` creates an annotated tag only on a validated public commit, with the D19-1 tagger, and
runs the tag checks. Local identity, signing and hook settings are written to the clone's own config only; the global Git configuration is never read or
changed (a scratch ``GIT_CONFIG_GLOBAL``) and ``remote.origin.pushurl`` is set to an invalid value so a stray ``git push`` in the clone cannot work.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import export_guard as G  # noqa: E402

PUBLIC_REMOTE = 'https://github.com/Neburyx/graph-loader.git'
PRIVATE_SLUG = 'shashankshukla96/graph-loader'
PUBLIC_SLUG = 'neburyx/graph-loader'
MARKER = '.export-quarantine'
MAPPING = G.EXPORT_DIR / 'mapping.jsonl'
BRANCH = 'main'


class ExportError(Exception):
    pass


class Rejected(ExportError):
    def __init__(self, hits):
        super().__init__(f'{len(hits)} guard hit(s)')
        self.hits = hits


# -- remotes ---------------------------------------------------------------------------------------------------------------------------------
def slug_of(url: str) -> str | None:
    """``owner/repo`` (lower case, without ``.git``) of a GitHub URL in any of its forms, else None."""
    text = url.strip().lower()
    for pattern in (r'^(?:https?|ssh|git)://(?:[^/@]+@)?(?:www\.)?github\.com[:/]+([^/]+/[^/]+?)(?:\.git)?/?$', r'^[^/@]+@github\.com:([^/]+/[^/]+?)(?:\.git)?/?$',
                    r'^github\.com[:/]+([^/]+/[^/]+?)(?:\.git)?/?$'):
        match = re.match(pattern, text)
        if match:
            return match.group(1)
    return None


def check_remote(remote: str, source: Path | None = None, local_remote: Path | None = None) -> str:
    """Return the URL to clone. The private repository is refused in every spelling; only the public one is accepted, or a local bare repository for tests."""
    if local_remote is not None:
        path = Path(local_remote).resolve()
        if not path.is_dir() or not (path / 'HEAD').exists():
            raise ExportError('--local-remote must be an existing bare repository directory')
        if PRIVATE_SLUG in str(path).lower():
            raise ExportError('refusing the private repository')
        return str(path)
    slug = slug_of(remote)
    if slug == PRIVATE_SLUG or PRIVATE_SLUG in remote.lower():
        raise ExportError('refusing to target the private repository shashankshukla96/graph-loader')
    if slug != PUBLIC_SLUG:
        raise ExportError(f'the only public repository is {PUBLIC_REMOTE}')
    if source is not None:
        done = subprocess.run(['git', '-C', str(source), 'remote', '-v'], capture_output=True, text=True)
        for line in done.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and slug_of(parts[1]) == PRIVATE_SLUG and remote.strip() == parts[1]:
                raise ExportError('refusing the private remote of the worktree')
    return PUBLIC_REMOTE


# -- git helpers (a scratch global config, no prompts) ---------------------------------------------------------------------------------------------
def git_env(extra: dict | None = None) -> dict:
    return {**os.environ, 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_SYSTEM': os.devnull, 'GIT_TERMINAL_PROMPT': '0', 'GIT_ASKPASS': 'true', **(extra or {})}


def git(repo: Path, *args: str, env: dict | None = None, check: bool = True, input_text: str | None = None) -> subprocess.CompletedProcess:
    done = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True, env=git_env(env), input=input_text)
    if check and done.returncode:
        raise ExportError(f'git {args[0]} failed: {done.stderr.strip()[:300]}')
    return done


def identity_env(config: dict, when: str | None = None) -> dict:
    who = config['identity']
    stamp = when or datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+0000')
    return {'GIT_AUTHOR_NAME': who['name'], 'GIT_AUTHOR_EMAIL': who['email'], 'GIT_COMMITTER_NAME': who['name'], 'GIT_COMMITTER_EMAIL': who['email'],
            'GIT_AUTHOR_DATE': stamp, 'GIT_COMMITTER_DATE': stamp}


def tree_digest(entries: list) -> str:
    return hashlib.sha256(json.dumps(sorted((e.path, e.mode, G.sha256(e.data or b'')) for e in entries)).encode()).hexdigest()


# -- sources ---------------------------------------------------------------------------------------------------------------------------------------
def materialize_commit(repo: Path, rev: str, dest: Path, config: dict) -> dict:
    """The public-set files of a *private commit*, written into the new directory ``dest``."""
    if dest.exists():
        raise ExportError('the destination must be a new directory')
    top_dirs, top_files = set(config['directories']), set(config['files'])
    copied = skipped = 0
    for entry in G.git_entries(repo, rev):
        parts = entry.path.split('/')
        keep = ((parts[0] in top_dirs and len(parts) > 1) or (len(parts) == 1 and parts[0] in top_files)) and entry.path not in config['excluded_paths'] \
            and not any(p in config['excluded_names'] for p in parts) and not entry.path.endswith(tuple(config['excluded_suffixes']))
        if not keep:
            skipped += 1
            continue
        target = dest / entry.path
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry.mode == '120000':
            target.symlink_to(git(repo, 'cat-file', 'blob', git(repo, 'rev-parse', f'{rev}:{entry.path}').stdout.strip()).stdout)
        elif entry.mode in G.REGULAR:
            target.write_bytes(entry.data)
            if entry.mode == '100755':
                target.chmod(0o755)
        else:
            raise ExportError(f'{entry.path}: unsupported entry mode {entry.mode}')
        copied += 1
    return {'files': copied, 'excluded_entries': skipped}


def listing(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file() or p.is_symlink())


# -- prepare ---------------------------------------------------------------------------------------------------------------------------------------
def outside_repositories(path: Path, source: Path) -> None:
    """The quarantine must be outside the worktree, the repository it belongs to and the main checkout."""
    resolved = path.resolve()
    forbidden = {Path(source).resolve()}
    for query in ('--show-toplevel', '--git-common-dir'):
        done = subprocess.run(['git', '-C', str(source), 'rev-parse', '--path-format=absolute', query], capture_output=True, text=True)
        if done.returncode == 0:
            found = Path(done.stdout.strip())
            forbidden.add(found.parent if found.name == '.git' else found)
    for repo in forbidden:
        if resolved == repo or repo in resolved.parents:
            raise ExportError(f'the quarantine must be outside every repository ({repo})')


def prepare(source: Path, message: str, expect_parent: str, quarantine_root: Path, *, commit: str | None = None, remote: str = PUBLIC_REMOTE,
            local_remote: Path | None = None, allow: G.Allow | None = None, secrets: dict | None = None, mapping: Path | None = None,
            when: str | None = None, keep_on_reject: bool = False) -> dict:
    config = G.public_set()
    source, quarantine_root = Path(source).resolve(), Path(quarantine_root).resolve()
    url = check_remote(remote, source, local_remote)
    if not message.strip():
        raise ExportError('a commit message is required (it is approved by the user)')
    if expect_parent != 'none' and not re.fullmatch(r'[0-9a-f]{40}', expect_parent):
        raise ExportError('--expect-parent is "none" (the first export) or the 40-hex id of the last approved public commit')
    quarantine_root.mkdir(parents=True, exist_ok=True)
    outside_repositories(quarantine_root, source)
    work = Path(tempfile.mkdtemp(prefix='export-', dir=quarantine_root))
    (work / MARKER).write_text('owned by export_public.py; safe to delete\n')
    clone = work / 'clone'
    try:
        done = subprocess.run(['git', 'clone', '--quiet', '--no-hardlinks', url, str(clone)], capture_output=True, text=True, env=git_env())
        if done.returncode:
            raise ExportError('cannot clone the public repository: ' + done.stderr.strip()[:300])
        for key, value in (('user.name', config['identity']['name']), ('user.email', config['identity']['email']), ('commit.gpgsign', 'false'),
                           ('tag.gpgsign', 'false'), ('core.hooksPath', os.devnull), ('remote.origin.pushurl', 'DISABLED-by-export-public-no-push'),
                           ('advice.detachedHead', 'false')):
            git(clone, 'config', '--local', key, value)
        head = git(clone, 'rev-parse', '--verify', 'HEAD', check=False)
        actual_parent = head.stdout.strip() if head.returncode == 0 else 'none'
        if actual_parent != expect_parent:
            raise ExportError(f'the public repository is at {actual_parent[:12]}, not at the expected {expect_parent[:12]}: refusing to build on an unexpected state')
        git(clone, 'checkout', '-q', '-B', BRANCH)

        staging = work / 'staging'
        summary = materialize_commit(source, commit, staging, config) if commit else G.materialize(source, staging, config)
        private_head = git(source, 'rev-parse', commit or 'HEAD').stdout.strip()
        guard = G.Guard(allow or G.Allow.from_file(None), secrets, config)
        entries = G.tree_entries(staging)
        hits = guard.scan(entries) + guard.check_text(message, 'commit message')
        if hits:
            raise Rejected(hits)

        for child in clone.iterdir():                              # full replacement: everything but .git goes
            if child.name != '.git':
                shutil.rmtree(child) if child.is_dir() and not child.is_symlink() else child.unlink()
        for child in staging.iterdir():
            shutil.move(str(child), clone / child.name)
        git(clone, 'add', '-A', '--force')
        if not git(clone, 'status', '--porcelain').stdout.strip():
            raise ExportError('nothing to export: the public tree is identical to the last public snapshot')
        message_file = work / 'message.txt'
        message_file.write_text(message.rstrip('\n') + '\n', encoding='utf-8')
        git(clone, 'commit', '--quiet', '--no-verify', '--no-gpg-sign', '-F', str(message_file), env=identity_env(config, when))
        new = git(clone, 'rev-parse', 'HEAD').stdout.strip()

        parent_ok = set() if expect_parent == 'none' else {expect_parent}
        after = guard.scan(G.git_entries(clone, new)) + G.check_commit(guard, clone, new, parent_ok, set())
        if after:
            raise Rejected(after)
        (clone / '.git' / 'export-validated').write_text('\n'.join(sorted(parent_ok | {new})) + '\n')
    except (Rejected, ExportError):
        if not keep_on_reject:
            shutil.rmtree(work, ignore_errors=True)                 # nothing rejected stays reachable
        raise
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        raise

    if expect_parent == 'none':
        names = ''.join(f'A\t{path}\n' for path in git(clone, 'ls-tree', '-r', '--name-only', new).stdout.splitlines())
        patch = git(clone, 'show', '--no-renames', '--format=', new).stdout
    else:
        names = git(clone, 'diff', '--name-status', '--no-renames', expect_parent, new).stdout
        patch = git(clone, 'diff', '--no-renames', expect_parent, new).stdout
    report = work / 'report'
    report.mkdir()
    (report / 'files.txt').write_text(names, encoding='utf-8')
    (report / 'diff.patch').write_text(patch, encoding='utf-8')
    status = {'A': 0, 'M': 0, 'D': 0}
    for line in names.splitlines():
        status[line[0]] = status.get(line[0], 0) + 1
    record = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'), 'private_head': private_head,
              'private_source': commit or 'worktree', 'materialized_tree_sha256': tree_digest(entries), 'public_parent': None if expect_parent == 'none' else expect_parent,
              'public_commit': new, 'public_tree': git(clone, 'rev-parse', f'{new}^{{tree}}').stdout.strip(), 'remote': PUBLIC_REMOTE if local_remote is None else 'local test remote',
              'status': 'proposed'}
    with open(mapping or MAPPING, 'a', encoding='utf-8') as handle:
        handle.write(json.dumps(record) + '\n')
    return {'quarantine': str(work), 'clone': str(clone), 'commit': new, 'parent': record['public_parent'], 'proposed_reference': f'refs/heads/{BRANCH}',
            'files': summary['files'], 'changes': status, 'file_list': str(report / 'files.txt'), 'diff': str(report / 'diff.patch'), 'mapping': record}


# -- tags ------------------------------------------------------------------------------------------------------------------------------------------
def make_tag(quarantine: Path, name: str, target: str, message: str, allow: G.Allow | None = None) -> dict:
    work = Path(quarantine).resolve()
    if not (work / MARKER).exists():
        raise ExportError('not an export quarantine')
    clone = work / 'clone'
    if not re.fullmatch(r'v?[0-9][A-Za-z0-9._-]*', name):
        raise ExportError('the tag name must look like a version (v0.1.0a2)')
    validated = set((clone / '.git' / 'export-validated').read_text().split())
    full = git(clone, 'rev-parse', '--verify', f'{target}^{{commit}}', check=False).stdout.strip()
    if not full or full not in validated:
        raise ExportError('tags are created only on a validated public commit of this quarantine')
    config = G.public_set()
    guard = G.Guard(allow or G.Allow.from_file(None), None, config)
    pre = guard.check_text(message, 'tag message')
    if pre:
        raise Rejected(pre)
    git(clone, 'tag', '-a', name, full, '-m', message, env=identity_env(config))
    hits = G.check_tag(guard, clone, name, validated)
    if hits:
        git(clone, 'tag', '-d', name, check=False)
        raise Rejected(hits)
    return {'tag': name, 'reference': f'refs/tags/{name}', 'target': full}


def discard(quarantine: Path) -> None:
    work = Path(quarantine).resolve()
    if not (work / MARKER).exists():
        raise ExportError('not an export quarantine: refusing to delete')
    shutil.rmtree(work)


# -- command line ----------------------------------------------------------------------------------------------------------------------------------
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    sub = commands.add_parser('prepare')
    sub.add_argument('--source', type=Path, required=True)
    sub.add_argument('--commit')
    sub.add_argument('--message', required=True)
    sub.add_argument('--expect-parent', required=True)
    sub.add_argument('--quarantine-root', type=Path, required=True)
    sub.add_argument('--remote', default=PUBLIC_REMOTE)
    sub.add_argument('--local-remote', type=Path, help='a local bare repository standing in for the public one (tests)')
    sub.add_argument('--allow', type=Path)
    sub.add_argument('--secret-file', type=Path, action='append', default=[])
    sub.add_argument('--mapping', type=Path)
    sub = commands.add_parser('tag')
    sub.add_argument('--quarantine', type=Path, required=True)
    sub.add_argument('--name', required=True)
    sub.add_argument('--target', required=True)
    sub.add_argument('--message', required=True)
    sub.add_argument('--allow', type=Path)
    sub = commands.add_parser('discard')
    sub.add_argument('--quarantine', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'discard':
            discard(args.quarantine)
            print('quarantine removed')
            return 0
        allow = G.Allow.from_file(args.allow)
        if args.command == 'tag':
            print(json.dumps(make_tag(args.quarantine, args.name, args.target, args.message, allow), indent=2))
            return 0
        example = args.source / '.env.example'
        secrets = G.secret_hashes(args.secret_file, example.read_text(errors='replace') if example.is_file() else '')
        for item in allow.defaults:
            secrets.pop(G.sha256(item['value']), None)
        result = prepare(args.source, args.message, args.expect_parent, args.quarantine_root, commit=args.commit, remote=args.remote,
                         local_remote=args.local_remote, allow=allow, secrets=secrets, mapping=args.mapping)
        print(f'proposed: {result["proposed_reference"]} -> {result["commit"]}  (parent {result["parent"] or "none"})')
        print(f'files in the public tree: {result["files"]}; against the last snapshot: {result["changes"]}')
        print(f'file list: {result["file_list"]}\ndiff:      {result["diff"]}\nquarantine clone: {result["clone"]} (nothing is pushed; discard it when done)')
        return 0
    except Rejected as rejection:
        if args.command == 'tag':
            print('REJECTED: no tag was created')
        else:
            print('REJECTED: the quarantine clone was deleted and nothing was committed or proposed')
        for hit in rejection.hits:
            print('  ' + hit.render())
        return 1
    except (ExportError, G.GuardError) as exc:
        print(f'export error: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
