#!/usr/bin/env python3
"""Export guard (private): decides whether a tree, a commit or a tag may be published in the public repository. It fails closed.

    export_guard.py materialize WORKTREE DEST        copy the allowlisted product files of a worktree into a new directory
    export_guard.py tree DIR                         scan a materialized tree
    export_guard.py commits REPO REV [REV ...]       scan the tree of each commit and check its author, committer and message
    export_guard.py tags REPO TAG [TAG ...] --validated REV ...   check annotated tags and their targets

Options: --allow FILE (reviewed allow-entries and binary list; default export/guard-allow.json), --secret-file FILE (repeatable: private credential
files whose values must not occur; compared by sha256, never printed), --json, --parent-ok REV (a commit that may be a parent: the last public commit).

What it checks (D19-7): the type of every entry (regular files 100644/100755 only: no symlink, gitlink, submodule or nested ``.git``); UTF-8 text;
binary files only by exact path and sha256 from the reviewed list (archives included); path and file-name safety; that every path is in the public set
(an explicit allowlist of top-level paths); a case-insensitive denylist of AI-tool and agent words, internal plan pointers, RFC 1918 addresses, local
user paths, e-mail addresses other than the public contact and the noreply address, credential patterns and the values of the private credential files;
for commits the D19-1 identity as author and committer and no trailers; for annotated tags the tagger, the message and a peeled target that is a
validated public commit with only public ancestry. Allow-entries are exact (rule, path, sha256 of the line, reason) and never directory-wide; an entry
that matches nothing is itself an error, so the list stays minimal. A denylist cannot prove absence (paraphrase, encodings, images): the file list and
the diff are reviewed by a person before every push.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import unicodedata

HERE = Path(__file__).resolve().parent
EXPORT_DIR = HERE.parent / 'export'
REGULAR = {'100644', '100755'}
MAX_COMPONENT, MAX_PATH = 255, 4096
WINDOWS_RESERVED = re.compile(r'(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$')

# word rules: (rule, regex). Content rules run per line; the path rule set runs on every path as well.
WORDS = re.compile(r'(?i)claude|codex|anthropic|openai|gpt-|subagent|cartographer|\.agents|\.claude|co-authored-by')
PLAN_POINTER = re.compile(r'(?i)plan/(?:v[0-9]|library-implementation|[a-z0-9-]+\.md)')
RFC1918 = re.compile(r'(?<![\d.])(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})(?!\d)')
LOCAL_PATH = re.compile(r'/Users/|(?i:/home/)')
EMAIL = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}')
CREDENTIALS = [
    ('credential-private-key', re.compile(r'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----')),
    ('credential-pem-header', re.compile(r'-----BEGIN')),
    ('credential-aws-key', re.compile(r'AKIA[0-9A-Z]{16}')),
    ('credential-github-token', re.compile(r'gh[pousr]_[A-Za-z0-9]{36,}|github_pat_')),
    ('credential-pypi-token', re.compile(r'pypi-AgE')),
    ('credential-slack-token', re.compile(r'xox[abpr]-')),
    ('credential-url-userinfo', re.compile(r'[A-Za-z][A-Za-z0-9+.-]*://[^/\s@\'"<>]+@')),
    ('credential-literal', re.compile(r'''(?i)(?:password|secret|token)[A-Za-z0-9_]*["']?\s*[:=]\s*["'][^"'\s]+["']''')),
]
ARCHIVE_MAGIC = (b'PK\x03\x04', b'\x1f\x8b', b'BZh', b'\xfd7zXZ\x00', b'7z\xbc\xaf', b'Rar!', b'\x28\xb5\x2f\xfd')


@dataclass(frozen=True)
class Entry:
    path: str
    mode: str                      # git-style: 100644, 100755, 120000 (symlink), 160000 (gitlink), 040000 (tree)
    data: bytes | None = None


@dataclass(frozen=True)
class Hit:
    rule: str
    path: str
    line: int = 0
    detail: str = ''
    line_sha256: str = ''

    def render(self) -> str:
        where = f'{self.path}:{self.line}' if self.line else self.path
        return f'{self.rule:<24} {where}' + (f'  {self.detail}' if self.detail else '')


class GuardError(Exception):
    pass


def sha256(data: bytes | str) -> str:
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def load_json(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding='utf-8'))


def public_set() -> dict:
    return load_json(EXPORT_DIR / 'public-set.json')


# -- allow-entries -----------------------------------------------------------------------------------------------------------------------------
class Allow:
    """Reviewed exceptions: ``entries`` {rule, path, line_sha256, reason} and ``binaries`` {path, sha256, reason}. Exact, never directory-wide."""

    def __init__(self, data: dict | None = None):
        data = data or {}
        self.entries, self.binaries = list(data.get('entries', [])), list(data.get('binaries', []))
        self.defaults = list(data.get('documented_defaults', []))
        for item in self.defaults:
            if set(item) != {'value', 'reason'} or not all(isinstance(v, str) and v for v in item.values()) or len(item['reason']) < 12:
                raise GuardError('invalid documented default (a value and a reason are required)')
        for entry in self.entries:
            if (set(entry) != {'rule', 'path', 'line_sha256', 'reason'} or not all(isinstance(v, str) and v for v in entry.values())
                    or not re.fullmatch(r'[0-9a-f]{64}', entry['line_sha256']) or any(c in entry['path'] for c in '*?[]') or entry['path'].endswith('/')
                    or entry['rule'] in ('*', 'any') or len(entry['reason']) < 12):
                raise GuardError(f'invalid allow-entry (exact rule, file path, line sha256 and a reason are required): {entry.get("path")!r}')
        for entry in self.binaries:
            if (set(entry) != {'path', 'sha256', 'reason'} or not re.fullmatch(r'[0-9a-f]{64}', str(entry.get('sha256')))
                    or any(c in str(entry['path']) for c in '*?[]') or str(entry['path']).endswith('/') or len(str(entry.get('reason', ''))) < 12):
                raise GuardError(f'invalid binary allow-entry: {entry.get("path")!r}')
        self.used: set = set()

    @classmethod
    def from_file(cls, path: Path | None) -> 'Allow':
        path = Path(path) if path else EXPORT_DIR / 'guard-allow.json'
        return cls(load_json(path) if path.exists() else {})

    def allows(self, hit: Hit) -> bool:
        for index, entry in enumerate(self.entries):
            if entry['rule'] == hit.rule and entry['path'] == hit.path and hit.line and entry['line_sha256'] == hit.line_sha256:
                self.used.add(('entry', index))
                return True
        return False

    def allows_binary(self, path: str, digest: str) -> bool:
        for index, entry in enumerate(self.binaries):
            if entry['path'] == path and entry['sha256'] == digest:
                self.used.add(('binary', index))
                return True
        return False

    def stale(self) -> list[Hit]:
        found = [Hit('stale-allow-entry', e['path'], 0, f'{e["rule"]} (matches nothing now)') for i, e in enumerate(self.entries) if ('entry', i) not in self.used]
        found += [Hit('stale-binary-entry', e['path'], 0, 'matches nothing now') for i, e in enumerate(self.binaries) if ('binary', i) not in self.used]
        return found


# -- secret values -----------------------------------------------------------------------------------------------------------------------------
TOKEN = re.compile(r'[^\s\'"<>(){}\[\],;`\\]{8,256}')
SENSITIVE_KEY = re.compile(r'(?i)pass(word|wd)?|secret|token|api[_-]?key|private[_-]?key|credential|auth\b|authorization')
SENSITIVE_JSON_KEY = re.compile(r'(?i)pass(word|wd)?|secret|token|key|credential|auth|identitytoken|registrytoken')


def _json_secrets(node, key='') -> set:
    found = set()
    if isinstance(node, dict):
        for name, value in node.items():
            found |= _json_secrets(value, str(name))
    elif isinstance(node, list):
        for value in node:
            found |= _json_secrets(value, key)
    elif isinstance(node, str) and SENSITIVE_JSON_KEY.search(key) and len(node) >= 8:
        found.add(node)
        try:                                                         # Docker's ``auth`` is base64 of ``user:password``
            decoded = base64.b64decode(node, validate=True).decode()
            found.update(part for part in decoded.split(':') if len(part) >= 8)
            found.add(decoded)
        except (ValueError, UnicodeDecodeError):
            pass
    return found


def secret_candidates(text: str) -> set:
    """The values of a private credential file that must never appear in the product: the values of sensitive keys (``KEY=value``, ``key: value``,
    ini and .env files) and, for JSON, the string values under sensitive keys (a Docker ``auth`` is also decoded)."""
    values = set()
    try:
        values |= _json_secrets(json.loads(text))
    except ValueError:
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith(('#', ';')):
                continue
            match = re.match(r'(?:export\s+)?([A-Za-z0-9_.-]+)\s*[=:]\s*(.*)$', line)
            if match and SENSITIVE_KEY.search(match.group(1)):
                values.add(match.group(2).strip().strip('\'"'))
    return {v for v in values if len(v) >= 8}


def secret_hashes(files: list[Path], public_text: str = '') -> dict[str, str]:
    """``{sha256: file name}`` of the secret values of the private credential files, minus any value that also occurs in ``public_text`` (the public
    ``.env.example``: a documented default is not a secret). The plaintext is dropped at once and never printed."""
    public = {m.group(0) for m in TOKEN.finditer(public_text)} | {m.group(2).strip().strip('\'"') for m in re.finditer(r'(?m)^([A-Za-z0-9_.-]+)=(.*)$', public_text)}
    found: dict[str, str] = {}
    for path in files:
        for value in secret_candidates(Path(path).read_text(encoding='utf-8', errors='replace')):
            if value not in public:
                found[sha256(value)] = Path(path).name
    return found


# -- the guard ---------------------------------------------------------------------------------------------------------------------------------
class Guard:
    def __init__(self, allow: Allow | None = None, secrets: dict[str, str] | None = None, config: dict | None = None):
        self.allow, self.secrets, self.config = allow or Allow(), secrets or {}, config or public_set()
        identity = self.config['identity']
        self.allowed_emails = {identity['public_contact'].lower(), identity['email'].lower()}
        self.top_dirs, self.top_files = set(self.config['directories']), set(self.config['files'])

    # entry level
    def scan(self, entries: list[Entry], *, report_stale: bool = True) -> list[Hit]:
        hits: list[Hit] = []
        seen: dict[str, str] = {}
        for entry in sorted(entries, key=lambda e: e.path):
            hits += self.check_path(entry)
            folded = unicodedata.normalize('NFC', entry.path).casefold()
            if folded in seen and seen[folded] != entry.path:
                hits.append(Hit('path-case-collision', entry.path, 0, f'collides with {seen[folded]}'))
            seen[folded] = entry.path
            if entry.mode in REGULAR and entry.data is not None:
                hits += self.check_content(entry)
        if report_stale:
            hits += self.allow.stale()
        return hits

    def check_path(self, entry: Entry) -> list[Hit]:
        hits, path = [], entry.path
        if entry.mode not in REGULAR:
            kind = {'120000': 'symlink', '160000': 'gitlink / submodule', '040000': 'directory entry'}.get(entry.mode, f'mode {entry.mode}')
            hits.append(Hit('entry-type', path, 0, f'{kind}: only regular files (100644, 100755) are allowed'))
        parts = path.split('/')
        if any(part in ('.git', '.gitmodules') for part in parts):
            hits.append(Hit('nested-git', path, 0, 'git metadata inside the tree'))
        if not path or path.startswith('/') or any(part in ('', '.', '..') for part in parts):
            hits.append(Hit('bad-filename', path, 0, 'empty, absolute or relative component'))
        for part in parts:
            if (any(ord(c) < 0x20 or ord(c) == 0x7f for c in part) or '\\' in part or part.startswith('-') or part != part.rstrip(' .')
                    or WINDOWS_RESERVED.match(part) or unicodedata.normalize('NFC', part) != part or len(part.encode()) > MAX_COMPONENT):
                hits.append(Hit('bad-filename', path, 0, 'control character, backslash, leading dash, trailing dot or space, reserved name, non-NFC or too long'))
                break
        if len(path.encode()) > MAX_PATH:
            hits.append(Hit('bad-filename', path, 0, 'path too long'))
        top = parts[0]
        in_set = (top in self.top_dirs and len(parts) > 1) or (len(parts) == 1 and top in self.top_files)
        excluded = (path in self.config['excluded_paths'] or any(p in self.config['excluded_names'] for p in parts)
                    or any(path.endswith(s) for s in self.config['excluded_suffixes']) or any(p.endswith(tuple(self.config['excluded_directory_suffixes'])) for p in parts[:-1]))
        if not in_set or excluded:
            hits.append(Hit('path-not-allowlisted', path, 0, 'not in the public set'))
        for rule, pattern in (('denylist-path', WORDS), ('plan-pointer-path', PLAN_POINTER), ('local-path-path', LOCAL_PATH), ('private-address-path', RFC1918)):
            if pattern.search(path):
                hits.append(Hit(rule, path, 0, 'in the file name'))
        return hits

    def check_content(self, entry: Entry) -> list[Hit]:
        data, path = entry.data, entry.path
        if b'\0' in data[:8192] or data.startswith(ARCHIVE_MAGIC):
            if self.allow.allows_binary(path, sha256(data)):
                return []
            return [Hit('binary-not-listed', path, 0, f'binary or archive file with sha256 {sha256(data)[:16]}… is not in the reviewed binary list')]
        try:
            text = data.decode('utf-8')
        except UnicodeDecodeError as exc:
            return [Hit('non-utf8-text', path, 0, f'byte {exc.start} is not valid UTF-8')]
        hits = []
        for number, raw in enumerate(text.split('\n'), 1):
            line = raw.rstrip('\r')
            if not line.strip():
                continue
            digest = sha256(line)
            found = self.line_hits(line)
            for rule, detail in found:
                hit = Hit(rule, path, number, detail, digest)
                if not self.allow.allows(hit):
                    hits.append(hit)
            if self.secrets:
                for match in TOKEN.finditer(line):
                    owner = self.secrets.get(sha256(match.group(0)))
                    if owner:
                        hit = Hit('secret-value', path, number, f'a value from {owner} (sha256 {sha256(match.group(0))[:12]}…)', digest)
                        if not self.allow.allows(hit):
                            hits.append(hit)
        return hits

    def line_hits(self, line: str) -> list[tuple[str, str]]:
        found = []
        for rule, pattern, shown in (('denylist-word', WORDS, True), ('plan-pointer', PLAN_POINTER, True), ('private-address', RFC1918, True),
                                     ('local-path', LOCAL_PATH, True)):
            match = pattern.search(line)
            if match:
                found.append((rule, match.group(0) if shown else ''))
        for match in EMAIL.finditer(line):
            if match.group(0).lower() not in self.allowed_emails:
                found.append(('email-address', match.group(0)))
                break
        for rule, pattern in CREDENTIALS:
            if pattern.search(line):
                found.append((rule, ''))                    # the matched text may be the credential: never shown
        return found

    # commits and tags
    def check_text(self, text: str, where: str) -> list[Hit]:
        hits = []
        for number, line in enumerate(text.split('\n'), 1):
            for rule, detail in self.line_hits(line):
                hits.append(Hit(rule, where, number, detail, sha256(line)))
        return hits


# -- providers ---------------------------------------------------------------------------------------------------------------------------------
def tree_entries(root: Path) -> list[Entry]:
    """Every file, symlink and nested ``.git`` under ``root`` as git would record it (the working tree is a materialized copy, not a repository)."""
    entries = []
    root = Path(root)
    for current, dirs, files in os.walk(root, followlinks=False):
        base = Path(current)
        for name in list(dirs):
            full = base / name
            relative = full.relative_to(root).as_posix()
            if full.is_symlink():
                entries.append(Entry(relative, '120000'))
                dirs.remove(name)
            elif name == '.git':
                entries.append(Entry(relative + '/', '160000'))
                dirs.remove(name)
        for name in files:
            full = base / name
            relative = full.relative_to(root).as_posix()
            info = full.lstat()
            if stat.S_ISLNK(info.st_mode):
                entries.append(Entry(relative, '120000'))
            elif stat.S_ISREG(info.st_mode):
                entries.append(Entry(relative, '100755' if info.st_mode & 0o111 else '100644', full.read_bytes()))
            else:
                entries.append(Entry(relative, '000000'))
    return entries


def git(repo: Path, *args: str, input_bytes: bytes | None = None, check: bool = True) -> subprocess.CompletedProcess:
    done = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, input=input_bytes)
    if check and done.returncode:
        raise GuardError(f'git {args[0]} failed: {done.stderr.decode(errors="replace").strip()[:200]}')
    return done


def git_entries(repo: Path, rev: str) -> list[Entry]:
    listing = git(repo, 'ls-tree', '-r', '-z', '--full-tree', rev).stdout
    entries = []
    for record in filter(None, listing.split(b'\0')):
        meta, path = record.split(b'\t', 1)
        mode, kind, oid = meta.decode().split()
        text_path = path.decode('utf-8', errors='surrogateescape')
        if kind == 'blob' and mode in REGULAR:
            entries.append(Entry(text_path, mode, git(repo, 'cat-file', 'blob', oid).stdout))
        else:
            entries.append(Entry(text_path, mode))
    return entries


def materialize(worktree: Path, dest: Path, config: dict | None = None) -> dict:
    """Copy the tracked-or-new, not ignored files of a worktree that the public set allows into the new directory ``dest`` (a full replacement tree)."""
    config = config or public_set()
    worktree, dest = Path(worktree).resolve(), Path(dest).resolve()
    if dest.exists() or dest.is_relative_to(worktree):
        raise GuardError('the destination must be a new directory outside the worktree')
    names = git(worktree, 'ls-files', '-z', '--cached', '--others', '--exclude-standard').stdout.split(b'\0')
    copied, skipped = [], {}
    top_dirs, top_files = set(config['directories']), set(config['files'])
    for raw in sorted(set(filter(None, names))):
        path = raw.decode('utf-8', errors='surrogateescape')
        source = worktree / path
        parts = path.split('/')
        if not (source.is_file() or source.is_symlink()):
            continue                                               # listed in the index but deleted in the worktree
        keep = ((parts[0] in top_dirs and len(parts) > 1) or (len(parts) == 1 and parts[0] in top_files)) and path not in config['excluded_paths'] \
            and not any(p in config['excluded_names'] for p in parts) and not path.endswith(tuple(config['excluded_suffixes']))
        if not keep:
            skipped[parts[0]] = skipped.get(parts[0], 0) + 1
            continue
        target = dest / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_symlink():
            target.symlink_to(os.readlink(source))               # kept as a symlink so the guard can refuse it
        else:
            shutil.copy2(source, target)
        copied.append(path)
    return {'files': len(copied), 'excluded_top_level': skipped}


# -- commits and tags --------------------------------------------------------------------------------------------------------------------------
def parse_commit(raw: bytes) -> tuple[dict, str]:
    header, _, message = raw.decode('utf-8', errors='replace').partition('\n\n')
    fields: dict = {}
    for line in header.split('\n'):
        if line.startswith(' '):
            continue                                                   # a continuation line (a signature block)
        key, _, value = line.partition(' ')
        fields.setdefault(key, []).append(value)
    return fields, message


def identity_line(value: str) -> tuple[str, str]:
    match = re.fullmatch(r'(.*?) <([^<>]*)> \d+ [+-]\d{4}', value)
    return (match.group(1), match.group(2)) if match else ('', '')


def check_commit(guard: Guard, repo: Path, rev: str, parent_ok: set[str], validated: set[str]) -> list[Hit]:
    commit = git(repo, 'rev-parse', '--verify', f'{rev}^{{commit}}').stdout.decode().strip()
    fields, message = parse_commit(git(repo, 'cat-file', 'commit', commit).stdout)
    identity = guard.config['identity']
    expected = (identity['name'], identity['email'])
    hits = []
    for role in ('author', 'committer'):
        values = fields.get(role, [])
        if len(values) != 1 or identity_line(values[0]) != expected:
            hits.append(Hit('commit-identity', commit[:12], 0, f'{role} is not the public identity'))
    if 'gpgsig' in fields or 'gpgsig-sha256' in fields or 'mergetag' in fields or 'encoding' in fields:
        hits.append(Hit('commit-header', commit[:12], 0, 'signature, merge tag or encoding header'))
    trailers = subprocess.run(['git', 'interpret-trailers', '--parse'], input=message.encode(), capture_output=True).stdout.decode().strip()
    if trailers or re.search(r'(?im)^(co-authored-by|signed-off-by|reviewed-by|acked-by|tested-by|reported-by|cc|change-id):', message):
        hits.append(Hit('commit-trailer', commit[:12], 0, 'the message carries a trailer'))
    hits += [Hit(h.rule, f'commit {commit[:12]} message', h.line, h.detail, h.line_sha256) for h in guard.check_text(message, 'message')]
    for parent in fields.get('parent', []):
        if parent not in parent_ok and parent not in validated:
            hits.append(Hit('commit-parent', commit[:12], 0, f'parent {parent[:12]} is not a validated public commit'))
    return hits


def check_tag(guard: Guard, repo: Path, tag: str, validated: set[str]) -> list[Hit]:
    ref = f'refs/tags/{tag}'
    kind = git(repo, 'cat-file', '-t', ref, check=False).stdout.decode().strip()
    if kind != 'tag':
        return [Hit('tag-not-annotated', tag, 0, 'only annotated tags are published' if kind else 'no such tag')]
    identity = guard.config['identity']
    expected = (identity['name'], identity['email'])
    hits, current, seen = [], ref, 0
    while True:
        raw = git(repo, 'cat-file', 'tag', current).stdout.decode('utf-8', errors='replace')
        header, _, message = raw.partition('\n\n')
        fields = dict(line.partition(' ')[::2] for line in header.split('\n') if not line.startswith(' '))
        if identity_line(fields.get('tagger', '')) != expected:
            hits.append(Hit('tag-identity', tag, 0, 'the tagger is not the public identity'))
        if re.search(r'-----BEGIN (PGP|SSH) SIGNATURE', message):
            hits.append(Hit('tag-signature', tag, 0, 'signature block in the tag'))
        hits += [Hit(h.rule, f'tag {tag} message', h.line, h.detail, h.line_sha256) for h in guard.check_text(message, 'message')]
        seen += 1
        if fields.get('type') == 'tag' and seen < 5:
            current = fields['object']
            continue
        break
    target = git(repo, 'rev-parse', '--verify', f'{ref}^{{commit}}', check=False).stdout.decode().strip()
    if not target or target not in validated:
        hits.append(Hit('tag-target', tag, 0, 'the peeled target is not a validated public commit'))
    else:
        ancestry = set(git(repo, 'rev-list', target).stdout.decode().split())
        if ancestry - validated:
            hits.append(Hit('tag-ancestry', tag, 0, f'{len(ancestry - validated)} ancestor(s) are not validated public commits'))
    return hits


# -- command line ------------------------------------------------------------------------------------------------------------------------------
def report(hits: list[Hit], as_json: bool, header: str) -> int:
    if as_json:
        print(json.dumps({'ok': not hits, 'hits': [h.__dict__ for h in hits]}, indent=2))
    else:
        print(header)
        for hit in hits:
            print('  ' + hit.render())
        print('PASS' if not hits else f'FAIL: {len(hits)} hit(s)')
    return 1 if hits else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--allow', type=Path, help='reviewed allow-entries (default export/guard-allow.json)')
    parser.add_argument('--secret-file', type=Path, action='append', default=[], help='private credential file whose values must not occur (repeatable)')
    parser.add_argument('--json', action='store_true')
    commands = parser.add_subparsers(dest='command', required=True)
    sub = commands.add_parser('materialize')
    sub.add_argument('worktree', type=Path)
    sub.add_argument('dest', type=Path)
    sub = commands.add_parser('tree')
    sub.add_argument('directory', type=Path)
    sub = commands.add_parser('commits')
    sub.add_argument('repo', type=Path)
    sub.add_argument('revs', nargs='+')
    sub.add_argument('--parent-ok', action='append', default=[])
    sub = commands.add_parser('tags')
    sub.add_argument('repo', type=Path)
    sub.add_argument('tags', nargs='+')
    sub.add_argument('--validated', nargs='+', required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'materialize':
            print(json.dumps(materialize(args.worktree, args.dest)))
            return 0
        example = Path(args.directory) / '.env.example' if args.command == 'tree' else None
        public_text = example.read_text(encoding='utf-8', errors='replace') if example is not None and example.is_file() else ''
        allow = Allow.from_file(args.allow)
        secrets = secret_hashes(args.secret_file, public_text)
        for item in allow.defaults:                       # a value that the public tree documents as a default is not a secret
            secrets.pop(sha256(item['value']), None)
        guard = Guard(allow, secrets)
        if args.command == 'tree':
            return report(guard.scan(tree_entries(args.directory)), args.json, f'guard: tree {args.directory}')
        if args.command == 'commits':
            validated = set(args.parent_ok)
            hits = []
            for rev in args.revs:
                hits += guard.scan(git_entries(args.repo, rev), report_stale=False)
                hits += check_commit(guard, args.repo, rev, set(args.parent_ok), validated)
                validated.add(git(args.repo, 'rev-parse', rev).stdout.decode().strip())
            return report(hits + guard.allow.stale(), args.json, f'guard: {len(args.revs)} commit(s) in {args.repo}')
        validated = {git(args.repo, 'rev-parse', '--verify', f'{rev}^{{commit}}').stdout.decode().strip() for rev in args.validated}
        hits = [h for tag in args.tags for h in check_tag(guard, args.repo, tag, validated)]
        return report(hits, args.json, f'guard: {len(args.tags)} tag(s) in {args.repo}')
    except GuardError as exc:
        print(f'guard error: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
