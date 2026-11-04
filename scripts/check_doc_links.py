"""Check Markdown links: relative paths, in-file/cross-file #anchors (GitHub slugs) and published absolute repository links.

    python check_doc_links.py REPO_ROOT FILE.md [FILE.md ...]

An absolute https://github.com/Neburyx/graph-loader/blob/main/PATH link is mapped to REPO_ROOT/PATH.
Other external links are listed, not fetched. Exit 1 if any internal link or anchor is broken.
"""
import re
import sys
from pathlib import Path

PUBLISHED = re.compile(r"^https://github\.com/Neburyx/graph-loader/(?:blob|tree)/main/(.*)$")


def slug(heading):
    text = re.sub(r"`", "", heading.strip().lower())
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path, cache={}):
    if path not in cache:
        found, seen, fenced = set(), {}, False
        for line in path.read_text().splitlines():
            if line.lstrip().startswith("```"):
                fenced = not fenced
            if fenced:
                continue
            m = re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
            if m:
                base = slug(m.group(1))
                n = seen.get(base, 0)
                seen[base] = n + 1
                found.add(base if n == 0 else f"{base}-{n}")
        cache[path] = found
    return cache[path]


def links(text):
    text = re.sub(r"^```.*?^```", "", text, flags=re.S | re.M)
    text = re.sub(r"`[^`\n]*`", "", text)
    for m in re.finditer(r"\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", text):
        yield m.group(1)
    for m in re.finditer(r"<(https?://[^>\s]+)>", text):
        yield m.group(1)


def main():
    root = Path(sys.argv[1]).resolve()
    bad, external = [], set()
    for name in sys.argv[2:]:
        src = (root / name).resolve()
        for link in links(src.read_text()):
            target, _, frag = link.partition("#")
            m = PUBLISHED.match(target)
            if m:
                path = root / m.group(1)
            elif re.match(r"^[a-z]+:", target):
                external.add(link)
                continue
            elif not target:
                path = src
            else:
                path = (src.parent / target).resolve()
            if not path.exists():
                bad.append((name, link, "missing path"))
            elif frag and path.suffix == ".md" and frag not in anchors(path):
                bad.append((name, link, "missing anchor"))
    for item in bad:
        print("BROKEN", *item)
    print(f"checked {len(sys.argv) - 2} files; {len(bad)} broken; {len(external)} external links not fetched:")
    for link in sorted(external):
        print("  ", link)
    return 1 if bad else 0


sys.exit(main())
