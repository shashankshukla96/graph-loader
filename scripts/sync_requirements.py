#!/usr/bin/env python3
"""Generate/check Docker's runtime requirements from pyproject metadata."""
import argparse
from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parents[1]
HEADER = """# Generated from pyproject.toml [project].dependencies; do not edit.
# Regenerate: python scripts/sync_requirements.py
# Check: python scripts/sync_requirements.py --check
# neo4j-rust-ext selects its exactly matching driver; both remain below driver 7.
"""


def render(project):
    dependencies = tomllib.loads(project.read_text(encoding="utf-8"))["project"]["dependencies"]
    if not isinstance(dependencies, list) or not dependencies or any(
        not isinstance(item, str) or not item.strip() or "\n" in item or "\r" in item
        or item.lstrip().startswith(("-", "#")) for item in dependencies
    ):
        raise ValueError("project.dependencies must be nonempty single-line requirement strings")
    return HEADER + "\n".join(dependencies) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=ROOT / "pyproject.toml")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    output = args.project.parent / "requirements.txt"
    expected = render(args.project)
    if args.check:
        if not output.is_file() or output.read_text(encoding="utf-8") != expected:
            parser.exit(1, "requirements.txt differs from project.dependencies; run scripts/sync_requirements.py\n")
        print("Runtime requirements match project.dependencies")
    else:
        output.write_text(expected, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
