"""Run the ```bash blocks of a Markdown guide in order, unchanged except for named substitutions.

Used by E10 to execute docs/QUICKSTART.md from a fresh wheel environment outside any checkout.
Every substitution is printed so the deviations from the document are on record.

    python run_doc_blocks.py DOC --workdir DIR --sub 'OLD=>NEW' [--after-install 'SHELL SNIPPET'] [--keep-going]
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path


def blocks(text):
    return [m.group(1) for m in re.finditer(r"^```bash\n(.*?)^```$", text, re.S | re.M)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("doc")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--sub", action="append", default=[])
    ap.add_argument("--after-install", default="")
    args = ap.parse_args()
    subs = [s.split("=>", 1) for s in args.sub]
    work = Path(args.workdir)
    work.mkdir(parents=True, exist_ok=False)
    script = ["set -euo pipefail"]
    for index, block in enumerate(blocks(Path(args.doc).read_text()), 1):
        for old, new in subs:
            if old in block:
                print(f"[block {index}] substitution: {old!r} -> {new!r}", file=sys.stderr)
                block = block.replace(old, new)
        script.append(f"echo '=== block {index} ==='")
        script.append(block)
        if args.after_install and "pip install" in block:
            script.append(args.after_install)
    result = subprocess.run(["bash", "-c", "\n".join(script)], cwd=work)
    return result.returncode


sys.exit(main())
