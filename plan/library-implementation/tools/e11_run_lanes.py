"""Run installed lanes of scripts/verify_installed.py one after another and record each result.

    python e11_run_lanes.py ROOT WHEEL OUTDIR LANE [LANE ...] [-- extra verify_installed args]

ROOT is a clean export of the candidate commit (the runner stages test files from it and compares them with the
frozen inventory). Run it with the controller Python (psutil, packaging, docker installed). A shell job started
with `&` or `nohup` inherits SIGINT as ignored, which breaks tests that send SIGINT to children; every lane
therefore starts with SIGINT reset to the default.
"""
import json
import signal
import subprocess
import sys
import time
from pathlib import Path


def main():
    argv = sys.argv[1:]
    extra = argv[argv.index('--') + 1:] if '--' in argv else []
    argv = argv[:argv.index('--')] if '--' in argv else argv
    root, wheel, outdir, lanes = Path(argv[0]).resolve(), Path(argv[1]).resolve(), Path(argv[2]).resolve(), argv[3:]
    inventory = root / 'tests/inventory/test-inventory.json'
    outdir.mkdir(parents=True, exist_ok=False)
    summary = {}
    for lane in lanes:
        out = outdir / lane
        command = [sys.executable, str(root / 'scripts/verify_installed.py'), '--lane', lane, '--wheel', str(wheel),
                   '--inventory', str(inventory), '--output', str(out), '--timeout', '3000', *extra]
        started = time.time()
        result = subprocess.run(command, cwd=root, capture_output=True, text=True,
                                preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL))
        last = (result.stdout.strip().splitlines() or [''])[-1]
        try:
            line = json.loads(last)
        except ValueError:
            line = {'status': 'unparsed', 'stdout_tail': result.stdout[-400:], 'stderr_tail': result.stderr[-600:]}
        summary[lane] = {'exit': result.returncode, 'seconds': round(time.time() - started, 1), **line}
        (outdir / f'{lane}.stdout').write_text(result.stdout)
        (outdir / f'{lane}.stderr').write_text(result.stderr)
        (outdir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(lane, summary[lane].get('status'), summary[lane].get('test_ids'), summary[lane]['seconds'], flush=True)
    return 0 if all(v['exit'] == 0 and v.get('status') == 'passed' for v in summary.values()) else 1


sys.exit(main())
