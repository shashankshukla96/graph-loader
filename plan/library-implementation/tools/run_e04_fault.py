#!/usr/bin/env python3
"""Externally supervise only one owned E04 fault client/process group."""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario')
    parser.add_argument('--mode', choices=('timeout','cancel'), default='timeout')
    parser.add_argument('--python',default=sys.executable)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--hard-timeout',type=float,default=20)
    parser.add_argument('--context',choices=('spawn','inline'),help='explicit E08 client context; legacy faults keep their defaults')
    parser.add_argument('--workers',type=int,help='explicit E08 measured worker count')
    args=parser.parse_args()
    if not math.isfinite(args.hard_timeout) or not 0 < args.hard_timeout <= 3600:
        parser.error('--hard-timeout must be positive, finite and at most3600 seconds')
    if args.workers is not None and not 1 <= args.workers <= 64:
        parser.error('--workers must be in the measured1–64 range')
    external=args.scenario.startswith('external-')
    if external and (args.context is None or args.workers is None):
        parser.error('external clients require explicit --context and --workers')
    if not external:
        if (args.context is None)!=(args.workers is None):
            parser.error('fault context and workers must be provided together')
        if args.context is not None and args.context!='spawn':
            parser.error('framed process faults require spawn; use external clients for inline')
        if args.workers is not None and (args.scenario=='public-startup' or args.scenario.startswith('public-mixed-error')) and args.workers<2:
            parser.error('second-worker and mixed-peer faults require at least two workers')
    args.output.mkdir(parents=True,exist_ok=False)
    command=([args.python,'-m','tests.fixtures.e08_process_clients',args.context,str(args.workers),
              args.scenario.removeprefix('external-'),str(args.output)] if external else
             [args.python,'-m','tests.fixtures.e04_faults',args.scenario,args.mode,str(args.output/'client.json')])
    if not external and args.workers is not None:
        command.extend(['--context',args.context,'--workers',str(args.workers)])
    env={k:v for k,v in os.environ.items() if k not in {'PYTHONPATH','PYTHONHOME'}}
    started=time.monotonic(); expired=False; emergency=False
    with (args.output/'client.log').open('w') as log:
        child=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            code=child.wait(timeout=args.hard_timeout)
        except subprocess.TimeoutExpired:
            expired=True; code=124
        finally:
            # Only this client's newly created session/group is ours.
            try: os.killpg(child.pid,signal.SIGTERM)
            except ProcessLookupError: pass
            else:
                emergency=True
                try: child.wait(timeout=1)
                except subprocess.TimeoutExpired: pass
                try: os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError: pass
            child.wait(timeout=5)
    record={'command':command,'cwd':str(ROOT),'exit_code':code,'timeout':expired,
            'emergency_group_cleanup':emergency,'elapsed_seconds':time.monotonic()-started}
    (args.output/'supervisor.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record))
    return code


if __name__=='__main__':
    raise SystemExit(main())
