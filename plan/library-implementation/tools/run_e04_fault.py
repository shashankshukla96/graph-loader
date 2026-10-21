#!/usr/bin/env python3
"""Externally supervise only one owned E04 fault client/process group."""
import argparse
import json
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
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    command=[args.python,'-m','tests.fixtures.e04_faults',args.scenario,args.mode,str(args.output/'client.json')]
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
