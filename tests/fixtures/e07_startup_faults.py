"""Owned missing-hook children with deliberately invalid startup records."""
import json
from pathlib import Path
import subprocess
import sys
import sysconfig

import pytest


def reject_startup_record(kind):
    site = Path(sysconfig.get_paths()['purelib'])
    config = json.loads((site/'_e07_guard_config.json').read_text())
    guard = site/'sitecustomize.py'
    saved = guard.read_bytes()
    script = '''import json,os,sys,sysconfig,time
from pathlib import Path
import psutil
kind=KIND;log=Path(LOG)
row=dict(kind='process',pid=os.getpid(),ppid=os.getppid(),created=psutil.Process().create_time(),tick=time.monotonic_ns(),prefix=sys.prefix,site=str(Path(sysconfig.get_paths()['purelib']).resolve()))
if kind=='stale':row['tick']=0
if kind=='wrong-prefix':row['prefix']='/unrelated-environment'
with log.open('a') as output:output.write('malformed complete record\\n' if kind=='malformed' else json.dumps(row)+'\\n')
log.with_name(kind+'-child.json').write_text(json.dumps(dict(pid=os.getpid(),created=psutil.Process().create_time())))
if kind=='malformed':time.sleep(60)
'''.replace('KIND', repr(kind)).replace('LOG', repr(config['provenance_log']))
    guard.write_text('# intentionally absent startup hook\n')
    try:
        with pytest.raises(RuntimeError, match='startup provenance failed'):
            subprocess.Popen([sys.executable, '-I', '-c', script], start_new_session=kind == 'malformed')
    finally:
        guard.write_bytes(saved)
