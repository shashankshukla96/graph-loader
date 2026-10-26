"""Leaked detached target Python must fail the runner and be reclaimed."""
import json
from pathlib import Path
import subprocess
import sys
import sysconfig
import time

import psutil


def test_detached_owned_child():
    child=subprocess.Popen([sys.executable,'-I','-c','import time;time.sleep(60)'],start_new_session=True)
    created=psutil.Process(child.pid).create_time()
    (Path(sys.prefix).parent/'detached.json').write_text(json.dumps({'pid':child.pid,'created':created}))
    config=json.loads((Path(sysconfig.get_paths()['purelib'])/'_e07_guard_config.json').read_text())
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        rows=[json.loads(line) for line in Path(config['provenance_log']).read_text().splitlines()]
        if any(row['kind']=='process' and row['pid']==child.pid for row in rows):
            break
        time.sleep(.025)
    else:
        raise AssertionError('detached child did not establish provenance')
    assert child.poll() is None
