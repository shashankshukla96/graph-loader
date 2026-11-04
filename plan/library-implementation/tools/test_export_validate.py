"""Tests of the helpers of the exact-tree validation (private). The validation itself is exercised on a real materialized tree."""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import export_validate as V  # noqa: E402

LINUX = 'tests/test_process_runtime.py::test_exit_codes_after_ready[self-signal-args3-138]'
MAC = 'tests/test_process_runtime.py::test_exit_codes_after_ready[self-signal-args3-158]'


def test_collection_comparison_tolerates_only_the_platform_signal_id():
    frozen = ['a::t1', LINUX, 'a::t2']
    ok = V.compare_collection(frozen, ['a::t1', MAC, 'a::t2', 'a::new'])
    assert ok['lost'] == [] and ok['added'] == 1 and ok['order_preserved'] and ok['platform_id_differs']
    assert V.compare_collection(frozen, ['a::t1', LINUX, 'a::t2'])['platform_id_differs'] is False
    assert V.compare_collection(frozen, ['a::t1', MAC])['lost'] == ['a::t2']
    assert V.compare_collection(frozen, ['a::t2', MAC, 'a::t1'])['order_preserved'] is False
    assert V.compare_collection(frozen, ['a::t1', LINUX.replace('138', '999').replace('args3', 'args4'), 'a::t2'])['lost'] == [
        LINUX.replace('138', '<platform>')]


def test_pytest_summaries_are_read():
    assert V.parse_pytest_summary('....\n4874 passed, 59 skipped, 24 deselected in 330.05s (0:05:30)\n') == {'passed': 4874, 'failed': 0, 'skipped': 59, 'deselected': 24}
    assert V.parse_pytest_summary('1 failed, 4652 passed, 58 skipped in 315.02s')['failed'] == 1
    assert V.parse_pytest_summary('nothing') == {'passed': 0, 'failed': 0, 'skipped': 0, 'deselected': 0}


def test_stale_staged_files_are_listed(tmp_path):
    (tmp_path / 'tests').mkdir()
    (tmp_path / 'tests' / 'a.py').write_text('x = 1\n')
    (tmp_path / 'tests' / 'b.py').write_text('y = 2\n')
    good = hashlib.sha256(b'x = 1\n').hexdigest()
    inventory = {'lanes': {'core': {'stage_files': {'tests/a.py': good, 'tests/b.py': 'f' * 64, 'tests/gone.py': good}}, 'other': {'stage_files': {'tests/b.py': 'f' * 64}}}}
    assert V.stale_stage_files(tmp_path, inventory) == ['tests/b.py', 'tests/gone.py']
