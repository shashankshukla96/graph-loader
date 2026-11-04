"""E14.F3.S8 (process side): the cgroup OOM report on a real systemd user scope - group level, never a named victim.

Needs a systemd user manager with the memory controller delegated (the same prerequisite as ``test_service_process_limits.py``) and
no Neo4j or Kafka. A memory hog is killed by the kernel inside a ``MemoryMax`` scope: the command reports
``stage=oom cgroup OOM kill observed; victim unknown`` once and writes one ``oom`` registry record. The control kills a child by
hand in the same kind of scope: the exit looks the same (SIGKILL) and nothing is reported."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures'
pytestmark = pytest.mark.integration


def scope_run(config, tag, mode, mem_mb):
    root = Path(config['run_dir']) / f'oom-{tag}'
    root.mkdir()
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR') if key in os.environ}
    env['XDG_RUNTIME_DIR'] = f'/run/user/{os.getuid()}'
    python = Path(config['venv_bin']) / 'python'
    unit = f'e14-oom-{tag}-{os.getpid()}'
    # OOMPolicy=continue: with systemd's default (stop) an OOM kill ends the whole scope, the command included, before it can report.
    command = ['systemd-run', '--user', '--scope', '--quiet', f'--unit={unit}', '-p', f'MemoryMax={mem_mb}M',
               '-p', 'MemorySwapMax=0', '-p', 'OOMPolicy=continue', '--', str(python), str(FIXTURES / 'e14_oom_standin.py'), str(root), mode]
    try:
        done = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
    finally:
        subprocess.run(['systemctl', '--user', 'reset-failed', f'{unit}.scope'], env=env, capture_output=True, timeout=30)
    lines = [line for line in done.stdout.splitlines() if line.startswith('{')]
    return done, json.loads(lines[-1]) if lines else None


def test_the_kernel_oom_kill_of_a_role_is_reported_once_at_group_level(service_config):
    done, result = scope_run(service_config, 'kill', 'hog', 250)
    assert result is not None, done.stderr[-2000:]
    assert result['victim_code'] in (137, -9), result                       # SIGKILL: indistinguishable from a manual kill
    assert result['idle_alive'] is True, 'the other role must not be named or touched'
    reports = [line for line in done.stderr.splitlines() if 'stage=oom' in line]
    assert len(reports) == 1 and 'cgroup OOM kill observed; victim unknown' in reports[0], done.stderr[-2000:]
    assert 'hog-0' not in reports[0] and 'idle-0' not in reports[0], 'the report never names a child'
    assert len(result['oom_records']) == 1 and result['oom_records'][0]['oom_kill_after'] > result['oom_records'][0]['oom_kill_before']


def test_a_manual_kill_9_in_the_same_scope_is_not_reported_as_an_oom(service_config):
    done, result = scope_run(service_config, 'manual', 'manual', 250)
    assert result is not None, done.stderr[-2000:]
    assert result['victim_code'] in (137, -9), result
    assert 'stage=oom' not in done.stderr and result['oom_records'] == [], 'no oom_kill counter rise: nothing to report'


def test_no_scope_left_from_this_module():
    out = subprocess.run(['systemctl', '--user', 'list-units', 'e14-oom-*', '--all', '--no-legend'], capture_output=True, text=True,
                         env={**os.environ, 'XDG_RUNTIME_DIR': f'/run/user/{os.getuid()}'}, timeout=30).stdout
    assert out.strip() == '', out
