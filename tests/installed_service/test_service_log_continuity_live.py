"""Destructive live control: runs after every other service test because it breaks log continuity on purpose.

Ordering relies on the module file name sorting last; the frozen lane order enforces it.
"""
from __future__ import annotations

import pytest

from tests.installed_service import query_window as qw

pytestmark = pytest.mark.integration


def shell(stack, script):
    result = stack.neo.exec_run(['sh', '-c', script])
    assert result.exit_code == 0, result.output[:200]


def test_live_truncation_and_rotation_fail_closed(stack):
    before = qw.capture(stack.neo, stack.marker)
    lines = before.content.splitlines(keepends=True)
    cut = next(n for n in range(len(lines) - 1, 0, -1) if parses(before, lines[:n], stack.marker))
    size = sum(len(line) for line in lines[:cut])
    assert 0 < size < len(before.content)

    shell(stack, f'head -c {size} /logs/query.log > /logs/q.cut && cat /logs/q.cut > /logs/query.log && rm /logs/q.cut')
    truncated = qw.capture(stack.neo, stack.marker)
    assert (truncated.inode, truncated.size) == (before.inode, size)
    with pytest.raises(RuntimeError, match='truncated or rewritten'):
        qw.window(before, truncated)

    shell(stack, 'cp /logs/query.log /logs/q.copy && mv /logs/q.copy /logs/query.log')
    rotated = qw.capture(stack.neo, stack.marker)
    assert rotated.inode != truncated.inode and rotated.content == truncated.content
    with pytest.raises(RuntimeError, match='rotated'):
        qw.window(truncated, rotated)

    shell(stack, 'mv /logs/query.log /logs/query.log.lost')
    with pytest.raises(RuntimeError):
        qw.capture(stack.neo, stack.marker, timeout=5)


def parses(capture, lines, marker):
    try:
        qw.parse(capture.inode, b''.join(lines), marker)
    except RuntimeError:
        return False
    return True
