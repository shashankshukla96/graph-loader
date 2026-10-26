"""Exact collection/outcome receipt for the isolated installed-code runner."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


class Receipt:
    def __init__(self, expected, allowed_skips, output, collect_only=False, *, full_expected=None, excluded=None):
        self.expected = expected
        self.allowed_skips = allowed_skips
        self.output = Path(output)
        self.collect_only = collect_only
        self.collected = []
        self.outcomes = {}
        self.deselected = []
        self.errors = []
        self.full_expected = expected if full_expected is None else full_expected
        self.excluded = excluded or {}
        self.full_collected = []
        self.approved_exclusions = []

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_collection_modifyitems(self, session, config, items):
        self.full_collected = [item.nodeid for item in items]
        if self.full_collected != self.full_expected:
            self.errors.append('full collection IDs/order differ from frozen inventory')
        yield
        # Any other hook's filtering must remain visible as an error. Only the
        # inventory's reviewed per-ID dispositions may omit execution here.
        if [item.nodeid for item in items] != self.full_collected:
            self.errors.append('another hook modified the frozen full collection')
        self.approved_exclusions = [item.nodeid for item in items if item.nodeid in self.excluded]
        items[:] = [item for item in items if item.nodeid not in self.excluded]

    def pytest_collection_finish(self, session):
        self.collected = [item.nodeid for item in session.items]
        if self.collected != self.expected:
            self.errors.append('collected test IDs/order differ from frozen inventory')

    def pytest_deselected(self, items):
        self.deselected.extend(item.nodeid for item in items)

    def pytest_runtest_logreport(self, report):
        self.outcomes.setdefault(report.nodeid, []).append({
            'phase': report.when, 'outcome': report.outcome,
            'wasxfail': getattr(report, 'wasxfail', None),
            'detail': str(report.longrepr) if report.skipped or report.failed else None,
        })

    def pytest_sessionfinish(self, session, exitstatus):
        if self.deselected:
            self.errors.append('selected tests were deselected')
        if not self.collect_only:
            for nodeid in self.expected:
                reports = self.outcomes.get(nodeid, [])
                skipped = [r for r in reports if r['outcome'] == 'skipped']
                allowed = self.allowed_skips.get(nodeid)
                if skipped:
                    if not allowed or any(allowed not in r['detail'] or r['wasxfail'] for r in skipped):
                        self.errors.append('unapproved skip/xfail: ' + nodeid)
                elif not any(r['phase'] == 'call' and r['outcome'] == 'passed' and not r['wasxfail'] for r in reports):
                    self.errors.append('missing successful call: ' + nodeid)
                if any(r['outcome'] == 'failed' or r['wasxfail'] for r in reports):
                    self.errors.append('failed phase/xfail: ' + nodeid)
        if self.errors:
            session.exitstatus = 1
        self.output.write_text(json.dumps({
            'collected': self.collected, 'outcomes': self.outcomes,
            'deselected': self.deselected, 'errors': self.errors,
            'full_collected': self.full_collected,
            'approved_exclusions': {nodeid: self.excluded[nodeid] for nodeid in self.approved_exclusions},
            'pytest_exit_code': int(session.exitstatus),
        }, indent=2) + '\n')
