"""Tests for scripts/validate_monitoring.py (P6-S3 Story 5).

Permitted exception to this slice's "no tests/ edits" scope confinement
(plan/v6/slice-3/stories.md's own text: "tests/test_validate_monitoring.py"
is explicitly carved out) because it tests this slice's own new,
dependency-free script, not any src/ production code.

Uses importlib to load scripts/validate_monitoring.py by file path rather
than a package import, since scripts/ has no __init__.py and is not meant
to become an importable package for this one script (matching the
script's own "no project dependency" design goal).
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "validate_monitoring.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("validate_monitoring", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _scratch_monitoring_copy(tmp_path):
    scratch_monitoring = tmp_path / "monitoring"
    shutil.copytree(REPO_ROOT / "monitoring", scratch_monitoring)
    return scratch_monitoring


def _module_pointed_at(scratch_monitoring):
    vm = _load_module()
    vm.DASHBOARDS_DIR = scratch_monitoring / "grafana" / "dashboards"
    vm.ALERTS_GLOB = str(scratch_monitoring / "prometheus" / "alerts" / "*.yml")
    return vm


@pytest.fixture(scope="module")
def vm():
    return _load_module()


class TestQueryableNames:
    """Type-aware queryable-name checks (reviewer finding S5-SF1): a
    type-blind suffix stripper would wrongly accept dead queries like a
    Counter's bare family name (never exposed without _total) or a
    Gauge's family name with a Counter/Histogram/Info suffix tacked on.
    """

    def test_counter_requires_total_suffix(self, vm):
        assert "graph_loader_records_consumed_total" in vm.QUERYABLE_METRIC_NAMES
        assert "graph_loader_records_consumed" not in vm.QUERYABLE_METRIC_NAMES

    def test_gauge_has_no_total_suffix(self, vm):
        assert "graph_loader_process_up" in vm.QUERYABLE_METRIC_NAMES
        assert "graph_loader_process_up_total" not in vm.QUERYABLE_METRIC_NAMES

    def test_gauge_has_no_bucket_suffix(self, vm):
        assert "graph_loader_consumer_lag_records_bucket" not in vm.QUERYABLE_METRIC_NAMES

    def test_gauge_has_no_count_suffix(self, vm):
        assert "graph_loader_lease_waiters_count" not in vm.QUERYABLE_METRIC_NAMES

    def test_gauge_has_no_sum_suffix(self, vm):
        assert "graph_loader_process_start_time_seconds_sum" not in vm.QUERYABLE_METRIC_NAMES

    def test_histogram_exposes_exactly_three_names(self, vm):
        expected = {
            "graph_loader_neo4j_write_duration_seconds_bucket",
            "graph_loader_neo4j_write_duration_seconds_sum",
            "graph_loader_neo4j_write_duration_seconds_count",
        }
        actual = {
            name
            for name, family in vm.QUERYABLE_METRIC_NAMES.items()
            if family == "graph_loader_neo4j_write_duration_seconds"
        }
        assert actual == expected
        # The bare histogram name is never itself queryable.
        assert "graph_loader_neo4j_write_duration_seconds" not in vm.QUERYABLE_METRIC_NAMES

    def test_info_requires_info_suffix(self, vm):
        assert "graph_loader_process_info" in vm.QUERYABLE_METRIC_NAMES
        assert "graph_loader_process" not in vm.QUERYABLE_METRIC_NAMES

    def test_catalogue_has_exactly_26_families(self, vm):
        # 22 as of P6-S2, plus graph_loader_dlq_backlog (P5-S4 Story 7,
        # the P5-Q4 follow-up's true DLQ backlog gauge), plus
        # graph_loader_retry_stalled_seconds (P5-S4 round 4, decision D1), plus
        # graph_loader_parked_pair_index_wait_seconds (v7-S4 admission barrier), plus
        # graph_loader_write_outage_seconds (stream outage ride-out).
        assert len(vm.EXPECTED_METRIC_FAMILIES) == 26


class TestCheckMetricNames:
    def test_accepts_known_metric(self, vm):
        names = vm._check_metric_names([("t", "graph_loader_records_consumed_total{kind=\"node\"}")])
        assert names == {"graph_loader_records_consumed"}

    def test_rejects_unknown_metric(self, vm):
        with pytest.raises(vm.ValidationError):
            vm._check_metric_names([("t", "graph_loader_typo_totally_fake_total{}")])

    @pytest.mark.parametrize(
        "bad_name",
        [
            "graph_loader_process_up_total",
            "graph_loader_records_consumed",
            "graph_loader_consumer_lag_records_bucket",
            "graph_loader_lease_waiters_count",
            "graph_loader_process_start_time_seconds_sum",
        ],
    )
    def test_rejects_wrong_suffix_for_metric_type(self, vm, bad_name):
        # Reviewer finding S5-SF1: these all passed the old blind-
        # stripping check because each is a real family name with some
        # known suffix attached — none of them is an actual name
        # Prometheus would ever expose for that metric's type.
        with pytest.raises(vm.ValidationError):
            vm._check_metric_names([("t", bad_name + "{}")])

    def test_rejects_blank_expr(self, vm):
        with pytest.raises(vm.ValidationError):
            vm._check_metric_names([("t", "")])

    def test_allows_expr_with_no_graph_loader_token(self, vm):
        # Prometheus's own built-in series (e.g. `up`) are legitimate
        # query targets (Story 4's "Scrape health" panel uses
        # up{job="graph-loader",...} deliberately, per reviewer finding
        # S4-SF1) — an expr referencing zero graph_loader_* names is not
        # itself an error.
        names = vm._check_metric_names([("t", "up{job=\"prometheus\"}")])
        assert names == set()


class TestTemplateVariableQueries:
    """Reviewer finding S4-SF2: a typo in a template variable's
    label_values(...) query silently empties that dropdown instead of
    erroring — check it the same way as panel exprs.
    """

    def test_template_variable_query_is_checked(self, tmp_path, vm):
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        dashboard_path = (
            scratch_monitoring / "grafana" / "dashboards" / "graph-loader-overview.json"
        )
        data = json.loads(dashboard_path.read_text(encoding="utf-8"))
        data["templating"]["list"][0]["query"] = "label_values(graph_loader_proces_up, kind)"
        dashboard_path.write_text(json.dumps(data), encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1


class TestCollapsedRowPanels:
    """Reviewer finding S4-SF2 (nested panels): a collapsed row nests its
    children under the row panel's own `panels` key instead of the
    dashboard's top-level list — must still be validated.
    """

    def test_nested_row_panels_are_validated(self, vm):
        nested_bad_panel = {
            "type": "timeseries",
            "title": "nested",
            "targets": [{"refId": "A", "expr": "graph_loader_totally_fake_total{}"}],
        }
        row = {"type": "row", "title": "a row", "collapsed": True, "panels": [nested_bad_panel]}
        flattened = vm._collect_panels([row])
        assert flattened == [nested_bad_panel]


class TestRealMonitoringFiles:
    """End-to-end proof against this repo's actual monitoring/ tree."""

    def test_script_exits_zero_against_real_files(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "all known" in result.stdout

    def test_guard_actually_guards_on_typoed_dashboard(self, tmp_path, capsys):
        """Copy the real monitoring/ tree, corrupt one metric name in the
        dashboard JSON, and confirm the script exits nonzero *and* names
        the exact bad token in its error message — the same "prove the
        guard actually guards" proof done manually once at Story 5 time,
        kept here as a permanent regression test so a future edit can't
        silently reintroduce a name drift undetected. Reviewer finding
        S5-SF4: asserting only the exit code would also pass for an
        unrelated failure (e.g. a missing directory); the message must
        name the specific bad name.
        """
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)

        dashboard_path = (
            scratch_monitoring / "grafana" / "dashboards" / "graph-loader-overview.json"
        )
        text = dashboard_path.read_text(encoding="utf-8")
        assert "graph_loader_records_consumed_total" in text
        corrupted = text.replace(
            "graph_loader_records_consumed_total", "graph_loader_recrods_consumed_total"
        )
        dashboard_path.write_text(corrupted, encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1
        captured = capsys.readouterr()
        assert "graph_loader_recrods_consumed_total" in captured.err

    def test_guard_catches_typo_in_folded_alert_expr(self, tmp_path, capsys):
        """The hand-rolled block-scalar (`expr: >`) parser is the most
        fragile part of this script (reviewer finding S5-SF4) and was
        previously only exercised on the happy path — corrupt a folded
        rule (progress.yml's GraphLoaderPlannedRunStalled) and confirm
        the corrupted name is actually extracted and rejected, not
        silently skipped by the parser.
        """
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        progress_path = scratch_monitoring / "prometheus" / "alerts" / "progress.yml"
        text = progress_path.read_text(encoding="utf-8")
        assert "graph_loader_plan_chunks_committed" in text
        corrupted = text.replace(
            "graph_loader_plan_chunks_committed", "graph_loader_pln_chunks_committed"
        )
        progress_path.write_text(corrupted, encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1
        captured = capsys.readouterr()
        assert "graph_loader_pln_chunks_committed" in captured.err

    def test_guard_catches_typo_in_single_line_alert_expr(self, tmp_path, capsys):
        """Same proof for a single-line `expr:` scalar (concurrency.yml's
        GraphLoaderLeaseWaitersStuck), the other code path through the
        hand-rolled parser.
        """
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        concurrency_path = scratch_monitoring / "prometheus" / "alerts" / "concurrency.yml"
        text = concurrency_path.read_text(encoding="utf-8")
        assert "graph_loader_lease_waiters" in text
        corrupted = text.replace(
            "expr: graph_loader_lease_waiters > 0",
            "expr: graph_loader_lease_waitrs > 0",
        )
        assert corrupted != text
        concurrency_path.write_text(corrupted, encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1
        captured = capsys.readouterr()
        assert "graph_loader_lease_waitrs" in captured.err

    def test_guard_passes_once_typo_reverted(self, tmp_path):
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 0

    def test_invalid_json_fails_clearly(self, tmp_path):
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        dashboard_path = (
            scratch_monitoring / "grafana" / "dashboards" / "graph-loader-overview.json"
        )
        dashboard_path.write_text("{not valid json", encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1

    def test_missing_title_fails_clearly(self, tmp_path):
        scratch_monitoring = _scratch_monitoring_copy(tmp_path)
        dashboard_path = (
            scratch_monitoring / "grafana" / "dashboards" / "graph-loader-overview.json"
        )
        data = json.loads(dashboard_path.read_text(encoding="utf-8"))
        data["title"] = ""
        dashboard_path.write_text(json.dumps(data), encoding="utf-8")

        scoped_vm = _module_pointed_at(scratch_monitoring)
        assert scoped_vm.main() == 1


class TestCatalogueDriftAgainstRealMetricsModule:
    """Reviewer finding S5-SF3: nothing previously detected the hardcoded
    catalogue drifting from src/utils/metrics.py. P6-S2 has since merged
    (src/utils/metrics.py exists on this branch as of that merge), so
    this now runs for real rather than skipping; importorskip is kept
    (rather than a plain import) so this file still degrades to a clean
    skip, instead of an unrelated collection error, in any future
    worktree where that module or prometheus_client is temporarily
    absent again.
    """

    def test_families_and_types_match_real_module(self, vm):
        metrics = pytest.importorskip("src.utils.metrics")
        assert vm.EXPECTED_METRIC_FAMILIES == metrics.EXPECTED_METRIC_FAMILIES
        # Reviewer follow-up: also compare the metric *type* half, not
        # just the family-name set, so a future Counter<->Gauge retype
        # in metrics.py is caught too (a name-only comparison would miss
        # it entirely).
        real_types = {
            family.name: family.type
            for family in metrics.GraphLoaderMetrics().registry.collect()
        }
        assert real_types == vm.FAMILY_TYPES
