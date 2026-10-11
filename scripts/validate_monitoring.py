#!/usr/bin/env python3
"""Validate P6-S3's monitoring config files with no project dependencies.

Deliberately stdlib-only (``json``, ``re``, ``pathlib``, ``sys``, ``glob``):
this script must run from a worktree where P6-S2's own ``src/`` changes are
mid-flight or not yet merged, so it never imports ``src`` and never
requires ``prometheus_client`` to be installed. Because of that, the
metric-name catalogue below is a **hardcoded copy** of
``src/utils/metrics.py``'s ``EXPECTED_METRIC_FAMILIES`` (plus each family's
Prometheus metric *type*, needed to know which suffixed name(s) are
actually queryable — see ``FAMILY_TYPES`` below) — the same reasoning that
module gives for duplicating (rather than importing)
``src.loader.dead_letter.FAILURE_CLASSES``. If P6-S2 renames, retypes or
adds a metric, update this copy in the same pass that updates the
monitoring config files that reference it (plan/v6/slice-3/stories.md's
cross-cutting note: "do not merge this slice's Stories 1/2/4 against a
stale reading of the catalogue"). ``tests/test_validate_monitoring.py``
compares this copy against the real module whenever it is importable, so
drift is caught automatically once P6-S2 merges (it skips cleanly today,
since ``src/utils/metrics.py`` does not exist yet on this branch).

Checks:
  1. Every ``*.json`` under ``monitoring/grafana/dashboards/`` is valid
     JSON, has a nonblank ``title``, and at least one non-row panel
     (including panels nested inside a collapsed ``row`` panel).
  2. Every ``targets[].expr`` string in those dashboards (at any nesting
     depth), every ``templating.list[].query`` string (the source of a
     dashboard's dropdown variables — a typo here silently empties a
     dropdown rather than erroring), and every ``expr:`` in
     ``monitoring/prometheus/alerts/*.yml``, references only metric names
     that are actually queryable given the registered metric's *type*:
     a Counter's only queryable name is ``F_total``; a Histogram's are
     ``F_bucket``/``F_sum``/``F_count`` (never the bare family name); an
     Info metric's is ``F_info``; a Gauge's is the bare family name only.
     This is deliberately type-aware, not "strip any known suffix" — a
     type-blind stripper would accept a dead query like
     ``graph_loader_records_consumed`` (a Counter, never exposed without
     ``_total``) or ``graph_loader_process_up_total`` (a Gauge has no
     ``_total``).
  3. Exits nonzero with a clear message on any failure; exits 0 and
     prints a one-line summary otherwise.

Usage:
    python3 scripts/validate_monitoring.py
"""
from __future__ import annotations

import glob
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = REPO_ROOT / "monitoring" / "grafana" / "dashboards"
ALERTS_GLOB = str(REPO_ROOT / "monitoring" / "prometheus" / "alerts" / "*.yml")

# Hardcoded copy of feat/p6-s2-metrics:src/utils/metrics.py's
# EXPECTED_METRIC_FAMILIES (22 series as of P6-S2, 23 after P5-S4 Story 7
# adds graph_loader_dlq_backlog per the P5-Q4 follow-up, 24 with P5-S4 round (25 with v7-S4 graph_loader_parked_pair_index_wait_seconds, 26 with the stream outage gauge graph_loader_write_outage_seconds)
# 4's graph_loader_retry_stalled_seconds), paired with each
# family's Prometheus metric type as constructed in that module (Counter/
# Gauge/Histogram/Info). The type determines which suffixed name(s) are
# actually queryable in Prometheus's own exposition format — see
# QUERYABLE_METRIC_NAMES below, which is derived from this table.
FAMILY_TYPES: dict[str, str] = {
    "graph_loader_records_consumed": "counter",
    "graph_loader_neo4j_write_duration_seconds": "histogram",
    "graph_loader_deadlocks_recovered": "counter",
    "graph_loader_retry": "counter",
    "graph_loader_dlq_routed": "counter",
    "graph_loader_parked": "counter",
    "graph_loader_dlq_backlog": "gauge",
    "graph_loader_retry_stalled_seconds": "gauge",
    "graph_loader_parked_pair_index_wait_seconds": "gauge",
    "graph_loader_write_outage_seconds": "gauge",
    "graph_loader_consumer_lag_records": "gauge",
    "graph_loader_lease_wait_seconds": "histogram",
    "graph_loader_lease_grants": "counter",
    "graph_loader_lease_denials": "counter",
    "graph_loader_lease_wait_timeouts": "counter",
    "graph_loader_lease_held_resources": "gauge",
    "graph_loader_lease_waiters": "gauge",
    "graph_loader_plan_chunks_planned": "gauge",
    "graph_loader_plan_chunks_committed": "gauge",
    "graph_loader_plan_active_writers": "gauge",
    "graph_loader_plan_resource_wait_seconds": "histogram",
    "graph_loader_planner_rows_read": "counter",
    "graph_loader_planner_chunks_published": "counter",
    "graph_loader_process_start_time_seconds": "gauge",
    "graph_loader_process_up": "gauge",
    "graph_loader_process": "info",
}

EXPECTED_METRIC_FAMILIES = frozenset(FAMILY_TYPES)
assert len(EXPECTED_METRIC_FAMILIES) == 26, "catalogue drifted from its own 26-series count"


def _queryable_names_for(family: str, metric_type: str) -> frozenset[str]:
    if metric_type == "counter":
        return frozenset({f"{family}_total"})
    if metric_type == "histogram":
        return frozenset({f"{family}_bucket", f"{family}_sum", f"{family}_count"})
    if metric_type == "info":
        return frozenset({f"{family}_info"})
    if metric_type == "gauge":
        return frozenset({family})
    raise ValueError(f"unknown metric type {metric_type!r} for family {family!r}")


# Every name an expr/query is actually allowed to reference, mapped back
# to its owning family for error messages. Built once from FAMILY_TYPES
# rather than hand-duplicated, so the two can never silently disagree.
QUERYABLE_METRIC_NAMES: dict[str, str] = {}
for _family, _type in FAMILY_TYPES.items():
    for _queryable in _queryable_names_for(_family, _type):
        QUERYABLE_METRIC_NAMES[_queryable] = _family

# [a-z0-9_]+, not just [a-z_]+: graph_loader_neo4j_write_duration_seconds
# has a digit in "neo4j". stories.md's own Story 5 text says
# "graph_loader_[a-z_]+" literally; corrected here since that pattern
# would fail to match this catalogue's own metric names.
_METRIC_TOKEN_RE = re.compile(r"graph_loader_[a-z0-9_]+")


class ValidationError(Exception):
    """Raised with a clear, user-facing message on any validation failure."""


def _check_metric_names(exprs: list[tuple[str, str]]) -> set[str]:
    """``exprs`` is a list of (source_description, expr_string).

    Returns the set of unique metric *families* found. Raises
    ValidationError naming the first unknown/unqueryable metric name and
    where it was found.

    An expr with zero ``graph_loader_*`` tokens is not itself an error —
    Prometheus's own built-in series (``up``, ``scrape_duration_seconds``,
    etc.) are legitimate query targets this dashboard/alert set uses (e.g.
    the "Scrape health" panel queries plain ``up{job="graph-loader",...}``,
    deliberately not ``graph_loader_process_up``, per reviewer finding
    S4-SF1: the latter never reads 0). Only a *present* graph_loader_*
    token that is not one of QUERYABLE_METRIC_NAMES is a failure.
    """
    unique_families: set[str] = set()
    for source, expr in exprs:
        if not isinstance(expr, str) or not expr.strip():
            raise ValidationError(f"{source}: expr/query is blank or not a string")
        tokens = _METRIC_TOKEN_RE.findall(expr)
        for raw in tokens:
            family = QUERYABLE_METRIC_NAMES.get(raw)
            if family is None:
                raise ValidationError(
                    f"{source}: {raw!r} is not a queryable graph_loader_* name "
                    f"(not in QUERYABLE_METRIC_NAMES; check the exact suffix for "
                    f"its metric type) in expr/query: {expr!r}"
                )
            unique_families.add(family)
    return unique_families


def _collect_panels(panels: list[dict]) -> list[dict]:
    """Flatten panels, descending into a collapsed row's nested ``panels``.

    A Grafana row panel can carry its own children in ``panels`` (when the
    row is collapsed on save) instead of them appearing as siblings in the
    top-level list — skipping this nesting would silently validate zero of
    a collapsed row's panels.
    """
    result: list[dict] = []
    for panel in panels:
        if panel.get("type") == "row":
            result.extend(_collect_panels(panel.get("panels", []) or []))
        else:
            result.append(panel)
    return result


def _validate_dashboards() -> tuple[int, int, list[tuple[str, str]]]:
    """Return (dashboard_count, panel_count, [(source, expr_or_query), ...])."""
    if not DASHBOARDS_DIR.is_dir():
        raise ValidationError(f"dashboards directory not found: {DASHBOARDS_DIR}")

    dashboard_files = sorted(DASHBOARDS_DIR.glob("*.json"))
    if not dashboard_files:
        raise ValidationError(f"no dashboard JSON files found under {DASHBOARDS_DIR}")

    dashboard_count = 0
    panel_count = 0
    exprs: list[tuple[str, str]] = []

    for path in dashboard_files:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValidationError(f"{path}: could not read file: {exc}") from exc
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"{path}: invalid JSON: {exc}") from exc

        title = data.get("title")
        if not isinstance(title, str) or not title.strip():
            raise ValidationError(f"{path}: dashboard has a blank or missing title")

        panels = data.get("panels")
        if not isinstance(panels, list) or not panels:
            raise ValidationError(f"{path}: dashboard has no panels")

        non_row_panels = _collect_panels(panels)
        if not non_row_panels:
            raise ValidationError(f"{path}: dashboard has no non-row panels")

        dashboard_count += 1
        panel_count += len(non_row_panels)

        for panel in non_row_panels:
            panel_title = panel.get("title", "<untitled panel>")
            targets = panel.get("targets", [])
            if not targets:
                raise ValidationError(
                    f"{path}: panel {panel_title!r} has no targets"
                )
            for target in targets:
                expr = target.get("expr")
                source = f"{path.name}::{panel_title}::{target.get('refId', '?')}"
                exprs.append((source, expr))

        # Template variables (dropdowns): a typo in a `label_values(...)`
        # query silently empties that dropdown rather than erroring —
        # reviewer finding S4-SF2. Checked the same way as panel exprs.
        for tvar in data.get("templating", {}).get("list", []) or []:
            if tvar.get("type") != "query":
                continue
            query = tvar.get("query")
            if isinstance(query, dict):
                query = query.get("query")
            if query is None:
                continue
            source = f"{path.name}::templating::{tvar.get('name', '?')}"
            exprs.append((source, query))

    return dashboard_count, panel_count, exprs


def _validate_alerts() -> list[tuple[str, str]]:
    """Return [(source, expr), ...] from every rule's expr: in alerts/*.yml.

    Deliberately does not use a YAML parser (stdlib has none) — this is a
    line-oriented `expr:` extraction, sufficient because every alert rule
    file in this repo is authored with `expr:` as a scalar or a `>`-folded
    block scalar starting on the same line or the following indented
    lines. This is intentionally narrow in scope (it only has to catch a
    metric-name typo, not fully parse Prometheus rule YAML — promtool
    check rules is the source of truth for full semantic validation).
    """
    exprs: list[tuple[str, str]] = []
    for path_str in sorted(glob.glob(ALERTS_GLOB)):
        path = Path(path_str)
        lines = path.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            stripped = lines[i].strip()
            if stripped.startswith("expr:"):
                value = stripped[len("expr:"):].strip()
                collected = []
                if value and value not in (">", "|", ">-", "|-"):
                    collected.append(value.strip('"').strip("'"))
                else:
                    # Folded/literal block scalar: consume subsequent
                    # more-indented lines.
                    base_indent = len(lines[i]) - len(lines[i].lstrip())
                    j = i + 1
                    while j < len(lines):
                        line = lines[j]
                        if not line.strip():
                            j += 1
                            continue
                        indent = len(line) - len(line.lstrip())
                        if indent <= base_indent:
                            break
                        collected.append(line.strip())
                        j += 1
                    i = j - 1
                exprs.append((f"{path.name}::expr", " ".join(collected)))
            i += 1
    if not exprs:
        raise ValidationError(f"no expr: lines found under {ALERTS_GLOB}")
    return exprs


def main() -> int:
    try:
        dashboard_count, panel_count, dashboard_exprs = _validate_dashboards()
        alert_exprs = _validate_alerts()
        all_names = _check_metric_names(dashboard_exprs + alert_exprs)
    except ValidationError as exc:
        print(f"validate_monitoring: FAIL: {exc}", file=sys.stderr)
        return 1

    print(
        f"{dashboard_count} dashboard(s), {panel_count} panel(s), "
        f"{len(alert_exprs)} alert expr(s), {len(all_names)} unique metric "
        f"name(s), all known"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
