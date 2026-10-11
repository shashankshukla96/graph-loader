"""Metrics facade (Story 1), catalogue (Story 2), and HTTP exposition
(Story 3) tests."""
from __future__ import annotations

import ast
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from src.loader import dead_letter
from src.utils import metrics

# tests/conftest.py's autouse `_reset_metrics_globals` fixture already
# resets `metrics._process_metrics` and the DLQ-listener target/attribute
# before and after every test in the whole repo (P6-S2 review finding B3:
# these are process-wide globals that must never leak between tests, not
# only between tests in this file).


# --------------------------------------------------------------------------
# Story 1: facade skeleton
# --------------------------------------------------------------------------


def test_import_performs_no_graph_loader_registry():
    """Importing the module must not construct a GraphLoaderMetrics/
    CollectorRegistry of its own. Run in a fresh subprocess (not just
    `metrics._process_metrics is None` in-process) because the autouse
    fixture's own reset would make an in-process assertion pass trivially
    even if some other code path had already called process_metrics()
    earlier in the same interpreter (P6-S2 review finding B3b)."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import src.utils.metrics as m; "
            "assert m._process_metrics is None; "
            "print('ok')",
        ],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_independent_registries_no_duplicate_timeseries():
    a = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    b = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    assert a.registry is not b.registry
    # Constructing both did not raise; also confirm they don't share state.
    a.records_consumed.labels(kind="node", name="Person", label_or_type="Person").inc()
    assert b.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "node", "name": "Person", "label_or_type": "Person"},
    ) is None


def test_process_metrics_singleton():
    first = metrics.process_metrics()
    second = metrics.process_metrics()
    assert first is second


def test_label_policy_docstring_present():
    doc = metrics.__doc__ or ""
    assert "kind" in doc
    assert "failure_class" in doc
    assert "Never used as a label" in doc


# --------------------------------------------------------------------------
# Story 2: the metric catalogue
# --------------------------------------------------------------------------

# name -> (prometheus type, sorted labelnames)
EXPECTED_CATALOGUE: dict[str, tuple[str, tuple[str, ...]]] = {
    "graph_loader_records_consumed": ("counter", ("kind", "label_or_type", "name")),
    "graph_loader_neo4j_write_duration_seconds": ("histogram", ("kind", "name", "stage")),
    "graph_loader_deadlocks_recovered": ("counter", ("kind", "name")),
    "graph_loader_retry": ("counter", ("kind", "name", "reason")),
    "graph_loader_dlq_routed": ("counter", ("failure_class", "kind", "name")),
    "graph_loader_parked": ("counter", ("kind", "name")),
    "graph_loader_dlq_backlog": ("gauge", ("kind", "name", "queue")),
    "graph_loader_retry_stalled_seconds": ("gauge", ("kind", "name")),
    "graph_loader_parked_pair_index_wait_seconds": ("gauge", ("kind", "name")),
    "graph_loader_write_outage_seconds": ("gauge", ("kind", "name")),
    "graph_loader_consumer_lag_records": ("gauge", ("kind", "name", "topic")),
    "graph_loader_lease_wait_seconds": ("histogram", ("kind", "name")),
    "graph_loader_lease_grants": ("counter", ("kind",)),
    "graph_loader_lease_denials": ("counter", ("kind",)),
    "graph_loader_lease_wait_timeouts": ("counter", ("kind",)),
    "graph_loader_lease_held_resources": ("gauge", ("kind",)),
    "graph_loader_lease_waiters": ("gauge", ("kind",)),
    "graph_loader_plan_chunks_planned": ("gauge", ("kind",)),
    "graph_loader_plan_chunks_committed": ("gauge", ("kind",)),
    "graph_loader_plan_active_writers": ("gauge", ("kind",)),
    "graph_loader_plan_resource_wait_seconds": ("histogram", ("kind",)),
    "graph_loader_planner_rows_read": ("counter", ("kind",)),
    "graph_loader_planner_chunks_published": ("counter", ("kind",)),
    "graph_loader_process_start_time_seconds": ("gauge", ("kind", "name")),
    "graph_loader_process_up": ("gauge", ("kind", "name")),
    "graph_loader_process": ("info", ("kind", "name")),
}


def test_catalogue_completeness_names():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    names = {family.name for family in m.registry.collect()}
    assert names == metrics.EXPECTED_METRIC_FAMILIES
    assert names == set(EXPECTED_CATALOGUE)


def test_catalogue_types_exact():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    for family in m.registry.collect():
        expected_type, _ = EXPECTED_CATALOGUE[family.name]
        assert family.type == expected_type, family.name


def test_catalogue_labelnames_match_exactly_when_probed():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metric_by_name = {
        "graph_loader_records_consumed": m.records_consumed,
        "graph_loader_neo4j_write_duration_seconds": m.write_duration,
        "graph_loader_deadlocks_recovered": m.deadlocks_recovered,
        "graph_loader_retry": m.retry,
        "graph_loader_dlq_routed": m.dlq_routed,
        "graph_loader_parked": m.parked,
        "graph_loader_dlq_backlog": m.dlq_backlog,
        "graph_loader_retry_stalled_seconds": m.retry_stalled,
        "graph_loader_parked_pair_index_wait_seconds": m.parked_pair_index_wait,
        "graph_loader_write_outage_seconds": m.write_outage,
        "graph_loader_consumer_lag_records": m.consumer_lag,
        "graph_loader_lease_wait_seconds": m.lease_wait,
        "graph_loader_lease_grants": m.lease_grants,
        "graph_loader_lease_denials": m.lease_denials,
        "graph_loader_lease_wait_timeouts": m.lease_wait_timeouts,
        "graph_loader_lease_held_resources": m.lease_held_resources,
        "graph_loader_lease_waiters": m.lease_waiters,
        "graph_loader_plan_chunks_planned": m.plan_chunks_planned,
        "graph_loader_plan_chunks_committed": m.plan_chunks_committed,
        "graph_loader_plan_active_writers": m.plan_active_writers,
        "graph_loader_plan_resource_wait_seconds": m.plan_resource_wait,
        "graph_loader_planner_rows_read": m.planner_rows_read,
        "graph_loader_planner_chunks_published": m.planner_chunks_published,
        "graph_loader_process_start_time_seconds": m.process_start_time,
        "graph_loader_process_up": m.process_up,
        "graph_loader_process": m.process_info,
    }
    assert set(metric_by_name) == set(EXPECTED_CATALOGUE)
    for name, metric_obj in metric_by_name.items():
        _, expected_labels = EXPECTED_CATALOGUE[name]
        assert tuple(sorted(metric_obj._labelnames)) == expected_labels, name


def test_every_histogram_uses_shared_buckets():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    histogram_attrs = [
        attr for attr in vars(m) if isinstance(getattr(m, attr), type(m.write_duration))
    ]
    assert set(histogram_attrs) == {"write_duration", "lease_wait", "plan_resource_wait"}
    for attr in histogram_attrs:
        h = getattr(m, attr)
        assert tuple(h._upper_bounds[:-1]) == metrics.WRITE_DURATION_BUCKETS


def test_failure_class_matches_dead_letter_enum():
    assert metrics.FAILURE_CLASSES == dead_letter.FAILURE_CLASSES


def test_retry_reasons_are_closed_enum():
    assert metrics.RETRY_REASONS == ("neo4j-transient", "missing-endpoint")


def test_parked_and_missing_endpoint_retry_read_real_zero_once_touched():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.initialize_zero_series(m, kind="node", name="Person")
    assert (
        m.registry.get_sample_value(
            "graph_loader_parked_total", {"kind": "node", "name": "Person"}
        )
        == 0.0
    )
    assert (
        m.registry.get_sample_value(
            "graph_loader_retry_total",
            {"kind": "node", "name": "Person", "reason": "missing-endpoint"},
        )
        == 0.0
    )
    assert (
        m.registry.get_sample_value(
            "graph_loader_deadlocks_recovered_total", {"kind": "node", "name": "Person"}
        )
        == 0.0
    )


@pytest.mark.parametrize(
    "kind,expect_metric",
    [
        ("lease-server", "graph_loader_lease_wait_timeouts_total"),
        ("planned-dispatcher", "graph_loader_plan_active_writers"),
        ("cli", "graph_loader_planner_rows_read_total"),
    ],
)
def test_kind_only_series_read_real_zero_once_touched(kind, expect_metric):
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.initialize_zero_series(m, kind=kind, name="-")
    assert m.registry.get_sample_value(expect_metric, {"kind": kind}) == 0.0


# --------------------------------------------------------------------------
# Cardinality guard: allowlist-based static scan (P6-S2 review finding B2 —
# the original scan only denylisted ast.Subscript inside keyword args,
# which missed positional args, Call/JoinedStr/bare-name label values, and
# proved nothing about its own ability to fail).
# --------------------------------------------------------------------------

_ALLOWED_BARE_LABEL_NAMES = {
    "kind", "name", "failure_class", "reason", "stage", "topic",
    "edge_type", "label_or_type", "node_label", "queue",  # queue: closed dict keys dlq|parked
}
_ALLOWED_ATTRIBUTE_LEAVES = {"label", "type", "_kind", "_edge_type", "edge_type"}
# `_kind`/`_edge_type` are private instance attributes set once, at
# construction, from a validated closed-enum string (`_kind`) or the
# schema's own `edge_config.type` (`_edge_type`) - e.g.
# `ApocLockedEdgeWriter._kind`/`._edge_type` (Story 5). `edge_type` (no
# leading underscore) is the same value read off a schema-derived object
# instead of `self` - e.g. `chunk.edge_type` (Story 6, `PlannedChunk`'s own
# schema-validated field, never a raw record). Allowing these specific
# attribute names (not attribute access in general) keeps the guard
# tight: a hypothetical `self._raw_message` or `record.value` attribute
# would still be rejected.


def _is_pure_name_or_attr_chain(node: ast.expr) -> bool:
    """True if `node` is a Name, or an Attribute chain resolving to a Name
    at its base — i.e. never a Call or Subscript anywhere in the chain."""
    if isinstance(node, ast.Name):
        return True
    if isinstance(node, ast.Attribute):
        return _is_pure_name_or_attr_chain(node.value)
    return False


def _is_safe_label_value(node: ast.expr) -> bool:
    """Allowlist: a string literal, a bare name from the closed set of
    parameter/loop-variable names this catalogue's call sites use, or an
    attribute chain ending in `.label`/`.type` (schema-derived:
    `self._node_config.label`, `edge_config.type`) with no Call/Subscript
    anywhere in the chain. Anything else (Call, JoinedStr/f-string, BinOp,
    Subscript, or an unlisted bare name) is unsafe."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str)
    if isinstance(node, ast.Name):
        return node.id in _ALLOWED_BARE_LABEL_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in _ALLOWED_ATTRIBUTE_LEAVES and _is_pure_name_or_attr_chain(node.value)
    return False


def _scan_labels_calls(tree: ast.AST, filename: str) -> list[str]:
    violations: list[str] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "labels":
            continue
        args_to_check = list(node.args) + [kw.value for kw in node.keywords]
        for value in args_to_check:
            if not _is_safe_label_value(value):
                violations.append(
                    f"{filename}:{node.lineno}: labels(...) argument "
                    f"{ast.dump(value)[:60]} is not a literal, a documented "
                    f"enum/parameter name, or a .label/.type attribute chain"
                )
    return violations


def test_cardinality_guard_checker_flags_known_bad_patterns():
    """Self-test proving the checker can actually fail (P6-S2 review B2):
    every one of these five shapes must be flagged."""
    bad_snippets = [
        'x.labels("node", record["id"])',
        'x.labels(name=record.get("id"))',
        'x.labels(reason=str(exc))',
        'x.labels(name=f"{key}")',
        'key = rec["id"]\nx.labels(name=key)',
    ]
    for snippet in bad_snippets:
        tree = ast.parse(snippet)
        assert _scan_labels_calls(tree, "<test>") != [], snippet


def test_cardinality_guard_checker_allows_known_good_patterns():
    good_snippets = [
        'x.labels(kind="node", name=self._node_config.label, stage="node-write")',
        'x.labels(kind=kind, name=name, reason=reason)',
        'x.labels(kind="edge", name=edge_config.type, failure_class="malformed")',
        'x.labels(kind="lease-server")',
    ]
    for snippet in good_snippets:
        tree = ast.parse(snippet)
        assert _scan_labels_calls(tree, "<test>") == [], snippet


def test_cardinality_guard_no_unsafe_labels_call_under_src():
    src_root = Path(__file__).resolve().parent.parent / "src"
    violations: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(_scan_labels_calls(tree, str(path)))
    assert violations == [], "\n".join(violations)


# --------------------------------------------------------------------------
# P6-S2 review finding R1 (blocking): the `.labels(...)` scan above cannot
# see a value that flows through `start_metrics_server`/
# `initialize_zero_series`'s own `kind=`/`name=` parameters before reaching
# a `.labels(...)` call inside `src/utils/metrics.py` itself — those two
# bare names are allowlisted there by design (metrics.py's own code passes
# `kind=kind, name=name` internally). Without this second scan,
# `start_metrics_server(m, kind="cli", name=run_id)` would pass the first
# scan even though `run_id` is exactly the unbounded-cardinality value
# Decision 2 forbids. This scan checks the *call site* (node_loader.py,
# lease_server.py, etc.), not the helper's own internals.
# --------------------------------------------------------------------------

_METRICS_ENTRYPOINT_FUNCS = {"start_metrics_server", "initialize_zero_series"}
_ALLOWED_NAME_ATTRIBUTE_LEAVES = {"label", "type", "node_label", "edge_type"}


def _is_safe_kind_literal(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in metrics.PROCESS_KINDS
    )


def _is_safe_name_value(node: ast.expr) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return True
    if isinstance(node, ast.Attribute):
        return node.attr in _ALLOWED_NAME_ATTRIBUTE_LEAVES and _is_pure_name_or_attr_chain(node.value)
    return False


def _scan_metrics_entrypoint_calls(tree: ast.AST, filename: str) -> list[str]:
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        func_name = func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else None
        if func_name not in _METRICS_ENTRYPOINT_FUNCS:
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        if "kind" in kwargs and not _is_safe_kind_literal(kwargs["kind"]):
            violations.append(
                f"{filename}:{node.lineno}: {func_name}(...) kind= is not a "
                f"string literal from PROCESS_KINDS"
            )
        if "name" in kwargs and not _is_safe_name_value(kwargs["name"]):
            violations.append(
                f"{filename}:{node.lineno}: {func_name}(...) name= is not a "
                f"literal or a .label/.type/.node_label/.edge_type attribute "
                f"chain (a run_id/plan_id variable would be flagged here)"
            )
    return violations


def test_metrics_entrypoint_guard_flags_run_id_and_plan_id():
    bad_snippets = [
        'start_metrics_server(m, kind="cli", name=run_id)',
        'start_metrics_server(m, kind="planned-dispatcher", name=plan.plan_id)',
        'start_metrics_server(m, kind="clock", name=args.run_id)',
        'start_metrics_server(m, kind=some_variable, name="-")',
        'initialize_zero_series(m, kind="node", name=run_id)',
    ]
    for snippet in bad_snippets:
        tree = ast.parse(snippet)
        assert _scan_metrics_entrypoint_calls(tree, "<test>") != [], snippet


def test_metrics_entrypoint_guard_allows_current_call_sites():
    good_snippets = [
        'start_metrics_server(m, kind="node", name=args.node_label)',
        'start_metrics_server(m, kind="edge", name=args.edge_type)',
        'start_metrics_server(m, kind="planned-writer", name="-")',
        'start_metrics_server(m, kind="lease-server", name="-")',
        'start_metrics_server(m, kind="clock", name="-")',
        'start_metrics_server(m, kind="planned-dispatcher", name="-")',
        'initialize_zero_series(m, kind="node", name=node_config.label)',
        'initialize_zero_series(m, kind="edge", name=edge_config.type)',
    ]
    for snippet in good_snippets:
        tree = ast.parse(snippet)
        assert _scan_metrics_entrypoint_calls(tree, "<test>") == [], snippet


def test_metrics_entrypoint_guard_no_violations_under_src():
    src_root = Path(__file__).resolve().parent.parent / "src"
    violations: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(_scan_metrics_entrypoint_calls(tree, str(path)))
    assert violations == [], "\n".join(violations)


# Iterables whose element is treated as "one record" for the per-record-
# loop guard below (matches this codebase's own singular-loop-variable
# style: `for record in records`, `for row in cursor.fetchall()`, etc.).
# Deliberately excludes "batch"/"batches": a `for batch in batches:` loop is
# this codebase's legitimate per-*batch* unit (e.g. edge_execution.py's lane
# coordinator), and a correct per-batch `.observe()` there must not be
# flagged (P6-S2 review finding R4).
_PER_RECORD_ITERABLE_HINTS = ("record", "records", "row", "rows", "msg", "msgs", "message", "messages")


def _iterable_root_name(node: ast.expr) -> str | None:
    """Best-effort: the base Name id of a for-loop's iterable, unwrapping
    one level of enumerate()/zip() so `for i, row in enumerate(rows)`
    resolves to `rows`, not `enumerate`."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("enumerate", "zip") and node.args:
        return _iterable_root_name(node.args[0])
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _for_loop_target_names(target: ast.expr) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        names: list[str] = []
        for elt in target.elts:
            names.extend(_for_loop_target_names(elt))
        return names
    return []


def _scan_per_record_loop_metrics_calls(tree: ast.AST, filename: str) -> list[str]:
    violations: list[str] = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if not isinstance(node, (ast.For, ast.AsyncFor)):
                continue
            target_names = {n.lower() for n in _for_loop_target_names(node.target)}
            root = _iterable_root_name(node.iter)
            is_per_record = bool(target_names & set(_PER_RECORD_ITERABLE_HINTS)) or (
                root is not None and root.lower() in _PER_RECORD_ITERABLE_HINTS
            )
            if not is_per_record:
                continue
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Attribute)
                    and inner.func.attr in ("inc", "observe")
                    and isinstance(inner.func.value, ast.Call)
                    and isinstance(inner.func.value.func, ast.Attribute)
                    and inner.func.value.func.attr == "labels"
                ):
                    violations.append(f"{filename}:{inner.lineno}: metrics call inside a per-record loop")
    return violations


def test_cardinality_guard_no_per_record_loop_metrics_call():
    src_root = Path(__file__).resolve().parent.parent / "src"
    violations: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(_scan_per_record_loop_metrics_calls(tree, str(path)))
    assert violations == [], "\n".join(violations)


def test_per_record_loop_guard_flags_known_bad_pattern():
    """Self-test proving the scan can fail (P6-S2 review R4: it previously
    had no negative test at all)."""
    bad = (
        "def f():\n"
        "    for record in records:\n"
        "        metrics.x.labels(kind='node').inc()\n"
    )
    tree = ast.parse(bad)
    assert _scan_per_record_loop_metrics_calls(tree, "<test>") != []


def test_per_record_loop_guard_allows_per_batch_pattern():
    """A `for batch in batches:` loop with one `.observe()` per batch must
    never be flagged — it is this codebase's legitimate unit, not the
    per-record regression this guard exists to catch."""
    good = (
        "def f():\n"
        "    for batch in batches:\n"
        "        metrics.x.labels(kind='edge').observe(batch.elapsed)\n"
    )
    tree = ast.parse(good)
    assert _scan_per_record_loop_metrics_calls(tree, "<test>") == []


# --------------------------------------------------------------------------
# Story 3: HTTP exposition
# --------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_start_metrics_server_disabled(monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="node", name="Person")
    assert server is None


def test_start_metrics_server_binds_and_serves(monkeypatch):
    port = _free_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="node", name="Person", addr="127.0.0.1", port=port)
    try:
        assert server is not None
        assert (
            m.registry.get_sample_value("graph_loader_process_up", {"kind": "node", "name": "Person"})
            == 1.0
        )
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
        assert "# HELP" in body
        assert "# TYPE" in body
    finally:
        metrics.stop_metrics_server(server)


def test_start_metrics_server_port_conflict_logs_and_returns_none(monkeypatch, caplog):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    try:
        m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
        with caplog.at_level("WARNING"):
            server = metrics.start_metrics_server(m, kind="node", name="Person", addr="127.0.0.1", port=port)
        assert server is None
        assert any("reason=bind_failed" in rec.message for rec in caplog.records)
    finally:
        blocker.close()


def test_start_metrics_server_ephemeral_fallback(monkeypatch, caplog):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    try:
        m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
        with caplog.at_level("WARNING"):
            server = metrics.start_metrics_server(
                m, kind="cli", name="run-1", addr="127.0.0.1", port=port, allow_ephemeral_fallback=True
            )
        try:
            assert server is not None
            assert server.server_port != port
            warnings = [rec.message for rec in caplog.records if rec.levelname == "WARNING"]
            assert any("reason=port_in_use" in msg and f"fallback_port={server.server_port}" in msg for msg in warnings)
        finally:
            metrics.stop_metrics_server(server)
    finally:
        blocker.close()


def test_start_metrics_server_never_raises_on_unexpected_exception(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setattr(metrics, "start_http_server", boom)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="node", name="Person")
    assert server is None


def test_start_metrics_server_bad_port_env_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", "not-a-number")
    monkeypatch.setattr(metrics, "start_http_server", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not be reached with a raw ValueError")))
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    # A bad port env value must never raise out of start_metrics_server.
    server = metrics.start_metrics_server(m, kind="node", name="Person")
    assert server is None  # the patched start_http_server always raises


@pytest.mark.parametrize("bad_port_value", ["0", "-1", "70000", "999999"])
def test_start_metrics_server_out_of_range_port_env_falls_back_to_default_port(monkeypatch, bad_port_value):
    """P6-S2 review finding F1: '0' and out-of-range values parse as a
    valid int but are not valid ports - they must fall back to
    DEFAULT_METRICS_PORT (9464), not be handed to start_http_server."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", bad_port_value)
    seen_ports = []

    def fake_start_http_server(port, addr, registry):
        seen_ports.append(port)
        return (MagicMock(server_port=port), MagicMock())

    monkeypatch.setattr(metrics, "start_http_server", fake_start_http_server)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.start_metrics_server(m, kind="node", name="Person")
    assert seen_ports == [metrics.DEFAULT_METRICS_PORT]


def test_start_metrics_server_ephemeral_branch_never_raises_on_unexpected_exception(monkeypatch):
    """Covers the reviewer's B1 finding: an exception inside the ephemeral-
    fallback branch (not just the primary bind) must not escape."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    calls = {"n": 0}

    def flaky(port, addr, registry):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("primary bind refused")
        raise RuntimeError("fallback bind exploded unexpectedly")

    monkeypatch.setattr(metrics, "start_http_server", flaky)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(
        m, kind="cli", name="run-1", allow_ephemeral_fallback=True
    )
    assert server is None


def test_default_port_is_9464_not_9090():
    assert metrics.DEFAULT_METRICS_PORT == 9464


# --------------------------------------------------------------------------
# DLQ listener seam
# --------------------------------------------------------------------------


def test_install_dlq_routed_listener_targets_isolated_metrics():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.install_dlq_routed_listener(m)
    dead_letter.on_dlq_routed("node", "Person", "person-events.dlq", "malformed")
    assert (
        m.registry.get_sample_value(
            "graph_loader_dlq_routed_total",
            {"kind": "node", "name": "Person", "failure_class": "malformed"},
        )
        == 1.0
    )


def test_install_dlq_routed_listener_defaults_to_process_metrics():
    metrics.install_dlq_routed_listener(None)
    dead_letter.on_dlq_routed("edge", "KNOWS", "knows-events.dlq", "malformed")
    pm = metrics.process_metrics()
    assert (
        pm.registry.get_sample_value(
            "graph_loader_dlq_routed_total",
            {"kind": "edge", "name": "KNOWS", "failure_class": "malformed"},
        )
        == 1.0
    )


def test_dlq_routed_listener_does_not_leak_into_a_later_test():
    """P6-S2 review B3a: after a previous test installed a listener bound
    to its own isolated metrics instance, a later test that never touches
    the DLQ seam must see dead_letter.on_dlq_routed back to its original
    (no-op) function, restored by tests/conftest.py's autouse fixture."""
    assert dead_letter.on_dlq_routed("node", "Person", "t", "malformed") is None


# --------------------------------------------------------------------------
# Parked listener seam (P5-S4 Story 7)
# --------------------------------------------------------------------------


def test_install_parked_listener_targets_isolated_metrics():
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.install_parked_listener(m)
    dead_letter.on_parked("edge", "KNOWS", "knows.dlq.parked")
    assert (
        m.registry.get_sample_value(
            "graph_loader_parked_total", {"kind": "edge", "name": "KNOWS"},
        )
        == 1.0
    )


def test_install_parked_listener_defaults_to_process_metrics():
    metrics.install_parked_listener(None)
    dead_letter.on_parked("edge", "KNOWS", "knows.dlq.parked")
    pm = metrics.process_metrics()
    assert (
        pm.registry.get_sample_value(
            "graph_loader_parked_total", {"kind": "edge", "name": "KNOWS"},
        )
        == 1.0
    )


def test_parked_listener_does_not_leak_into_a_later_test():
    assert dead_letter.on_parked("edge", "KNOWS", "t") is None


# --------------------------------------------------------------------------
# Story 9: snapshot writer (facade-level behavior; per-process SIGTERM
# tests live alongside each process kind's own test file)
# --------------------------------------------------------------------------


def test_write_metrics_snapshot_noop_when_unset(tmp_path):
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    metrics.write_metrics_snapshot(m, None)
    metrics.write_metrics_snapshot(m, "")
    assert list(tmp_path.iterdir()) == []


def test_write_metrics_snapshot_noop_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    path = tmp_path / "metrics-node-Person-0.prom"
    metrics.write_metrics_snapshot(m, path)
    assert not path.exists()


def test_write_metrics_snapshot_writes_prometheus_text(tmp_path, monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    m.records_consumed.labels(kind="node", name="Person", label_or_type="Person").inc(5)
    path = tmp_path / "metrics-node-Person-0.prom"
    metrics.write_metrics_snapshot(m, path)
    text = path.read_text()
    assert "graph_loader_records_consumed_total" in text
    assert 'kind="node"' in text
    assert not path.with_name(path.name + ".tmp").exists()


def test_write_metrics_snapshot_failure_is_logged_not_raised(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    bad_path = tmp_path / "does" / "not" / "exist" / "metrics.prom"
    with caplog.at_level("WARNING"):
        # Parent directory does not exist -> OSError, must be swallowed.
        metrics.write_metrics_snapshot(m, bad_path)
    assert any("stage=metrics-snapshot" in rec.message for rec in caplog.records)


# ── P6-MET-07: periodic snapshots ───────────────────────────────────────────


def _wait_for(predicate, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_periodic_snapshots_rewrite_file_and_stop_on_final_write(tmp_path, monkeypatch):
    from prometheus_client.parser import text_string_to_metric_families

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    counter = m.records_consumed.labels(kind="node", name="Person", label_or_type="Person")
    path = tmp_path / "metrics-node-Person.prom"
    assert metrics.start_periodic_snapshots(m, path, interval_s=0.05)
    try:
        assert _wait_for(path.exists)
        counter.inc(7)

        def _sees_seven():
            try:
                fams = {f.name: f for f in text_string_to_metric_families(path.read_text())}
            except Exception:
                return False
            fam = fams.get("graph_loader_records_consumed")
            return bool(fam) and any(s.value == 7 for s in fam.samples if s.name.endswith("_total"))

        assert _wait_for(_sees_seven)  # later tick picked up the change, file parseable
        assert not list(tmp_path.glob("*.tmp"))  # atomic: no temp leftovers
    finally:
        metrics.write_metrics_snapshot(m, path)  # final write stops the timer
    assert id(m) not in metrics._snapshotters
    mtime = path.stat().st_mtime_ns
    import time

    time.sleep(0.2)
    assert path.stat().st_mtime_ns == mtime  # no writes after shutdown


def test_periodic_snapshot_uses_atomic_replace(tmp_path, monkeypatch):
    import os

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    path = tmp_path / "m.prom"
    calls = []
    real = os.replace
    monkeypatch.setattr(metrics.os, "replace", lambda a, b: (calls.append((str(a), str(b))), real(a, b))[1])
    metrics._write_snapshot_file(m, path)
    (src, dst), = calls
    assert src != dst and src.endswith(".tmp") and dst == str(path)


def test_periodic_snapshot_errors_do_not_propagate_and_timer_survives(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    path = tmp_path / "missing-dir" / "m.prom"  # write fails every tick
    assert metrics.start_periodic_snapshots(m, path, interval_s=0.02)
    try:
        import time

        time.sleep(0.15)
        assert metrics._snapshotters[id(m)]._thread.is_alive()
        path.parent.mkdir()
        assert _wait_for(path.exists)  # recovers once the dir exists
    finally:
        metrics.stop_periodic_snapshots(m)


def test_start_metrics_server_starts_timer_from_env_and_respects_interval_env(tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "1")
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "1")
    path = tmp_path / "metrics-node-X.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(path))
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", "0")  # invalid -> default; use explicit port below
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="node", name="X", addr="127.0.0.1", port=0)
    try:
        assert _wait_for(path.exists)
    finally:
        metrics.stop_metrics_server(server)
        metrics.write_metrics_snapshot(m, path)
    assert id(m) not in metrics._snapshotters


def test_periodic_snapshots_disabled_by_zero_interval_or_no_path(tmp_path, monkeypatch):
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    assert metrics.start_periodic_snapshots(m, tmp_path / "a.prom", interval_s=0) is False
    assert metrics.start_periodic_snapshots(m, None, interval_s=1) is False
    monkeypatch.delenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", raising=False)
    assert metrics._snapshot_interval_seconds() == 15.0
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "junk")
    assert metrics._snapshot_interval_seconds() == 15.0
    assert not metrics._snapshotters


def test_snapshot_interval_rejects_non_finite_and_clamps(monkeypatch):
    for bad in ("inf", "-inf", "nan"):
        monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", bad)
        assert metrics._snapshot_interval_seconds() == 15.0
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "1e300")
    assert metrics._snapshot_interval_seconds() == 86400.0
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "0.05")
    assert metrics._snapshot_interval_seconds() == 1.0
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "-3")
    assert metrics._snapshot_interval_seconds() == 0.0


def test_cli_periodic_snapshot_uses_timing_dir_file_and_wins_over_snapshot_path(tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "1")
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "1")
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(tmp_path / "other.prom"))
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="cli", name="run1", addr="127.0.0.1", port=0)
    try:
        assert _wait_for((tmp_path / "metrics-cli.prom").exists)
    finally:
        metrics.stop_metrics_server(server)
        metrics.stop_periodic_snapshots(m)
    assert not (tmp_path / "other.prom").exists()


def test_cli_kind_without_timing_dir_starts_no_timer_even_with_snapshot_path(tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "1")
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S", "1")
    monkeypatch.delenv("GRAPH_LOADER_TIMING_DIR", raising=False)
    other = tmp_path / "other.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(other))
    assert metrics.cli_snapshot_path() is None
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    server = metrics.start_metrics_server(m, kind="cli", name="run1", addr="127.0.0.1", port=0)
    try:
        assert id(m) not in metrics._snapshotters
        assert not other.exists()
    finally:
        metrics.stop_metrics_server(server)
        metrics.stop_periodic_snapshots(m)


def test_metrics_enabled_false_disables_case_insensitively(tmp_path, monkeypatch):
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    for value in ("false", "FALSE", "0"):
        monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", value)
        path = tmp_path / f"{value}.prom"
        metrics.write_metrics_snapshot(m, path)
        assert not path.exists()


def test_no_periodic_write_lands_after_stop_returns(tmp_path, monkeypatch):
    import threading
    import time

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    m = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    path = tmp_path / "m.prom"
    in_write, release = threading.Event(), threading.Event()
    writes = []

    def slow_write(_m, p):
        writes.append(p)
        in_write.set()
        release.wait(timeout=10)

    monkeypatch.setattr(metrics, "_write_snapshot_file", slow_write)
    assert metrics.start_periodic_snapshots(m, path, interval_s=0.01)
    assert in_write.wait(5)
    snapshotter = metrics._snapshotters[id(m)]
    stopper = threading.Thread(target=snapshotter.stop)
    stopper.start()
    time.sleep(0.1)
    assert stopper.is_alive()  # stop() waits for the in-flight write
    release.set()
    stopper.join(5)
    count = len(writes)
    time.sleep(0.1)
    assert len(writes) == count
    metrics.stop_periodic_snapshots(m)
