"""Bulk relationship-mode default (owner decision 5): planned, lease for DLQ schemas, classic on request."""
from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from src import cli
from src.cli import AUTO_EDGE_PLAN, build_parser
from src.models.schema import GraphSchema

ROOT = Path(__file__).resolve().parents[1]
PLAN_OPTIONS = ("plan_writers", "plan_workers", "plan_buckets", "plan_hub_degree", "plan_chunk_rows",
                "plan_partitions", "plan_topic", "plan_dir", "plan_memory_mb", "plan_spill_dir")


def _data(*, precedence: bool = False) -> dict:
    data = yaml.safe_load((ROOT / "config" / "graph_schema.yaml").read_text())
    for edge in data["edges"]:
        edge.pop("dead_letter", None)
    if precedence:
        data.get("loading", {}).pop("mode", None)
        first = data["edges"][0]
        first.pop("topic")
        first.pop("order_by", None)
        first["topics"] = ["t-a", "t-b"]
        first["topic_conflict"] = "precedence"
    return data


def _schema(dead_letter: dict | None = None, *, precedence: bool = False, on: int = 0) -> GraphSchema:
    data = _data(precedence=precedence)
    if dead_letter is not None:
        data["edges"][on]["dead_letter"] = dead_letter
    return GraphSchema.model_validate(data)


def _args(*flags: str, mode: str = "bulk") -> Namespace:
    return build_parser().parse_args(["start", "--mode", mode, "--config", "c.yaml", *flags])


def _modes(args: Namespace) -> tuple[bool, bool, bool]:
    return bool(args.edge_plan), bool(args.edge_leases), bool(args.edge_classic)


def test_parser_has_edge_classic_flag() -> None:
    assert _args().edge_classic is False
    assert _args("--edge-classic").edge_classic is True


def test_default_is_planned_with_auto_settings_and_logs_it(caplog) -> None:
    args = _args()
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema(), args)
    assert args.edge_plan == AUTO_EDGE_PLAN and _modes(args) == (True, False, False)
    assert "stage=launch edge_mode=planned reason=default" in caplog.text
    # the same state a bare ``--edge-plan`` produces: every planned option stays auto-derived
    bare = _args("--edge-plan")
    assert bare.edge_plan == AUTO_EDGE_PLAN
    assert all(getattr(args, name) is None and getattr(bare, name) is None for name in PLAN_OPTIONS)
    assert (args.bulk_edge_overlap, args.max_concurrent_bulk_edges) == ("labels", 0)


@pytest.mark.parametrize("dead_letter", [
    {"enabled": True},
    {"enabled": True, "route_missing_endpoints_in_bulk": True},
    {"enabled": True, "max_retry_attempts": 3, "parked_topic": "x.parked"},
    {"enabled": True, "topic": "custom.dlq"},
])
def test_dead_letter_schema_defaults_to_lease(dead_letter, caplog) -> None:
    args = _args()
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema(dead_letter), args)
    assert _modes(args) == (False, True, False)
    assert "stage=launch edge_mode=lease reason=dead_letter" in caplog.text


def test_disabled_dead_letter_block_does_not_force_lease(caplog) -> None:
    args = _args()
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema({"enabled": False}), args)
    assert _modes(args) == (True, False, False)
    assert "edge_mode=planned reason=default" in caplog.text


@pytest.mark.parametrize("flag,expected,label", [
    ("--edge-plan", (True, False, False), "planned"),
    ("--edge-leases", (False, True, False), "lease"),
    ("--edge-classic", (False, False, True), "classic"),
])
def test_explicit_flags_win_even_for_a_dead_letter_schema(flag, expected, label, caplog) -> None:
    args = _args(flag)
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema({"enabled": True}), args)
    assert _modes(args) == expected
    assert f"edge_mode={label} reason=flag" in caplog.text


def test_explicit_plan_file_is_kept() -> None:
    args = _args("--edge-plan", "p.json")
    cli._resolve_bulk_edge_mode(_schema(), args)
    assert args.edge_plan == "p.json"


@pytest.mark.parametrize("flags", [
    ("--edge-plan", "--edge-leases"), ("--edge-plan", "--edge-classic"), ("--edge-leases", "--edge-classic"),
    ("--edge-plan", "p.json", "--edge-classic"),
])
def test_edge_mode_flags_are_mutually_exclusive(flags) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        cli._validate_edge_mode_flags(_args(*flags))


def test_handle_start_reports_exclusive_flags_before_anything_else(caplog) -> None:
    with patch.object(cli, "load_schema") as load:
        assert cli.handle_start(_args("--edge-plan", "--edge-classic")) == 1
    load.assert_not_called()
    assert "--edge-plan and --edge-classic are mutually exclusive" in caplog.text


def test_edge_classic_is_refused_in_stream() -> None:
    with pytest.raises(ValueError, match="--edge-classic requires --mode bulk"):
        cli._validate_edge_mode_flags(_args("--edge-classic", mode="stream"))


def test_stream_mode_is_unchanged() -> None:
    args = _args(mode="stream")
    cli._resolve_bulk_edge_mode(_schema({"enabled": True}), args)
    assert _modes(args) == (False, False, False)


@pytest.mark.parametrize("mode_flags,label", [((), "planned"), (("--edge-plan",), "planned"), (("--edge-leases",), "lease")])
def test_classic_tuning_flags_warn_and_are_ignored_outside_classic(mode_flags, label, caplog) -> None:
    args = _args(*mode_flags, "--bulk-edge-overlap", "shared", "--max-concurrent-bulk-edges", "2")
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema(), args)
    assert "tune classic only; ignored (add --edge-classic to use classic)" in caplog.text
    assert f"edge_mode={label}" in caplog.text
    assert args.max_concurrent_bulk_edges == 0
    assert args.bulk_edge_overlap == ("labels" if label == "planned" else "shared")


def test_classic_tuning_flags_are_honoured_with_edge_classic(caplog) -> None:
    args = _args("--edge-classic", "--bulk-edge-overlap", "shared", "--max-concurrent-bulk-edges", "2")
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_schema(), args)
    assert "ignored" not in caplog.text
    assert (args.bulk_edge_overlap, args.max_concurrent_bulk_edges) == ("shared", 2)


def _native_schema(dead_letter=None):
    return SimpleNamespace(edges=[SimpleNamespace(
        type="T", dead_letter=dead_letter, dead_letter_entity="t", execution=SimpleNamespace(mode="native_disjoint"))])


@pytest.mark.parametrize("dead_letter", [None, SimpleNamespace(enabled=True, topic=None)])
def test_native_disjoint_is_refused_without_edge_classic(dead_letter) -> None:
    with pytest.raises(ValueError, match=r"T use execution.mode native_disjoint.*pass --edge-classic"):
        cli._resolve_bulk_edge_mode(_native_schema(dead_letter), _args())


def test_native_disjoint_runs_with_explicit_edge_classic(caplog) -> None:
    args = _args("--edge-classic")
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(_native_schema(), args)
    assert args.edge_classic and "edge_mode=classic reason=flag" in caplog.text


def test_schema_without_edges_selects_nothing(caplog) -> None:
    args = _args()
    with caplog.at_level(logging.INFO, logger="src.cli"):
        cli._resolve_bulk_edge_mode(SimpleNamespace(edges=[]), args)
    assert _modes(args) == (False, False, False)
    assert "edge_mode" not in caplog.text


def test_precedence_works_under_the_planned_default() -> None:
    schema = _schema(precedence=True)
    args = _args()
    cli._resolve_bulk_edge_mode(schema, args)
    assert args.edge_plan == AUTO_EDGE_PLAN
    cli._require_supported_topic_conflicts(schema, args)


def test_precedence_runs_under_the_lease_default_when_another_type_has_dead_letter() -> None:
    schema = _schema({"enabled": True}, precedence=True, on=1)
    args = _args()
    cli._resolve_bulk_edge_mode(schema, args)
    assert args.edge_leases and args.edge_mode_reason == "dead_letter"
    cli._require_supported_topic_conflicts(schema, args)  # sub-stages order the topics


def test_precedence_type_with_its_own_dead_letter_runs_under_the_lease_default() -> None:
    """v7-S4 lifted the refusal: the type has one DLQ for all its topics."""
    schema = _schema({"enabled": True}, precedence=True, on=0)
    args = _args()
    cli._resolve_bulk_edge_mode(schema, args)
    assert args.edge_leases and args.edge_mode_reason == "dead_letter"
    cli._require_supported_topic_conflicts(schema, args)
