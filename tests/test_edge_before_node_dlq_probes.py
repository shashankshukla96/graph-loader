"""Unit tests (no Docker) for tests/integration/test_edge_before_node_dlq.py's own capability probe.

Proves the probe is correct in isolation before trusting it to gate the
real integration test (P6-S4 Story 10's own DoD).

P5-S3 Story 7 update: the original ``_row_level_dlq_still_missing``
behavior-detection probe (written before Slice 3's interface existed) and
its three tests here were removed along with it -- Slice 3 has landed
with a known, committed interface, so the integration test now drives it
directly instead of probing for it at runtime. ``_retry_consumer_available``
stays as a guard.
"""
from __future__ import annotations

from unittest.mock import patch

from tests.integration.test_edge_before_node_dlq import _retry_consumer_available


def test_retry_consumer_available_true_for_the_real_module() -> None:
    # BUG-1: the probe used to name src.orchestrator.retry_consumer (wrong).
    assert _retry_consumer_available() is True


def test_retry_consumer_available_false_when_find_spec_finds_nothing() -> None:
    with patch("importlib.util.find_spec", return_value=None):
        assert _retry_consumer_available() is False


def test_retry_consumer_available_true_once_the_module_exists() -> None:
    with patch("importlib.util.find_spec", return_value=object()):
        assert _retry_consumer_available() is True


def test_retry_consumer_available_never_raises_even_if_find_spec_does() -> None:
    with patch("importlib.util.find_spec", side_effect=RuntimeError("boom")):
        assert _retry_consumer_available() is False
