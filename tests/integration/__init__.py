"""CI-runnable correctness regressions (Phase 6 Slice 4), distinct from ``tests/stress/``'s scale focus.

Everything here runs on a stock GitHub-hosted runner via testcontainers,
with no named `GRAPH_LOADER_IT_*` scratch stack -- unlike
`tests/test_social_planned_integration.py` and
`tests/test_relationship_bulk_e2e.py`, which need one.
"""
