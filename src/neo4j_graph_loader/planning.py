"""Supported producer planning contracts. Planning publishes chunks and requires Kafka.

Use producer_edge_planner to construct the canonical ParallelEdgePlanner. Worker,
transport, fleet and dependency-injection hooks remain implementation details.
"""
from .loader.edge_plan import EdgePlan, EdgePlanCancelledError, EdgePlanError, EdgePlanTimeoutError
from .loader.edge_planner import (
    EdgePlanInputError, EdgeRecordSource, IterableEdgeSource, ParallelEdgePlanner,
    PlanResult, PlanSettings, plan_and_publish_edges, producer_edge_planner,
)

__all__ = [
    "PlanSettings", "EdgeRecordSource", "IterableEdgeSource", "ParallelEdgePlanner",
    "PlanResult", "EdgePlan", "producer_edge_planner", "plan_and_publish_edges",
    "EdgePlanError", "EdgePlanInputError", "EdgePlanCancelledError", "EdgePlanTimeoutError",
]
