"""Execution-mode capability checks for concurrent relationship loading."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from neo4j import Driver
from neo4j.exceptions import ClientError, CypherSyntaxError

from src.models.schema import EdgeConfig

if TYPE_CHECKING:
    from src.loader.edge_loader import EdgeRecord
    from src.loader.mix_and_batch import RoutedEdgeRecord


class ExecutionModeError(RuntimeError):
    """Raised when the selected edge execution strategy is unsupported."""


@dataclass(frozen=True)
class ServerCapabilities:
    neo4j_version: str = "unknown"
    cypher_25: bool = False


def probe_server_capabilities(driver: Driver) -> ServerCapabilities:
    """Read-only probe for Cypher 25 support; connectivity errors propagate."""
    server_info = driver.get_server_info()
    version = str(getattr(server_info, "agent", "unknown"))
    try:
        with driver.session() as session:
            session.run("CYPHER 25 RETURN 1 AS supported").consume()
        return ServerCapabilities(neo4j_version=version, cypher_25=True)
    except CypherSyntaxError:
        return ServerCapabilities(neo4j_version=version, cypher_25=False)
    except ClientError as exc:
        # Neo4j 5.26 reports an unavailable Cypher 25 dialect as an argument
        # error rather than a syntax error.  This probe issues only the fixed
        # Cypher-25 statement, so recognize that exact server response while
        # preserving authentication, authorization, and unrelated failures.
        message = str(getattr(exc, "message", "")).lower()
        if (
            exc.code == "Neo.ClientError.Statement.ArgumentError"
            and "cypher version" in message
            and "25" in message
        ):
            return ServerCapabilities(neo4j_version=version, cypher_25=False)
        raise


def require_execution_mode(mode: str, capabilities: ServerCapabilities) -> None:
    """Fail closed before polling Kafka when native scheduling is unavailable."""
    if mode == "native_disjoint" and not capabilities.cypher_25:
        raise ExecutionModeError(
            f"Execution mode native_disjoint requires Cypher 25; server={capabilities.neo4j_version}"
        )


def build_apoc_locked_upsert_query(edge_config: EdgeConfig) -> str:
    """Return the schema-derived globally ordered APOC lock and merge query."""
    return (
        "UNWIND $rows AS row\n"
        f"MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
        f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
        "WITH collect({s:s,t:t,props:row.properties}) AS rels, collect(s)+collect(t) AS all_nodes\n"
        "CALL (all_nodes) { UNWIND all_nodes AS n WITH DISTINCT n AS dist_n "
        "ORDER BY elementId(dist_n) RETURN collect(dist_n) AS sorted_nodes }\n"
        "CALL apoc.lock.nodes(sorted_nodes)\n"
        f"UNWIND rels AS rel WITH rel.s AS s, rel.t AS t, rel.props AS props "
        f"MERGE (s)-[r:`{edge_config.type}`]->(t) SET r += props"
    )


def _preflight_query(edge_config: EdgeConfig) -> str:
    """Return one aggregate endpoint-validity row for an entire transaction."""
    return (
        "UNWIND range(0, size($rows) - 1) AS row_index\n"
        "WITH row_index, $rows[row_index] AS row\n"
        f"OPTIONAL MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}})\n"
        "WITH row_index, row, count(s) AS source_matches\n"
        f"OPTIONAL MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}})\n"
        "WITH row_index, source_matches, count(t) AS target_matches\n"
        "RETURN count(*) AS checked_rows, "
        "sum(CASE WHEN source_matches = 1 AND target_matches = 1 THEN 0 ELSE 1 END) AS invalid_rows"
    )


class ApocLockedEdgeWriter:
    """Atomically preflight, globally lock, and merge one batch of edges."""

    def __init__(
        self, driver: Driver, edge_config: EdgeConfig, *,
        missing_endpoint_error_factory: Callable[[str], Exception],
    ) -> None:
        self._driver = driver
        self._error_factory = missing_endpoint_error_factory
        self._edge_type = edge_config.type
        self._preflight = _preflight_query(edge_config)
        self._query = build_apoc_locked_upsert_query(edge_config)

    def write(self, record: "EdgeRecord") -> None:
        """Write one record through the same atomic batch path."""
        self.write_batch([record])

    def write_batch(self, records: Sequence["EdgeRecord"]) -> None:
        """Preflight all endpoints, acquire sorted locks, then merge atomically."""
        if not records:
            return
        rows = [{"source_key": r.source_key, "target_key": r.target_key, "properties": r.properties} for r in records]
        with self._driver.session() as session:
            with session.begin_transaction() as transaction:
                result = transaction.run(self._preflight, rows=rows)
                counts = list(result)
                result.consume()
                if (
                    len(counts) != 1
                    or counts[0]["checked_rows"] != len(rows)
                    or counts[0]["invalid_rows"] != 0
                ):
                    raise self._error_factory(f"Edge '{self._edge_type}' did not resolve exactly one source and target endpoint")
                transaction.run(self._query, rows=rows).consume()
                transaction.commit()


class NativeDisjointEdgeWriter:
    """Cypher-25 native concurrent relationship writer, capability-gated."""

    def __init__(self, driver: Driver, edge_config: EdgeConfig, capabilities: ServerCapabilities,
                 *, missing_endpoint_error_factory: Callable[[str], Exception]) -> None:
        require_execution_mode("native_disjoint", capabilities)
        self._driver = driver; self._error_factory = missing_endpoint_error_factory
        self._edge_type = edge_config.type; self._preflight = _preflight_query(edge_config)
        self._concurrency = edge_config.execution.worker_count
        self._batch_size = edge_config.mix_and_batch.batch_size
        self._query = (
            "CYPHER 25\nUNWIND $rows AS row\n"
            f"CALL (row) {{ MATCH (s:`{edge_config.nodes.source}` {{`{edge_config.source_key_property}`: row.source_key}}) "
            f"MATCH (t:`{edge_config.nodes.target}` {{`{edge_config.target_key_property}`: row.target_key}}) "
            f"MERGE (s)-[r:`{edge_config.type}`]->(t) SET r += row.properties RETURN 1 AS wrote }} "
            "IN $concurrency CONCURRENT TRANSACTIONS OF $batch_size ROWS "
            "DISJOINT BY (row.source_key, row.target_key) ON ERROR CONTINUE REPORT STATUS AS status RETURN status"
        )

    def write_batch(self, records: Sequence["EdgeRecord"]) -> None:
        """Preflight endpoints then run native transactions and inspect all statuses."""
        if not records: return
        rows = [{"source_key": r.source_key, "target_key": r.target_key, "properties": r.properties} for r in records]
        with self._driver.session() as session:
            preflight = session.run(self._preflight, rows=rows)
            counts = list(preflight); preflight.consume()
            if (
                len(counts) != 1
                or counts[0]["checked_rows"] != len(rows)
                or counts[0]["invalid_rows"] != 0
            ):
                raise self._error_factory(f"Edge '{self._edge_type}' did not resolve exactly one source and target endpoint")
        with self._driver.session() as session:
            result = session.run(self._query, rows=rows, concurrency=self._concurrency, batch_size=self._batch_size)
            statuses = list(result); result.consume()
            if not statuses:
                raise ExecutionModeError(f"native_disjoint produced no batch status for edge '{self._edge_type}'")
            for item in statuses:
                status = item["status"]
                committed = status.get("committed") if isinstance(status, dict) else status["committed"]
                error = status.get("errorMessage") if isinstance(status, dict) else status["errorMessage"]
                if committed is not True or (isinstance(error, str) and error.strip()):
                    raise ExecutionModeError(f"native_disjoint batch failed for edge '{self._edge_type}'")

    def write(self, record: "EdgeRecord") -> None:
        """Write one record through the same preflighted native path."""
        self.write_batch([record])


@dataclass(frozen=True)
class LaneExecutionResult:
    """One lane's durable write outcome, with original routing provenance."""
    lane_id: int | None
    routed_records: tuple["RoutedEdgeRecord", ...]
    success: bool
    exception: Exception | None = None


class LaneExecutionCoordinator:
    """Bound concurrent APOC lanes while preserving FIFO within each lane."""
    def __init__(self, writer, *, mode: str, worker_count: int,
                 executor_factory=ThreadPoolExecutor) -> None:
        self._writer, self._mode, self._workers, self._executor_factory = writer, mode, worker_count, executor_factory

    def execute(self, lane_batches: Sequence[Sequence["RoutedEdgeRecord"]]) -> tuple[LaneExecutionResult, ...]:
        grouped: dict[int, list[tuple["RoutedEdgeRecord", ...]]] = {}
        for batch in lane_batches:
            if batch:
                grouped.setdefault(batch[0].lane_id, []).append(tuple(batch))
        if self._mode == "native_disjoint":
            records = tuple(item for lane in sorted(grouped) for batch in grouped[lane] for item in batch)
            try:
                self._writer.write_batch([item.record for item in records])
                return (LaneExecutionResult(None, records, True),)
            except Exception as exc:
                return (LaneExecutionResult(None, records, False, exc),)
        def run_lane(lane_id: int, batches: list[tuple["RoutedEdgeRecord", ...]]):
            items = tuple(item for batch in batches for item in batch)
            try:
                for batch in batches:
                    self._writer.write_batch([item.record for item in batch])
                return LaneExecutionResult(lane_id, items, True)
            except Exception as exc:
                return LaneExecutionResult(lane_id, items, False, exc)
        with self._executor_factory(max_workers=self._workers) as executor:
            futures = [(lane, executor.submit(run_lane, lane, items)) for lane, items in sorted(grouped.items())]
            return tuple(future.result() for _lane, future in futures)


def build_edge_execution(edge_config: EdgeConfig, driver: Driver, *, executor_factory=ThreadPoolExecutor):
    """Construct the selected writer before the loader subscribes to Kafka."""
    from src.loader.edge_loader import EdgeWriter, MissingRelationshipEndpointError
    from src.loader.mix_and_batch import LaneBatcher, MixAndBatchPartitioner
    if edge_config.execution.mode == "native_disjoint":
        capabilities = probe_server_capabilities(driver)
        require_execution_mode(edge_config.execution.mode, capabilities)
        writer = NativeDisjointEdgeWriter(driver, edge_config, capabilities,
            missing_endpoint_error_factory=MissingRelationshipEndpointError)
    else:
        writer = ApocLockedEdgeWriter(driver, edge_config,
            missing_endpoint_error_factory=MissingRelationshipEndpointError)
    return writer, MixAndBatchPartitioner(edge_config), LaneBatcher(edge_config, clock=__import__("time").monotonic), LaneExecutionCoordinator(
        writer, mode=edge_config.execution.mode, worker_count=edge_config.execution.worker_count,
        executor_factory=executor_factory
    )
