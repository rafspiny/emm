import logging
from collections import defaultdict
from decimal import Decimal

from sqlalchemy import select, text

from src.emm.engine.data import BenchmarkRequest, ReadOnlyWorkloadType
from src.emm.models.database_base import context_session
from src.emm.models.performance import (
    Analysis,
    AnalysisReport,
    EmmAnalysisType,
    RawPerformanceRecord,
)
from src.emm.models.schema import Schema
from src.emm.operations.constants import (
    METRICS_RAW_ALL,
    PG_STAT_STATEMENTS,
    ROW_ESTIMATE_METRIC_NAME,
)

logger = logging.getLogger(__name__)


# This query is taken from stackoverflow:
# https://stackoverflow.com/questions/21738408/postgresql-list-and-order-tables-by-size
QUERY_FOR_TABLE_SIZES = """
SELECT *, pg_size_pretty(total_bytes) AS total
, pg_size_pretty(index_bytes) AS INDEX
, pg_size_pretty(toast_bytes) AS toast
, pg_size_pretty(table_bytes) AS TABLE
FROM (
SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
FROM (
  SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
          , c.reltuples AS row_estimate
          , pg_total_relation_size(c.oid) AS total_bytes
          , pg_indexes_size(c.oid) AS index_bytes
          , pg_total_relation_size(reltoastrelid) AS toast_bytes
      FROM pg_class c
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE relkind = 'r'
        AND nspname = :schema_name
  ) a
) a ORDER BY total_bytes DESC;
"""


def check_permutations_sizes(schema: Schema) -> None:
    # Fetch the table sizes information
    with context_session() as session:
        query_result = session.execute(
            text(QUERY_FOR_TABLE_SIZES), {"schema_name": schema.name}
        )

        # Analysis
        analysis = Analysis(
            name=f"{schema.name}_disk_analysis",
            description="Analysis of the tables sizes",
            type=EmmAnalysisType.SIZE,
            schema_id=schema.id,
            schema=schema,
        )
        session.add(analysis)

        # Save raw results
        permutations_by_name = {
            permutation.name: permutation for permutation in schema.permutations
        }
        raw_performance_list: list[RawPerformanceRecord] = []
        for measure in query_result:
            for metric_name in METRICS_RAW_ALL:
                metric_value = getattr(measure, metric_name)
                permutation = permutations_by_name.get(measure.table_name, None)

                if permutation is None:
                    logger.warning(
                        f"Permutation with name {measure.table_name} not found. Skipping it"
                    )
                    continue

                raw_performance = RawPerformanceRecord(
                    analysis=analysis,
                    analysis_id=analysis.id,
                    permutation_id=permutation.id,
                    permutation=permutation,
                    metric=metric_name,
                    notes="",
                    value=metric_value,
                )
                raw_performance_list.append(raw_performance)
                session.add(raw_performance)
        session.commit()

        # Build results
        performance_baseline_name = schema.original_table_name
        baseline_raw_performances = {
            raf_performance.metric: raf_performance
            for raf_performance in raw_performance_list
            if raf_performance.permutation.name == performance_baseline_name
        }

        baseline_size = _get_row_estimation_for_baseline(baseline_raw_performances)

        raw_performances_by_metric_name: dict[
            str, dict[int, RawPerformanceRecord]
        ] = defaultdict(dict)
        for raw_performance in raw_performance_list:
            # Skip baseline
            if raw_performance.permutation.name == performance_baseline_name:
                continue
            # Skip row_estimate
            if raw_performance.metric == ROW_ESTIMATE_METRIC_NAME:
                continue
            if raw_performance.metric not in raw_performances_by_metric_name:
                raw_performances_by_metric_name[raw_performance.metric] = {}
            raw_performances_by_metric_name[raw_performance.metric][
                raw_performance.permutation.id
            ] = raw_performance

        # Build reports per permutation
        for (
            metric_name,
            metrics_by_permutation_id,
        ) in raw_performances_by_metric_name.items():
            base_permutation_metric_value = baseline_raw_performances.get(
                metric_name
            ).value  # type: ignore

            computed_metric_by_permutation_id: list[tuple[int, Decimal]] = []

            for permutation_id, raw_metric in metrics_by_permutation_id.items():
                analysis = raw_metric.analysis
                permutation_metric_value = raw_metric.value
                if base_permutation_metric_value != 0:
                    improvement_percentage = (
                        Decimal(
                            base_permutation_metric_value - permutation_metric_value
                        )
                        / Decimal(base_permutation_metric_value)
                        * Decimal(100)
                    )
                    improvement_percentage = improvement_percentage.quantize(
                        Decimal("0.01")
                    )
                else:
                    improvement_percentage = Decimal(0)

                computed_metric_by_permutation_id.append(
                    (permutation_id, improvement_percentage)
                )

            # Choose the best by storing first the permutation id and the improvement percentage.
            # Later, sort them by improvement percentage descending and choose the best first element
            best_option = sorted(
                computed_metric_by_permutation_id, key=lambda x: x[1], reverse=True
            )[0]

            report = AnalysisReport(
                analysis=analysis,
                analysis_id=analysis.id,
                metric=metric_name,
                best_permutation_name=[
                    a for a in schema.permutations if a.id == best_option[0]
                ][0].name,
                improvement_percentage_over_baseline=best_option[1],
                original_metric_value=baseline_size,
                permutation_metric_value=metrics_by_permutation_id[
                    best_option[0]
                ].value,
            )
            session.add(report)
        session.commit()


def _get_row_estimation_for_baseline(
    baseline_raw_performances: dict[str, RawPerformanceRecord]
) -> int:
    if raw_performance_for_row_estimation := baseline_raw_performances.get(
        ROW_ESTIMATE_METRIC_NAME
    ):
        baseline_size = raw_performance_for_row_estimation.value
    else:
        baseline_size = -1
    return baseline_size


def generate_ro_workload_for_schema(schema: Schema):
    """
    Generate a read-only workload for the schema.
    This is a placeholder function. The actual implementation should generate
    a workload based on the schema.
    """
    # FIXME They should not be hardcoded but generated based on the schema

    return {
        ReadOnlyWorkloadType.READ_ALL: "SELECT * FROM {}",
        ReadOnlyWorkloadType.READ_PRIMARY_KEY_FILTER: "SELECT * FROM {} WHERE :priamry_key_column < 100;",
        ReadOnlyWorkloadType.READ_AGGREGATION: "SELECT COUNT(*) FROM {};",
        ReadOnlyWorkloadType.READ_AGGREGATION_FILTER: "SELECT COUNT(*) FROM {} WHERE :priamry_key_column < 100;",
        ReadOnlyWorkloadType.READ_LIKE: "SELECT name FROM {} WHERE original_table_name LIKE "
        "'original_schema_name_value%';",
        ReadOnlyWorkloadType.READ_ORDER_BY: "SELECT * FROM {} ORDER BY created DESC LIMIT 50;",
        ReadOnlyWorkloadType.READ_RANGE_FILTER: "SELECT * FROM {} WHERE :priamry_key_column BETWEEN 500 AND 1000;",
        ReadOnlyWorkloadType.READ_PAGINATION: "SELECT * FROM {} LIMIT 100 OFFSET 200;",
    }


def check_permutation_requests_performance(
    schema: Schema, benchmark_request: BenchmarkRequest
):
    """
    Create a workload and start a sequence of requests to flask, accordingly to the benchmark request.
    First, we generate some queries.
    * Standard select * - Expect maximum gain
    * Standard select where primary_key = <XXX> - Expect minimum gain
    * Standard select * non_primary_key = <XXX> - Do not know what to expect

    ADD https://www.postgresql.org/docs/current/pgstatstatements.html later on
    """

    ro_workload = generate_ro_workload_for_schema(schema)

    # Proceed with the tests
    with context_session() as session:
        # Set the search path to the new schema
        session.execute(text(f"SET search_path TO {schema.name}"))

        # Enable stats collection for statements
        session.execute(text(PG_STAT_STATEMENTS))
        session.commit()

        times_by_permutation_id: dict[int, list[float]] = defaultdict(list)
        for permutation in schema.permutations:
            # Make sure to reset the pg_stat_statements records
            reset_pg_stat_statement_records()

            for _ in range(1, 50):
                # Generate the workload
                query = ro_workload.get(ReadOnlyWorkloadType.READ_ALL)

                # Execute the workload
                session.execute(text(query.format(permutation.name))).fetchall()

            session.commit()
            times_by_permutation_id[permutation.id] = [
                a
                for a in session.execute(
                    text(
                        "select query, calls, total_exec_time, mean_exec_time, rows, 100.0 * shared_blks_hit/nullif("
                        "shared_blks_hit + shared_blks_read, 0) AS hit_percent from pg_stat_statements"
                    )
                )
                if permutation.name in a.query
            ]

        session.execute(text("SET search_path TO public"))

        # Analysis
        analysis = Analysis(
            name=f"{schema.name}_{benchmark_request.value}",
            description="Analysis of the performance",
            type=EmmAnalysisType.PERFORMANCE_RO,
            schema_id=schema.id,
            schema=schema,
        )
        session.add(analysis)

        # Get stats from pg_stats_statements

        # Save raw results
        permutations_by_id = {
            permutation.id: permutation for permutation in schema.permutations
        }
        raw_performance_list: list[RawPerformanceRecord] = []
        for permutation_id, times in times_by_permutation_id.items():
            # metric_value = getattr(measure, metric_name)
            metric_name = f"{ReadOnlyWorkloadType.READ_ALL.value}"
            metric_value = times[0].mean_exec_time  # type: ignore
            permutation = permutations_by_id.get(permutation_id, None)

            if permutation is None:
                logger.warning(
                    f"Permutation with id {permutation_id} not found. Skipping it"
                )
                continue

            raw_performance = RawPerformanceRecord(
                analysis=analysis,
                analysis_id=analysis.id,
                permutation_id=permutation.id,
                permutation=permutation,
                metric=metric_name,
                notes=f"Mean execution time over {times[0].calls} iterations",  # type: ignore
                value=metric_value,
            )
            raw_performance_list.append(raw_performance)
            session.add(raw_performance)
        session.commit()

        # Build results
        performance_baseline_name = schema.original_table_name
        baseline_raw_performances = {
            raw_performance.metric: raw_performance
            for raw_performance in raw_performance_list
            if raw_performance.permutation.name == performance_baseline_name
        }

        def _average(param: list[float]) -> float:
            return sum(param) / len(param)

        baseline_performance = _average(
            [
                raw_performance.value
                for raw_performance in baseline_raw_performances.values()
            ]
        )

        raw_performances_by_metric_name: dict[
            str, dict[int, RawPerformanceRecord]
        ] = defaultdict(dict)
        for raw_performance in raw_performance_list:
            # Skip baseline
            if raw_performance.permutation.name == performance_baseline_name:
                continue
            # Skip row_estimate
            if raw_performance.metric == ROW_ESTIMATE_METRIC_NAME:
                continue
            if raw_performance.metric not in raw_performances_by_metric_name:
                raw_performances_by_metric_name[raw_performance.metric] = {}
            raw_performances_by_metric_name[raw_performance.metric][
                raw_performance.permutation.id
            ] = raw_performance

        # Build reports per permutation
        for (
            metric_name,
            metrics_by_permutation_id,
        ) in raw_performances_by_metric_name.items():
            base_permutation_metric_value = baseline_raw_performances.get(
                metric_name
            ).value  # type: ignore

            computed_metric_by_permutation_id: list[tuple[int, Decimal, float]] = []

            for permutation_id, raw_metric in metrics_by_permutation_id.items():
                analysis = raw_metric.analysis
                permutation_metric_value = raw_metric.value
                if base_permutation_metric_value != 0:
                    improvement_percentage = (
                        Decimal(
                            base_permutation_metric_value - permutation_metric_value
                        )
                        / Decimal(base_permutation_metric_value)
                        * Decimal(100)
                    )
                    improvement_percentage = improvement_percentage.quantize(
                        Decimal("0.01")
                    )
                else:
                    improvement_percentage = Decimal(0)

                computed_metric_by_permutation_id.append(
                    (permutation_id, improvement_percentage, permutation_metric_value)
                )

            # Choose the best by storing first the permutation id and the improvement percentage.
            # Later, sort them by improvement percentage descending and choose the best first element
            best_option = sorted(
                computed_metric_by_permutation_id, key=lambda x: x[1], reverse=True
            )[0]
        best_performance_by_metric: dict[str, tuple[int, float]] = {}
        for metric_name in [ReadOnlyWorkloadType.READ_ALL.value]:
            best_performance_by_metric[metric_name] = (1, 0)

        for metric_name in [ReadOnlyWorkloadType.READ_ALL.value]:
            report = AnalysisReport(
                analysis=analysis,
                analysis_id=analysis.id,
                metric=metric_name,
                best_permutation_name=[
                    a for a in schema.permutations if a.id == best_option[0]
                ][0].name,
                improvement_percentage_over_baseline=best_option[1],
                original_metric_value=baseline_performance,
                permutation_metric_value=best_option[2],
            )
            session.add(report)
        session.commit()


def benchmark_schema(schema: Schema, benchmark_request: BenchmarkRequest):
    """
    Check size of the different permutation tables and store them in the permutation performance table.
    Additionally, it starts the analysis of the reading and writing performance of the tables.
    It also triggers the benchmark for the flask/fastapi container to see if there are improvements or
    degradations in a standard app
    """
    if benchmark_request in [BenchmarkRequest.ALL, BenchmarkRequest.TABLE_SIZE]:
        check_permutations_sizes(schema)
    if benchmark_request in [
        BenchmarkRequest.ALL,
        BenchmarkRequest.FLASK_RO,
        BenchmarkRequest.FLASK_RW,
        BenchmarkRequest.FLASK_MIX,
    ]:
        check_permutation_requests_performance(schema, benchmark_request)


def load_analysis_for_schema(schema: Schema) -> list[Analysis]:
    with context_session() as session:
        stmt = select(Analysis).filter(Analysis.schema_id == schema.id)
        return session.scalars(stmt)


def reset_pg_stat_statement_records():
    """
    Based on the documentation, it is safer to reset the collected metrics, so that we do not incur into messing up
    our stats.
    """
    with context_session() as session:
        session.execute(text("SELECT pg_stat_statements_reset()"))
        session.commit()
