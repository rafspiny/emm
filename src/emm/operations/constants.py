ROW_ESTIMATE_METRIC_NAME = "row_estimate"
METRICS_RAW_SIZES_ALL = ["total_bytes", "index_bytes", "toast_bytes", "table_bytes"]
METRICS_RAW_ALL = [ROW_ESTIMATE_METRIC_NAME] + METRICS_RAW_SIZES_ALL

PG_STAT_STATEMENTS = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"