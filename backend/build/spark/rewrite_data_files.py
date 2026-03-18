import json
import os
import re
import sys

from datetime import date

from callback import post_procedure_result, report_callback_failure


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")

    return value


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bool_string(name: str, default: str) -> str:
    value = os.getenv(name, default).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be 'true' or 'false', got: {value}")

    return value


def quote_identifier_path(column: str) -> str:
    parts = column.split(".")
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts):
        raise ValueError(
            "ICEBERG_WHERE_COLUMN must be a dot-separated identifier path, "
            f"got: {column}"
        )

    return ".".join(f"`{part}`" for part in parts)


def build_where_clause() -> str:
    column = require_env("ICEBERG_WHERE_COLUMN")
    from_value = require_env("ICEBERG_WHERE_FROM")
    until_value = require_env("ICEBERG_WHERE_UNTIL")
    column_expr = quote_identifier_path(column)

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", from_value):
        raise ValueError(f"ICEBERG_WHERE_FROM must be in YYYY-MM-DD format, got: {from_value}")

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", until_value):
        raise ValueError(f"ICEBERG_WHERE_UNTIL must be in YYYY-MM-DD format, got: {until_value}")

    start_date = date.fromisoformat(from_value)
    end_date = date.fromisoformat(until_value)

    if start_date >= end_date:
        raise ValueError(
            "ICEBERG_WHERE_FROM must be earlier than ICEBERG_WHERE_UNTIL "
            f"(got {from_value} and {until_value})"
        )

    return (
        f'{column_expr} >= "{start_date.isoformat()} 00:00:00" '
        f'AND {column_expr} < "{end_date.isoformat()} 00:00:00"'
    )


def build_query() -> str:
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse").strip() or "lakehouse"
    database = os.getenv("ICEBERG_DATABASE", "main").strip() or "main"
    table = require_env("ICEBERG_TABLE")
    where = build_where_clause()
    target_file_size_bytes = os.getenv("TARGET_FILE_SIZE_BYTES", "536870912").strip()
    min_input_files = os.getenv("MIN_INPUT_FILES", "2").strip()
    partial_progress_enabled = bool_string("PARTIAL_PROGRESS_ENABLED", "true")
    partial_progress_max_commits = os.getenv("PARTIAL_PROGRESS_MAX_COMMITS", "10").strip()

    qualified_table = f"{database}.{table}"

    return f"""
CALL {catalog}.system.rewrite_data_files(
  table => {sql_literal(qualified_table)},
  where => {sql_literal(where)},
  strategy => 'binpack',
  options => map(
    'target-file-size-bytes', {sql_literal(target_file_size_bytes)},
    'min-input-files', {sql_literal(min_input_files)},
    'partial-progress.enabled', {sql_literal(partial_progress_enabled)},
    'partial-progress.max-commits', {sql_literal(partial_progress_max_commits)}
  )
)
""".strip()


def main() -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("rewrite-data-files").getOrCreate()

    try:
        query = build_query()
        print(json.dumps({"query": query}, indent=2))

        rows = [row.asDict(recursive=True) for row in spark.sql(query).collect()]
        print(json.dumps({"result": rows}, indent=2))

        try:
            post_procedure_result(query, rows, {"procedure": "rewrite_data_files"})
        except Exception as callback_err:
            report_callback_failure(callback_err)

        return 0
    except Exception as err:
        print(json.dumps({"error": str(err)}, indent=2), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
