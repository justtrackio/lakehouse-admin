import json
import os
import re
import sys
import urllib.error
import urllib.request

from datetime import date, datetime, timezone

PROCEDURE_REWRITE_DATA_FILES = "rewrite_data_files"
PROCEDURE_EXPIRE_SNAPSHOTS = "expire_snapshots"
PROCEDURE_REMOVE_ORPHAN_FILES = "remove_orphan_files"


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


def callback_enabled() -> bool:
    value = os.getenv("TASK_CALLBACK_ENABLED", "false").strip().lower()
    return value == "true"


def callback_url() -> str:
    return os.getenv("TASK_CALLBACK_URL", "").strip()


def post_procedure_result(query: str, rows: list[dict], meta: dict | None = None) -> None:
    if not callback_enabled():
        return

    url = callback_url()
    if not url:
        raise ValueError("TASK_CALLBACK_URL is required when callback is enabled")

    payload = {
        "query": query,
        "rows": rows,
        "meta": {
            "sent_at": datetime.now(timezone.utc).isoformat(),
            **(meta or {}),
        },
    }

    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status < 200 or response.status >= 300:
            raise urllib.error.HTTPError(
                url,
                response.status,
                f"unexpected callback status {response.status}",
                response.headers,
                None,
            )


def report_callback_failure(err: Exception) -> None:
    print(json.dumps({"callback_error": str(err)}, indent=2), file=sys.stderr)


def task_procedure() -> str:
    procedure = os.getenv("TASK_PROCEDURE", PROCEDURE_REWRITE_DATA_FILES).strip()
    if not procedure:
        procedure = PROCEDURE_REWRITE_DATA_FILES

    if procedure not in {
        PROCEDURE_REWRITE_DATA_FILES,
        PROCEDURE_EXPIRE_SNAPSHOTS,
        PROCEDURE_REMOVE_ORPHAN_FILES,
    }:
        raise ValueError(f"unsupported TASK_PROCEDURE: {procedure}")

    return procedure


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


def older_than_timestamp() -> str:
    raw_value = require_env("OLDER_THAN")

    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError(f"OLDER_THAN must be RFC3339, got: {raw_value}") from err

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def build_rewrite_data_files_query() -> str:
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


def build_expire_snapshots_query() -> str:
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse").strip() or "lakehouse"
    database = os.getenv("ICEBERG_DATABASE", "main").strip() or "main"
    table = require_env("ICEBERG_TABLE")
    require_env("RETENTION_DAYS")
    clean_expired_metadata = bool_string("CLEAN_EXPIRED_METADATA", "true")

    qualified_table = f"{database}.{table}"
    older_than = older_than_timestamp()

    return f"""
CALL {catalog}.system.expire_snapshots(
  table => {sql_literal(qualified_table)},
  older_than => TIMESTAMP {sql_literal(older_than)},
  clean_expired_metadata => {clean_expired_metadata}
)
""".strip()


def build_remove_orphan_files_query() -> str:
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse").strip() or "lakehouse"
    database = os.getenv("ICEBERG_DATABASE", "main").strip() or "main"
    table = require_env("ICEBERG_TABLE")
    require_env("RETENTION_DAYS")

    qualified_table = f"{database}.{table}"
    older_than = older_than_timestamp()

    return f"""
CALL {catalog}.system.remove_orphan_files(
  table => {sql_literal(qualified_table)},
  older_than => TIMESTAMP {sql_literal(older_than)}
)
""".strip()


def build_query(procedure: str) -> str:
    if procedure == PROCEDURE_REWRITE_DATA_FILES:
        return build_rewrite_data_files_query()

    if procedure == PROCEDURE_EXPIRE_SNAPSHOTS:
        return build_expire_snapshots_query()

    if procedure == PROCEDURE_REMOVE_ORPHAN_FILES:
        return build_remove_orphan_files_query()

    raise ValueError(f"unsupported TASK_PROCEDURE: {procedure}")


def app_name(procedure: str) -> str:
    if procedure == PROCEDURE_REWRITE_DATA_FILES:
        return "rewrite-data-files"

    if procedure == PROCEDURE_EXPIRE_SNAPSHOTS:
        return "expire-snapshots"

    if procedure == PROCEDURE_REMOVE_ORPHAN_FILES:
        return "remove-orphan-files"

    raise ValueError(f"unsupported TASK_PROCEDURE: {procedure}")


def main() -> int:
    spark = None

    try:
        procedure = task_procedure()

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName(app_name(procedure)).getOrCreate()

        query = build_query(procedure)
        print(json.dumps({"query": query}, indent=2))

        rows = [row.asDict(recursive=True) for row in spark.sql(query).collect()]
        print(json.dumps({"result": rows}, indent=2))

        try:
            post_procedure_result(query, rows, {"procedure": procedure})
        except Exception as callback_err:
            report_callback_failure(callback_err)

        return 0
    except Exception as err:
        print(json.dumps({"error": str(err)}, indent=2), file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
