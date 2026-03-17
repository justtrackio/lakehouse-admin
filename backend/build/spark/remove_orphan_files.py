import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")

    return value


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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


def build_query() -> str:
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse").strip() or "lakehouse"
    database = os.getenv("ICEBERG_DATABASE", "main").strip() or "main"
    table = require_env("ICEBERG_TABLE")
    retention_days = require_env("RETENTION_DAYS")

    qualified_table = f"{database}.{table}"
    older_than = older_than_timestamp()

    return f"""
CALL {catalog}.system.remove_orphan_files(
  table => {sql_literal(qualified_table)},
  older_than => TIMESTAMP {sql_literal(older_than)}
)
""".strip()


def main() -> int:
    spark = SparkSession.builder.appName("remove-orphan-files").getOrCreate()

    try:
        query = build_query()
        print(json.dumps({"query": query}, indent=2))

        rows = [row.asDict(recursive=True) for row in spark.sql(query).collect()]
        print(json.dumps({"result": rows}, indent=2))

        return 0
    except Exception as err:
        print(json.dumps({"error": str(err)}, indent=2), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
