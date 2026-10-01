"""Read-only Databricks runtime preflight for the packaged RailPulse project."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Protocol

from railpulse import __version__

PREFLIGHT_CONTRACT_VERSION = "databricks-runtime-preflight-v1"


class DatabricksPreflightError(RuntimeError):
    """Raised when the managed runtime cannot provide required preflight evidence."""


class _QueryResult(Protocol):
    def first(self) -> object | None: ...


class _SparkSession(Protocol):
    version: str

    def sql(self, query: str) -> _QueryResult: ...


@dataclass(frozen=True)
class DatabricksPreflightResult:
    """Versioned, non-writing evidence returned by the managed runtime."""

    contract_version: str
    package_version: str
    spark_version: str
    databricks_runtime_version: str
    current_catalog: str
    current_schema: str


def _required_text(value: object, *, field_name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise DatabricksPreflightError(f"Managed runtime returned no {field_name}.")
    return text


def collect_databricks_preflight(spark: _SparkSession) -> DatabricksPreflightResult:
    """Collect read-only package, Spark, and catalog evidence from an active session."""

    spark_version = _required_text(getattr(spark, "version", None), field_name="Spark version")
    row = spark.sql(
        "SELECT current_catalog() AS current_catalog, current_schema() AS current_schema"
    ).first()
    if row is None:
        raise DatabricksPreflightError("Managed runtime returned no catalog context row.")

    try:
        current_catalog = row["current_catalog"]  # type: ignore[index]
        current_schema = row["current_schema"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise DatabricksPreflightError(
            "Managed runtime returned an incompatible catalog context row."
        ) from error

    return DatabricksPreflightResult(
        contract_version=PREFLIGHT_CONTRACT_VERSION,
        package_version=__version__,
        spark_version=spark_version,
        databricks_runtime_version=os.environ.get("DATABRICKS_RUNTIME_VERSION", "unknown"),
        current_catalog=_required_text(current_catalog, field_name="current catalog"),
        current_schema=_required_text(current_schema, field_name="current schema"),
    )


def main() -> int:
    """Run the non-writing preflight inside a managed Spark task."""

    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = collect_databricks_preflight(spark)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
