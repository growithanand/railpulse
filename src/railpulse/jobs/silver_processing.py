"""Managed Silver processing from the two governed Bronze tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from railpulse.catalog import CatalogNamespace
from railpulse.ingestion.bronze import (
    FAILURE_TABLE,
    TELEMETRY_TABLE,
    catalog_bronze_table,
)
from railpulse.validation.silver_failures import split_failure_events_by_quality
from railpulse.validation.silver_storage import (
    CatalogFailurePersistenceResult,
    CatalogTelemetryPersistenceResult,
    persist_catalog_failure_outputs,
    persist_catalog_telemetry_outputs,
)
from railpulse.validation.silver_telemetry import split_telemetry_by_quality

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

SILVER_PROCESSING_JOB_CONTRACT_VERSION = "databricks-silver-processing-v1"


@dataclass(frozen=True)
class ManagedSilverProcessingResult:
    """Versioned source and write evidence emitted by one managed Silver run."""

    contract_version: str
    telemetry_source_table: str
    failure_source_table: str
    telemetry: CatalogTelemetryPersistenceResult
    failures: CatalogFailurePersistenceResult


def run_managed_silver_processing(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedSilverProcessingResult:
    """Validate both managed Bronze tables and reconcile their Silver outputs."""

    telemetry_source_table = catalog_bronze_table(namespace, TELEMETRY_TABLE)
    failure_source_table = catalog_bronze_table(namespace, FAILURE_TABLE)

    telemetry_bronze = spark.table(telemetry_source_table)
    failure_bronze = spark.table(failure_source_table)
    telemetry_split = split_telemetry_by_quality(telemetry_bronze)
    failure_split = split_failure_events_by_quality(failure_bronze)

    telemetry_result = persist_catalog_telemetry_outputs(telemetry_split, namespace)
    failure_result = persist_catalog_failure_outputs(failure_split, namespace)
    return ManagedSilverProcessingResult(
        contract_version=SILVER_PROCESSING_JOB_CONTRACT_VERSION,
        telemetry_source_table=telemetry_source_table,
        failure_source_table=failure_source_table,
        telemetry=telemetry_result,
        failures=failure_result,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run managed Bronze-to-Silver validation and persistence."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_silver_processing(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
