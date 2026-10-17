"""Managed Gold failure-horizon processing from verified catalog tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, catalog_gold_table
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.features.failure_horizon_storage import (
    CatalogFailureHorizonWriteResult,
    persist_catalog_failure_horizons,
)
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    assign_cycle_failure_horizons,
)
from railpulse.validation.silver_storage import (
    FAILURE_ACCEPTED_TABLE,
    TELEMETRY_ACCEPTED_TABLE,
    catalog_silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION = (
    "databricks-gold-failure-horizon-processing-v1"
)
_SOURCE_LINEAGE_COLUMNS = ("dataset_version", "source_sha256", "ingestion_batch_id")


class ManagedGoldFailureHorizonProcessingError(RuntimeError):
    """Raised when managed horizon inputs do not form one safe source snapshot."""


@dataclass(frozen=True)
class ManagedSourceLineage:
    """One complete source identity read from a managed Silver table."""

    dataset_version: str
    source_sha256: str
    ingestion_batch_id: str


@dataclass(frozen=True)
class ManagedGoldFailureHorizonProcessingResult:
    """Versioned source and write evidence emitted by one managed horizon run."""

    contract_version: str
    cycle_id_version: str
    horizon_version: str
    horizon_seconds: int
    label_observation_end: str
    cycle_source_table: str
    telemetry_source_table: str
    failure_source_table: str
    telemetry_lineage: ManagedSourceLineage
    failure_lineage: ManagedSourceLineage
    write: CatalogFailureHorizonWriteResult


def _single_source_lineage(frame: DataFrame, *, label: str) -> ManagedSourceLineage:
    missing_columns = sorted(set(_SOURCE_LINEAGE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ManagedGoldFailureHorizonProcessingError(
            f"{label} is missing source-lineage columns: " + ", ".join(missing_columns)
        )

    rows = frame.select(*_SOURCE_LINEAGE_COLUMNS).distinct().limit(2).collect()
    if len(rows) != 1:
        raise ManagedGoldFailureHorizonProcessingError(
            f"{label} must contain exactly one non-empty source lineage"
        )
    values = tuple(rows[0][column] for column in _SOURCE_LINEAGE_COLUMNS)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ManagedGoldFailureHorizonProcessingError(
            f"{label} contains incomplete source lineage"
        )
    return ManagedSourceLineage(*values)


def _latest_telemetry_timestamp(frame: DataFrame) -> datetime:
    if "event_timestamp" not in frame.columns:
        raise ManagedGoldFailureHorizonProcessingError(
            "Managed accepted telemetry is missing event_timestamp"
        )
    row = frame.agg(F.max("event_timestamp").alias("label_observation_end")).first()
    value = None if row is None else row.label_observation_end
    if not isinstance(value, datetime):
        raise ManagedGoldFailureHorizonProcessingError(
            "Managed accepted telemetry has no label observation boundary"
        )
    if value.tzinfo is not None and value.utcoffset() is not None:
        raise ManagedGoldFailureHorizonProcessingError(
            "Managed telemetry observation boundary must be timezone-free"
        )
    return value


def run_managed_gold_failure_horizon_processing(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldFailureHorizonProcessingResult:
    """Derive immutable two-hour failure horizons and reconcile managed Gold."""

    cycle_source_table = catalog_gold_table(namespace, LOADED_CYCLES_TABLE)
    telemetry_source_table = catalog_silver_table(namespace, TELEMETRY_ACCEPTED_TABLE)
    failure_source_table = catalog_silver_table(namespace, FAILURE_ACCEPTED_TABLE)

    cycles = spark.table(cycle_source_table)
    telemetry = spark.table(telemetry_source_table)
    failures = spark.table(failure_source_table)
    telemetry_lineage = _single_source_lineage(
        telemetry,
        label="Managed accepted telemetry",
    )
    failure_lineage = _single_source_lineage(
        failures,
        label="Managed accepted failure events",
    )
    if telemetry_lineage.dataset_version != failure_lineage.dataset_version:
        raise ManagedGoldFailureHorizonProcessingError(
            "Managed telemetry and failure-event dataset versions do not match"
        )

    observation_end = _latest_telemetry_timestamp(telemetry)
    horizons = assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        observation_end=observation_end,
    )
    write = persist_catalog_failure_horizons(horizons, namespace)
    return ManagedGoldFailureHorizonProcessingResult(
        contract_version=GOLD_FAILURE_HORIZON_PROCESSING_JOB_CONTRACT_VERSION,
        cycle_id_version=LOADED_CYCLE_ID_VERSION,
        horizon_version=FAILURE_HORIZON_VERSION,
        horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        label_observation_end=observation_end.isoformat(sep=" "),
        cycle_source_table=cycle_source_table,
        telemetry_source_table=telemetry_source_table,
        failure_source_table=failure_source_table,
        telemetry_lineage=telemetry_lineage,
        failure_lineage=failure_lineage,
        write=write,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run managed Silver/Gold-to-Gold failure-horizon processing."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_failure_horizon_processing(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
