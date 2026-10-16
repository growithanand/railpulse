"""Managed Gold loaded-cycle processing from verified Silver telemetry."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycle_storage import (
    CatalogGoldCycleWriteResult,
    persist_catalog_loaded_cycles,
)
from railpulse.features.cycles import (
    LOADED_CYCLE_ID_VERSION,
    aggregate_loaded_cycles,
    annotate_loaded_cycle_boundaries,
    assign_loaded_cycle_segments,
)
from railpulse.validation.silver_storage import (
    TELEMETRY_ACCEPTED_TABLE,
    catalog_silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_CYCLE_PROCESSING_JOB_CONTRACT_VERSION = "databricks-gold-cycle-processing-v1"


@dataclass(frozen=True)
class ManagedGoldCycleProcessingResult:
    """Versioned source and write evidence emitted by one managed Gold run."""

    contract_version: str
    cycle_id_version: str
    telemetry_source_table: str
    write: CatalogGoldCycleWriteResult


def run_managed_gold_cycle_processing(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldCycleProcessingResult:
    """Derive loaded cycles from managed accepted telemetry and reconcile Gold."""

    telemetry_source_table = catalog_silver_table(namespace, TELEMETRY_ACCEPTED_TABLE)
    accepted_telemetry = spark.table(telemetry_source_table)
    boundaries = annotate_loaded_cycle_boundaries(accepted_telemetry)
    segments = assign_loaded_cycle_segments(boundaries)
    cycles = aggregate_loaded_cycles(segments)
    write = persist_catalog_loaded_cycles(cycles, namespace)
    return ManagedGoldCycleProcessingResult(
        contract_version=GOLD_CYCLE_PROCESSING_JOB_CONTRACT_VERSION,
        cycle_id_version=LOADED_CYCLE_ID_VERSION,
        telemetry_source_table=telemetry_source_table,
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
    """Run managed Silver-to-Gold cycle derivation and persistence."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_cycle_processing(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
