"""Managed Bronze ingestion gated by the manifest-backed source preflight task."""

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
    CatalogBronzeIngestionResult,
    load_dataset_artifacts,
    merge_catalog_bronze_records,
    read_failure_reports_bronze,
    read_telemetry_bronze,
)
from railpulse.jobs.source_preflight import SOURCE_PREFLIGHT_CONTRACT_VERSION
from railpulse.landing import DatabricksSourceLanding

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

BRONZE_INGESTION_JOB_CONTRACT_VERSION = "databricks-bronze-ingestion-v1"


@dataclass(frozen=True)
class ManagedBronzeIngestionResult:
    """Versioned source and write evidence emitted by one managed Bronze run."""

    contract_version: str
    required_source_preflight_contract_version: str
    landing_contract_version: str
    dataset_version: str
    telemetry_source_path: str
    failure_reference_source_path: str
    writes: tuple[CatalogBronzeIngestionResult, ...]


def run_managed_bronze_ingestion(
    spark: SparkSession,
    namespace: CatalogNamespace,
    *,
    source_volume: str,
    manifest_path: str,
) -> ManagedBronzeIngestionResult:
    """Read both governed files and reconcile their allowlisted catalog tables.

    The deployed workflow must run the source-preflight task successfully before calling this
    function. That task verifies file sizes and hashes; this function uses the same manifest and
    deterministic landing contract to preserve those identities on the Bronze rows.
    """

    artifacts = load_dataset_artifacts(manifest_path)
    landing = DatabricksSourceLanding(
        namespace=namespace,
        dataset_version=artifacts.dataset_version,
        volume_name=source_volume,
    )

    telemetry = read_telemetry_bronze(
        spark,
        landing.telemetry_path,
        source_sha256=artifacts.telemetry_sha256,
        dataset_version=artifacts.dataset_version,
    )
    telemetry_write = merge_catalog_bronze_records(
        telemetry,
        namespace,
        table_name=TELEMETRY_TABLE,
        source_sha256=artifacts.telemetry_sha256,
        dataset_version=artifacts.dataset_version,
    )

    failures = read_failure_reports_bronze(
        spark,
        landing.failure_reference_path,
        source_sha256=artifacts.failure_transcription_sha256,
        source_document_sha256=artifacts.failure_source_document_sha256,
        dataset_version=artifacts.dataset_version,
    )
    failure_write = merge_catalog_bronze_records(
        failures,
        namespace,
        table_name=FAILURE_TABLE,
        source_sha256=artifacts.failure_transcription_sha256,
        dataset_version=artifacts.dataset_version,
    )

    return ManagedBronzeIngestionResult(
        contract_version=BRONZE_INGESTION_JOB_CONTRACT_VERSION,
        required_source_preflight_contract_version=SOURCE_PREFLIGHT_CONTRACT_VERSION,
        landing_contract_version=landing.contract_version,
        dataset_version=artifacts.dataset_version,
        telemetry_source_path=landing.telemetry_path,
        failure_reference_source_path=landing.failure_reference_path,
        writes=(telemetry_write, failure_write),
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    parser.add_argument("--source_volume", default="source")
    parser.add_argument("--manifest_path", default="docs/dataset_manifest.json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run both managed Bronze writes after the workflow's source-preflight gate."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_bronze_ingestion(
        spark,
        namespace,
        source_volume=args.source_volume,
        manifest_path=args.manifest_path,
    )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
