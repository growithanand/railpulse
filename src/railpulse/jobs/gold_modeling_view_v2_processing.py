"""Managed Gold modelling-view v2 processing from governed catalog tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_profile import (
    ModelingViewProfileError,
    ModelingViewSummary,
    collect_modeling_view_summary,
)
from railpulse.evaluation.modeling_view_v2 import ModelingViewV2Error, build_modeling_view_v2
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_V2_EXCLUSION_REASON_ORDER,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.evaluation.modeling_view_v2_storage import (
    CatalogModelingViewV2WriteResult,
    ModelingViewV2PersistenceError,
    persist_catalog_modeling_view_v2,
)
from railpulse.features.failure_horizon_profile import (
    FailureHorizonProfileError,
    _collect_failure_events,
)
from railpulse.features.failure_horizon_storage import (
    FAILURE_HORIZONS_TABLE,
    catalog_failure_horizon_table,
)
from railpulse.features.failure_horizons import FAILURE_HORIZON_VERSION
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_VERSION,
    FEATURE_SNAPSHOTS_V2_TABLE,
)
from railpulse.features.feature_snapshot_v2_storage import catalog_feature_snapshot_v2_table
from railpulse.validation.silver_storage import FAILURE_ACCEPTED_TABLE, catalog_silver_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION = (
    "databricks-gold-modeling-view-v2-processing-v1"
)
_FEATURE_LINEAGE_COLUMNS = (
    "dataset_version",
    "telemetry_source_sha256",
    "telemetry_ingestion_batch_id",
)


class ManagedGoldModelingViewV2ProcessingError(RuntimeError):
    """Raised when managed inputs cannot produce one safe modelling view."""


@dataclass(frozen=True)
class ManagedModelingFeatureLineage:
    """One complete feature-snapshot source identity."""

    dataset_version: str
    source_sha256: str
    ingestion_batch_id: str


@dataclass(frozen=True)
class ManagedModelingFailureLineage:
    """One complete accepted-failure source identity."""

    dataset_version: str
    source_sha256: str
    source_document_sha256: str
    ingestion_batch_id: str


@dataclass(frozen=True)
class ManagedGoldModelingViewV2ProcessingResult:
    """Versioned lineage, population, and write evidence for one managed run."""

    contract_version: str
    modeling_view_version: str
    feature_snapshot_version: str
    failure_horizon_version: str
    feature_source_table: str
    horizon_source_table: str
    failure_source_table: str
    feature_lineage: ManagedModelingFeatureLineage
    failure_lineage: ManagedModelingFailureLineage
    accepted_failure_event_count: int
    matched_trainable_failure_event_count: int
    summary: ModelingViewSummary
    write: CatalogModelingViewV2WriteResult


def _single_feature_lineage(snapshots: DataFrame) -> ManagedModelingFeatureLineage:
    missing_columns = sorted(set(_FEATURE_LINEAGE_COLUMNS) - set(snapshots.columns))
    if missing_columns:
        raise ManagedGoldModelingViewV2ProcessingError(
            "Managed feature snapshots are missing lineage columns: " + ", ".join(missing_columns)
        )

    rows = snapshots.select(*_FEATURE_LINEAGE_COLUMNS).distinct().limit(2).collect()
    if len(rows) != 1:
        raise ManagedGoldModelingViewV2ProcessingError(
            "Managed feature snapshots must contain exactly one non-empty source lineage"
        )
    values = tuple(rows[0][column] for column in _FEATURE_LINEAGE_COLUMNS)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ManagedGoldModelingViewV2ProcessingError(
            "Managed feature snapshots contain incomplete source lineage"
        )
    return ManagedModelingFeatureLineage(*values)


def run_managed_gold_modeling_view_v2_processing(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldModelingViewV2ProcessingResult:
    """Join immutable managed features and labels, then reconcile managed Gold."""

    feature_source_table = catalog_feature_snapshot_v2_table(
        namespace,
        FEATURE_SNAPSHOTS_V2_TABLE,
    )
    horizon_source_table = catalog_failure_horizon_table(namespace, FAILURE_HORIZONS_TABLE)
    failure_source_table = catalog_silver_table(namespace, FAILURE_ACCEPTED_TABLE)

    snapshots = spark.table(feature_source_table)
    horizons = spark.table(horizon_source_table)
    failures = spark.table(failure_source_table)
    feature_lineage = _single_feature_lineage(snapshots)
    try:
        failure_rows, failure_lineage_values = _collect_failure_events(failures)
    except FailureHorizonProfileError as exc:
        raise ManagedGoldModelingViewV2ProcessingError(str(exc)) from exc
    failure_lineage = ManagedModelingFailureLineage(*failure_lineage_values)
    if feature_lineage.dataset_version != failure_lineage.dataset_version:
        raise ManagedGoldModelingViewV2ProcessingError(
            "Managed feature and failure-event dataset versions do not match"
        )

    try:
        modeling_rows = build_modeling_view_v2(
            snapshots,
            horizons,
            failure_dataset_version=failure_lineage.dataset_version,
            failure_source_sha256=failure_lineage.source_sha256,
            failure_ingestion_batch_id=failure_lineage.ingestion_batch_id,
        )
        summary = collect_modeling_view_summary(
            modeling_rows,
            failure_rows,
            modeling_view_version=MODELING_VIEW_V2_VERSION,
            exclusion_reason_order=MODELING_V2_EXCLUSION_REASON_ORDER,
        )
        write = persist_catalog_modeling_view_v2(modeling_rows, namespace)
    except (
        ModelingViewProfileError,
        ModelingViewV2Error,
        ModelingViewV2PersistenceError,
    ) as exc:
        raise ManagedGoldModelingViewV2ProcessingError(str(exc)) from exc

    if write.source_row_count != summary.cycle_count:
        raise ManagedGoldModelingViewV2ProcessingError(
            "Managed modelling rows and profiled cycles do not reconcile: "
            f"rows={write.source_row_count}, cycles={summary.cycle_count}"
        )
    matched_event_count = sum(
        event.trainable_positive_cycle_count > 0 for event in summary.failure_event_counts
    )
    return ManagedGoldModelingViewV2ProcessingResult(
        contract_version=GOLD_MODELING_VIEW_V2_PROCESSING_JOB_CONTRACT_VERSION,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        feature_snapshot_version=FEATURE_SNAPSHOT_V2_VERSION,
        failure_horizon_version=FAILURE_HORIZON_VERSION,
        feature_source_table=feature_source_table,
        horizon_source_table=horizon_source_table,
        failure_source_table=failure_source_table,
        feature_lineage=feature_lineage,
        failure_lineage=failure_lineage,
        accepted_failure_event_count=len(failure_rows),
        matched_trainable_failure_event_count=matched_event_count,
        summary=summary,
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
    """Run managed Gold-to-Gold modelling-view v2 processing."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_modeling_view_v2_processing(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
