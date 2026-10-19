"""Managed Gold feature-snapshot v2 processing from governed catalog tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.catalog import CatalogNamespace
from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_VERSION,
    CycleOperatingContextError,
    add_cycle_operating_context,
)
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, catalog_gold_table
from railpulse.features.eligibility_profile import (
    MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
    EligibilityProfileError,
    EligibilityReasonCount,
    EligibilityStatusCount,
    _collect_eligibility_counts,
)
from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
    MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
    FeatureEligibilityError,
    add_motor_current_eligibility,
)
from railpulse.features.feature_snapshot_v2 import (
    FeatureSnapshotV2Error,
    build_feature_snapshot_v2,
)
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.features.feature_snapshot_v2_storage import (
    CatalogFeatureSnapshotV2WriteResult,
    persist_catalog_feature_snapshots_v2,
)
from railpulse.features.temporal_feature_profile import (
    MotorCurrentFeatureProfileError,
    _assert_feature_semantics,
    _assert_tail_context_is_complete,
    _available_with_tail_context,
    _collect_cycle_count,
    _collect_telemetry_lineage,
    _with_internal_gap_context,
)
from railpulse.features.temporal_features import (
    MOTOR_CURRENT_FEATURE_VERSION,
    MOTOR_CURRENT_WINDOW_SECONDS,
    STATUS_AVAILABLE,
    TemporalFeatureError,
    add_motor_current_15m_features,
)
from railpulse.validation.silver_storage import (
    TELEMETRY_ACCEPTED_TABLE,
    TELEMETRY_VALIDATION_VERSION,
    catalog_silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION = (
    "databricks-gold-feature-snapshot-v2-processing-v1"
)


class ManagedGoldFeatureSnapshotV2ProcessingError(RuntimeError):
    """Raised when managed inputs cannot produce one safe feature snapshot."""


@dataclass(frozen=True)
class ManagedFeatureSnapshotSourceLineage:
    """One complete accepted-telemetry source identity."""

    dataset_version: str
    source_sha256: str
    ingestion_batch_id: str


@dataclass(frozen=True)
class _ManagedFeatureSnapshotV2Build:
    telemetry_lineage: ManagedFeatureSnapshotSourceLineage
    accepted_telemetry_record_count: int
    cycle_count: int
    eligibility_status_counts: tuple[EligibilityStatusCount, ...]
    eligibility_reason_counts: tuple[EligibilityReasonCount, ...]
    snapshots: DataFrame


@dataclass(frozen=True)
class ManagedGoldFeatureSnapshotV2ProcessingResult:
    """Versioned source, feature, eligibility, and write evidence for one run."""

    contract_version: str
    snapshot_version: str
    feature_version: str
    feature_window_seconds: int
    eligibility_profile_version: str
    eligibility_version: str
    minimum_excluded_gap_seconds: int
    cycle_context_version: str
    telemetry_validation_version: str
    cycle_source_table: str
    telemetry_source_table: str
    telemetry_lineage: ManagedFeatureSnapshotSourceLineage
    accepted_telemetry_record_count: int
    cycle_count: int
    eligibility_status_counts: tuple[EligibilityStatusCount, ...]
    eligibility_reason_counts: tuple[EligibilityReasonCount, ...]
    write: CatalogFeatureSnapshotV2WriteResult


def _count_unique_rows(frame: DataFrame, *, key: str) -> tuple[int, int]:
    summary = frame.agg(
        F.count(F.lit(1)).cast(LongType()).alias("row_count"),
        F.countDistinct(key).cast(LongType()).alias("distinct_key_count"),
    ).first()
    return int(summary.row_count), int(summary.distinct_key_count)


def _build_managed_feature_snapshots_v2(
    cycles: DataFrame,
    telemetry: DataFrame,
) -> _ManagedFeatureSnapshotV2Build:
    try:
        telemetry_count, dataset_version, source_sha256, ingestion_batch_id = (
            _collect_telemetry_lineage(telemetry)
        )
        cycle_count = _collect_cycle_count(cycles)

        boundaries = cycles.select(
            "loaded_cycle_id",
            F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
        )
        features = add_motor_current_15m_features(boundaries, telemetry)
        feature_count, distinct_feature_count = _count_unique_rows(
            features,
            key="loaded_cycle_id",
        )
        if feature_count != cycle_count or distinct_feature_count != cycle_count:
            raise ManagedGoldFeatureSnapshotV2ProcessingError(
                "Managed motor-current features require one row per Gold cycle"
            )
        _assert_feature_semantics(features)

        tail_context = _available_with_tail_context(features, telemetry)
        _assert_tail_context_is_complete(tail_context)
        gap_context = _with_internal_gap_context(tail_context, telemetry)
        available_count = features.where(
            F.col("motor_current_15m_status") == STATUS_AVAILABLE
        ).count()
        gap_count, distinct_gap_count = _count_unique_rows(
            gap_context,
            key="loaded_cycle_id",
        )
        if gap_count != available_count or distinct_gap_count != available_count:
            raise ManagedGoldFeatureSnapshotV2ProcessingError(
                "Managed coverage context does not reconcile with available features"
            )

        eligibility = add_motor_current_eligibility(features, gap_context)
        eligibility_count, distinct_eligibility_count = _count_unique_rows(
            eligibility,
            key="loaded_cycle_id",
        )
        if eligibility_count != cycle_count or distinct_eligibility_count != cycle_count:
            raise ManagedGoldFeatureSnapshotV2ProcessingError(
                "Managed eligibility requires one row per Gold cycle"
            )
        status_counts, reason_counts = _collect_eligibility_counts(
            eligibility,
            cycle_count=cycle_count,
        )

        cycle_context = add_cycle_operating_context(cycles)
        snapshots = build_feature_snapshot_v2(
            eligibility,
            cycle_context,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
        )
    except (
        CycleOperatingContextError,
        EligibilityProfileError,
        FeatureEligibilityError,
        FeatureSnapshotV2Error,
        MotorCurrentFeatureProfileError,
        TemporalFeatureError,
    ) as exc:
        raise ManagedGoldFeatureSnapshotV2ProcessingError(str(exc)) from exc

    return _ManagedFeatureSnapshotV2Build(
        telemetry_lineage=ManagedFeatureSnapshotSourceLineage(
            dataset_version=dataset_version,
            source_sha256=source_sha256,
            ingestion_batch_id=ingestion_batch_id,
        ),
        accepted_telemetry_record_count=telemetry_count,
        cycle_count=cycle_count,
        eligibility_status_counts=status_counts,
        eligibility_reason_counts=reason_counts,
        snapshots=snapshots,
    )


def run_managed_gold_feature_snapshot_v2_processing(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldFeatureSnapshotV2ProcessingResult:
    """Build label-free point-in-time features and reconcile managed Gold."""

    cycle_source_table = catalog_gold_table(namespace, LOADED_CYCLES_TABLE)
    telemetry_source_table = catalog_silver_table(namespace, TELEMETRY_ACCEPTED_TABLE)
    cycles = spark.table(cycle_source_table)
    telemetry = spark.table(telemetry_source_table)
    build = _build_managed_feature_snapshots_v2(cycles, telemetry)
    write = persist_catalog_feature_snapshots_v2(build.snapshots, namespace)
    if write.source_snapshot_count != build.cycle_count:
        raise ManagedGoldFeatureSnapshotV2ProcessingError(
            "Managed feature snapshots and Gold cycles do not reconcile: "
            f"snapshots={write.source_snapshot_count}, cycles={build.cycle_count}"
        )

    return ManagedGoldFeatureSnapshotV2ProcessingResult(
        contract_version=GOLD_FEATURE_SNAPSHOT_V2_PROCESSING_JOB_CONTRACT_VERSION,
        snapshot_version=FEATURE_SNAPSHOT_V2_VERSION,
        feature_version=MOTOR_CURRENT_FEATURE_VERSION,
        feature_window_seconds=MOTOR_CURRENT_WINDOW_SECONDS,
        eligibility_profile_version=MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
        eligibility_version=MOTOR_CURRENT_ELIGIBILITY_VERSION,
        minimum_excluded_gap_seconds=MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
        cycle_context_version=CYCLE_OPERATING_CONTEXT_VERSION,
        telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
        cycle_source_table=cycle_source_table,
        telemetry_source_table=telemetry_source_table,
        telemetry_lineage=build.telemetry_lineage,
        accepted_telemetry_record_count=build.accepted_telemetry_record_count,
        cycle_count=build.cycle_count,
        eligibility_status_counts=build.eligibility_status_counts,
        eligibility_reason_counts=build.eligibility_reason_counts,
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
    """Run managed Silver/Gold-to-Gold feature-snapshot v2 processing."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_feature_snapshot_v2_processing(spark, namespace)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
