"""Build and persist full-source Gold motor-current feature snapshots."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.eligibility_profile import (
    MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
    EligibilityProfileError,
    FullSourceEligibilityProfile,
    _collect_eligibility_counts,
)
from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
    MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
    add_motor_current_eligibility,
)
from railpulse.features.feature_snapshot import build_feature_snapshot
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOT_VERSION
from railpulse.features.feature_snapshot_storage import (
    FeatureSnapshotWriteResult,
    persist_feature_snapshots,
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
    add_motor_current_15m_features,
)
from railpulse.ingestion.bronze import TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

FEATURE_SNAPSHOT_BUILD_VERSION = "motor-current-15m-snapshot-build-v1"


class FeatureSnapshotBuildError(RuntimeError):
    """Raised when a full-source feature-snapshot build cannot be reconciled."""


@dataclass(frozen=True)
class FullSourceFeatureSnapshotBuildResult:
    """Reconciled eligibility profile and Delta write evidence."""

    build_version: str
    snapshot_version: str
    profile: FullSourceEligibilityProfile
    write: FeatureSnapshotWriteResult


def _build_and_persist_snapshot(
    cycles: DataFrame,
    telemetry: DataFrame,
    config: RailPulseConfig,
) -> FullSourceFeatureSnapshotBuildResult:
    try:
        telemetry_count, dataset_version, source_sha256, ingestion_batch_id = (
            _collect_telemetry_lineage(telemetry)
        )
        cycle_count = _collect_cycle_count(cycles)
    except MotorCurrentFeatureProfileError as exc:
        raise FeatureSnapshotBuildError(str(exc)) from exc
    if dataset_version != config.dataset_version:
        raise FeatureSnapshotBuildError(
            "Snapshot dataset version does not match the configured dataset version"
        )

    boundaries = cycles.select(
        "loaded_cycle_id",
        F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
    )
    features = add_motor_current_15m_features(boundaries, telemetry).persist(StorageLevel.DISK_ONLY)
    tail_context = None
    gap_context = None
    eligibility = None
    try:
        summary = features.agg(
            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
            F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
        ).first()
        if (
            int(summary.cycle_count) != cycle_count
            or int(summary.distinct_cycle_count) != cycle_count
        ):
            raise FeatureSnapshotBuildError("Motor-current features require one row per Gold cycle")
        _assert_feature_semantics(features)

        tail_context = _available_with_tail_context(features, telemetry).persist(
            StorageLevel.DISK_ONLY
        )
        _assert_tail_context_is_complete(tail_context)
        gap_context = _with_internal_gap_context(tail_context, telemetry).persist(
            StorageLevel.DISK_ONLY
        )
        available_count = features.where(
            F.col("motor_current_15m_status") == STATUS_AVAILABLE
        ).count()
        gap_summary = gap_context.agg(
            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
            F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
        ).first()
        if (
            int(gap_summary.cycle_count) != available_count
            or int(gap_summary.distinct_cycle_count) != available_count
        ):
            raise FeatureSnapshotBuildError(
                "Coverage context does not reconcile with available features"
            )

        eligibility = add_motor_current_eligibility(features, gap_context).persist(
            StorageLevel.DISK_ONLY
        )
        eligibility_summary = eligibility.agg(
            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
            F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
        ).first()
        if (
            int(eligibility_summary.cycle_count) != cycle_count
            or int(eligibility_summary.distinct_cycle_count) != cycle_count
        ):
            raise FeatureSnapshotBuildError("Eligibility requires one row per Gold cycle")
        try:
            status_counts, reason_counts = _collect_eligibility_counts(
                eligibility,
                cycle_count=cycle_count,
            )
        except EligibilityProfileError as exc:
            raise FeatureSnapshotBuildError(str(exc)) from exc

        profile = FullSourceEligibilityProfile(
            profile_version=MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
            eligibility_version=MOTOR_CURRENT_ELIGIBILITY_VERSION,
            feature_version=MOTOR_CURRENT_FEATURE_VERSION,
            feature_window_seconds=MOTOR_CURRENT_WINDOW_SECONDS,
            minimum_excluded_gap_seconds=MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
            telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
            accepted_telemetry_record_count=telemetry_count,
            cycle_count=cycle_count,
            status_counts=status_counts,
            reason_counts=reason_counts,
        )
        snapshot = build_feature_snapshot(
            eligibility,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
        )
        write = persist_feature_snapshots(snapshot, config)
        if write.source_snapshot_count != cycle_count:
            raise FeatureSnapshotBuildError(
                "Eligibility profile and snapshot write counts do not reconcile: "
                f"profile={cycle_count}, write={write.source_snapshot_count}"
            )
        return FullSourceFeatureSnapshotBuildResult(
            build_version=FEATURE_SNAPSHOT_BUILD_VERSION,
            snapshot_version=FEATURE_SNAPSHOT_VERSION,
            profile=profile,
            write=write,
        )
    except MotorCurrentFeatureProfileError as exc:
        raise FeatureSnapshotBuildError(str(exc)) from exc
    finally:
        if eligibility is not None:
            eligibility.unpersist()
        if gap_context is not None:
            gap_context.unpersist()
        if tail_context is not None:
            tail_context.unpersist()
        features.unpersist()


def build_full_source_feature_snapshots(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceFeatureSnapshotBuildResult:
    """Rebuild accepted telemetry from Bronze and materialize Gold feature snapshots."""

    bronze = spark.read.format("delta").load(str(bronze_table_path(config, TELEMETRY_TABLE)))
    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    split = split_telemetry_by_quality(bronze)
    validated = split.all_records.persist(StorageLevel.DISK_ONLY)
    try:
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        cached_split = TelemetryQualitySplit(
            all_records=validated,
            accepted=validated.where(reason_count == 0),
            quarantined=validated.where(reason_count > 0),
        )
        return _build_and_persist_snapshot(cycles, cached_split.accepted, config)
    finally:
        validated.unpersist()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the full-source snapshot build and print reconciled JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-feature-snapshot-build", master=args.master)
    try:
        result = build_full_source_feature_snapshots(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
