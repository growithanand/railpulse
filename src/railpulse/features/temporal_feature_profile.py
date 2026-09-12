"""Profile full-source motor-current feature coverage without writing data."""

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
from railpulse.features.temporal_features import (
    MOTOR_CURRENT_FEATURE_VERSION,
    MOTOR_CURRENT_WINDOW_SECONDS,
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
    add_motor_current_15m_features,
)
from railpulse.ingestion.bronze import TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_storage import (
    TELEMETRY_LINEAGE_COLUMNS,
    TELEMETRY_VALIDATION_VERSION,
)
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

MOTOR_CURRENT_FEATURE_PROFILE_VERSION = "motor-current-15m-profile-v1"
MOTOR_CURRENT_FEATURE_STATUS_ORDER = (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)


class MotorCurrentFeatureProfileError(RuntimeError):
    """Raised when motor-current feature coverage cannot be reconciled."""


@dataclass(frozen=True)
class MotorCurrentFeatureStatusCount:
    """Cycle count for one explicit motor-current feature status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class MotorCurrentWindowSupport:
    """Descriptive support statistics for available motor-current windows."""

    available_cycle_count: int
    minimum_observation_count: int
    p05_observation_count: int
    median_observation_count: int
    p95_observation_count: int
    maximum_observation_count: int
    minimum_observation_span_seconds: int
    p05_observation_span_seconds: int
    median_observation_span_seconds: int
    p95_observation_span_seconds: int
    maximum_observation_span_seconds: int


@dataclass(frozen=True)
class FullSourceMotorCurrentFeatureProfile:
    """Reconciled read-only profile of motor-current feature coverage."""

    profile_version: str
    feature_version: str
    window_seconds: int
    telemetry_validation_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    cycle_count: int
    status_counts: tuple[MotorCurrentFeatureStatusCount, ...]
    available_window_support: MotorCurrentWindowSupport


def _collect_telemetry_lineage(telemetry: DataFrame) -> tuple[int, str, str, str]:
    required_columns = {
        "event_timestamp",
        "motor_current",
        *TELEMETRY_LINEAGE_COLUMNS,
    }
    missing_columns = sorted(required_columns - set(telemetry.columns))
    if missing_columns:
        raise MotorCurrentFeatureProfileError(
            "Accepted telemetry is missing feature-profile columns: " + ", ".join(missing_columns)
        )

    lineage_rows = telemetry.select(*TELEMETRY_LINEAGE_COLUMNS).distinct().limit(2).collect()
    if len(lineage_rows) != 1:
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires exactly one telemetry lineage"
        )
    lineage = tuple(lineage_rows[0][column] for column in TELEMETRY_LINEAGE_COLUMNS)
    if any(not isinstance(value, str) or not value.strip() for value in lineage):
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires complete telemetry lineage"
        )

    record_count = telemetry.count()
    if record_count <= 0:
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires accepted telemetry"
        )
    dataset_version, source_sha256, ingestion_batch_id = lineage
    return record_count, dataset_version, source_sha256, ingestion_batch_id


def _collect_cycle_count(cycles: DataFrame) -> int:
    required_columns = {"loaded_cycle_id", "loaded_cycle_stop_timestamp"}
    missing_columns = sorted(required_columns - set(cycles.columns))
    if missing_columns:
        raise MotorCurrentFeatureProfileError(
            "Gold cycles are missing feature-profile columns: " + ", ".join(missing_columns)
        )

    summary = cycles.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
    ).first()
    cycle_count = int(summary.cycle_count)
    if cycle_count <= 0:
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires Gold cycles"
        )
    if int(summary.distinct_cycle_count) != cycle_count:
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires unique non-null cycle IDs"
        )
    return cycle_count


def _assert_feature_semantics(features: DataFrame) -> None:
    available = F.col("motor_current_15m_status") == STATUS_AVAILABLE
    missing_prediction = F.col("motor_current_15m_status") == STATUS_MISSING_PREDICTION
    missing_observation = F.col("motor_current_15m_status") == STATUS_MISSING_PREDICTION_OBSERVATION
    has_support = (
        (F.col("motor_current_15m_observation_count") > 0)
        & F.col("motor_current_15m_first_observation_timestamp").isNotNull()
        & F.col("motor_current_15m_last_observation_timestamp").isNotNull()
        & F.col("motor_current_15m_minimum_amperes").isNotNull()
        & F.col("motor_current_15m_mean_amperes").isNotNull()
        & F.col("motor_current_15m_maximum_amperes").isNotNull()
    )
    has_any_support = (
        (F.col("motor_current_15m_observation_count") != 0)
        | F.col("motor_current_15m_first_observation_timestamp").isNotNull()
        | F.col("motor_current_15m_last_observation_timestamp").isNotNull()
        | F.col("motor_current_15m_minimum_amperes").isNotNull()
        | F.col("motor_current_15m_mean_amperes").isNotNull()
        | F.col("motor_current_15m_maximum_amperes").isNotNull()
    )
    invalid = features.where(
        ~F.col("motor_current_15m_feature_version").eqNullSafe(F.lit(MOTOR_CURRENT_FEATURE_VERSION))
        | ~F.col("motor_current_15m_window_seconds").eqNullSafe(F.lit(MOTOR_CURRENT_WINDOW_SECONDS))
        | (
            available
            & (
                F.col("prediction_timestamp").isNull()
                | F.col("motor_current_15m_window_start").isNull()
                | ~has_support
                | (
                    F.col("motor_current_15m_first_observation_timestamp")
                    <= F.col("motor_current_15m_window_start")
                )
                | (
                    F.col("motor_current_15m_first_observation_timestamp")
                    > F.col("motor_current_15m_last_observation_timestamp")
                )
                | ~F.col("motor_current_15m_last_observation_timestamp").eqNullSafe(
                    F.col("prediction_timestamp")
                )
            )
        )
        | (
            missing_prediction
            & (
                F.col("prediction_timestamp").isNotNull()
                | F.col("motor_current_15m_window_start").isNotNull()
                | has_any_support
            )
        )
        | (
            missing_observation
            & (
                F.col("prediction_timestamp").isNull()
                | F.col("motor_current_15m_window_start").isNull()
                | has_any_support
            )
        )
    ).limit(1)
    if invalid.count():
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature availability semantics do not reconcile"
        )


def _collect_available_support(features: DataFrame) -> MotorCurrentWindowSupport:
    available = features.where(F.col("motor_current_15m_status") == STATUS_AVAILABLE).withColumn(
        "observation_span_seconds",
        F.timestamp_diff(
            "SECOND",
            F.col("motor_current_15m_first_observation_timestamp"),
            F.col("motor_current_15m_last_observation_timestamp"),
        ),
    )
    summary = available.agg(
        F.count(F.lit(1)).cast(LongType()).alias("available_cycle_count"),
        F.min("motor_current_15m_observation_count").alias("minimum_observation_count"),
        F.percentile_approx(
            "motor_current_15m_observation_count",
            [0.05, 0.5, 0.95],
            10_000,
        ).alias("observation_count_percentiles"),
        F.max("motor_current_15m_observation_count").alias("maximum_observation_count"),
        F.min("observation_span_seconds").alias("minimum_observation_span_seconds"),
        F.percentile_approx(
            "observation_span_seconds",
            [0.05, 0.5, 0.95],
            10_000,
        ).alias("observation_span_percentiles"),
        F.max("observation_span_seconds").alias("maximum_observation_span_seconds"),
    ).first()
    available_count = int(summary.available_cycle_count)
    if available_count <= 0:
        raise MotorCurrentFeatureProfileError(
            "Motor-current feature profiling requires at least one available feature"
        )
    count_percentiles = summary.observation_count_percentiles
    span_percentiles = summary.observation_span_percentiles
    return MotorCurrentWindowSupport(
        available_cycle_count=available_count,
        minimum_observation_count=int(summary.minimum_observation_count),
        p05_observation_count=int(count_percentiles[0]),
        median_observation_count=int(count_percentiles[1]),
        p95_observation_count=int(count_percentiles[2]),
        maximum_observation_count=int(summary.maximum_observation_count),
        minimum_observation_span_seconds=int(summary.minimum_observation_span_seconds),
        p05_observation_span_seconds=int(span_percentiles[0]),
        median_observation_span_seconds=int(span_percentiles[1]),
        p95_observation_span_seconds=int(span_percentiles[2]),
        maximum_observation_span_seconds=int(summary.maximum_observation_span_seconds),
    )


def collect_motor_current_feature_profile(
    cycles: DataFrame,
    telemetry: DataFrame,
) -> FullSourceMotorCurrentFeatureProfile:
    """Apply and reconcile motor-current feature coverage against source snapshots."""

    telemetry_count, dataset_version, source_sha256, ingestion_batch_id = (
        _collect_telemetry_lineage(telemetry)
    )
    cycle_count = _collect_cycle_count(cycles)
    cycle_boundaries = cycles.select(
        "loaded_cycle_id",
        F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
    )
    features = add_motor_current_15m_features(cycle_boundaries, telemetry).persist(
        StorageLevel.DISK_ONLY
    )
    try:
        status_rows = features.groupBy("motor_current_15m_status").count().collect()
        status_counts_by_name = {
            row.motor_current_15m_status: int(row["count"]) for row in status_rows
        }
        unexpected_statuses = set(status_counts_by_name) - set(MOTOR_CURRENT_FEATURE_STATUS_ORDER)
        if unexpected_statuses:
            raise MotorCurrentFeatureProfileError(
                "Motor-current feature output contains unexpected statuses: "
                + ", ".join(sorted(str(status) for status in unexpected_statuses))
            )
        if sum(status_counts_by_name.values()) != cycle_count:
            raise MotorCurrentFeatureProfileError(
                "Motor-current feature rows do not reconcile with Gold cycles"
            )
        _assert_feature_semantics(features)
        support = _collect_available_support(features)
        if support.available_cycle_count != status_counts_by_name.get(STATUS_AVAILABLE, 0):
            raise MotorCurrentFeatureProfileError(
                "Available motor-current feature counts do not reconcile"
            )

        status_counts = tuple(
            MotorCurrentFeatureStatusCount(
                status=status,
                cycle_count=status_counts_by_name.get(status, 0),
            )
            for status in MOTOR_CURRENT_FEATURE_STATUS_ORDER
        )
        return FullSourceMotorCurrentFeatureProfile(
            profile_version=MOTOR_CURRENT_FEATURE_PROFILE_VERSION,
            feature_version=MOTOR_CURRENT_FEATURE_VERSION,
            window_seconds=MOTOR_CURRENT_WINDOW_SECONDS,
            telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
            accepted_telemetry_record_count=telemetry_count,
            cycle_count=cycle_count,
            status_counts=status_counts,
            available_window_support=support,
        )
    finally:
        features.unpersist()


def profile_full_source_motor_current_features(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceMotorCurrentFeatureProfile:
    """Rebuild accepted Silver telemetry and profile Gold cycles without writing."""

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
        profile = collect_motor_current_feature_profile(cycles, cached_split.accepted)
    finally:
        validated.unpersist()
    if profile.dataset_version != config.dataset_version:
        raise MotorCurrentFeatureProfileError(
            "Profile dataset version does not match the configured dataset version"
        )
    return profile


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source motor-current profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session(
        "railpulse-motor-current-feature-profile", master=args.master
    )
    try:
        profile = profile_full_source_motor_current_features(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
