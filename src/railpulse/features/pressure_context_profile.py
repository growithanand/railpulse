"""Profile full-source pressure-context coverage without writing data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.failure_horizon_profile import (
    FailureHorizonProfileError,
    _collect_observation_boundary,
)
from railpulse.features.pressure_context import (
    PRESSURE_CONTEXT_FEATURE_VERSION,
    PRESSURE_CONTEXT_WINDOW_SECONDS,
    add_pressure_context_15m_features,
)
from railpulse.features.temporal_features import (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)
from railpulse.ingestion.bronze import TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

PRESSURE_CONTEXT_PROFILE_VERSION = "panel-reservoir-pressure-difference-15m-profile-v1"
PRESSURE_CONTEXT_STATUS_ORDER = (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)


class PressureContextProfileError(RuntimeError):
    """Raised when pressure-context coverage cannot be reconciled."""


@dataclass(frozen=True)
class PressureContextStatusCount:
    """Cycle count for one explicit pressure-context status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class PressureContextAvailableDistribution:
    """Support and pressure-difference statistics for available windows."""

    available_cycle_count: int
    minimum_observation_count: int
    p05_observation_count: int
    median_observation_count: int
    p95_observation_count: int
    maximum_observation_count: int
    minimum_mean_difference_bar: float
    p10_mean_difference_bar: float
    median_mean_difference_bar: float
    p90_mean_difference_bar: float
    maximum_mean_difference_bar: float
    minimum_mean_absolute_difference_bar: float
    median_mean_absolute_difference_bar: float
    p90_mean_absolute_difference_bar: float
    p95_mean_absolute_difference_bar: float
    maximum_mean_absolute_difference_bar: float
    minimum_maximum_absolute_difference_bar: float
    median_maximum_absolute_difference_bar: float
    p90_maximum_absolute_difference_bar: float
    p95_maximum_absolute_difference_bar: float
    maximum_maximum_absolute_difference_bar: float


@dataclass(frozen=True)
class FullSourcePressureContextProfile:
    """Source lineage and reconciled pressure-context profile."""

    profile_version: str
    feature_version: str
    window_seconds: int
    telemetry_validation_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    cycle_count: int
    status_counts: tuple[PressureContextStatusCount, ...]
    available_distribution: PressureContextAvailableDistribution


def _percentile(column: str, probability: float, alias: str):
    return F.percentile(F.col(column), F.lit(probability)).alias(alias)


def collect_pressure_context_profile(
    cycles: DataFrame,
    telemetry: DataFrame,
) -> FullSourcePressureContextProfile:
    """Build and reconcile a read-only pressure-context profile."""

    try:
        (
            telemetry_count,
            _observation_end,
            dataset_version,
            source_sha256,
            ingestion_batch_id,
        ) = _collect_observation_boundary(telemetry)
    except FailureHorizonProfileError as exc:
        raise PressureContextProfileError(str(exc)) from exc

    cycle_summary = cycles.agg(
        F.count(F.lit(1)).alias("cycle_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
    ).first()
    cycle_count = int(cycle_summary.cycle_count)
    if cycle_count <= 0:
        raise PressureContextProfileError("Pressure-context profiling requires Gold cycles")
    if int(cycle_summary.distinct_cycle_count) != cycle_count:
        raise PressureContextProfileError("Gold cycles require unique cycle IDs")

    boundaries = cycles.select(
        "loaded_cycle_id",
        F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
    )
    features = add_pressure_context_15m_features(boundaries, telemetry).persist(
        StorageLevel.DISK_ONLY
    )
    try:
        status_rows = features.groupBy("pressure_context_15m_status").count().collect()
        status_by_name = {row.pressure_context_15m_status: int(row["count"]) for row in status_rows}
        unexpected_statuses = set(status_by_name) - set(PRESSURE_CONTEXT_STATUS_ORDER)
        if unexpected_statuses:
            raise PressureContextProfileError(
                "Pressure context contains unexpected statuses: "
                + ", ".join(sorted(str(status) for status in unexpected_statuses))
            )
        if sum(status_by_name.values()) != cycle_count:
            raise PressureContextProfileError("Pressure-context statuses do not reconcile")

        available = F.col("pressure_context_15m_status") == STATUS_AVAILABLE
        value_columns = (
            "pressure_context_15m_mean_difference_bar",
            "pressure_context_15m_mean_absolute_difference_bar",
            "pressure_context_15m_maximum_absolute_difference_bar",
        )
        invalid = features.where(
            ~F.col("pressure_context_15m_feature_version").eqNullSafe(
                F.lit(PRESSURE_CONTEXT_FEATURE_VERSION)
            )
            | ~F.col("pressure_context_15m_window_seconds").eqNullSafe(
                F.lit(PRESSURE_CONTEXT_WINDOW_SECONDS)
            )
            | (
                available
                & (
                    (F.col("pressure_context_15m_observation_count") <= 0)
                    | F.col("pressure_context_15m_first_observation_timestamp").isNull()
                    | F.col("pressure_context_15m_last_observation_timestamp").isNull()
                    | F.col(value_columns[0]).isNull()
                    | F.col(value_columns[1]).isNull()
                    | F.col(value_columns[2]).isNull()
                )
            )
            | (
                ~available
                & (
                    (F.col("pressure_context_15m_observation_count") != 0)
                    | F.col(value_columns[0]).isNotNull()
                    | F.col(value_columns[1]).isNotNull()
                    | F.col(value_columns[2]).isNotNull()
                )
            )
        ).limit(1)
        if invalid.count():
            raise PressureContextProfileError("Pressure-context feature semantics do not reconcile")

        available_summary = (
            features.where(available)
            .agg(
                F.count(F.lit(1)).alias("available_cycle_count"),
                F.min("pressure_context_15m_observation_count").alias("minimum_observation_count"),
                _percentile(
                    "pressure_context_15m_observation_count", 0.05, "p05_observation_count"
                ),
                _percentile(
                    "pressure_context_15m_observation_count", 0.5, "median_observation_count"
                ),
                _percentile(
                    "pressure_context_15m_observation_count", 0.95, "p95_observation_count"
                ),
                F.max("pressure_context_15m_observation_count").alias("maximum_observation_count"),
                F.min(value_columns[0]).alias("minimum_mean_difference_bar"),
                _percentile(value_columns[0], 0.1, "p10_mean_difference_bar"),
                _percentile(value_columns[0], 0.5, "median_mean_difference_bar"),
                _percentile(value_columns[0], 0.9, "p90_mean_difference_bar"),
                F.max(value_columns[0]).alias("maximum_mean_difference_bar"),
                F.min(value_columns[1]).alias("minimum_mean_absolute_difference_bar"),
                _percentile(value_columns[1], 0.5, "median_mean_absolute_difference_bar"),
                _percentile(value_columns[1], 0.9, "p90_mean_absolute_difference_bar"),
                _percentile(value_columns[1], 0.95, "p95_mean_absolute_difference_bar"),
                F.max(value_columns[1]).alias("maximum_mean_absolute_difference_bar"),
                F.min(value_columns[2]).alias("minimum_maximum_absolute_difference_bar"),
                _percentile(value_columns[2], 0.5, "median_maximum_absolute_difference_bar"),
                _percentile(value_columns[2], 0.9, "p90_maximum_absolute_difference_bar"),
                _percentile(value_columns[2], 0.95, "p95_maximum_absolute_difference_bar"),
                F.max(value_columns[2]).alias("maximum_maximum_absolute_difference_bar"),
            )
            .first()
        )
        available_count = int(available_summary.available_cycle_count)
        if available_count <= 0:
            raise PressureContextProfileError("Pressure-context profiling requires available rows")

        status_counts = tuple(
            PressureContextStatusCount(
                status=status,
                cycle_count=status_by_name.get(status, 0),
            )
            for status in PRESSURE_CONTEXT_STATUS_ORDER
        )
        distribution = PressureContextAvailableDistribution(
            **{
                field: (
                    int(available_summary[field])
                    if field.endswith("count")
                    else float(available_summary[field])
                )
                for field in PressureContextAvailableDistribution.__dataclass_fields__
            }
        )
        return FullSourcePressureContextProfile(
            profile_version=PRESSURE_CONTEXT_PROFILE_VERSION,
            feature_version=PRESSURE_CONTEXT_FEATURE_VERSION,
            window_seconds=PRESSURE_CONTEXT_WINDOW_SECONDS,
            telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
            dataset_version=dataset_version,
            telemetry_source_sha256=source_sha256,
            telemetry_ingestion_batch_id=ingestion_batch_id,
            accepted_telemetry_record_count=telemetry_count,
            cycle_count=cycle_count,
            status_counts=status_counts,
            available_distribution=distribution,
        )
    finally:
        features.unpersist()


def profile_full_source_pressure_context(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourcePressureContextProfile:
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
        profile = collect_pressure_context_profile(cycles, cached_split.accepted)
    finally:
        validated.unpersist()
    if profile.dataset_version != config.dataset_version:
        raise PressureContextProfileError(
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
    """Run the read-only full-source pressure-context profile."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-pressure-context-profile", master=args.master)
    try:
        profile = profile_full_source_pressure_context(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
