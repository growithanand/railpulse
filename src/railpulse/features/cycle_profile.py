"""Profile loaded-cycle outputs against a validated telemetry source."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycles import (
    LOADED_CYCLE_ID_VERSION,
    aggregate_loaded_cycles,
    annotate_loaded_cycle_boundaries,
    assign_loaded_cycle_segments,
)
from railpulse.ingestion.bronze import TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_storage import (
    TELEMETRY_LINEAGE_COLUMNS,
    TELEMETRY_VALIDATION_VERSION,
)
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualityMetrics,
    TelemetryQualitySplit,
    collect_telemetry_quality_metrics,
    split_telemetry_by_quality,
)

CYCLE_PROFILE_VERSION = "loaded-cycle-profile-v1"


class CycleProfileError(RuntimeError):
    """Raised when a loaded-cycle profile cannot be reconciled."""


@dataclass(frozen=True)
class LoadedCycleStatistics:
    """Reconciled descriptive counts for one cycle aggregate snapshot."""

    cycle_count: int
    observed_start_cycle_count: int
    left_censored_cycle_count: int
    observed_stop_cycle_count: int
    right_censored_cycle_count: int
    complete_cycle_count: int
    loaded_observation_count: int
    duration_cycle_count: int
    minimum_observed_duration_seconds: int | None
    median_observed_duration_seconds: int | None
    p95_observed_duration_seconds: int | None
    maximum_observed_duration_seconds: int | None


@dataclass(frozen=True)
class FullSourceCycleProfile:
    """Source lineage, telemetry quality, and loaded-cycle statistics."""

    profile_version: str
    cycle_id_version: str
    telemetry_validation_version: str
    dataset_version: str
    source_sha256: str
    ingestion_batch_id: str
    telemetry_quality: TelemetryQualityMetrics
    loaded_cycles: LoadedCycleStatistics


def _count_when(condition: Column) -> Column:
    return F.coalesce(
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))),
        F.lit(0),
    ).cast(LongType())


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def collect_loaded_cycle_statistics(cycles: DataFrame) -> LoadedCycleStatistics:
    """Collect one internally reconciled summary from aggregated loaded cycles."""

    required_columns = {
        "loaded_cycle_id",
        "loaded_cycle_start_type",
        "loaded_observation_count",
        "is_right_censored",
        "observed_duration_seconds",
    }
    missing_columns = sorted(required_columns - set(cycles.columns))
    if missing_columns:
        raise CycleProfileError(
            "Aggregated cycles are missing profile columns: " + ", ".join(missing_columns)
        )

    summary = cycles.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        _count_when(F.col("loaded_cycle_start_type") == F.lit("observed")).alias(
            "observed_start_cycle_count"
        ),
        _count_when(F.col("loaded_cycle_start_type") == F.lit("left_censored")).alias(
            "left_censored_cycle_count"
        ),
        _count_when(~F.col("is_right_censored")).alias("observed_stop_cycle_count"),
        _count_when(F.col("is_right_censored")).alias("right_censored_cycle_count"),
        _count_when(
            (F.col("loaded_cycle_start_type") == F.lit("observed")) & ~F.col("is_right_censored")
        ).alias("complete_cycle_count"),
        F.coalesce(F.sum("loaded_observation_count"), F.lit(0))
        .cast(LongType())
        .alias("loaded_observation_count"),
        _count_when(F.col("observed_duration_seconds").isNotNull()).alias("duration_cycle_count"),
        F.min("observed_duration_seconds").alias("minimum_observed_duration_seconds"),
        F.percentile_approx("observed_duration_seconds", [0.5, 0.95], 10_000).alias(
            "duration_percentiles"
        ),
        F.max("observed_duration_seconds").alias("maximum_observed_duration_seconds"),
        _count_when(F.col("observed_duration_seconds") <= F.lit(0)).alias(
            "non_positive_duration_count"
        ),
    ).first()

    cycle_count = int(summary.cycle_count)
    observed_start_count = int(summary.observed_start_cycle_count)
    left_censored_count = int(summary.left_censored_cycle_count)
    observed_stop_count = int(summary.observed_stop_cycle_count)
    right_censored_count = int(summary.right_censored_cycle_count)
    duration_count = int(summary.duration_cycle_count)
    loaded_observation_count = int(summary.loaded_observation_count)
    if observed_start_count + left_censored_count != cycle_count:
        raise CycleProfileError("Cycle start classifications do not reconcile")
    if observed_stop_count + right_censored_count != cycle_count:
        raise CycleProfileError("Cycle stop classifications do not reconcile")
    if duration_count != observed_stop_count:
        raise CycleProfileError("Observed durations do not reconcile with observed stops")
    if cycle_count and loaded_observation_count < cycle_count:
        raise CycleProfileError("A loaded cycle has no loaded observation")
    if int(summary.non_positive_duration_count):
        raise CycleProfileError("Observed cycle durations must be positive")

    percentiles = summary.duration_percentiles or (None, None)
    return LoadedCycleStatistics(
        cycle_count=cycle_count,
        observed_start_cycle_count=observed_start_count,
        left_censored_cycle_count=left_censored_count,
        observed_stop_cycle_count=observed_stop_count,
        right_censored_cycle_count=right_censored_count,
        complete_cycle_count=int(summary.complete_cycle_count),
        loaded_observation_count=loaded_observation_count,
        duration_cycle_count=duration_count,
        minimum_observed_duration_seconds=_optional_int(summary.minimum_observed_duration_seconds),
        median_observed_duration_seconds=_optional_int(percentiles[0]),
        p95_observed_duration_seconds=_optional_int(percentiles[1]),
        maximum_observed_duration_seconds=_optional_int(summary.maximum_observed_duration_seconds),
    )


def _single_source_lineage(frame: DataFrame) -> tuple[str, str, str]:
    missing_columns = sorted(set(TELEMETRY_LINEAGE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise CycleProfileError(
            "Validated telemetry is missing profile-lineage columns: " + ", ".join(missing_columns)
        )
    rows = frame.select(*TELEMETRY_LINEAGE_COLUMNS).distinct().limit(2).collect()
    if len(rows) != 1:
        raise CycleProfileError("Cycle profiling requires exactly one telemetry source lineage")
    lineage = tuple(rows[0][column] for column in TELEMETRY_LINEAGE_COLUMNS)
    if any(not isinstance(value, str) or not value.strip() for value in lineage):
        raise CycleProfileError("Cycle profiling requires complete telemetry source lineage")
    return lineage


def collect_full_source_cycle_profile(
    split: TelemetryQualitySplit,
    cycles: DataFrame,
    config: RailPulseConfig,
) -> FullSourceCycleProfile:
    """Collect one source-bound profile from validated telemetry and derived cycles."""

    telemetry_quality = collect_telemetry_quality_metrics(split)
    dataset_version, source_sha256, ingestion_batch_id = _single_source_lineage(split.all_records)
    if dataset_version != config.dataset_version:
        raise CycleProfileError(
            "Telemetry dataset version does not match the configured dataset version"
        )

    return FullSourceCycleProfile(
        profile_version=CYCLE_PROFILE_VERSION,
        cycle_id_version=LOADED_CYCLE_ID_VERSION,
        telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
        dataset_version=dataset_version,
        source_sha256=source_sha256,
        ingestion_batch_id=ingestion_batch_id,
        telemetry_quality=telemetry_quality,
        loaded_cycles=collect_loaded_cycle_statistics(cycles),
    )


def profile_full_source_cycles(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceCycleProfile:
    """Rebuild accepted Silver telemetry from Bronze and profile loaded cycles without writing."""

    bronze = spark.read.format("delta").load(str(bronze_table_path(config, TELEMETRY_TABLE)))
    split = split_telemetry_by_quality(bronze)
    validated = split.all_records.persist(StorageLevel.DISK_ONLY)
    try:
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        cached_split = TelemetryQualitySplit(
            all_records=validated,
            accepted=validated.where(reason_count == 0),
            quarantined=validated.where(reason_count > 0),
        )
        boundaries = annotate_loaded_cycle_boundaries(cached_split.accepted)
        segments = assign_loaded_cycle_segments(boundaries)
        cycles = aggregate_loaded_cycles(segments)
        return collect_full_source_cycle_profile(cached_split, cycles, config)
    finally:
        validated.unpersist()


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the full-source, read-only cycle profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-cycle-profile", master=args.master)
    try:
        profile = profile_full_source_cycles(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
