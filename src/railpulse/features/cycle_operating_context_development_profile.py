"""Compare cycle operating context by development period and label."""

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
from railpulse.evaluation.chronological_splits import (
    CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_VERSION,
    STATUS_AVAILABLE,
    add_cycle_operating_context,
)
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.failure_horizon_profile import (
    FailureHorizonProfileError,
    _collect_failure_events,
    _collect_observation_boundary,
)
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
    assign_cycle_failure_horizons,
)
from railpulse.ingestion.bronze import FAILURE_TABLE, TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_failures import split_failure_events_by_quality
from railpulse.validation.silver_telemetry import split_telemetry_by_quality

CYCLE_OPERATING_CONTEXT_DEVELOPMENT_PROFILE_VERSION = (
    "loaded-cycle-operating-context-development-profile-v1"
)
_PARTITION_ORDER = (PARTITION_TRAIN, PARTITION_VALIDATION)
_LABEL_ORDER = (STATUS_POSITIVE, STATUS_NEGATIVE)
_VALUE_COLUMNS = (
    "cycle_context_current_duration_seconds",
    "cycle_context_previous_duration_seconds",
    "cycle_context_previous_idle_seconds",
)


class CycleOperatingContextDevelopmentProfileError(RuntimeError):
    """Raised when development cycle-context evidence cannot be reconciled."""


@dataclass(frozen=True)
class CycleOperatingContextLabelDistribution:
    """Cycle-context distribution for one development period and label."""

    partition: str
    label_status: str
    row_count: int
    p10_current_duration_seconds: float
    median_current_duration_seconds: float
    p90_current_duration_seconds: float
    p10_previous_duration_seconds: float
    median_previous_duration_seconds: float
    p90_previous_duration_seconds: float
    p10_previous_idle_seconds: float
    median_previous_idle_seconds: float
    p90_previous_idle_seconds: float


@dataclass(frozen=True)
class CycleOperatingContextDevelopmentSummary:
    """Reconciled train/validation cycle-context evidence."""

    development_row_count: int
    train_row_count: int
    validation_row_count: int
    distributions: tuple[CycleOperatingContextLabelDistribution, ...]


@dataclass(frozen=True)
class FullSourceCycleOperatingContextDevelopmentProfile:
    """Source-bound development-only cycle operating-context comparison."""

    profile_version: str
    feature_version: str
    split_selection_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    accepted_failure_event_count: int
    summary: CycleOperatingContextDevelopmentSummary


def _percentile(column: str, probability: float, alias: str):
    return F.percentile(F.col(column), F.lit(probability)).alias(alias)


def _validate_unique_keys(frame: DataFrame, *, label: str) -> int:
    summary = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
    ).first()
    row_count = int(summary.row_count)
    if row_count <= 0:
        raise CycleOperatingContextDevelopmentProfileError(f"{label} requires cycle rows")
    if int(summary.distinct_cycle_count) != row_count:
        raise CycleOperatingContextDevelopmentProfileError(f"{label} requires unique cycle IDs")
    return row_count


def collect_cycle_operating_context_development_summary(
    features: DataFrame,
    horizons: DataFrame,
) -> CycleOperatingContextDevelopmentSummary:
    """Compare complete cycle context by development label, excluding test rows."""

    feature_columns = (
        "loaded_cycle_id",
        "cycle_context_feature_version",
        "cycle_context_status",
        *_VALUE_COLUMNS,
    )
    horizon_columns = (
        "loaded_cycle_id",
        "prediction_timestamp",
        "failure_horizon_status",
    )
    missing_features = sorted(set(feature_columns) - set(features.columns))
    if missing_features:
        raise CycleOperatingContextDevelopmentProfileError(
            "Cycle context is missing development columns: " + ", ".join(missing_features)
        )
    missing_horizons = sorted(set(horizon_columns) - set(horizons.columns))
    if missing_horizons:
        raise CycleOperatingContextDevelopmentProfileError(
            "Failure horizons are missing development columns: " + ", ".join(missing_horizons)
        )
    feature_count = _validate_unique_keys(features, label="Cycle context")
    horizon_count = _validate_unique_keys(horizons, label="Failure horizons")
    if feature_count != horizon_count:
        raise CycleOperatingContextDevelopmentProfileError(
            "Cycle context and failure horizons must have equal populations"
        )

    joined = (
        features.select(*feature_columns)
        .join(
            horizons.select(*horizon_columns),
            on="loaded_cycle_id",
            how="inner",
        )
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        if joined.count() != feature_count:
            raise CycleOperatingContextDevelopmentProfileError(
                "Cycle context and failure horizons do not join one-to-one"
            )

        selected = get_selected_split_candidate()
        development = joined.where(
            (F.col("prediction_timestamp") < F.lit(selected.test_start))
            & (F.col("cycle_context_status") == STATUS_AVAILABLE)
            & F.col("failure_horizon_status").isin(*_LABEL_ORDER)
        ).withColumn(
            "_development_partition",
            F.when(
                F.col("prediction_timestamp") < F.lit(selected.validation_start),
                F.lit(PARTITION_TRAIN),
            ).otherwise(F.lit(PARTITION_VALIDATION)),
        )
        invalid = development.where(
            ~F.col("cycle_context_feature_version").eqNullSafe(
                F.lit(CYCLE_OPERATING_CONTEXT_VERSION)
            )
            | F.col("prediction_timestamp").isNull()
            | F.col(_VALUE_COLUMNS[0]).isNull()
            | F.col(_VALUE_COLUMNS[1]).isNull()
            | F.col(_VALUE_COLUMNS[2]).isNull()
        ).limit(1)
        if invalid.count():
            raise CycleOperatingContextDevelopmentProfileError(
                "Development rows contain invalid cycle operating-context values"
            )

        rows = (
            development.groupBy("_development_partition", "failure_horizon_status")
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                _percentile(_VALUE_COLUMNS[0], 0.1, "p10_current_duration_seconds"),
                _percentile(_VALUE_COLUMNS[0], 0.5, "median_current_duration_seconds"),
                _percentile(_VALUE_COLUMNS[0], 0.9, "p90_current_duration_seconds"),
                _percentile(_VALUE_COLUMNS[1], 0.1, "p10_previous_duration_seconds"),
                _percentile(_VALUE_COLUMNS[1], 0.5, "median_previous_duration_seconds"),
                _percentile(_VALUE_COLUMNS[1], 0.9, "p90_previous_duration_seconds"),
                _percentile(_VALUE_COLUMNS[2], 0.1, "p10_previous_idle_seconds"),
                _percentile(_VALUE_COLUMNS[2], 0.5, "median_previous_idle_seconds"),
                _percentile(_VALUE_COLUMNS[2], 0.9, "p90_previous_idle_seconds"),
            )
            .collect()
        )
        by_group = {(row._development_partition, row.failure_horizon_status): row for row in rows}
        distributions = []
        for partition in _PARTITION_ORDER:
            for label_status in _LABEL_ORDER:
                row = by_group.get((partition, label_status))
                if row is None:
                    raise CycleOperatingContextDevelopmentProfileError(
                        f"Cycle-context profile requires {label_status} rows in {partition}"
                    )
                distributions.append(
                    CycleOperatingContextLabelDistribution(
                        **{
                            field: (
                                partition
                                if field == "partition"
                                else label_status
                                if field == "label_status"
                                else int(row[field])
                                if field == "row_count"
                                else float(row[field])
                            )
                            for field in CycleOperatingContextLabelDistribution.__dataclass_fields__
                        }
                    )
                )

        counts = {
            partition: sum(
                distribution.row_count
                for distribution in distributions
                if distribution.partition == partition
            )
            for partition in _PARTITION_ORDER
        }
        development_count = development.count()
        if sum(counts.values()) != development_count:
            raise CycleOperatingContextDevelopmentProfileError(
                "Cycle operating-context development groups do not reconcile"
            )
        return CycleOperatingContextDevelopmentSummary(
            development_row_count=development_count,
            train_row_count=counts[PARTITION_TRAIN],
            validation_row_count=counts[PARTITION_VALIDATION],
            distributions=tuple(distributions),
        )
    finally:
        joined.unpersist()


def profile_full_source_cycle_operating_context_development(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceCycleOperatingContextDevelopmentProfile:
    """Rebuild labels, then profile complete development cycle context without writing."""

    telemetry_bronze = spark.read.format("delta").load(
        str(bronze_table_path(config, TELEMETRY_TABLE))
    )
    failure_bronze = spark.read.format("delta").load(str(bronze_table_path(config, FAILURE_TABLE)))
    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    telemetry = split_telemetry_by_quality(telemetry_bronze).accepted
    failures = split_failure_events_by_quality(failure_bronze).accepted
    try:
        (
            telemetry_count,
            observation_end,
            dataset_version,
            telemetry_source_sha256,
            telemetry_ingestion_batch_id,
        ) = _collect_observation_boundary(telemetry)
        failure_rows, failure_lineage = _collect_failure_events(failures)
    except FailureHorizonProfileError as exc:
        raise CycleOperatingContextDevelopmentProfileError(str(exc)) from exc
    (
        failure_dataset_version,
        failure_source_sha256,
        _failure_source_document_sha256,
        failure_ingestion_batch_id,
    ) = failure_lineage
    if dataset_version != config.dataset_version or failure_dataset_version != dataset_version:
        raise CycleOperatingContextDevelopmentProfileError(
            "Telemetry, failure-event, and configured dataset versions must match"
        )

    features = add_cycle_operating_context(cycles)
    horizons = assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        observation_end=observation_end,
    )
    summary = collect_cycle_operating_context_development_summary(features, horizons)
    return FullSourceCycleOperatingContextDevelopmentProfile(
        profile_version=CYCLE_OPERATING_CONTEXT_DEVELOPMENT_PROFILE_VERSION,
        feature_version=CYCLE_OPERATING_CONTEXT_VERSION,
        split_selection_version=CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
        failure_source_sha256=failure_source_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
        accepted_telemetry_record_count=telemetry_count,
        accepted_failure_event_count=len(failure_rows),
        summary=summary,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the full-source cycle operating-context development profile."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session(
        "railpulse-cycle-operating-context-development-profile", master=args.master
    )
    try:
        profile = profile_full_source_cycle_operating_context_development(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
