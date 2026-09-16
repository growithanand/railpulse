"""Compare diagnostic motor-current coverage policies without writing data."""

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
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.failure_horizon_profile import (
    FailureHorizonProfileError,
    _collect_failure_events,
    _collect_observation_boundary,
)
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
    assign_cycle_failure_horizons,
)
from railpulse.features.temporal_feature_profile import (
    MotorCurrentFeatureProfileError,
    _assert_feature_semantics,
    _assert_tail_context_is_complete,
    _available_with_tail_context,
    _collect_available_support,
    _collect_cycle_count,
    _with_internal_gap_context,
)
from railpulse.features.temporal_features import (
    MOTOR_CURRENT_FEATURE_VERSION,
    MOTOR_CURRENT_WINDOW_SECONDS,
    add_motor_current_15m_features,
)
from railpulse.ingestion.bronze import FAILURE_TABLE, TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_failures import split_failure_events_by_quality
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION
from railpulse.validation.silver_telemetry import (
    MATERIAL_GAP_SECONDS,
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

COVERAGE_POLICY_PROFILE_VERSION = "motor-current-coverage-policy-profile-v1"
LARGE_GAP_SENSITIVITY_SECONDS = 120
KNOWN_HORIZON_STATUSES = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)


class CoveragePolicyProfileError(RuntimeError):
    """Raised when diagnostic coverage-policy counts cannot be reconciled."""


@dataclass(frozen=True)
class CoveragePolicyCount:
    """Feature and horizon-label retention for one diagnostic policy."""

    policy_id: str
    excludes_strict_tail: bool
    minimum_excluded_window_gap_seconds: int | None
    retained_feature_cycle_count: int
    excluded_feature_cycle_count: int
    retained_positive_cycle_count: int
    retained_negative_cycle_count: int
    retained_null_label_cycle_count: int
    excluded_positive_cycle_count: int
    excluded_negative_cycle_count: int
    excluded_null_label_cycle_count: int


@dataclass(frozen=True)
class CoveragePolicyComparison:
    """Reconciled comparison over all available motor-current windows."""

    available_feature_cycle_count: int
    p05_observation_count: int
    p05_observation_span_seconds: int
    available_positive_cycle_count: int
    available_negative_cycle_count: int
    available_null_label_cycle_count: int
    policies: tuple[CoveragePolicyCount, ...]


@dataclass(frozen=True)
class FullSourceCoveragePolicyProfile:
    """Lineage and counts for the read-only full-source policy comparison."""

    profile_version: str
    feature_version: str
    feature_window_seconds: int
    horizon_version: str
    horizon_seconds: int
    label_observation_end: str
    telemetry_validation_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    cycle_count: int
    accepted_failure_event_count: int
    comparison: CoveragePolicyComparison


@dataclass(frozen=True)
class _CoveragePolicyDefinition:
    policy_id: str
    excludes_strict_tail: bool
    minimum_excluded_window_gap_seconds: int | None


POLICY_DEFINITIONS = (
    _CoveragePolicyDefinition("available_baseline", False, None),
    _CoveragePolicyDefinition("exclude_strict_tail", True, None),
    _CoveragePolicyDefinition("exclude_material_gap_20s", False, MATERIAL_GAP_SECONDS),
    _CoveragePolicyDefinition(
        "exclude_strict_tail_or_material_gap_20s",
        True,
        MATERIAL_GAP_SECONDS,
    ),
    _CoveragePolicyDefinition(
        "exclude_strict_tail_or_gap_120s",
        True,
        LARGE_GAP_SENSITIVITY_SECONDS,
    ),
)


def _validate_positive_threshold(value: int, *, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CoveragePolicyProfileError(f"{name} must be a positive integer")


def _validate_policy_inputs(available: DataFrame, labels: DataFrame) -> tuple[int, int]:
    coverage_columns = {
        "loaded_cycle_id",
        "motor_current_15m_observation_count",
        "observation_span_seconds",
        "leading_unobserved_seconds",
        "first_observation_follows_forward_gap",
        "maximum_internal_forward_gap_seconds",
    }
    missing_coverage_columns = sorted(coverage_columns - set(available.columns))
    if missing_coverage_columns:
        raise CoveragePolicyProfileError(
            "Available feature context is missing policy columns: "
            + ", ".join(missing_coverage_columns)
        )
    label_columns = {"loaded_cycle_id", "failure_horizon_status"}
    missing_label_columns = sorted(label_columns - set(labels.columns))
    if missing_label_columns:
        raise CoveragePolicyProfileError(
            "Failure horizons are missing policy columns: " + ", ".join(missing_label_columns)
        )

    invalid_context = available.where(
        F.col("loaded_cycle_id").isNull()
        | (F.col("motor_current_15m_observation_count") <= 0)
        | F.col("observation_span_seconds").isNull()
        | (F.col("observation_span_seconds") < 0)
        | F.col("leading_unobserved_seconds").isNull()
        | (F.col("leading_unobserved_seconds") < 0)
        | F.col("first_observation_follows_forward_gap").isNull()
        | (F.col("maximum_internal_forward_gap_seconds") < 0)
    ).limit(1)
    if invalid_context.count():
        raise CoveragePolicyProfileError("Available feature context contains invalid policy values")

    available_summary = available.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
    ).first()
    available_count = int(available_summary.cycle_count)
    if available_count <= 0:
        raise CoveragePolicyProfileError("Coverage-policy profiling requires available features")
    if int(available_summary.distinct_cycle_count) != available_count:
        raise CoveragePolicyProfileError("Available feature context requires unique cycle IDs")

    label_summary = labels.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
    ).first()
    label_count = int(label_summary.cycle_count)
    if label_count <= 0:
        raise CoveragePolicyProfileError("Coverage-policy profiling requires failure horizons")
    if int(label_summary.distinct_cycle_count) != label_count:
        raise CoveragePolicyProfileError("Failure horizons require unique non-null cycle IDs")

    unexpected_statuses = {
        row.failure_horizon_status
        for row in labels.select("failure_horizon_status").distinct().collect()
    } - set(KNOWN_HORIZON_STATUSES)
    if unexpected_statuses:
        raise CoveragePolicyProfileError(
            "Failure horizons contain unexpected statuses: "
            + ", ".join(sorted(str(status) for status in unexpected_statuses))
        )
    return available_count, label_count


def _maximum_window_gap_seconds() -> Column:
    leading_gap_seconds = F.when(
        F.col("first_observation_follows_forward_gap"),
        F.col("leading_unobserved_seconds"),
    ).otherwise(F.lit(0))
    internal_gap_seconds = F.coalesce(
        F.col("maximum_internal_forward_gap_seconds"),
        F.lit(0),
    )
    return F.greatest(leading_gap_seconds, internal_gap_seconds)


def collect_coverage_policy_counts(
    available: DataFrame,
    labels: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> CoveragePolicyComparison:
    """Compare fixed diagnostic policies over available windows and horizon labels."""

    _validate_positive_threshold(p05_observation_count, name="P05 observation count")
    _validate_positive_threshold(
        p05_observation_span_seconds,
        name="P05 observation span seconds",
    )
    available_count, _ = _validate_policy_inputs(available, labels)
    joined = available.join(
        labels.select("loaded_cycle_id", "failure_horizon_status"),
        on="loaded_cycle_id",
        how="left",
    )
    if joined.where(F.col("failure_horizon_status").isNull()).limit(1).count():
        raise CoveragePolicyProfileError("Every available feature requires one failure horizon")

    strict_tail = (F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)) | (
        F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    )
    maximum_gap_seconds = _maximum_window_gap_seconds()
    positive = F.col("failure_horizon_status") == STATUS_POSITIVE
    negative = F.col("failure_horizon_status") == STATUS_NEGATIVE
    null_label = ~F.col("failure_horizon_status").isin(STATUS_POSITIVE, STATUS_NEGATIVE)

    aggregations: list[Column] = []
    for definition in POLICY_DEFINITIONS:
        excluded = F.lit(False)
        if definition.excludes_strict_tail:
            excluded = excluded | strict_tail
        if definition.minimum_excluded_window_gap_seconds is not None:
            excluded = excluded | (
                maximum_gap_seconds >= F.lit(definition.minimum_excluded_window_gap_seconds)
            )
        retained = ~excluded
        for population_name, population in (
            ("retained", retained),
            ("excluded", excluded),
        ):
            aggregations.extend(
                (
                    F.count(F.when(population, F.lit(1)))
                    .cast(LongType())
                    .alias(f"{definition.policy_id}_{population_name}_total"),
                    F.count(F.when(population & positive, F.lit(1)))
                    .cast(LongType())
                    .alias(f"{definition.policy_id}_{population_name}_positive"),
                    F.count(F.when(population & negative, F.lit(1)))
                    .cast(LongType())
                    .alias(f"{definition.policy_id}_{population_name}_negative"),
                    F.count(F.when(population & null_label, F.lit(1)))
                    .cast(LongType())
                    .alias(f"{definition.policy_id}_{population_name}_null"),
                )
            )
    summary = joined.agg(*aggregations).first()

    policies = []
    for definition in POLICY_DEFINITIONS:
        prefix = definition.policy_id
        retained_total = int(summary[f"{prefix}_retained_total"])
        excluded_total = int(summary[f"{prefix}_excluded_total"])
        retained_positive = int(summary[f"{prefix}_retained_positive"])
        retained_negative = int(summary[f"{prefix}_retained_negative"])
        retained_null = int(summary[f"{prefix}_retained_null"])
        excluded_positive = int(summary[f"{prefix}_excluded_positive"])
        excluded_negative = int(summary[f"{prefix}_excluded_negative"])
        excluded_null = int(summary[f"{prefix}_excluded_null"])
        if retained_total + excluded_total != available_count:
            raise CoveragePolicyProfileError(f"Policy {prefix} does not reconcile its population")
        if retained_positive + retained_negative + retained_null != retained_total:
            raise CoveragePolicyProfileError(f"Policy {prefix} does not reconcile retained labels")
        if excluded_positive + excluded_negative + excluded_null != excluded_total:
            raise CoveragePolicyProfileError(f"Policy {prefix} does not reconcile excluded labels")
        policies.append(
            CoveragePolicyCount(
                policy_id=prefix,
                excludes_strict_tail=definition.excludes_strict_tail,
                minimum_excluded_window_gap_seconds=(
                    definition.minimum_excluded_window_gap_seconds
                ),
                retained_feature_cycle_count=retained_total,
                excluded_feature_cycle_count=excluded_total,
                retained_positive_cycle_count=retained_positive,
                retained_negative_cycle_count=retained_negative,
                retained_null_label_cycle_count=retained_null,
                excluded_positive_cycle_count=excluded_positive,
                excluded_negative_cycle_count=excluded_negative,
                excluded_null_label_cycle_count=excluded_null,
            )
        )

    baseline = policies[0]
    if baseline.excluded_feature_cycle_count != 0:
        raise CoveragePolicyProfileError("Available baseline must not exclude feature windows")
    return CoveragePolicyComparison(
        available_feature_cycle_count=available_count,
        p05_observation_count=p05_observation_count,
        p05_observation_span_seconds=p05_observation_span_seconds,
        available_positive_cycle_count=baseline.retained_positive_cycle_count,
        available_negative_cycle_count=baseline.retained_negative_cycle_count,
        available_null_label_cycle_count=baseline.retained_null_label_cycle_count,
        policies=tuple(policies),
    )


def collect_coverage_policy_profile(
    cycles: DataFrame,
    failures: DataFrame,
    telemetry: DataFrame,
    *,
    horizon_seconds: int = DEFAULT_FAILURE_HORIZON_SECONDS,
) -> FullSourceCoveragePolicyProfile:
    """Build and reconcile a read-only coverage-policy comparison."""

    try:
        (
            telemetry_count,
            observation_end,
            dataset_version,
            telemetry_source_sha256,
            telemetry_ingestion_batch_id,
        ) = _collect_observation_boundary(telemetry)
        failure_rows, failure_lineage = _collect_failure_events(failures)
        cycle_count = _collect_cycle_count(cycles)
    except (FailureHorizonProfileError, MotorCurrentFeatureProfileError) as exc:
        raise CoveragePolicyProfileError(str(exc)) from exc

    (
        failure_dataset_version,
        failure_source_sha256,
        failure_source_document_sha256,
        failure_ingestion_batch_id,
    ) = failure_lineage
    if failure_dataset_version != dataset_version:
        raise CoveragePolicyProfileError(
            "Telemetry and failure-event dataset versions do not match"
        )

    labels = assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=horizon_seconds,
        observation_end=observation_end,
    ).persist(StorageLevel.DISK_ONLY)
    boundaries = cycles.select(
        "loaded_cycle_id",
        F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
    )
    features = add_motor_current_15m_features(boundaries, telemetry).persist(StorageLevel.DISK_ONLY)
    try:
        label_count = labels.count()
        if label_count != cycle_count:
            raise CoveragePolicyProfileError("Failure horizons do not reconcile with Gold cycles")
        feature_summary = features.agg(
            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
            F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
        ).first()
        feature_count = int(feature_summary.cycle_count)
        if feature_count != cycle_count:
            raise CoveragePolicyProfileError(
                "Motor-current features do not reconcile with Gold cycles"
            )
        if int(feature_summary.distinct_cycle_count) != feature_count:
            raise CoveragePolicyProfileError("Motor-current features require unique cycle IDs")
        try:
            _assert_feature_semantics(features)
            support = _collect_available_support(features)
            tail_context = _available_with_tail_context(features, telemetry).persist(
                StorageLevel.DISK_ONLY
            )
            try:
                _assert_tail_context_is_complete(tail_context)
                gap_context = _with_internal_gap_context(tail_context, telemetry).persist(
                    StorageLevel.DISK_ONLY
                )
                try:
                    comparison = collect_coverage_policy_counts(
                        gap_context,
                        labels,
                        p05_observation_count=support.p05_observation_count,
                        p05_observation_span_seconds=support.p05_observation_span_seconds,
                    )
                finally:
                    gap_context.unpersist()
            finally:
                tail_context.unpersist()
        except MotorCurrentFeatureProfileError as exc:
            raise CoveragePolicyProfileError(str(exc)) from exc
    finally:
        features.unpersist()
        labels.unpersist()

    return FullSourceCoveragePolicyProfile(
        profile_version=COVERAGE_POLICY_PROFILE_VERSION,
        feature_version=MOTOR_CURRENT_FEATURE_VERSION,
        feature_window_seconds=MOTOR_CURRENT_WINDOW_SECONDS,
        horizon_version=FAILURE_HORIZON_VERSION,
        horizon_seconds=horizon_seconds,
        label_observation_end=observation_end.isoformat(sep=" ", timespec="seconds"),
        telemetry_validation_version=TELEMETRY_VALIDATION_VERSION,
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
        failure_source_sha256=failure_source_sha256,
        failure_source_document_sha256=failure_source_document_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
        accepted_telemetry_record_count=telemetry_count,
        cycle_count=cycle_count,
        accepted_failure_event_count=len(failure_rows),
        comparison=comparison,
    )


def profile_full_source_coverage_policies(
    spark: SparkSession,
    config: RailPulseConfig,
    *,
    horizon_seconds: int = DEFAULT_FAILURE_HORIZON_SECONDS,
) -> FullSourceCoveragePolicyProfile:
    """Rebuild accepted Silver views and compare policies without writing data."""

    telemetry_bronze = spark.read.format("delta").load(
        str(bronze_table_path(config, TELEMETRY_TABLE))
    )
    failure_bronze = spark.read.format("delta").load(str(bronze_table_path(config, FAILURE_TABLE)))
    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    telemetry_split = split_telemetry_by_quality(telemetry_bronze)
    failure_split = split_failure_events_by_quality(failure_bronze)
    validated = telemetry_split.all_records.persist(StorageLevel.DISK_ONLY)
    try:
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        cached_split = TelemetryQualitySplit(
            all_records=validated,
            accepted=validated.where(reason_count == 0),
            quarantined=validated.where(reason_count > 0),
        )
        profile = collect_coverage_policy_profile(
            cycles,
            failure_split.accepted,
            cached_split.accepted,
            horizon_seconds=horizon_seconds,
        )
    finally:
        validated.unpersist()
    if profile.dataset_version != config.dataset_version:
        raise CoveragePolicyProfileError(
            "Profile dataset version does not match the configured dataset version"
        )
    return profile


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument(
        "--horizon-seconds",
        type=int,
        default=DEFAULT_FAILURE_HORIZON_SECONDS,
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source policy comparison and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-coverage-policy-profile", master=args.master)
    try:
        profile = profile_full_source_coverage_policies(
            spark,
            config,
            horizon_seconds=args.horizon_seconds,
        )
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
