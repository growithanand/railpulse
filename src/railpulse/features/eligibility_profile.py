"""Profile full-source motor-current feature eligibility without writing data."""

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
from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
    MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS,
    REASON_MATERIAL_WINDOW_GAP,
    REASON_MISSING_OR_INVALID_CONTEXT,
    REASON_MISSING_PREDICTION,
    REASON_MISSING_PREDICTION_OBSERVATION,
    REASON_UNSUPPORTED_FEATURE_STATUS,
    STATUS_ELIGIBLE,
    STATUS_INELIGIBLE,
    add_motor_current_eligibility,
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
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
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

MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION = "motor-current-15m-eligibility-profile-v1"
ELIGIBILITY_STATUS_ORDER = (STATUS_ELIGIBLE, STATUS_INELIGIBLE)
ELIGIBILITY_REASON_ORDER = (
    REASON_MISSING_PREDICTION,
    REASON_MISSING_PREDICTION_OBSERVATION,
    REASON_MISSING_OR_INVALID_CONTEXT,
    REASON_MATERIAL_WINDOW_GAP,
    REASON_UNSUPPORTED_FEATURE_STATUS,
)


class EligibilityProfileError(RuntimeError):
    """Raised when full-source eligibility counts cannot be reconciled."""


@dataclass(frozen=True)
class EligibilityStatusCount:
    """Cycle count for one eligibility status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class EligibilityReasonCount:
    """Cycle count for one ineligibility reason."""

    reason: str
    cycle_count: int


@dataclass(frozen=True)
class FullSourceEligibilityProfile:
    """Reconciled read-only profile of motor-current feature eligibility."""

    profile_version: str
    eligibility_version: str
    feature_version: str
    feature_window_seconds: int
    minimum_excluded_gap_seconds: int
    telemetry_validation_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    cycle_count: int
    status_counts: tuple[EligibilityStatusCount, ...]
    reason_counts: tuple[EligibilityReasonCount, ...]


def _collect_eligibility_counts(
    eligibility: DataFrame,
    *,
    cycle_count: int,
) -> tuple[tuple[EligibilityStatusCount, ...], tuple[EligibilityReasonCount, ...]]:
    status_rows = eligibility.groupBy("motor_current_eligibility_status").count().collect()
    status_counts_by_name = {
        row.motor_current_eligibility_status: int(row["count"]) for row in status_rows
    }
    unexpected_statuses = set(status_counts_by_name) - set(ELIGIBILITY_STATUS_ORDER)
    if unexpected_statuses:
        raise EligibilityProfileError(
            "Eligibility output contains unexpected statuses: "
            + ", ".join(sorted(str(status) for status in unexpected_statuses))
        )
    if sum(status_counts_by_name.values()) != cycle_count:
        raise EligibilityProfileError("Eligibility statuses do not reconcile with Gold cycles")

    reason_rows = (
        eligibility.select(F.explode("motor_current_eligibility_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    reason_counts_by_name = {row.reason: int(row["count"]) for row in reason_rows}
    unexpected_reasons = set(reason_counts_by_name) - set(ELIGIBILITY_REASON_ORDER)
    if unexpected_reasons:
        raise EligibilityProfileError(
            "Eligibility output contains unexpected reasons: "
            + ", ".join(sorted(str(reason) for reason in unexpected_reasons))
        )
    ineligible_count = status_counts_by_name.get(STATUS_INELIGIBLE, 0)
    if sum(reason_counts_by_name.values()) != ineligible_count:
        raise EligibilityProfileError(
            "Ineligibility reason counts do not reconcile with ineligible cycles"
        )

    eligibility_status = F.col("motor_current_eligibility_status")
    feature_status = F.col("motor_current_15m_status")
    maximum_gap = F.col("motor_current_maximum_window_gap_seconds")
    reasons = F.col("motor_current_eligibility_reasons")
    eligible = eligibility_status == STATUS_ELIGIBLE
    ineligible = eligibility_status == STATUS_INELIGIBLE
    available = feature_status == STATUS_AVAILABLE
    known_feature_status = feature_status.isin(
        STATUS_AVAILABLE,
        STATUS_MISSING_PREDICTION,
        STATUS_MISSING_PREDICTION_OBSERVATION,
    )

    def exact_reason(reason: str):
        return reasons.eqNullSafe(F.array(F.lit(reason)))

    valid_semantics = (
        (
            eligible
            & available
            & maximum_gap.isNotNull()
            & (maximum_gap < MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS)
            & (F.size(reasons) == 0)
        )
        | (
            ineligible
            & available
            & maximum_gap.isNotNull()
            & (maximum_gap >= MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS)
            & exact_reason(REASON_MATERIAL_WINDOW_GAP)
        )
        | (
            ineligible
            & available
            & maximum_gap.isNull()
            & exact_reason(REASON_MISSING_OR_INVALID_CONTEXT)
        )
        | (
            ineligible
            & (feature_status == STATUS_MISSING_PREDICTION)
            & maximum_gap.isNull()
            & exact_reason(REASON_MISSING_PREDICTION)
        )
        | (
            ineligible
            & (feature_status == STATUS_MISSING_PREDICTION_OBSERVATION)
            & maximum_gap.isNull()
            & exact_reason(REASON_MISSING_PREDICTION_OBSERVATION)
        )
        | (
            ineligible
            & ~F.coalesce(known_feature_status, F.lit(False))
            & maximum_gap.isNull()
            & exact_reason(REASON_UNSUPPORTED_FEATURE_STATUS)
        )
    )
    invalid_semantics = eligibility.where(
        ~F.col("motor_current_eligibility_version").eqNullSafe(
            F.lit(MOTOR_CURRENT_ELIGIBILITY_VERSION)
        )
        | ~F.col("motor_current_eligibility_minimum_excluded_gap_seconds").eqNullSafe(
            F.lit(MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS)
        )
        | ~valid_semantics
    ).limit(1)
    if invalid_semantics.count():
        raise EligibilityProfileError("Eligibility output semantics do not reconcile")

    status_counts = tuple(
        EligibilityStatusCount(
            status=status,
            cycle_count=status_counts_by_name.get(status, 0),
        )
        for status in ELIGIBILITY_STATUS_ORDER
    )
    reason_counts = tuple(
        EligibilityReasonCount(
            reason=reason,
            cycle_count=reason_counts_by_name.get(reason, 0),
        )
        for reason in ELIGIBILITY_REASON_ORDER
    )
    return status_counts, reason_counts


def collect_motor_current_eligibility_profile(
    cycles: DataFrame,
    telemetry: DataFrame,
) -> FullSourceEligibilityProfile:
    """Apply and reconcile feature eligibility against accepted source snapshots."""

    try:
        telemetry_count, dataset_version, source_sha256, ingestion_batch_id = (
            _collect_telemetry_lineage(telemetry)
        )
        cycle_count = _collect_cycle_count(cycles)
    except MotorCurrentFeatureProfileError as exc:
        raise EligibilityProfileError(str(exc)) from exc

    boundaries = cycles.select(
        "loaded_cycle_id",
        F.col("loaded_cycle_stop_timestamp").alias("prediction_timestamp"),
    )
    features = add_motor_current_15m_features(boundaries, telemetry).persist(StorageLevel.DISK_ONLY)
    try:
        feature_summary = features.agg(
            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
            F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
        ).first()
        if int(feature_summary.cycle_count) != cycle_count:
            raise EligibilityProfileError(
                "Motor-current features do not reconcile with Gold cycles"
            )
        if int(feature_summary.distinct_cycle_count) != cycle_count:
            raise EligibilityProfileError("Motor-current features require unique cycle IDs")
        try:
            _assert_feature_semantics(features)
            tail_context = _available_with_tail_context(features, telemetry).persist(
                StorageLevel.DISK_ONLY
            )
            try:
                _assert_tail_context_is_complete(tail_context)
                gap_context = _with_internal_gap_context(tail_context, telemetry).persist(
                    StorageLevel.DISK_ONLY
                )
                try:
                    available_count = features.where(
                        F.col("motor_current_15m_status") == STATUS_AVAILABLE
                    ).count()
                    gap_summary = gap_context.agg(
                        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
                        F.countDistinct("loaded_cycle_id")
                        .cast(LongType())
                        .alias("distinct_cycle_count"),
                    ).first()
                    if (
                        int(gap_summary.cycle_count) != available_count
                        or int(gap_summary.distinct_cycle_count) != available_count
                    ):
                        raise EligibilityProfileError(
                            "Coverage context does not reconcile with available features"
                        )
                    eligibility = add_motor_current_eligibility(features, gap_context).persist(
                        StorageLevel.DISK_ONLY
                    )
                    try:
                        eligibility_summary = eligibility.agg(
                            F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
                            F.countDistinct("loaded_cycle_id")
                            .cast(LongType())
                            .alias("distinct_cycle_count"),
                        ).first()
                        if (
                            int(eligibility_summary.cycle_count) != cycle_count
                            or int(eligibility_summary.distinct_cycle_count) != cycle_count
                        ):
                            raise EligibilityProfileError(
                                "Eligibility output requires one row per Gold cycle"
                            )
                        status_counts, reason_counts = _collect_eligibility_counts(
                            eligibility,
                            cycle_count=cycle_count,
                        )
                    finally:
                        eligibility.unpersist()
                finally:
                    gap_context.unpersist()
            finally:
                tail_context.unpersist()
        except MotorCurrentFeatureProfileError as exc:
            raise EligibilityProfileError(str(exc)) from exc
    finally:
        features.unpersist()

    return FullSourceEligibilityProfile(
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


def profile_full_source_motor_current_eligibility(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceEligibilityProfile:
    """Rebuild accepted Silver telemetry and profile eligibility without writing."""

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
        profile = collect_motor_current_eligibility_profile(cycles, cached_split.accepted)
    finally:
        validated.unpersist()
    if profile.dataset_version != config.dataset_version:
        raise EligibilityProfileError(
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
    """Run the read-only full-source eligibility profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-feature-eligibility-profile", master=args.master)
    try:
        profile = profile_full_source_motor_current_eligibility(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
