"""Profile full-source motor-current feature coverage without writing data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import pairwise
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
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
    MATERIAL_GAP_SECONDS,
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

MOTOR_CURRENT_FEATURE_PROFILE_VERSION = "motor-current-15m-profile-v10"
LOW_SUPPORT_EXAMPLE_LIMIT = 10
GAP_MAGNITUDE_THRESHOLDS_SECONDS = (1, MATERIAL_GAP_SECONDS, 60, 120, 300, 600)
MOTOR_CURRENT_FEATURE_STATUS_ORDER = (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)
TAIL_EXAMPLE_COLUMNS = (
    "loaded_cycle_id",
    "prediction_timestamp",
    "motor_current_15m_observation_count",
    "motor_current_15m_first_observation_timestamp",
    "observation_span_seconds",
    "leading_unobserved_seconds",
    "first_observation_follows_forward_gap",
    "preceding_interval_seconds",
)
GAP_TAIL_EXAMPLE_COLUMNS = (
    *TAIL_EXAMPLE_COLUMNS,
    "internal_forward_gap_count",
    "maximum_internal_forward_gap_seconds",
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
class MotorCurrentLowSupportWindow:
    """One deterministic example from the weakest available feature windows."""

    loaded_cycle_id: str
    prediction_timestamp: str
    observation_count: int
    first_observation_timestamp: str
    observation_span_seconds: int
    leading_unobserved_seconds: int
    first_observation_follows_forward_gap: bool
    preceding_interval_seconds: int | None


@dataclass(frozen=True)
class MotorCurrentLowSupportTail:
    """Count and ordered examples at or below the observed fifth percentile."""

    p05_observation_count: int
    cycle_count_below_p05: int
    cycle_count_equal_to_p05: int
    cycle_count_at_or_below_p05: int
    example_limit: int
    examples: tuple[MotorCurrentLowSupportWindow, ...]


@dataclass(frozen=True)
class MotorCurrentLowSpanTail:
    """Count and ordered examples at or below the observed fifth-percentile span."""

    p05_observation_span_seconds: int
    cycle_count_below_p05: int
    cycle_count_equal_to_p05: int
    cycle_count_at_or_below_p05: int
    example_limit: int
    examples: tuple[MotorCurrentLowSupportWindow, ...]


@dataclass(frozen=True)
class MotorCurrentStrictTailOverlap:
    """Membership overlap below the observed count and span fifth percentiles."""

    p05_observation_count: int
    p05_observation_span_seconds: int
    cycle_count_below_both: int
    cycle_count_below_count_only: int
    cycle_count_below_span_only: int
    cycle_count_below_either: int


@dataclass(frozen=True)
class MotorCurrentOneSidedTailExamples:
    """Deterministic examples that fall below only one support cutoff."""

    example_limit: int
    count_only_examples: tuple[MotorCurrentLowSupportWindow, ...]
    span_only_examples: tuple[MotorCurrentLowSupportWindow, ...]


@dataclass(frozen=True)
class MotorCurrentCountOnlyInternalGapProfile:
    """Material forward gaps after the first observation in count-only windows."""

    count_only_cycle_count: int
    cycle_count_with_internal_forward_gap: int
    cycle_count_without_internal_forward_gap: int
    internal_forward_gap_count: int
    maximum_internal_forward_gap_seconds: int | None


@dataclass(frozen=True)
class MotorCurrentGapTailComparison:
    """Compare explicit gap intersection with the strict percentile-tail union."""

    available_cycle_count: int
    strict_tail_union_cycle_count: int
    gap_intersecting_cycle_count: int
    cycle_count_in_tail_and_gap: int
    cycle_count_in_tail_only: int
    cycle_count_in_gap_only: int
    cycle_count_in_neither: int
    cycle_count_with_leading_forward_gap: int
    cycle_count_with_internal_forward_gap: int


@dataclass(frozen=True)
class MotorCurrentGapTailDisagreementWindow:
    """One deterministic example where tail and explicit-gap membership disagree."""

    loaded_cycle_id: str
    prediction_timestamp: str
    observation_count: int
    first_observation_timestamp: str
    observation_span_seconds: int
    leading_unobserved_seconds: int
    first_observation_follows_forward_gap: bool
    preceding_interval_seconds: int | None
    internal_forward_gap_count: int
    maximum_internal_forward_gap_seconds: int | None


@dataclass(frozen=True)
class MotorCurrentGapTailDisagreementExamples:
    """Bounded deterministic examples from both gap-tail disagreement groups."""

    example_limit: int
    tail_only_examples: tuple[MotorCurrentGapTailDisagreementWindow, ...]
    gap_only_examples: tuple[MotorCurrentGapTailDisagreementWindow, ...]


@dataclass(frozen=True)
class MotorCurrentTailOnlyCharacterization:
    """Complete trigger and support summary for strict-tail-only windows."""

    cycle_count: int
    cycle_count_below_count_only: int
    cycle_count_below_span_only: int
    cycle_count_below_both: int
    minimum_observation_count: int | None
    maximum_observation_count: int | None
    minimum_observation_span_seconds: int | None
    maximum_observation_span_seconds: int | None


@dataclass(frozen=True)
class MotorCurrentGapOnlyCharacterization:
    """Complete position, support, and magnitude summary for explicit-gap-only windows."""

    cycle_count: int
    cycle_count_with_leading_gap_only: int
    cycle_count_with_internal_gap_only: int
    cycle_count_with_leading_and_internal_gap: int
    minimum_observation_count: int | None
    maximum_observation_count: int | None
    minimum_observation_span_seconds: int | None
    maximum_observation_span_seconds: int | None
    minimum_leading_unobserved_seconds: int | None
    median_leading_unobserved_seconds: int | None
    maximum_leading_unobserved_seconds: int | None
    minimum_leading_gap_interval_seconds: int | None
    median_leading_gap_interval_seconds: int | None
    maximum_leading_gap_interval_seconds: int | None
    internal_forward_gap_count: int
    minimum_largest_internal_gap_seconds: int | None
    median_largest_internal_gap_seconds: int | None
    maximum_largest_internal_gap_seconds: int | None


@dataclass(frozen=True)
class MotorCurrentGapTailDisagreementCharacterization:
    """Complete summaries for both gap-tail disagreement groups."""

    tail_only: MotorCurrentTailOnlyCharacterization
    gap_only: MotorCurrentGapOnlyCharacterization


@dataclass(frozen=True)
class MotorCurrentGapMagnitudeThresholdCapture:
    """Strict-tail capture at one minimum in-window gap magnitude."""

    minimum_window_gap_seconds: int
    gap_intersecting_cycle_count: int
    strict_tail_cycle_count: int
    non_tail_cycle_count: int


@dataclass(frozen=True)
class MotorCurrentGapMagnitudeSensitivity:
    """Strict-tail capture as minimum in-window gap magnitude increases."""

    thresholds: tuple[MotorCurrentGapMagnitudeThresholdCapture, ...]


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
    low_support_tail: MotorCurrentLowSupportTail
    low_span_tail: MotorCurrentLowSpanTail
    strict_tail_overlap: MotorCurrentStrictTailOverlap
    one_sided_tail_examples: MotorCurrentOneSidedTailExamples
    count_only_internal_gaps: MotorCurrentCountOnlyInternalGapProfile
    gap_tail_comparison: MotorCurrentGapTailComparison
    gap_tail_disagreement_examples: MotorCurrentGapTailDisagreementExamples
    gap_tail_disagreement_characterization: MotorCurrentGapTailDisagreementCharacterization
    gap_magnitude_sensitivity: MotorCurrentGapMagnitudeSensitivity


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="seconds")


def _collect_telemetry_lineage(telemetry: DataFrame) -> tuple[int, str, str, str]:
    required_columns = {
        "event_timestamp",
        "interval_seconds",
        "is_forward_gap",
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


def _available_with_tail_context(features: DataFrame, telemetry: DataFrame) -> DataFrame:
    """Attach reusable span and preceding-gap context to available feature windows."""

    first_observation_markers = telemetry.select(
        F.col("event_timestamp").alias("_tail_first_observation_timestamp"),
        F.col("is_forward_gap").alias("first_observation_follows_forward_gap"),
        F.col("interval_seconds").alias("preceding_interval_seconds"),
    )
    return (
        features.where(F.col("motor_current_15m_status") == STATUS_AVAILABLE)
        .withColumn(
            "observation_span_seconds",
            F.timestamp_diff(
                "SECOND",
                F.col("motor_current_15m_first_observation_timestamp"),
                F.col("motor_current_15m_last_observation_timestamp"),
            ),
        )
        .withColumn(
            "leading_unobserved_seconds",
            F.timestamp_diff(
                "SECOND",
                F.col("motor_current_15m_window_start"),
                F.col("motor_current_15m_first_observation_timestamp"),
            ),
        )
        .join(
            first_observation_markers,
            F.col("motor_current_15m_first_observation_timestamp")
            == F.col("_tail_first_observation_timestamp"),
            how="left",
        )
    )


def _assert_tail_context_is_complete(available: DataFrame) -> None:
    missing_marker = available.where(
        F.col("_tail_first_observation_timestamp").isNull()
        | F.col("first_observation_follows_forward_gap").isNull()
    ).limit(1)
    if missing_marker.count():
        raise MotorCurrentFeatureProfileError(
            "A motor-current feature start does not match accepted telemetry sequence metadata"
        )


def _window_example(row: Row) -> MotorCurrentLowSupportWindow:
    return MotorCurrentLowSupportWindow(
        loaded_cycle_id=str(row.loaded_cycle_id),
        prediction_timestamp=_timestamp_text(row.prediction_timestamp),
        observation_count=int(row.motor_current_15m_observation_count),
        first_observation_timestamp=_timestamp_text(
            row.motor_current_15m_first_observation_timestamp
        ),
        observation_span_seconds=int(row.observation_span_seconds),
        leading_unobserved_seconds=int(row.leading_unobserved_seconds),
        first_observation_follows_forward_gap=bool(row.first_observation_follows_forward_gap),
        preceding_interval_seconds=(
            None if row.preceding_interval_seconds is None else int(row.preceding_interval_seconds)
        ),
    )


def _collect_tail_counts_and_examples(
    available: DataFrame,
    *,
    metric_column: str,
    p05_value: int,
    secondary_order_column: str,
) -> tuple[int, int, tuple[MotorCurrentLowSupportWindow, ...]]:
    tail = available.where(F.col(metric_column) <= F.lit(p05_value)).persist(StorageLevel.DISK_ONLY)
    try:
        tail_summary = tail.agg(
            F.count(F.lit(1)).cast(LongType()).alias("tail_count"),
            F.count(
                F.when(
                    F.col(metric_column) == F.lit(p05_value),
                    F.lit(1),
                )
            )
            .cast(LongType())
            .alias("equal_to_p05_count"),
        ).first()
        tail_count = int(tail_summary.tail_count)
        equal_to_p05_count = int(tail_summary.equal_to_p05_count)
        if tail_count <= 0:
            raise MotorCurrentFeatureProfileError(
                "Motor-current fifth-percentile tail contains no available windows"
            )
        rows = (
            tail.orderBy(
                metric_column,
                secondary_order_column,
                "prediction_timestamp",
                "loaded_cycle_id",
            )
            .select(*TAIL_EXAMPLE_COLUMNS)
            .limit(LOW_SUPPORT_EXAMPLE_LIMIT)
            .collect()
        )
    finally:
        tail.unpersist()

    return (
        tail_count - equal_to_p05_count,
        equal_to_p05_count,
        tuple(_window_example(row) for row in rows),
    )


def _collect_low_support_tail(
    available: DataFrame,
    *,
    p05_observation_count: int,
) -> MotorCurrentLowSupportTail:
    below_count, equal_count, examples = _collect_tail_counts_and_examples(
        available,
        metric_column="motor_current_15m_observation_count",
        p05_value=p05_observation_count,
        secondary_order_column="observation_span_seconds",
    )
    return MotorCurrentLowSupportTail(
        p05_observation_count=p05_observation_count,
        cycle_count_below_p05=below_count,
        cycle_count_equal_to_p05=equal_count,
        cycle_count_at_or_below_p05=below_count + equal_count,
        example_limit=LOW_SUPPORT_EXAMPLE_LIMIT,
        examples=examples,
    )


def _collect_low_span_tail(
    available: DataFrame,
    *,
    p05_observation_span_seconds: int,
) -> MotorCurrentLowSpanTail:
    below_count, equal_count, examples = _collect_tail_counts_and_examples(
        available,
        metric_column="observation_span_seconds",
        p05_value=p05_observation_span_seconds,
        secondary_order_column="motor_current_15m_observation_count",
    )
    return MotorCurrentLowSpanTail(
        p05_observation_span_seconds=p05_observation_span_seconds,
        cycle_count_below_p05=below_count,
        cycle_count_equal_to_p05=equal_count,
        cycle_count_at_or_below_p05=below_count + equal_count,
        example_limit=LOW_SUPPORT_EXAMPLE_LIMIT,
        examples=examples,
    )


def _collect_strict_tail_overlap(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentStrictTailOverlap:
    below_count = F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)
    below_span = F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    summary = available.agg(
        F.count(F.when(below_count & below_span, F.lit(1))).cast(LongType()).alias("below_both"),
        F.count(F.when(below_count & ~below_span, F.lit(1)))
        .cast(LongType())
        .alias("below_count_only"),
        F.count(F.when(~below_count & below_span, F.lit(1)))
        .cast(LongType())
        .alias("below_span_only"),
    ).first()
    below_both = int(summary.below_both)
    below_count_only = int(summary.below_count_only)
    below_span_only = int(summary.below_span_only)
    return MotorCurrentStrictTailOverlap(
        p05_observation_count=p05_observation_count,
        p05_observation_span_seconds=p05_observation_span_seconds,
        cycle_count_below_both=below_both,
        cycle_count_below_count_only=below_count_only,
        cycle_count_below_span_only=below_span_only,
        cycle_count_below_either=below_both + below_count_only + below_span_only,
    )


def _collect_one_sided_tail_examples(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentOneSidedTailExamples:
    below_count = F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)
    below_span = F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    count_only_rows = (
        available.where(below_count & ~below_span)
        .orderBy(
            "motor_current_15m_observation_count",
            F.col("observation_span_seconds").desc(),
            "prediction_timestamp",
            "loaded_cycle_id",
        )
        .select(*TAIL_EXAMPLE_COLUMNS)
        .limit(LOW_SUPPORT_EXAMPLE_LIMIT)
        .collect()
    )
    span_only_rows = (
        available.where(~below_count & below_span)
        .orderBy(
            "observation_span_seconds",
            F.col("motor_current_15m_observation_count").desc(),
            "prediction_timestamp",
            "loaded_cycle_id",
        )
        .select(*TAIL_EXAMPLE_COLUMNS)
        .limit(LOW_SUPPORT_EXAMPLE_LIMIT)
        .collect()
    )
    return MotorCurrentOneSidedTailExamples(
        example_limit=LOW_SUPPORT_EXAMPLE_LIMIT,
        count_only_examples=tuple(_window_example(row) for row in count_only_rows),
        span_only_examples=tuple(_window_example(row) for row in span_only_rows),
    )


def _with_internal_gap_context(available: DataFrame, telemetry: DataFrame) -> DataFrame:
    windows = available.select(
        "loaded_cycle_id",
        "prediction_timestamp",
        "motor_current_15m_first_observation_timestamp",
    ).alias("window")
    gap_markers = (
        telemetry.where(F.col("is_forward_gap"))
        .select(
            F.col("event_timestamp").alias("internal_gap_timestamp"),
            F.col("interval_seconds").alias("internal_gap_interval_seconds"),
        )
        .alias("gap")
    )
    joined = windows.join(
        F.broadcast(gap_markers),
        (
            F.col("gap.internal_gap_timestamp")
            > F.col("window.motor_current_15m_first_observation_timestamp")
        )
        & (F.col("gap.internal_gap_timestamp") <= F.col("window.prediction_timestamp")),
        how="left",
    ).select(
        F.col("window.loaded_cycle_id").alias("loaded_cycle_id"),
        F.col("gap.internal_gap_timestamp").alias("internal_gap_timestamp"),
        F.col("gap.internal_gap_interval_seconds").alias("internal_gap_interval_seconds"),
    )
    per_window = joined.groupBy("loaded_cycle_id").agg(
        F.count("internal_gap_timestamp").cast(LongType()).alias("internal_forward_gap_count"),
        F.max("internal_gap_interval_seconds").alias("maximum_internal_forward_gap_seconds"),
    )
    return available.join(per_window, on="loaded_cycle_id", how="left")


def _collect_count_only_internal_gaps(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentCountOnlyInternalGapProfile:
    per_window = available.where(
        (F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count))
        & (F.col("observation_span_seconds") >= F.lit(p05_observation_span_seconds))
    )
    summary = per_window.agg(
        F.count(F.lit(1)).cast(LongType()).alias("count_only_cycle_count"),
        F.count(F.when(F.col("internal_forward_gap_count") > F.lit(0), F.lit(1)))
        .cast(LongType())
        .alias("cycles_with_internal_gap"),
        F.sum("internal_forward_gap_count").cast(LongType()).alias("internal_gap_count"),
        F.max("maximum_internal_forward_gap_seconds").alias("maximum_internal_gap_seconds"),
    ).first()
    count_only_cycle_count = int(summary.count_only_cycle_count)
    cycles_with_internal_gap = int(summary.cycles_with_internal_gap)
    maximum_internal_gap_seconds = summary.maximum_internal_gap_seconds
    return MotorCurrentCountOnlyInternalGapProfile(
        count_only_cycle_count=count_only_cycle_count,
        cycle_count_with_internal_forward_gap=cycles_with_internal_gap,
        cycle_count_without_internal_forward_gap=(
            count_only_cycle_count - cycles_with_internal_gap
        ),
        internal_forward_gap_count=int(summary.internal_gap_count or 0),
        maximum_internal_forward_gap_seconds=(
            None if maximum_internal_gap_seconds is None else int(maximum_internal_gap_seconds)
        ),
    )


def _collect_gap_tail_comparison(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentGapTailComparison:
    in_tail = (F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)) | (
        F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    )
    has_leading_gap = F.col("first_observation_follows_forward_gap")
    has_internal_gap = F.col("internal_forward_gap_count") > F.lit(0)
    intersects_gap = has_leading_gap | has_internal_gap
    summary = available.agg(
        F.count(F.lit(1)).cast(LongType()).alias("available_count"),
        F.count(F.when(in_tail, F.lit(1))).cast(LongType()).alias("tail_count"),
        F.count(F.when(intersects_gap, F.lit(1))).cast(LongType()).alias("gap_count"),
        F.count(F.when(in_tail & intersects_gap, F.lit(1)))
        .cast(LongType())
        .alias("tail_and_gap_count"),
        F.count(F.when(in_tail & ~intersects_gap, F.lit(1)))
        .cast(LongType())
        .alias("tail_only_count"),
        F.count(F.when(~in_tail & intersects_gap, F.lit(1)))
        .cast(LongType())
        .alias("gap_only_count"),
        F.count(F.when(~in_tail & ~intersects_gap, F.lit(1)))
        .cast(LongType())
        .alias("neither_count"),
        F.count(F.when(has_leading_gap, F.lit(1))).cast(LongType()).alias("leading_gap_count"),
        F.count(F.when(has_internal_gap, F.lit(1))).cast(LongType()).alias("internal_gap_count"),
    ).first()
    return MotorCurrentGapTailComparison(
        available_cycle_count=int(summary.available_count),
        strict_tail_union_cycle_count=int(summary.tail_count),
        gap_intersecting_cycle_count=int(summary.gap_count),
        cycle_count_in_tail_and_gap=int(summary.tail_and_gap_count),
        cycle_count_in_tail_only=int(summary.tail_only_count),
        cycle_count_in_gap_only=int(summary.gap_only_count),
        cycle_count_in_neither=int(summary.neither_count),
        cycle_count_with_leading_forward_gap=int(summary.leading_gap_count),
        cycle_count_with_internal_forward_gap=int(summary.internal_gap_count),
    )


def _gap_tail_disagreement_example(row: Row) -> MotorCurrentGapTailDisagreementWindow:
    return MotorCurrentGapTailDisagreementWindow(
        loaded_cycle_id=str(row.loaded_cycle_id),
        prediction_timestamp=_timestamp_text(row.prediction_timestamp),
        observation_count=int(row.motor_current_15m_observation_count),
        first_observation_timestamp=_timestamp_text(
            row.motor_current_15m_first_observation_timestamp
        ),
        observation_span_seconds=int(row.observation_span_seconds),
        leading_unobserved_seconds=int(row.leading_unobserved_seconds),
        first_observation_follows_forward_gap=bool(row.first_observation_follows_forward_gap),
        preceding_interval_seconds=(
            None if row.preceding_interval_seconds is None else int(row.preceding_interval_seconds)
        ),
        internal_forward_gap_count=int(row.internal_forward_gap_count),
        maximum_internal_forward_gap_seconds=(
            None
            if row.maximum_internal_forward_gap_seconds is None
            else int(row.maximum_internal_forward_gap_seconds)
        ),
    )


def _collect_gap_tail_disagreement_examples(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentGapTailDisagreementExamples:
    in_tail = (F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)) | (
        F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    )
    has_leading_gap = F.col("first_observation_follows_forward_gap")
    has_internal_gap = F.col("internal_forward_gap_count") > F.lit(0)
    intersects_gap = has_leading_gap | has_internal_gap

    tail_only_rows = (
        available.where(in_tail & ~intersects_gap)
        .orderBy(
            "motor_current_15m_observation_count",
            "observation_span_seconds",
            "prediction_timestamp",
            "loaded_cycle_id",
        )
        .select(*GAP_TAIL_EXAMPLE_COLUMNS)
        .limit(LOW_SUPPORT_EXAMPLE_LIMIT)
        .collect()
    )
    leading_gap_seconds = F.when(
        has_leading_gap,
        F.coalesce(F.col("preceding_interval_seconds"), F.lit(0)),
    ).otherwise(F.lit(0))
    strongest_gap_seconds = F.greatest(
        leading_gap_seconds,
        F.coalesce(F.col("maximum_internal_forward_gap_seconds"), F.lit(0)),
    )
    gap_only_rows = (
        available.where(~in_tail & intersects_gap)
        .orderBy(
            strongest_gap_seconds.desc(),
            F.col("internal_forward_gap_count").desc(),
            "motor_current_15m_observation_count",
            "observation_span_seconds",
            "prediction_timestamp",
            "loaded_cycle_id",
        )
        .select(*GAP_TAIL_EXAMPLE_COLUMNS)
        .limit(LOW_SUPPORT_EXAMPLE_LIMIT)
        .collect()
    )
    return MotorCurrentGapTailDisagreementExamples(
        example_limit=LOW_SUPPORT_EXAMPLE_LIMIT,
        tail_only_examples=tuple(_gap_tail_disagreement_example(row) for row in tail_only_rows),
        gap_only_examples=tuple(_gap_tail_disagreement_example(row) for row in gap_only_rows),
    )


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _collect_gap_tail_disagreement_characterization(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentGapTailDisagreementCharacterization:
    below_count = F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)
    below_span = F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    in_tail = below_count | below_span
    has_leading_gap = F.col("first_observation_follows_forward_gap")
    has_internal_gap = F.col("internal_forward_gap_count") > F.lit(0)
    intersects_gap = has_leading_gap | has_internal_gap

    tail_only = available.where(in_tail & ~intersects_gap)
    tail_summary = tail_only.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        F.count(F.when(below_count & ~below_span, F.lit(1)))
        .cast(LongType())
        .alias("below_count_only"),
        F.count(F.when(~below_count & below_span, F.lit(1)))
        .cast(LongType())
        .alias("below_span_only"),
        F.count(F.when(below_count & below_span, F.lit(1))).cast(LongType()).alias("below_both"),
        F.min("motor_current_15m_observation_count").alias("minimum_observation_count"),
        F.max("motor_current_15m_observation_count").alias("maximum_observation_count"),
        F.min("observation_span_seconds").alias("minimum_observation_span_seconds"),
        F.max("observation_span_seconds").alias("maximum_observation_span_seconds"),
    ).first()

    gap_only = available.where(~in_tail & intersects_gap)
    leading_unobserved_seconds = F.when(
        has_leading_gap,
        F.col("leading_unobserved_seconds"),
    )
    leading_gap_interval_seconds = F.when(
        has_leading_gap,
        F.col("preceding_interval_seconds"),
    )
    largest_internal_gap_seconds = F.when(
        has_internal_gap,
        F.col("maximum_internal_forward_gap_seconds"),
    )
    gap_summary = gap_only.agg(
        F.count(F.lit(1)).cast(LongType()).alias("cycle_count"),
        F.count(F.when(has_leading_gap & ~has_internal_gap, F.lit(1)))
        .cast(LongType())
        .alias("leading_only"),
        F.count(F.when(~has_leading_gap & has_internal_gap, F.lit(1)))
        .cast(LongType())
        .alias("internal_only"),
        F.count(F.when(has_leading_gap & has_internal_gap, F.lit(1)))
        .cast(LongType())
        .alias("leading_and_internal"),
        F.min("motor_current_15m_observation_count").alias("minimum_observation_count"),
        F.max("motor_current_15m_observation_count").alias("maximum_observation_count"),
        F.min("observation_span_seconds").alias("minimum_observation_span_seconds"),
        F.max("observation_span_seconds").alias("maximum_observation_span_seconds"),
        F.min(leading_unobserved_seconds).alias("minimum_leading_unobserved_seconds"),
        F.percentile_approx(leading_unobserved_seconds, 0.5, 10_000).alias(
            "median_leading_unobserved_seconds"
        ),
        F.max(leading_unobserved_seconds).alias("maximum_leading_unobserved_seconds"),
        F.min(leading_gap_interval_seconds).alias("minimum_leading_gap_interval_seconds"),
        F.percentile_approx(leading_gap_interval_seconds, 0.5, 10_000).alias(
            "median_leading_gap_interval_seconds"
        ),
        F.max(leading_gap_interval_seconds).alias("maximum_leading_gap_interval_seconds"),
        F.sum("internal_forward_gap_count").cast(LongType()).alias("internal_gap_count"),
        F.min(largest_internal_gap_seconds).alias("minimum_largest_internal_gap_seconds"),
        F.percentile_approx(largest_internal_gap_seconds, 0.5, 10_000).alias(
            "median_largest_internal_gap_seconds"
        ),
        F.max(largest_internal_gap_seconds).alias("maximum_largest_internal_gap_seconds"),
    ).first()

    return MotorCurrentGapTailDisagreementCharacterization(
        tail_only=MotorCurrentTailOnlyCharacterization(
            cycle_count=int(tail_summary.cycle_count),
            cycle_count_below_count_only=int(tail_summary.below_count_only),
            cycle_count_below_span_only=int(tail_summary.below_span_only),
            cycle_count_below_both=int(tail_summary.below_both),
            minimum_observation_count=_optional_int(tail_summary.minimum_observation_count),
            maximum_observation_count=_optional_int(tail_summary.maximum_observation_count),
            minimum_observation_span_seconds=_optional_int(
                tail_summary.minimum_observation_span_seconds
            ),
            maximum_observation_span_seconds=_optional_int(
                tail_summary.maximum_observation_span_seconds
            ),
        ),
        gap_only=MotorCurrentGapOnlyCharacterization(
            cycle_count=int(gap_summary.cycle_count),
            cycle_count_with_leading_gap_only=int(gap_summary.leading_only),
            cycle_count_with_internal_gap_only=int(gap_summary.internal_only),
            cycle_count_with_leading_and_internal_gap=int(gap_summary.leading_and_internal),
            minimum_observation_count=_optional_int(gap_summary.minimum_observation_count),
            maximum_observation_count=_optional_int(gap_summary.maximum_observation_count),
            minimum_observation_span_seconds=_optional_int(
                gap_summary.minimum_observation_span_seconds
            ),
            maximum_observation_span_seconds=_optional_int(
                gap_summary.maximum_observation_span_seconds
            ),
            minimum_leading_unobserved_seconds=_optional_int(
                gap_summary.minimum_leading_unobserved_seconds
            ),
            median_leading_unobserved_seconds=_optional_int(
                gap_summary.median_leading_unobserved_seconds
            ),
            maximum_leading_unobserved_seconds=_optional_int(
                gap_summary.maximum_leading_unobserved_seconds
            ),
            minimum_leading_gap_interval_seconds=_optional_int(
                gap_summary.minimum_leading_gap_interval_seconds
            ),
            median_leading_gap_interval_seconds=_optional_int(
                gap_summary.median_leading_gap_interval_seconds
            ),
            maximum_leading_gap_interval_seconds=_optional_int(
                gap_summary.maximum_leading_gap_interval_seconds
            ),
            internal_forward_gap_count=int(gap_summary.internal_gap_count or 0),
            minimum_largest_internal_gap_seconds=_optional_int(
                gap_summary.minimum_largest_internal_gap_seconds
            ),
            median_largest_internal_gap_seconds=_optional_int(
                gap_summary.median_largest_internal_gap_seconds
            ),
            maximum_largest_internal_gap_seconds=_optional_int(
                gap_summary.maximum_largest_internal_gap_seconds
            ),
        ),
    )


def _collect_gap_magnitude_sensitivity(
    available: DataFrame,
    *,
    p05_observation_count: int,
    p05_observation_span_seconds: int,
) -> MotorCurrentGapMagnitudeSensitivity:
    in_tail = (F.col("motor_current_15m_observation_count") < F.lit(p05_observation_count)) | (
        F.col("observation_span_seconds") < F.lit(p05_observation_span_seconds)
    )
    leading_window_gap_seconds = F.when(
        F.col("first_observation_follows_forward_gap"),
        F.col("leading_unobserved_seconds"),
    ).otherwise(F.lit(0))
    internal_window_gap_seconds = F.coalesce(
        F.col("maximum_internal_forward_gap_seconds"),
        F.lit(0),
    )
    maximum_window_gap_seconds = F.greatest(
        leading_window_gap_seconds,
        internal_window_gap_seconds,
    )

    aggregations = []
    for threshold in GAP_MAGNITUDE_THRESHOLDS_SECONDS:
        at_or_above_threshold = maximum_window_gap_seconds >= F.lit(threshold)
        aggregations.extend(
            (
                F.count(F.when(at_or_above_threshold, F.lit(1)))
                .cast(LongType())
                .alias(f"gap_count_{threshold}"),
                F.count(F.when(at_or_above_threshold & in_tail, F.lit(1)))
                .cast(LongType())
                .alias(f"tail_count_{threshold}"),
            )
        )
    summary = available.agg(*aggregations).first()

    thresholds = []
    for threshold in GAP_MAGNITUDE_THRESHOLDS_SECONDS:
        gap_count = int(summary[f"gap_count_{threshold}"])
        tail_count = int(summary[f"tail_count_{threshold}"])
        thresholds.append(
            MotorCurrentGapMagnitudeThresholdCapture(
                minimum_window_gap_seconds=threshold,
                gap_intersecting_cycle_count=gap_count,
                strict_tail_cycle_count=tail_count,
                non_tail_cycle_count=gap_count - tail_count,
            )
        )
    return MotorCurrentGapMagnitudeSensitivity(thresholds=tuple(thresholds))


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
        tail_context = _available_with_tail_context(features, telemetry).persist(
            StorageLevel.DISK_ONLY
        )
        try:
            _assert_tail_context_is_complete(tail_context)
            low_support_tail = _collect_low_support_tail(
                tail_context,
                p05_observation_count=support.p05_observation_count,
            )
            low_span_tail = _collect_low_span_tail(
                tail_context,
                p05_observation_span_seconds=support.p05_observation_span_seconds,
            )
            strict_tail_overlap = _collect_strict_tail_overlap(
                tail_context,
                p05_observation_count=support.p05_observation_count,
                p05_observation_span_seconds=support.p05_observation_span_seconds,
            )
            one_sided_tail_examples = _collect_one_sided_tail_examples(
                tail_context,
                p05_observation_count=support.p05_observation_count,
                p05_observation_span_seconds=support.p05_observation_span_seconds,
            )
            gap_context = _with_internal_gap_context(tail_context, telemetry).persist(
                StorageLevel.DISK_ONLY
            )
            try:
                count_only_internal_gaps = _collect_count_only_internal_gaps(
                    gap_context,
                    p05_observation_count=support.p05_observation_count,
                    p05_observation_span_seconds=support.p05_observation_span_seconds,
                )
                gap_tail_comparison = _collect_gap_tail_comparison(
                    gap_context,
                    p05_observation_count=support.p05_observation_count,
                    p05_observation_span_seconds=support.p05_observation_span_seconds,
                )
                gap_tail_disagreement_examples = _collect_gap_tail_disagreement_examples(
                    gap_context,
                    p05_observation_count=support.p05_observation_count,
                    p05_observation_span_seconds=support.p05_observation_span_seconds,
                )
                gap_tail_disagreement_characterization = (
                    _collect_gap_tail_disagreement_characterization(
                        gap_context,
                        p05_observation_count=support.p05_observation_count,
                        p05_observation_span_seconds=support.p05_observation_span_seconds,
                    )
                )
                gap_magnitude_sensitivity = _collect_gap_magnitude_sensitivity(
                    gap_context,
                    p05_observation_count=support.p05_observation_count,
                    p05_observation_span_seconds=support.p05_observation_span_seconds,
                )
            finally:
                gap_context.unpersist()
            if (
                strict_tail_overlap.cycle_count_below_both
                + strict_tail_overlap.cycle_count_below_count_only
                != low_support_tail.cycle_count_below_p05
            ):
                raise MotorCurrentFeatureProfileError(
                    "Strict observation-count tail does not reconcile with overlap counts"
                )
            if (
                strict_tail_overlap.cycle_count_below_both
                + strict_tail_overlap.cycle_count_below_span_only
                != low_span_tail.cycle_count_below_p05
            ):
                raise MotorCurrentFeatureProfileError(
                    "Strict observed-span tail does not reconcile with overlap counts"
                )
            if len(one_sided_tail_examples.count_only_examples) != min(
                strict_tail_overlap.cycle_count_below_count_only,
                LOW_SUPPORT_EXAMPLE_LIMIT,
            ):
                raise MotorCurrentFeatureProfileError(
                    "Count-only tail examples do not reconcile with overlap counts"
                )
            if len(one_sided_tail_examples.span_only_examples) != min(
                strict_tail_overlap.cycle_count_below_span_only,
                LOW_SUPPORT_EXAMPLE_LIMIT,
            ):
                raise MotorCurrentFeatureProfileError(
                    "Span-only tail examples do not reconcile with overlap counts"
                )
            if (
                count_only_internal_gaps.count_only_cycle_count
                != strict_tail_overlap.cycle_count_below_count_only
            ):
                raise MotorCurrentFeatureProfileError(
                    "Count-only internal-gap profile does not reconcile with overlap counts"
                )
            if gap_tail_comparison.available_cycle_count != support.available_cycle_count:
                raise MotorCurrentFeatureProfileError(
                    "Gap-tail comparison does not reconcile with available feature counts"
                )
            if (
                gap_tail_comparison.strict_tail_union_cycle_count
                != strict_tail_overlap.cycle_count_below_either
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-tail comparison does not reconcile with the strict-tail union"
                )
            if (
                gap_tail_comparison.cycle_count_in_tail_and_gap
                + gap_tail_comparison.cycle_count_in_tail_only
                != gap_tail_comparison.strict_tail_union_cycle_count
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-tail comparison does not reconcile its tail partition"
                )
            if (
                gap_tail_comparison.cycle_count_in_tail_and_gap
                + gap_tail_comparison.cycle_count_in_gap_only
                != gap_tail_comparison.gap_intersecting_cycle_count
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-tail comparison does not reconcile its gap partition"
                )
            if (
                gap_tail_comparison.cycle_count_in_tail_and_gap
                + gap_tail_comparison.cycle_count_in_tail_only
                + gap_tail_comparison.cycle_count_in_gap_only
                + gap_tail_comparison.cycle_count_in_neither
                != gap_tail_comparison.available_cycle_count
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-tail comparison does not reconcile all available windows"
                )
            if len(gap_tail_disagreement_examples.tail_only_examples) != min(
                gap_tail_comparison.cycle_count_in_tail_only,
                LOW_SUPPORT_EXAMPLE_LIMIT,
            ):
                raise MotorCurrentFeatureProfileError(
                    "Tail-only gap-tail examples do not reconcile with comparison counts"
                )
            if len(gap_tail_disagreement_examples.gap_only_examples) != min(
                gap_tail_comparison.cycle_count_in_gap_only,
                LOW_SUPPORT_EXAMPLE_LIMIT,
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-only gap-tail examples do not reconcile with comparison counts"
                )
            tail_only_characterization = gap_tail_disagreement_characterization.tail_only
            if (
                tail_only_characterization.cycle_count
                != gap_tail_comparison.cycle_count_in_tail_only
            ):
                raise MotorCurrentFeatureProfileError(
                    "Tail-only characterization does not reconcile with comparison counts"
                )
            if (
                tail_only_characterization.cycle_count_below_count_only
                + tail_only_characterization.cycle_count_below_span_only
                + tail_only_characterization.cycle_count_below_both
                != tail_only_characterization.cycle_count
            ):
                raise MotorCurrentFeatureProfileError(
                    "Tail-only characterization does not reconcile its trigger partition"
                )
            gap_only_characterization = gap_tail_disagreement_characterization.gap_only
            if gap_only_characterization.cycle_count != gap_tail_comparison.cycle_count_in_gap_only:
                raise MotorCurrentFeatureProfileError(
                    "Gap-only characterization does not reconcile with comparison counts"
                )
            if (
                gap_only_characterization.cycle_count_with_leading_gap_only
                + gap_only_characterization.cycle_count_with_internal_gap_only
                + gap_only_characterization.cycle_count_with_leading_and_internal_gap
                != gap_only_characterization.cycle_count
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-only characterization does not reconcile its position partition"
                )
            sensitivity_rows = gap_magnitude_sensitivity.thresholds
            if tuple(row.minimum_window_gap_seconds for row in sensitivity_rows) != (
                GAP_MAGNITUDE_THRESHOLDS_SECONDS
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-magnitude sensitivity thresholds do not match the profile contract"
                )
            first_sensitivity_row = sensitivity_rows[0]
            if (
                first_sensitivity_row.gap_intersecting_cycle_count
                != gap_tail_comparison.gap_intersecting_cycle_count
                or first_sensitivity_row.strict_tail_cycle_count
                != gap_tail_comparison.cycle_count_in_tail_and_gap
                or first_sensitivity_row.non_tail_cycle_count
                != gap_tail_comparison.cycle_count_in_gap_only
            ):
                raise MotorCurrentFeatureProfileError(
                    "Gap-magnitude sensitivity does not reconcile with gap-tail comparison counts"
                )
            for row in sensitivity_rows:
                if (
                    row.strict_tail_cycle_count + row.non_tail_cycle_count
                    != row.gap_intersecting_cycle_count
                ):
                    raise MotorCurrentFeatureProfileError(
                        "Gap-magnitude sensitivity row does not reconcile"
                    )
            for previous, current in pairwise(sensitivity_rows):
                if (
                    current.gap_intersecting_cycle_count > previous.gap_intersecting_cycle_count
                    or current.strict_tail_cycle_count > previous.strict_tail_cycle_count
                    or current.non_tail_cycle_count > previous.non_tail_cycle_count
                ):
                    raise MotorCurrentFeatureProfileError(
                        "Gap-magnitude sensitivity counts are not monotonic"
                    )
        finally:
            tail_context.unpersist()

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
            low_support_tail=low_support_tail,
            low_span_tail=low_span_tail,
            strict_tail_overlap=strict_tail_overlap,
            one_sided_tail_examples=one_sided_tail_examples,
            count_only_internal_gaps=count_only_internal_gaps,
            gap_tail_comparison=gap_tail_comparison,
            gap_tail_disagreement_examples=gap_tail_disagreement_examples,
            gap_tail_disagreement_characterization=gap_tail_disagreement_characterization,
            gap_magnitude_sensitivity=gap_magnitude_sensitivity,
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
