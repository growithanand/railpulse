"""Assign label-independent eligibility to motor-current feature windows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.features.temporal_features import (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)

MOTOR_CURRENT_ELIGIBILITY_VERSION = "motor-current-15m-eligibility-v1"
MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS = 20
MOTOR_CURRENT_ELIGIBILITY_COLUMNS = (
    "motor_current_eligibility_version",
    "motor_current_eligibility_minimum_excluded_gap_seconds",
    "motor_current_maximum_window_gap_seconds",
    "motor_current_eligibility_status",
    "motor_current_eligibility_reasons",
)

STATUS_ELIGIBLE = "eligible"
STATUS_INELIGIBLE = "ineligible"

REASON_MISSING_PREDICTION = "feature_missing_prediction_boundary"
REASON_MISSING_PREDICTION_OBSERVATION = "feature_missing_prediction_observation"
REASON_MISSING_OR_INVALID_CONTEXT = "missing_or_invalid_coverage_context"
REASON_MATERIAL_WINDOW_GAP = "material_window_gap"
REASON_UNSUPPORTED_FEATURE_STATUS = "unsupported_feature_status"

_FEATURE_COLUMNS = ("loaded_cycle_id", "motor_current_15m_status")
_COVERAGE_COLUMNS = (
    "loaded_cycle_id",
    "leading_unobserved_seconds",
    "first_observation_follows_forward_gap",
    "maximum_internal_forward_gap_seconds",
)
_INTERNAL_COLUMNS = (
    "_eligibility_context_cycle_id",
    "_eligibility_leading_unobserved_seconds",
    "_eligibility_first_observation_follows_forward_gap",
    "_eligibility_maximum_internal_forward_gap_seconds",
)


class FeatureEligibilityError(ValueError):
    """Raised when motor-current eligibility cannot be assigned safely."""


def _validate_columns(features: DataFrame, coverage_context: DataFrame) -> None:
    missing_feature_columns = sorted(set(_FEATURE_COLUMNS) - set(features.columns))
    if missing_feature_columns:
        raise FeatureEligibilityError(
            "Motor-current features are missing eligibility columns: "
            + ", ".join(missing_feature_columns)
        )
    missing_coverage_columns = sorted(set(_COVERAGE_COLUMNS) - set(coverage_context.columns))
    if missing_coverage_columns:
        raise FeatureEligibilityError(
            "Coverage context is missing eligibility columns: "
            + ", ".join(missing_coverage_columns)
        )

    reserved_columns = set(MOTOR_CURRENT_ELIGIBILITY_COLUMNS).union(_INTERNAL_COLUMNS)
    conflicting_columns = sorted(reserved_columns.intersection(features.columns))
    if conflicting_columns:
        raise FeatureEligibilityError(
            "Motor-current features already contain eligibility columns: "
            + ", ".join(conflicting_columns)
        )


def add_motor_current_eligibility(
    features: DataFrame,
    coverage_context: DataFrame,
) -> DataFrame:
    """Append eligibility using feature availability and past-only gap context.

    Available windows are eligible only when their maximum in-window gap is strictly below the
    telemetry material-gap boundary. Missing features and missing or invalid coverage context fail
    closed with explicit reasons. Failure-horizon labels are neither required nor referenced.
    """

    _validate_columns(features, coverage_context)
    feature = features.alias("feature")
    context = coverage_context.select(
        F.col("loaded_cycle_id").alias("_eligibility_context_cycle_id"),
        F.col("leading_unobserved_seconds").alias("_eligibility_leading_unobserved_seconds"),
        F.col("first_observation_follows_forward_gap").alias(
            "_eligibility_first_observation_follows_forward_gap"
        ),
        F.col("maximum_internal_forward_gap_seconds").alias(
            "_eligibility_maximum_internal_forward_gap_seconds"
        ),
    ).alias("context")
    joined = feature.join(
        context,
        F.col("feature.loaded_cycle_id") == F.col("context._eligibility_context_cycle_id"),
        how="left",
    )

    feature_status = F.col("feature.motor_current_15m_status")
    is_available = feature_status == STATUS_AVAILABLE
    internal_gap_seconds = F.col("_eligibility_maximum_internal_forward_gap_seconds")
    has_valid_context = (
        F.col("_eligibility_context_cycle_id").isNotNull()
        & F.col("_eligibility_leading_unobserved_seconds").isNotNull()
        & (F.col("_eligibility_leading_unobserved_seconds") >= 0)
        & F.col("_eligibility_first_observation_follows_forward_gap").isNotNull()
        & (internal_gap_seconds.isNull() | (internal_gap_seconds >= 0))
    )
    leading_gap_seconds = F.when(
        F.col("_eligibility_first_observation_follows_forward_gap"),
        F.col("_eligibility_leading_unobserved_seconds"),
    ).otherwise(F.lit(0))
    maximum_window_gap_seconds = F.when(
        is_available & has_valid_context,
        F.greatest(
            leading_gap_seconds,
            F.coalesce(internal_gap_seconds, F.lit(0)),
        ).cast(LongType()),
    )
    has_material_gap = maximum_window_gap_seconds >= F.lit(
        MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS
    )
    is_eligible = is_available & has_valid_context & ~has_material_gap
    known_status = feature_status.isin(
        STATUS_AVAILABLE,
        STATUS_MISSING_PREDICTION,
        STATUS_MISSING_PREDICTION_OBSERVATION,
    )

    reasons = F.array_compact(
        F.array(
            F.when(feature_status == STATUS_MISSING_PREDICTION, F.lit(REASON_MISSING_PREDICTION)),
            F.when(
                feature_status == STATUS_MISSING_PREDICTION_OBSERVATION,
                F.lit(REASON_MISSING_PREDICTION_OBSERVATION),
            ),
            F.when(
                is_available & ~has_valid_context,
                F.lit(REASON_MISSING_OR_INVALID_CONTEXT),
            ),
            F.when(
                is_available & has_valid_context & has_material_gap,
                F.lit(REASON_MATERIAL_WINDOW_GAP),
            ),
            F.when(
                ~known_status | feature_status.isNull(), F.lit(REASON_UNSUPPORTED_FEATURE_STATUS)
            ),
        )
    )
    return joined.select(
        *(F.col(f"feature.{column_name}").alias(column_name) for column_name in features.columns),
        F.lit(MOTOR_CURRENT_ELIGIBILITY_VERSION).alias("motor_current_eligibility_version"),
        F.lit(MOTOR_CURRENT_MINIMUM_EXCLUDED_GAP_SECONDS)
        .cast(LongType())
        .alias("motor_current_eligibility_minimum_excluded_gap_seconds"),
        maximum_window_gap_seconds.alias("motor_current_maximum_window_gap_seconds"),
        F.when(is_eligible, F.lit(STATUS_ELIGIBLE))
        .otherwise(F.lit(STATUS_INELIGIBLE))
        .alias("motor_current_eligibility_status"),
        reasons.alias("motor_current_eligibility_reasons"),
    )
