"""Build past-only temporal features at cycle prediction timestamps."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampNTZType
from pyspark.sql.window import Window

MOTOR_CURRENT_FEATURE_VERSION = "motor-current-15m-v1"
MOTOR_CURRENT_WINDOW_SECONDS = 15 * 60
MOTOR_CURRENT_FEATURE_COLUMNS = (
    "motor_current_15m_feature_version",
    "motor_current_15m_window_seconds",
    "motor_current_15m_window_start",
    "motor_current_15m_status",
    "motor_current_15m_observation_count",
    "motor_current_15m_first_observation_timestamp",
    "motor_current_15m_last_observation_timestamp",
    "motor_current_15m_minimum_amperes",
    "motor_current_15m_mean_amperes",
    "motor_current_15m_maximum_amperes",
)

STATUS_AVAILABLE = "available"
STATUS_MISSING_PREDICTION = "missing_prediction_boundary"
STATUS_MISSING_PREDICTION_OBSERVATION = "missing_prediction_observation"

_CYCLE_COLUMNS = ("loaded_cycle_id", "prediction_timestamp")
_TELEMETRY_COLUMNS = ("event_timestamp", "motor_current")
_INTERNAL_COLUMNS = (
    "_motor_current_event_second",
    "_motor_current_prediction_timestamp",
)


class TemporalFeatureError(ValueError):
    """Raised when a past-only temporal feature cannot be derived safely."""


def _validate_columns(cycles: DataFrame, telemetry: DataFrame) -> None:
    missing_cycle_columns = sorted(set(_CYCLE_COLUMNS) - set(cycles.columns))
    if missing_cycle_columns:
        raise TemporalFeatureError(
            "Cycle horizons are missing temporal-feature columns: "
            + ", ".join(missing_cycle_columns)
        )
    missing_telemetry_columns = sorted(set(_TELEMETRY_COLUMNS) - set(telemetry.columns))
    if missing_telemetry_columns:
        raise TemporalFeatureError(
            "Accepted telemetry is missing temporal-feature columns: "
            + ", ".join(missing_telemetry_columns)
        )

    reserved_columns = set(MOTOR_CURRENT_FEATURE_COLUMNS).union(_INTERNAL_COLUMNS)
    conflicting_columns = sorted(reserved_columns.intersection(cycles.columns))
    if conflicting_columns:
        raise TemporalFeatureError(
            "Cycle horizons already contain motor-current feature columns: "
            + ", ".join(conflicting_columns)
        )


def add_motor_current_15m_features(cycles: DataFrame, telemetry: DataFrame) -> DataFrame:
    """Append motor-current summaries from ``(prediction - 15m, prediction]``.

    Accepted telemetry timestamps have whole-second precision. Ordering those timestamps on a
    timezone-free seconds axis makes ``rangeBetween(-899, 0)`` exactly represent the strict
    15-minute lower bound and inclusive prediction-time upper bound.
    """

    _validate_columns(cycles, telemetry)

    epoch = F.lit("1970-01-01 00:00:00").cast(TimestampNTZType())
    timeline = telemetry.select(
        "event_timestamp",
        "motor_current",
        F.timestamp_diff("SECOND", epoch, F.col("event_timestamp"))
        .cast(LongType())
        .alias("_motor_current_event_second"),
    )
    trailing_window = Window.orderBy(F.col("_motor_current_event_second")).rangeBetween(
        -(MOTOR_CURRENT_WINDOW_SECONDS - 1),
        0,
    )
    has_motor_current = F.col("motor_current").isNotNull()
    rolling_features = timeline.select(
        F.col("event_timestamp").alias("_motor_current_prediction_timestamp"),
        F.count("motor_current")
        .over(trailing_window)
        .cast(LongType())
        .alias("motor_current_15m_observation_count"),
        F.min(F.when(has_motor_current, F.col("event_timestamp")))
        .over(trailing_window)
        .alias("motor_current_15m_first_observation_timestamp"),
        F.max(F.when(has_motor_current, F.col("event_timestamp")))
        .over(trailing_window)
        .alias("motor_current_15m_last_observation_timestamp"),
        F.min("motor_current").over(trailing_window).alias("motor_current_15m_minimum_amperes"),
        F.avg("motor_current").over(trailing_window).alias("motor_current_15m_mean_amperes"),
        F.max("motor_current").over(trailing_window).alias("motor_current_15m_maximum_amperes"),
    )

    joined = cycles.alias("cycle").join(
        rolling_features.alias("feature"),
        F.col("cycle.prediction_timestamp") == F.col("feature._motor_current_prediction_timestamp"),
        how="left",
    )
    prediction_timestamp = F.col("cycle.prediction_timestamp")
    feature_anchor = F.col("feature._motor_current_prediction_timestamp")
    feature_status = (
        F.when(prediction_timestamp.isNull(), F.lit(STATUS_MISSING_PREDICTION))
        .when(feature_anchor.isNull(), F.lit(STATUS_MISSING_PREDICTION_OBSERVATION))
        .otherwise(F.lit(STATUS_AVAILABLE))
    )
    return joined.select(
        *(F.col(f"cycle.{column_name}").alias(column_name) for column_name in cycles.columns),
        F.lit(MOTOR_CURRENT_FEATURE_VERSION).alias("motor_current_15m_feature_version"),
        F.lit(MOTOR_CURRENT_WINDOW_SECONDS)
        .cast(LongType())
        .alias("motor_current_15m_window_seconds"),
        F.timestamp_add(
            "SECOND",
            F.lit(-MOTOR_CURRENT_WINDOW_SECONDS),
            prediction_timestamp,
        ).alias("motor_current_15m_window_start"),
        feature_status.alias("motor_current_15m_status"),
        F.coalesce(
            F.col("feature.motor_current_15m_observation_count"),
            F.lit(0),
        )
        .cast(LongType())
        .alias("motor_current_15m_observation_count"),
        F.col("feature.motor_current_15m_first_observation_timestamp"),
        F.col("feature.motor_current_15m_last_observation_timestamp"),
        F.col("feature.motor_current_15m_minimum_amperes"),
        F.col("feature.motor_current_15m_mean_amperes"),
        F.col("feature.motor_current_15m_maximum_amperes"),
    )
