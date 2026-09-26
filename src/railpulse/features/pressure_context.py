"""Build past-only panel-to-reservoir pressure context at prediction timestamps."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampNTZType
from pyspark.sql.window import Window

from railpulse.features.temporal_features import (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
)

PRESSURE_CONTEXT_FEATURE_VERSION = "panel-reservoir-pressure-difference-15m-v1"
PRESSURE_CONTEXT_WINDOW_SECONDS = 15 * 60
PRESSURE_CONTEXT_FEATURE_COLUMNS = (
    "pressure_context_15m_feature_version",
    "pressure_context_15m_window_seconds",
    "pressure_context_15m_window_start",
    "pressure_context_15m_status",
    "pressure_context_15m_observation_count",
    "pressure_context_15m_first_observation_timestamp",
    "pressure_context_15m_last_observation_timestamp",
    "pressure_context_15m_mean_difference_bar",
    "pressure_context_15m_mean_absolute_difference_bar",
    "pressure_context_15m_maximum_absolute_difference_bar",
)

_CYCLE_COLUMNS = ("loaded_cycle_id", "prediction_timestamp")
_TELEMETRY_COLUMNS = ("event_timestamp", "tp3", "reservoirs")
_INTERNAL_COLUMNS = (
    "_pressure_context_event_second",
    "_pressure_context_prediction_timestamp",
)


class PressureContextError(ValueError):
    """Raised when past-only pressure context cannot be derived safely."""


def _validate_columns(cycles: DataFrame, telemetry: DataFrame) -> None:
    missing_cycle_columns = sorted(set(_CYCLE_COLUMNS) - set(cycles.columns))
    if missing_cycle_columns:
        raise PressureContextError(
            "Cycle horizons are missing pressure-context columns: "
            + ", ".join(missing_cycle_columns)
        )
    missing_telemetry_columns = sorted(set(_TELEMETRY_COLUMNS) - set(telemetry.columns))
    if missing_telemetry_columns:
        raise PressureContextError(
            "Accepted telemetry is missing pressure-context columns: "
            + ", ".join(missing_telemetry_columns)
        )
    reserved_columns = set(PRESSURE_CONTEXT_FEATURE_COLUMNS).union(_INTERNAL_COLUMNS)
    conflicting_columns = sorted(reserved_columns.intersection(cycles.columns))
    if conflicting_columns:
        raise PressureContextError(
            "Cycle horizons already contain pressure-context columns: "
            + ", ".join(conflicting_columns)
        )


def add_pressure_context_15m_features(cycles: DataFrame, telemetry: DataFrame) -> DataFrame:
    """Append TP3-to-reservoir pressure summaries from ``(prediction - 15m, prediction]``."""

    _validate_columns(cycles, telemetry)

    epoch = F.lit("1970-01-01 00:00:00").cast(TimestampNTZType())
    difference = F.col("tp3") - F.col("reservoirs")
    timeline = telemetry.select(
        "event_timestamp",
        difference.alias("_pressure_difference_bar"),
        F.timestamp_diff("SECOND", epoch, F.col("event_timestamp"))
        .cast(LongType())
        .alias("_pressure_context_event_second"),
    )
    trailing_window = Window.orderBy(F.col("_pressure_context_event_second")).rangeBetween(
        -(PRESSURE_CONTEXT_WINDOW_SECONDS - 1),
        0,
    )
    has_pressure_pair = F.col("_pressure_difference_bar").isNotNull()
    rolling_features = timeline.select(
        F.col("event_timestamp").alias("_pressure_context_prediction_timestamp"),
        F.count("_pressure_difference_bar")
        .over(trailing_window)
        .cast(LongType())
        .alias("pressure_context_15m_observation_count"),
        F.min(F.when(has_pressure_pair, F.col("event_timestamp")))
        .over(trailing_window)
        .alias("pressure_context_15m_first_observation_timestamp"),
        F.max(F.when(has_pressure_pair, F.col("event_timestamp")))
        .over(trailing_window)
        .alias("pressure_context_15m_last_observation_timestamp"),
        F.avg("_pressure_difference_bar")
        .over(trailing_window)
        .alias("pressure_context_15m_mean_difference_bar"),
        F.avg(F.abs("_pressure_difference_bar"))
        .over(trailing_window)
        .alias("pressure_context_15m_mean_absolute_difference_bar"),
        F.max(F.abs("_pressure_difference_bar"))
        .over(trailing_window)
        .alias("pressure_context_15m_maximum_absolute_difference_bar"),
    )

    joined = cycles.alias("cycle").join(
        rolling_features.alias("feature"),
        F.col("cycle.prediction_timestamp")
        == F.col("feature._pressure_context_prediction_timestamp"),
        how="left",
    )
    prediction_timestamp = F.col("cycle.prediction_timestamp")
    feature_anchor = F.col("feature._pressure_context_prediction_timestamp")
    feature_status = (
        F.when(prediction_timestamp.isNull(), F.lit(STATUS_MISSING_PREDICTION))
        .when(feature_anchor.isNull(), F.lit(STATUS_MISSING_PREDICTION_OBSERVATION))
        .otherwise(F.lit(STATUS_AVAILABLE))
    )
    return joined.select(
        *(F.col(f"cycle.{column}").alias(column) for column in cycles.columns),
        F.lit(PRESSURE_CONTEXT_FEATURE_VERSION).alias("pressure_context_15m_feature_version"),
        F.lit(PRESSURE_CONTEXT_WINDOW_SECONDS)
        .cast(LongType())
        .alias("pressure_context_15m_window_seconds"),
        F.timestamp_add(
            "SECOND",
            F.lit(-PRESSURE_CONTEXT_WINDOW_SECONDS),
            prediction_timestamp,
        ).alias("pressure_context_15m_window_start"),
        feature_status.alias("pressure_context_15m_status"),
        F.coalesce(F.col("feature.pressure_context_15m_observation_count"), F.lit(0))
        .cast(LongType())
        .alias("pressure_context_15m_observation_count"),
        F.col("feature.pressure_context_15m_first_observation_timestamp"),
        F.col("feature.pressure_context_15m_last_observation_timestamp"),
        F.col("feature.pressure_context_15m_mean_difference_bar"),
        F.col("feature.pressure_context_15m_mean_absolute_difference_bar"),
        F.col("feature.pressure_context_15m_maximum_absolute_difference_bar"),
    )
