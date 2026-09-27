"""Build past-only loaded-cycle operating context at prediction boundaries."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

CYCLE_OPERATING_CONTEXT_VERSION = "loaded-cycle-operating-context-v1"
STATUS_AVAILABLE = "available"
STATUS_MISSING_PREDICTION = "missing_prediction_boundary"
STATUS_LEFT_CENSORED_CURRENT = "left_censored_current_cycle"
STATUS_INCOMPLETE_CURRENT = "incomplete_current_cycle"
STATUS_MISSING_PREVIOUS = "missing_previous_cycle"
STATUS_INCOMPLETE_PREVIOUS = "incomplete_previous_cycle"
STATUS_INVALID_PREVIOUS_INTERVAL = "invalid_previous_interval"

CYCLE_OPERATING_CONTEXT_COLUMNS = (
    "cycle_context_feature_version",
    "cycle_context_status",
    "cycle_context_current_duration_seconds",
    "cycle_context_previous_cycle_id",
    "cycle_context_previous_duration_seconds",
    "cycle_context_previous_idle_seconds",
)

_REQUIRED_COLUMNS = (
    "loaded_cycle_id",
    "loaded_cycle_start_type",
    "loaded_cycle_start_timestamp",
    "loaded_cycle_stop_timestamp",
    "observed_duration_seconds",
    "is_right_censored",
)
_INTERNAL_COLUMNS = (
    "_cycle_context_previous_cycle_id",
    "_cycle_context_previous_start_type",
    "_cycle_context_previous_stop_timestamp",
    "_cycle_context_previous_duration_seconds",
)


class CycleOperatingContextError(ValueError):
    """Raised when loaded cycles cannot satisfy the operating-context contract."""


def _validate_columns(cycles: DataFrame) -> None:
    missing_columns = sorted(set(_REQUIRED_COLUMNS) - set(cycles.columns))
    if missing_columns:
        raise CycleOperatingContextError(
            "Loaded cycles are missing operating-context columns: " + ", ".join(missing_columns)
        )
    reserved_columns = set(CYCLE_OPERATING_CONTEXT_COLUMNS).union(_INTERNAL_COLUMNS)
    conflicting_columns = sorted(reserved_columns.intersection(cycles.columns))
    if conflicting_columns:
        raise CycleOperatingContextError(
            "Loaded cycles already contain operating-context columns: "
            + ", ".join(conflicting_columns)
        )


def add_cycle_operating_context(cycles: DataFrame) -> DataFrame:
    """Append current-cycle and predecessor context known at each cycle stop."""

    _validate_columns(cycles)

    predecessor_window = Window.orderBy(
        F.col("loaded_cycle_start_timestamp"),
        F.col("loaded_cycle_id"),
    )
    ordered = cycles.select(
        *cycles.columns,
        F.lag("loaded_cycle_id").over(predecessor_window).alias("_cycle_context_previous_cycle_id"),
        F.lag("loaded_cycle_start_type")
        .over(predecessor_window)
        .alias("_cycle_context_previous_start_type"),
        F.lag("loaded_cycle_stop_timestamp")
        .over(predecessor_window)
        .alias("_cycle_context_previous_stop_timestamp"),
        F.lag("observed_duration_seconds")
        .over(predecessor_window)
        .alias("_cycle_context_previous_duration_seconds"),
    )

    current_has_prediction = (
        ~F.col("is_right_censored") & F.col("loaded_cycle_stop_timestamp").isNotNull()
    )
    current_is_complete = (
        current_has_prediction
        & (F.col("loaded_cycle_start_type") == F.lit("observed"))
        & F.col("observed_duration_seconds").isNotNull()
    )
    previous_exists = F.col("_cycle_context_previous_cycle_id").isNotNull()
    previous_is_complete = (
        previous_exists
        & (F.col("_cycle_context_previous_start_type") == F.lit("observed"))
        & F.col("_cycle_context_previous_stop_timestamp").isNotNull()
        & F.col("_cycle_context_previous_duration_seconds").isNotNull()
    )
    previous_idle_seconds = F.timestamp_diff(
        "SECOND",
        F.col("_cycle_context_previous_stop_timestamp"),
        F.col("loaded_cycle_start_timestamp"),
    ).cast(LongType())
    invalid_previous_interval = previous_is_complete & (previous_idle_seconds < F.lit(0))

    status = (
        F.when(~current_has_prediction, F.lit(STATUS_MISSING_PREDICTION))
        .when(
            F.col("loaded_cycle_start_type") == F.lit("left_censored"),
            F.lit(STATUS_LEFT_CENSORED_CURRENT),
        )
        .when(~current_is_complete, F.lit(STATUS_INCOMPLETE_CURRENT))
        .when(~previous_exists, F.lit(STATUS_MISSING_PREVIOUS))
        .when(~previous_is_complete, F.lit(STATUS_INCOMPLETE_PREVIOUS))
        .when(invalid_previous_interval, F.lit(STATUS_INVALID_PREVIOUS_INTERVAL))
        .otherwise(F.lit(STATUS_AVAILABLE))
    )

    return ordered.select(
        *(F.col(column) for column in cycles.columns),
        F.lit(CYCLE_OPERATING_CONTEXT_VERSION).alias("cycle_context_feature_version"),
        status.alias("cycle_context_status"),
        F.when(current_is_complete, F.col("observed_duration_seconds"))
        .cast(LongType())
        .alias("cycle_context_current_duration_seconds"),
        F.col("_cycle_context_previous_cycle_id").alias("cycle_context_previous_cycle_id"),
        F.when(
            previous_is_complete,
            F.col("_cycle_context_previous_duration_seconds"),
        )
        .cast(LongType())
        .alias("cycle_context_previous_duration_seconds"),
        F.when(
            current_is_complete
            & F.col("_cycle_context_previous_stop_timestamp").isNotNull()
            & (previous_idle_seconds >= F.lit(0)),
            previous_idle_seconds,
        )
        .cast(LongType())
        .alias("cycle_context_previous_idle_seconds"),
    )
