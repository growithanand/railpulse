"""Assign point-in-time cycle-to-failure horizon labels."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampNTZType

FAILURE_HORIZON_VERSION = "cycle-failure-horizon-v1"
DEFAULT_FAILURE_HORIZON_SECONDS = 2 * 60 * 60
FAILURE_HORIZON_COLUMNS = (
    "failure_horizon_version",
    "failure_horizon_seconds",
    "label_observation_end",
    "prediction_timestamp",
    "failure_horizon_end",
    "failure_horizon_status",
    "failure_within_horizon",
    "matched_failure_record_id",
    "matched_failure_source_row",
    "matched_failure_start",
    "matched_failure_end",
    "seconds_to_failure",
)

STATUS_POSITIVE = "positive"
STATUS_NEGATIVE = "negative"
STATUS_HORIZON_CENSORED = "horizon_censored"
STATUS_INSIDE_FAILURE = "inside_failure_interval"
STATUS_MISSING_PREDICTION = "missing_prediction_boundary"

_CYCLE_COLUMNS = (
    "loaded_cycle_id",
    "loaded_cycle_stop_timestamp",
    "is_right_censored",
)
_FAILURE_COLUMNS = (
    "record_id",
    "source_row",
    "failure_start",
    "failure_end",
)
_INTERNAL_COLUMNS = (
    "_horizon_cycle_id",
    "_inside_failure_interval",
    "_next_failure",
)


class FailureHorizonError(ValueError):
    """Raised when cycle-to-failure horizons cannot be assigned safely."""


def _validate_parameters(*, horizon_seconds: int, observation_end: datetime) -> None:
    if isinstance(horizon_seconds, bool) or not isinstance(horizon_seconds, int):
        raise FailureHorizonError("Failure horizon seconds must be an integer")
    if horizon_seconds <= 0:
        raise FailureHorizonError("Failure horizon seconds must be positive")
    if not isinstance(observation_end, datetime):
        raise FailureHorizonError("Label observation end must be a datetime")
    if observation_end.tzinfo is not None and observation_end.utcoffset() is not None:
        raise FailureHorizonError("Label observation end must be timezone-free")


def _validate_columns(cycles: DataFrame, failures: DataFrame) -> None:
    missing_cycle_columns = sorted(set(_CYCLE_COLUMNS) - set(cycles.columns))
    if missing_cycle_columns:
        raise FailureHorizonError(
            "Gold cycles are missing failure-horizon columns: " + ", ".join(missing_cycle_columns)
        )
    missing_failure_columns = sorted(set(_FAILURE_COLUMNS) - set(failures.columns))
    if missing_failure_columns:
        raise FailureHorizonError(
            "Accepted failure events are missing failure-horizon columns: "
            + ", ".join(missing_failure_columns)
        )

    reserved_columns = set(FAILURE_HORIZON_COLUMNS).union(_INTERNAL_COLUMNS)
    conflicting_columns = sorted(reserved_columns.intersection(cycles.columns))
    if conflicting_columns:
        raise FailureHorizonError(
            "Gold cycles already contain failure-horizon columns: " + ", ".join(conflicting_columns)
        )


def assign_cycle_failure_horizons(
    cycles: DataFrame,
    failures: DataFrame,
    *,
    horizon_seconds: int,
    observation_end: datetime,
) -> DataFrame:
    """Append causal prediction boundaries and future failure-onset labels.

    A cycle is scored at its observed exclusive stop. A future failure is positive when its start
    lies in ``(prediction_timestamp, failure_horizon_end]`` and is no later than the supplied label
    observation end. Cycles inside a published failure interval and cycles without an observed stop
    are ineligible. A no-failure result is negative only when the complete horizon was observable.
    """

    _validate_parameters(horizon_seconds=horizon_seconds, observation_end=observation_end)
    _validate_columns(cycles, failures)

    observation_end_column = F.lit(
        observation_end.isoformat(sep=" ", timespec="microseconds")
    ).cast(TimestampNTZType())
    has_prediction_boundary = (
        ~F.coalesce(F.col("is_right_censored"), F.lit(True))
        & F.col("loaded_cycle_stop_timestamp").isNotNull()
    )
    prepared = (
        cycles.withColumn("_horizon_cycle_id", F.col("loaded_cycle_id"))
        .withColumn(
            "failure_horizon_version",
            F.lit(FAILURE_HORIZON_VERSION),
        )
        .withColumn(
            "failure_horizon_seconds",
            F.lit(horizon_seconds).cast(LongType()),
        )
        .withColumn("label_observation_end", observation_end_column)
        .withColumn(
            "prediction_timestamp",
            F.when(has_prediction_boundary, F.col("loaded_cycle_stop_timestamp")),
        )
        .withColumn(
            "failure_horizon_end",
            F.timestamp_add(
                "SECOND",
                F.col("failure_horizon_seconds"),
                F.col("prediction_timestamp"),
            ),
        )
    )

    failure_candidates = failures.select(
        F.col("record_id").alias("_failure_record_id"),
        F.col("source_row").alias("_failure_source_row"),
        F.col("failure_start").alias("_failure_start"),
        F.col("failure_end").alias("_failure_end"),
    )
    prediction_points = prepared.select(
        "_horizon_cycle_id",
        "prediction_timestamp",
        "failure_horizon_end",
    )

    inside_failure = (
        prediction_points.alias("cycle")
        .join(
            failure_candidates.alias("failure"),
            F.col("cycle.prediction_timestamp").isNotNull()
            & (F.col("failure._failure_start") <= F.col("cycle.prediction_timestamp"))
            & (F.col("failure._failure_end") >= F.col("cycle.prediction_timestamp")),
            how="inner",
        )
        .select("_horizon_cycle_id")
        .distinct()
        .withColumn("_inside_failure_interval", F.lit(True))
    )
    eligible = prepared.join(inside_failure, on="_horizon_cycle_id", how="left")

    future_failures = (
        eligible.where(
            F.col("prediction_timestamp").isNotNull()
            & ~F.coalesce(F.col("_inside_failure_interval"), F.lit(False))
        )
        .select("_horizon_cycle_id", "prediction_timestamp", "failure_horizon_end")
        .alias("cycle")
        .join(
            failure_candidates.alias("failure"),
            (F.col("failure._failure_start") > F.col("cycle.prediction_timestamp"))
            & (F.col("failure._failure_start") <= F.col("cycle.failure_horizon_end"))
            & (F.col("failure._failure_start") <= observation_end_column),
            how="inner",
        )
    )
    earliest_failure = future_failures.groupBy("_horizon_cycle_id").agg(
        F.min(
            F.struct(
                F.col("_failure_start").alias("failure_start"),
                F.col("_failure_source_row").alias("source_row"),
                F.col("_failure_record_id").alias("record_id"),
                F.col("_failure_end").alias("failure_end"),
            )
        ).alias("_next_failure")
    )

    labeled = (
        eligible.join(earliest_failure, on="_horizon_cycle_id", how="left")
        .withColumn(
            "matched_failure_record_id",
            F.col("_next_failure.record_id"),
        )
        .withColumn(
            "matched_failure_source_row",
            F.col("_next_failure.source_row").cast(LongType()),
        )
        .withColumn(
            "matched_failure_start",
            F.col("_next_failure.failure_start"),
        )
        .withColumn(
            "matched_failure_end",
            F.col("_next_failure.failure_end"),
        )
    )
    has_prediction = F.col("prediction_timestamp").isNotNull()
    inside_failure_interval = F.coalesce(F.col("_inside_failure_interval"), F.lit(False))
    has_matched_failure = F.col("matched_failure_record_id").isNotNull()
    complete_horizon_observed = F.col("failure_horizon_end") <= F.col("label_observation_end")
    labeled = labeled.withColumn(
        "failure_horizon_status",
        F.when(~has_prediction, F.lit(STATUS_MISSING_PREDICTION))
        .when(inside_failure_interval, F.lit(STATUS_INSIDE_FAILURE))
        .when(has_matched_failure, F.lit(STATUS_POSITIVE))
        .when(complete_horizon_observed, F.lit(STATUS_NEGATIVE))
        .otherwise(F.lit(STATUS_HORIZON_CENSORED)),
    ).withColumn(
        "failure_within_horizon",
        F.when(F.col("failure_horizon_status") == STATUS_POSITIVE, F.lit(True)).when(
            F.col("failure_horizon_status") == STATUS_NEGATIVE,
            F.lit(False),
        ),
    )
    labeled = labeled.withColumn(
        "seconds_to_failure",
        F.when(
            F.col("failure_horizon_status") == STATUS_POSITIVE,
            F.timestamp_diff(
                "SECOND",
                F.col("prediction_timestamp"),
                F.col("matched_failure_start"),
            ),
        ).cast(LongType()),
    )
    return labeled.select(*cycles.columns, *FAILURE_HORIZON_COLUMNS)
