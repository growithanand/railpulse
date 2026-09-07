"""Derive loaded-cycle boundaries, segment identity, and cycle-level summaries."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

LOADED_SIGNAL_COLUMN = "dv_eletric"
LOADED_CYCLE_ID_VERSION = "loaded-cycle-v1"
CYCLE_BOUNDARY_COLUMNS = (
    "previous_dv_eletric",
    "is_loaded_operation",
    "is_cycle_start",
    "is_cycle_stop",
    "is_left_censored_cycle",
)
CYCLE_SEGMENT_COLUMNS = (
    "loaded_cycle_id",
    "loaded_cycle_start_record_id",
    "loaded_cycle_start_type",
)
CYCLE_AGGREGATION_COLUMNS = (
    "loaded_cycle_start_timestamp",
    "loaded_cycle_stop_record_id",
    "loaded_cycle_stop_timestamp",
    "loaded_observation_count",
    "is_right_censored",
    "observed_duration_seconds",
)


class CycleBoundaryError(ValueError):
    """Raised when telemetry cannot satisfy the loaded-cycle input contract."""


class CycleAggregationError(ValueError):
    """Raised when segmented telemetry cannot be aggregated safely."""


def annotate_loaded_cycle_boundaries(frame: DataFrame) -> DataFrame:
    """Append loaded-operation transition evidence using only current and prior state.

    The input is expected to contain accepted Silver telemetry. A material forward gap breaks
    continuity: an active observation after that gap is left-censored rather than an observed start.
    """

    required_columns = {
        "record_id",
        "source_index",
        "previous_source_index",
        LOADED_SIGNAL_COLUMN,
        "is_forward_gap",
        "rejection_reasons",
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise CycleBoundaryError(
            "Accepted Silver telemetry is missing cycle-boundary columns: "
            + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(set(CYCLE_BOUNDARY_COLUMNS).intersection(frame.columns))
    if conflicting_columns:
        raise CycleBoundaryError(
            "Telemetry already contains cycle-boundary columns: " + ", ".join(conflicting_columns)
        )

    current = frame.alias("current")
    previous = frame.select(
        F.col("source_index").alias("_cycle_previous_source_index"),
        F.col(LOADED_SIGNAL_COLUMN).alias("previous_dv_eletric"),
    ).alias("previous")
    annotated = current.join(
        previous,
        F.col("current.previous_source_index") == F.col("previous._cycle_previous_source_index"),
        how="left",
    ).select(
        *(F.col(f"current.{column_name}").alias(column_name) for column_name in frame.columns),
        F.col("previous.previous_dv_eletric"),
    )

    is_loaded = F.coalesce(F.col(LOADED_SIGNAL_COLUMN) == F.lit(1), F.lit(False))
    has_continuous_predecessor = F.col("previous_dv_eletric").isNotNull() & ~F.coalesce(
        F.col("is_forward_gap"), F.lit(False)
    )
    return (
        annotated.withColumn("is_loaded_operation", is_loaded)
        .withColumn(
            "is_cycle_start",
            is_loaded & has_continuous_predecessor & (F.col("previous_dv_eletric") == F.lit(0)),
        )
        .withColumn(
            "is_cycle_stop",
            ~is_loaded & has_continuous_predecessor & (F.col("previous_dv_eletric") == F.lit(1)),
        )
        .withColumn(
            "is_left_censored_cycle",
            is_loaded & ~has_continuous_predecessor,
        )
    )


def assign_loaded_cycle_segments(frame: DataFrame) -> DataFrame:
    """Assign stable identifiers to loaded rows and their exclusive stop boundaries.

    Segment identity is anchored to the first visible loaded record. Ordered propagation only uses
    the current row and preceding source rows, so later observations cannot change an earlier ID.
    """

    required_columns = {
        "record_id",
        "source_index",
        "is_loaded_operation",
        "is_cycle_start",
        "is_cycle_stop",
        "is_left_censored_cycle",
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise CycleBoundaryError(
            "Cycle-boundary telemetry is missing segment columns: " + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(set(CYCLE_SEGMENT_COLUMNS).intersection(frame.columns))
    if conflicting_columns:
        raise CycleBoundaryError(
            "Telemetry already contains cycle-segment columns: " + ", ".join(conflicting_columns)
        )

    causal_window = Window.orderBy(F.col("source_index")).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    is_segment_start = F.col("is_cycle_start") | F.col("is_left_censored_cycle")
    start_record_id = F.last(
        F.when(is_segment_start, F.col("record_id")),
        ignorenulls=True,
    ).over(causal_window)
    start_type = F.last(
        F.when(F.col("is_cycle_start"), F.lit("observed")).when(
            F.col("is_left_censored_cycle"),
            F.lit("left_censored"),
        ),
        ignorenulls=True,
    ).over(causal_window)
    belongs_to_segment = F.col("is_loaded_operation") | F.col("is_cycle_stop")
    has_segment_anchor = belongs_to_segment & start_record_id.isNotNull()

    return (
        frame.withColumn(
            "loaded_cycle_id",
            F.when(
                has_segment_anchor,
                F.sha2(
                    F.concat_ws("|", F.lit(LOADED_CYCLE_ID_VERSION), start_record_id),
                    256,
                ),
            ),
        )
        .withColumn(
            "loaded_cycle_start_record_id",
            F.when(has_segment_anchor, start_record_id),
        )
        .withColumn(
            "loaded_cycle_start_type",
            F.when(has_segment_anchor, start_type),
        )
    )


def aggregate_loaded_cycles(frame: DataFrame) -> DataFrame:
    """Return one row per visible loaded segment without inventing missing boundaries.

    The observed stop timestamp is exclusive. Duration remains null when no stop is present and is
    only a lower bound when the segment start is left-censored.
    """

    required_columns = {
        "record_id",
        "event_timestamp",
        "is_loaded_operation",
        "is_cycle_stop",
        *CYCLE_SEGMENT_COLUMNS,
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise CycleAggregationError(
            "Segmented telemetry is missing cycle-aggregation columns: "
            + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(set(CYCLE_AGGREGATION_COLUMNS).intersection(frame.columns))
    if conflicting_columns:
        raise CycleAggregationError(
            "Telemetry already contains cycle-aggregation columns: "
            + ", ".join(conflicting_columns)
        )

    start_row = F.col("record_id") == F.col("loaded_cycle_start_record_id")
    stop_row = F.col("is_cycle_stop")
    cycles = (
        frame.where(F.col("loaded_cycle_id").isNotNull())
        .groupBy("loaded_cycle_id")
        .agg(
            F.max("loaded_cycle_start_record_id").alias("loaded_cycle_start_record_id"),
            F.max("loaded_cycle_start_type").alias("loaded_cycle_start_type"),
            F.max(F.when(start_row, F.col("event_timestamp"))).alias(
                "loaded_cycle_start_timestamp"
            ),
            F.max(F.when(stop_row, F.col("record_id"))).alias("loaded_cycle_stop_record_id"),
            F.max(F.when(stop_row, F.col("event_timestamp"))).alias("loaded_cycle_stop_timestamp"),
            F.sum(F.when(F.col("is_loaded_operation"), F.lit(1)).otherwise(F.lit(0)))
            .cast(LongType())
            .alias("loaded_observation_count"),
        )
        .withColumn(
            "is_right_censored",
            F.col("loaded_cycle_stop_timestamp").isNull(),
        )
        .withColumn(
            "observed_duration_seconds",
            F.when(
                ~F.col("is_right_censored"),
                F.timestamp_diff(
                    "SECOND",
                    F.col("loaded_cycle_start_timestamp"),
                    F.col("loaded_cycle_stop_timestamp"),
                ),
            ).cast(LongType()),
        )
    )
    return cycles.select(
        "loaded_cycle_id",
        "loaded_cycle_start_record_id",
        "loaded_cycle_start_type",
        *CYCLE_AGGREGATION_COLUMNS,
    )
