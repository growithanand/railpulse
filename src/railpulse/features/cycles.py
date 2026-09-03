"""Derive causal loaded-cycle boundary evidence from accepted Silver telemetry."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOADED_SIGNAL_COLUMN = "dv_eletric"
CYCLE_BOUNDARY_COLUMNS = (
    "previous_dv_eletric",
    "is_loaded_operation",
    "is_cycle_start",
    "is_cycle_stop",
    "is_left_censored_cycle",
)


class CycleBoundaryError(ValueError):
    """Raised when telemetry cannot satisfy the loaded-cycle input contract."""


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
