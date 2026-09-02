"""Type and structurally validate the separately published failure events."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampNTZType
from pyspark.sql.window import Window

from railpulse.ingestion.schemas import CORRUPT_RECORD_FIELD, FAILURE_RAW_FIELDS

FAILURE_TIMESTAMP_PATTERN = "M/d/yyyy H:mm"
REJECTION_REASONS_FIELD = "rejection_reasons"
FAILURE_CANONICAL_COLUMNS = (
    "source_row",
    "failure_start",
    "failure_end",
    REJECTION_REASONS_FIELD,
)


class SilverFailureError(ValueError):
    """Raised when failure records cannot satisfy the Silver input contract."""


@dataclass(frozen=True)
class FailureQualitySplit:
    """Lazy accepted and quarantined failure-event frames."""

    all_records: DataFrame
    accepted: DataFrame
    quarantined: DataFrame


def _failure_timestamp(raw_column: str, canonical_name: str) -> Column:
    parsed = F.try_to_timestamp(F.col(raw_column), F.lit(FAILURE_TIMESTAMP_PATTERN))
    return parsed.cast(TimestampNTZType()).alias(canonical_name)


def _missing_text(column_name: str) -> Column:
    return F.col(column_name).isNull() | (F.length(F.trim(F.col(column_name))) == 0)


def split_failure_events_by_quality(frame: DataFrame) -> FailureQualitySplit:
    """Type failure events and retain every structural rejection with explicit reasons.

    Publisher report labels are deliberately not uniqueness keys. Source notes and report text stay
    raw because their ambiguity cannot be resolved from the supplied documentation.
    """

    required_columns = {*FAILURE_RAW_FIELDS, CORRUPT_RECORD_FIELD, "record_id"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverFailureError(
            "Bronze failure events are missing required columns: " + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(set(FAILURE_CANONICAL_COLUMNS).intersection(frame.columns))
    if conflicting_columns:
        raise SilverFailureError(
            "Bronze failure events already contain Silver columns: "
            + ", ".join(conflicting_columns)
        )

    typed = frame.select(
        "*",
        F.col("source_row_raw").try_cast(LongType()).alias("source_row"),
        _failure_timestamp("start_time_raw", "failure_start"),
        _failure_timestamp("end_time_raw", "failure_end"),
    )
    duplicate_source_row = F.col("source_row").isNotNull() & (
        F.count(F.lit(1)).over(Window.partitionBy("source_row")) > 1
    )
    reversed_interval = (
        F.col("failure_start").isNotNull()
        & F.col("failure_end").isNotNull()
        & (F.col("failure_end") < F.col("failure_start"))
    )
    reason_expressions = [
        F.when(F.col(CORRUPT_RECORD_FIELD).isNotNull(), F.lit("malformed_csv")),
        F.when(F.col("source_row").isNull(), F.lit("invalid_source_row")),
        F.when(duplicate_source_row, F.lit("duplicate_source_row")),
        F.when(_missing_text("source_report_number_raw"), F.lit("missing_report_number")),
        F.when(F.col("failure_start").isNull(), F.lit("invalid_failure_start")),
        F.when(F.col("failure_end").isNull(), F.lit("invalid_failure_end")),
        F.when(reversed_interval, F.lit("reversed_failure_interval")),
        F.when(_missing_text("failure_raw"), F.lit("missing_failure_type")),
        F.when(_missing_text("severity_raw"), F.lit("missing_severity")),
    ]
    annotated = typed.withColumn(
        REJECTION_REASONS_FIELD,
        F.filter(F.array(*reason_expressions), lambda reason: reason.isNotNull()),
    )
    reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
    return FailureQualitySplit(
        all_records=annotated,
        accepted=annotated.where(reason_count == 0),
        quarantined=annotated.where(reason_count > 0),
    )
