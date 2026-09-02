"""Type raw MetroPT-3 telemetry without discarding its Bronze representation."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, TimestampNTZType

from railpulse.ingestion.schemas import CORRUPT_RECORD_FIELD, TELEMETRY_RAW_FIELDS

TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"
TIMESTAMP_SHAPE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"

ANALOG_SENSOR_COLUMNS = (
    ("tp2_raw", "tp2"),
    ("tp3_raw", "tp3"),
    ("h1_raw", "h1"),
    ("dv_pressure_raw", "dv_pressure"),
    ("reservoirs_raw", "reservoirs"),
    ("oil_temperature_raw", "oil_temperature"),
    ("motor_current_raw", "motor_current"),
)

DIGITAL_SENSOR_COLUMNS = (
    ("comp_raw", "comp"),
    ("dv_eletric_raw", "dv_eletric"),
    ("towers_raw", "towers"),
    ("mpg_raw", "mpg"),
    ("lps_raw", "lps"),
    ("pressure_switch_raw", "pressure_switch"),
    ("oil_level_raw", "oil_level"),
    ("caudal_impulses_raw", "caudal_impulses"),
)

SENSOR_COLUMNS = (*ANALOG_SENSOR_COLUMNS, *DIGITAL_SENSOR_COLUMNS)
REJECTION_REASONS_FIELD = "rejection_reasons"


class SilverTelemetryError(ValueError):
    """Raised when a frame cannot be projected into the Silver telemetry contract."""


@dataclass(frozen=True)
class TelemetryParsingSplit:
    """Lazy accepted and quarantined frames produced by parsing validation."""

    accepted: DataFrame
    quarantined: DataFrame


def _timestamp_expression() -> Column:
    raw_timestamp = F.col("event_timestamp_raw")
    parsed_timestamp = raw_timestamp.try_cast(TimestampNTZType())
    has_exact_format = raw_timestamp.rlike(TIMESTAMP_SHAPE) & (
        F.date_format(parsed_timestamp, TIMESTAMP_PATTERN) == raw_timestamp
    )
    return F.when(has_exact_format, parsed_timestamp).alias("event_timestamp")


def parse_telemetry_types(frame: DataFrame) -> DataFrame:
    """Append canonical typed values while retaining every input column.

    Parsing failures become null canonical values so a later validation step can attach explicit
    rejection reasons. Digital values remain doubles until their binary domain is validated.
    """

    missing_columns = sorted(set(TELEMETRY_RAW_FIELDS) - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Bronze telemetry is missing required raw columns: " + ", ".join(missing_columns)
        )

    canonical_columns = {
        "source_index",
        "event_timestamp",
        *(canonical_name for _, canonical_name in SENSOR_COLUMNS),
    }
    conflicting_columns = sorted(canonical_columns.intersection(frame.columns))
    if conflicting_columns:
        raise SilverTelemetryError(
            "Bronze telemetry already contains canonical columns: " + ", ".join(conflicting_columns)
        )

    return frame.select(
        "*",
        F.col("source_index_raw").try_cast(LongType()).alias("source_index"),
        _timestamp_expression(),
        *(
            F.col(raw_name).try_cast(DoubleType()).alias(canonical_name)
            for raw_name, canonical_name in SENSOR_COLUMNS
        ),
    )


def _invalid_numeric(column_name: str) -> Column:
    value = F.col(column_name)
    return value.isNull() | F.isnan(value) | (F.abs(value) == F.lit(float("inf")))


def split_telemetry_by_parsing_quality(frame: DataFrame) -> TelemetryParsingSplit:
    """Parse Bronze telemetry and split records using explicit parsing reasons.

    This boundary covers structural and type validity only. Digital domains, engineering ranges,
    duplicate event times, and timestamp gaps are intentionally handled by later validators.
    """

    required_control_columns = {CORRUPT_RECORD_FIELD, "record_id"}
    missing_control_columns = sorted(required_control_columns - set(frame.columns))
    if missing_control_columns:
        raise SilverTelemetryError(
            "Bronze telemetry is missing required control columns: "
            + ", ".join(missing_control_columns)
        )
    if REJECTION_REASONS_FIELD in frame.columns:
        raise SilverTelemetryError(f"Bronze telemetry already contains {REJECTION_REASONS_FIELD}")

    typed = parse_telemetry_types(frame)
    reason_expressions = [
        F.when(F.col(CORRUPT_RECORD_FIELD).isNotNull(), F.lit("malformed_csv")),
        F.when(F.col("source_index").isNull(), F.lit("invalid_source_index")),
        F.when(F.col("event_timestamp").isNull(), F.lit("invalid_event_timestamp")),
        *(
            F.when(_invalid_numeric(canonical_name), F.lit(f"invalid_{canonical_name}"))
            for _, canonical_name in SENSOR_COLUMNS
        ),
    ]
    annotated = typed.withColumn(
        REJECTION_REASONS_FIELD,
        F.filter(F.array(*reason_expressions), lambda reason: reason.isNotNull()),
    )
    reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
    return TelemetryParsingSplit(
        accepted=annotated.where(reason_count == 0),
        quarantined=annotated.where(reason_count > 0),
    )
