"""Explicit source-aligned schemas for MetroPT-3 Bronze ingestion."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

TELEMETRY_RAW_FIELDS = (
    "source_index_raw",
    "event_timestamp_raw",
    "tp2_raw",
    "tp3_raw",
    "h1_raw",
    "dv_pressure_raw",
    "reservoirs_raw",
    "oil_temperature_raw",
    "motor_current_raw",
    "comp_raw",
    "dv_eletric_raw",
    "towers_raw",
    "mpg_raw",
    "lps_raw",
    "pressure_switch_raw",
    "oil_level_raw",
    "caudal_impulses_raw",
)

TELEMETRY_SOURCE_FIELDS = (
    "",
    "timestamp",
    "TP2",
    "TP3",
    "H1",
    "DV_pressure",
    "Reservoirs",
    "Oil_temperature",
    "Motor_current",
    "COMP",
    "DV_eletric",
    "Towers",
    "MPG",
    "LPS",
    "Pressure_switch",
    "Oil_level",
    "Caudal_impulses",
)

FAILURE_RAW_FIELDS = (
    "source_row_raw",
    "source_report_number_raw",
    "start_time_raw",
    "end_time_raw",
    "failure_raw",
    "severity_raw",
    "report_raw",
    "source_note_raw",
)

FAILURE_SOURCE_FIELDS = (
    "source_row",
    "source_report_number",
    "start_time_raw",
    "end_time_raw",
    "failure_raw",
    "severity_raw",
    "report_raw",
    "source_note",
)

CORRUPT_RECORD_FIELD = "corrupt_record"


def _raw_csv_schema(fields: tuple[str, ...]) -> StructType:
    return StructType(
        [StructField(name, StringType(), nullable=True) for name in (*fields, CORRUPT_RECORD_FIELD)]
    )


def telemetry_csv_schema() -> StructType:
    """Return the positional all-string schema for the official telemetry CSV."""

    return _raw_csv_schema(TELEMETRY_SOURCE_FIELDS)


def failure_csv_schema() -> StructType:
    """Return the positional all-string schema for the failure-reference CSV."""

    return _raw_csv_schema(FAILURE_SOURCE_FIELDS)
