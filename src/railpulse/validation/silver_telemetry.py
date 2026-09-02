"""Type raw MetroPT-3 telemetry without discarding its Bronze representation."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, DoubleType, LongType, TimestampNTZType
from pyspark.sql.window import Window

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
class TelemetryQualitySplit:
    """Lazy accepted and quarantined frames produced by telemetry validation."""

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


def _annotate_telemetry_parsing_quality(frame: DataFrame) -> DataFrame:
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
    return annotated


def _split_annotated_telemetry(annotated: DataFrame) -> TelemetryQualitySplit:
    reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
    return TelemetryQualitySplit(
        accepted=annotated.where(reason_count == 0),
        quarantined=annotated.where(reason_count > 0),
    )


def split_telemetry_by_parsing_quality(frame: DataFrame) -> TelemetryQualitySplit:
    """Parse Bronze telemetry and split records using explicit parsing reasons.

    This boundary covers structural and type validity only. Digital domains, engineering ranges,
    duplicate event times, and timestamp gaps are intentionally handled by later validators.
    """

    return _split_annotated_telemetry(_annotate_telemetry_parsing_quality(frame))


def validate_digital_sensor_domains(frame: DataFrame) -> DataFrame:
    """Append digital-domain reasons and normalize valid values to binary bytes."""

    required_columns = {
        REJECTION_REASONS_FIELD,
        *(canonical_name for _, canonical_name in DIGITAL_SENSOR_COLUMNS),
    }
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Typed telemetry is missing digital-validation columns: " + ", ".join(missing_columns)
        )

    domain_reason_expressions = [
        F.when(
            ~_invalid_numeric(canonical_name) & ~F.col(canonical_name).isin(0.0, 1.0),
            F.lit(f"invalid_{canonical_name}_domain"),
        )
        for _, canonical_name in DIGITAL_SENSOR_COLUMNS
    ]
    domain_reasons = F.filter(
        F.array(*domain_reason_expressions),
        lambda reason: reason.isNotNull(),
    )
    validated = frame.withColumn(
        REJECTION_REASONS_FIELD,
        F.concat(F.col(REJECTION_REASONS_FIELD), domain_reasons),
    )
    for _, canonical_name in DIGITAL_SENSOR_COLUMNS:
        value = F.col(canonical_name)
        validated = validated.withColumn(
            canonical_name,
            F.when(value.isin(0.0, 1.0), value.cast(ByteType())).otherwise(
                F.lit(None).cast(ByteType())
            ),
        )
    return validated


def validate_duplicate_identifiers(frame: DataFrame) -> DataFrame:
    """Append reasons to every row sharing a non-null source index or event time."""

    required_columns = {REJECTION_REASONS_FIELD, "source_index", "event_timestamp"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Typed telemetry is missing duplicate-validation columns: " + ", ".join(missing_columns)
        )

    duplicate_source_index = F.col("source_index").isNotNull() & (
        F.count(F.lit(1)).over(Window.partitionBy("source_index")) > 1
    )
    duplicate_event_timestamp = F.col("event_timestamp").isNotNull() & (
        F.count(F.lit(1)).over(Window.partitionBy("event_timestamp")) > 1
    )
    duplicate_reasons = F.filter(
        F.array(
            F.when(duplicate_source_index, F.lit("duplicate_source_index")),
            F.when(duplicate_event_timestamp, F.lit("duplicate_event_timestamp")),
        ),
        lambda reason: reason.isNotNull(),
    )
    return frame.withColumn(
        REJECTION_REASONS_FIELD,
        F.concat(F.col(REJECTION_REASONS_FIELD), duplicate_reasons),
    )


def split_telemetry_by_quality(frame: DataFrame) -> TelemetryQualitySplit:
    """Apply implemented parsing, domain, and duplicate rules, then split the records."""

    parsed = _annotate_telemetry_parsing_quality(frame)
    domain_validated = validate_digital_sensor_domains(parsed)
    duplicate_validated = validate_duplicate_identifiers(domain_validated)
    return _split_annotated_telemetry(duplicate_validated)
