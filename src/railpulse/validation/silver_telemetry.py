"""Type raw MetroPT-3 telemetry without discarding its Bronze representation."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, DoubleType, LongType, TimestampNTZType
from pyspark.sql.window import Window

from railpulse.ingestion.schemas import CORRUPT_RECORD_FIELD, TELEMETRY_RAW_FIELDS
from railpulse.validation.metropt3 import EXPECTED_INDEX_STEP, EXPECTED_INTERVAL_SECONDS

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

# Exact complete-file observed extrema recorded in docs/dataset_manifest.json. Some source tokens
# contain floating-point representation tails, so rounding these bounds would incorrectly quarantine
# records from the verified reference artifact. These are dataset-envelope checks for possible
# contract drift, not manufacturer operating or safety limits.
ANALOG_SENSOR_BOUNDS = {
    "tp2": (-0.032, 10.676),
    "tp3": (0.73, 10.302),
    "h1": (-0.0360000000000013, 10.288),
    "dv_pressure": (-0.032, 9.844),
    "reservoirs": (0.7119999999999997, 10.3),
    "oil_temperature": (15.4, 89.05000000000001),
    "motor_current": (0.0199999999999995, 9.295),
}

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
MATERIAL_GAP_SECONDS = EXPECTED_INTERVAL_SECONDS * 2
SEQUENCE_METADATA_COLUMNS = (
    "previous_source_index",
    "previous_event_timestamp",
    "interval_seconds",
    "is_forward_gap",
)


class SilverTelemetryError(ValueError):
    """Raised when a frame cannot be projected into the Silver telemetry contract."""


@dataclass(frozen=True)
class TelemetryQualitySplit:
    """Lazy accepted and quarantined frames produced by telemetry validation."""

    all_records: DataFrame
    accepted: DataFrame
    quarantined: DataFrame


@dataclass(frozen=True)
class TelemetryQualityMetrics:
    """Deterministic count summary for one validated telemetry frame."""

    total_record_count: int
    accepted_record_count: int
    quarantined_record_count: int
    forward_gap_count: int
    rejection_reason_counts: dict[str, int]


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
        all_records=annotated,
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


def validate_analog_sensor_ranges(frame: DataFrame) -> DataFrame:
    """Append dataset-envelope reasons while preserving parsed analogue values."""

    required_columns = {REJECTION_REASONS_FIELD, *ANALOG_SENSOR_BOUNDS}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Typed telemetry is missing analogue-validation columns: " + ", ".join(missing_columns)
        )

    range_reason_expressions = [
        F.when(
            ~_invalid_numeric(canonical_name)
            & (
                (F.col(canonical_name) < F.lit(lower_bound))
                | (F.col(canonical_name) > F.lit(upper_bound))
            ),
            F.lit(f"out_of_range_{canonical_name}"),
        )
        for canonical_name, (lower_bound, upper_bound) in ANALOG_SENSOR_BOUNDS.items()
    ]
    range_reasons = F.filter(
        F.array(*range_reason_expressions),
        lambda reason: reason.isNotNull(),
    )
    return frame.withColumn(
        REJECTION_REASONS_FIELD,
        F.concat(F.col(REJECTION_REASONS_FIELD), range_reasons),
    )


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


def annotate_timestamp_sequence(frame: DataFrame) -> DataFrame:
    """Add adjacent event-time intervals, forward-gap flags, and ordering reasons."""

    required_columns = {REJECTION_REASONS_FIELD, "source_index", "event_timestamp"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Typed telemetry is missing sequence-validation columns: " + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(set(SEQUENCE_METADATA_COLUMNS).intersection(frame.columns))
    if conflicting_columns:
        raise SilverTelemetryError(
            "Typed telemetry already contains sequence metadata columns: "
            + ", ".join(conflicting_columns)
        )

    has_duplicate_identifier = F.array_contains(
        F.col(REJECTION_REASONS_FIELD), "duplicate_source_index"
    ) | F.array_contains(F.col(REJECTION_REASONS_FIELD), "duplicate_event_timestamp")
    is_sequence_eligible = (
        F.col("source_index").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & ~has_duplicate_identifier
    )
    sequence_ready = frame.withColumn("_sequence_eligible", is_sequence_eligible)
    predecessors = (
        sequence_ready.where(F.col("_sequence_eligible"))
        .select(
            (F.col("source_index") + EXPECTED_INDEX_STEP).alias("_next_source_index"),
            F.col("source_index").alias("previous_source_index"),
            F.col("event_timestamp").alias("previous_event_timestamp"),
        )
        .alias("previous")
    )
    current = sequence_ready.alias("current")
    annotated = current.join(
        predecessors,
        F.col("current._sequence_eligible")
        & (F.col("current.source_index") == F.col("previous._next_source_index")),
        how="left",
    ).select(
        *(F.col(f"current.{column_name}").alias(column_name) for column_name in frame.columns),
        F.col("previous.previous_source_index"),
        F.col("previous.previous_event_timestamp"),
    )
    annotated = annotated.withColumn(
        "interval_seconds",
        F.when(
            F.col("previous_event_timestamp").isNotNull(),
            F.timestamp_diff(
                "SECOND",
                F.col("previous_event_timestamp"),
                F.col("event_timestamp"),
            ),
        ).cast(LongType()),
    ).withColumn(
        "is_forward_gap",
        F.coalesce(F.col("interval_seconds") >= MATERIAL_GAP_SECONDS, F.lit(False)),
    )
    ordering_reasons = F.filter(
        F.array(
            F.when(
                F.col("interval_seconds") < 0,
                F.lit("out_of_order_event_timestamp"),
            )
        ),
        lambda reason: reason.isNotNull(),
    )
    return annotated.withColumn(
        REJECTION_REASONS_FIELD,
        F.concat(F.col(REJECTION_REASONS_FIELD), ordering_reasons),
    )


def split_telemetry_by_quality(frame: DataFrame) -> TelemetryQualitySplit:
    """Apply implemented parsing, range, domain, duplicate, and sequence rules, then split."""

    parsed = _annotate_telemetry_parsing_quality(frame)
    range_validated = validate_analog_sensor_ranges(parsed)
    domain_validated = validate_digital_sensor_domains(range_validated)
    duplicate_validated = validate_duplicate_identifiers(domain_validated)
    sequence_annotated = annotate_timestamp_sequence(duplicate_validated)
    return _split_annotated_telemetry(sequence_annotated)


def _count_when(condition: Column) -> Column:
    return F.coalesce(
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))),
        F.lit(0),
    ).cast(LongType())


def collect_telemetry_quality_metrics(
    split: TelemetryQualitySplit,
) -> TelemetryQualityMetrics:
    """Materialize reconciled row and rejection-reason counts for a quality split."""

    required_columns = {REJECTION_REASONS_FIELD, "is_forward_gap"}
    missing_columns = sorted(required_columns - set(split.all_records.columns))
    if missing_columns:
        raise SilverTelemetryError(
            "Validated telemetry is missing quality-metric columns: " + ", ".join(missing_columns)
        )

    quality = split.all_records.select(*sorted(required_columns)).persist(StorageLevel.DISK_ONLY)
    try:
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        summary = quality.agg(
            F.count(F.lit(1)).cast(LongType()).alias("total_record_count"),
            _count_when(reason_count == 0).alias("accepted_record_count"),
            _count_when(reason_count > 0).alias("quarantined_record_count"),
            _count_when(F.col("is_forward_gap")).alias("forward_gap_count"),
        ).first()
        reason_rows = (
            quality.select(F.explode(F.col(REJECTION_REASONS_FIELD)).alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy("reason")
            .collect()
        )
    finally:
        quality.unpersist()

    total_count = int(summary.total_record_count)
    accepted_count = int(summary.accepted_record_count)
    quarantined_count = int(summary.quarantined_record_count)
    if accepted_count + quarantined_count != total_count:
        raise SilverTelemetryError(
            "Telemetry quality metrics do not reconcile: "
            f"total={total_count}, accepted={accepted_count}, quarantined={quarantined_count}"
        )

    return TelemetryQualityMetrics(
        total_record_count=total_count,
        accepted_record_count=accepted_count,
        quarantined_record_count=quarantined_count,
        forward_gap_count=int(summary.forward_gap_count),
        rejection_reason_counts={row.reason: int(row["count"]) for row in reason_rows},
    )
