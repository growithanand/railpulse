from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.ingestion.schemas import CORRUPT_RECORD_FIELD, TELEMETRY_RAW_FIELDS
from railpulse.validation.silver_telemetry import (
    DIGITAL_SENSOR_COLUMNS,
    MATERIAL_GAP_SECONDS,
    REJECTION_REASONS_FIELD,
    SENSOR_COLUMNS,
    SilverTelemetryError,
    annotate_timestamp_sequence,
    parse_telemetry_types,
    split_telemetry_by_parsing_quality,
    split_telemetry_by_quality,
    validate_digital_sensor_domains,
)


def _bronze_frame(spark: SparkSession, **overrides: Any) -> DataFrame:
    values = {
        "source_index_raw": "0",
        "event_timestamp_raw": "2020-02-01 00:00:00",
        "tp2_raw": "1.000",
        "tp3_raw": "9.100",
        "h1_raw": "8.900",
        "dv_pressure_raw": "0.000",
        "reservoirs_raw": "9.000",
        "oil_temperature_raw": "55.000",
        "motor_current_raw": "4.000",
        "comp_raw": "1.0",
        "dv_eletric_raw": "0.0",
        "towers_raw": "1.0",
        "mpg_raw": "1.0",
        "lps_raw": "0.0",
        "pressure_switch_raw": "1.0",
        "oil_level_raw": "1.0",
        "caudal_impulses_raw": "0.0",
        "record_id": "record-1",
        CORRUPT_RECORD_FIELD: None,
    }
    values.update(overrides)
    schema = StructType(
        [
            StructField(name, StringType(), nullable=True)
            for name in (*TELEMETRY_RAW_FIELDS, "record_id", CORRUPT_RECORD_FIELD)
        ]
    )
    return spark.createDataFrame([values], schema=schema)


@pytest.mark.spark
def test_parse_telemetry_types_adds_canonical_values_and_preserves_bronze(
    spark: SparkSession,
) -> None:
    typed = parse_telemetry_types(_bronze_frame(spark))
    row = typed.first()

    assert row.source_index == 0
    assert row.event_timestamp == datetime(2020, 2, 1, 0, 0)
    assert row.tp2 == 1.0
    assert row.comp == 1.0
    assert row.tp2_raw == "1.000"
    assert row.record_id == "record-1"
    assert isinstance(typed.schema["source_index"].dataType, LongType)
    assert isinstance(typed.schema["event_timestamp"].dataType, TimestampNTZType)
    assert all(
        isinstance(typed.schema[canonical_name].dataType, DoubleType)
        for _, canonical_name in SENSOR_COLUMNS
    )


@pytest.mark.spark
def test_parse_telemetry_types_keeps_invalid_tokens_for_later_rejection(
    spark: SparkSession,
) -> None:
    typed = parse_telemetry_types(
        _bronze_frame(
            spark,
            source_index_raw="not-an-index",
            event_timestamp_raw="2020-2-01 00:00:00",
            tp2_raw="not-a-number",
            comp_raw="2.0",
        )
    )
    row = typed.first()

    assert row.source_index is None
    assert row.event_timestamp is None
    assert row.tp2 is None
    assert row.comp == 2.0
    assert row.source_index_raw == "not-an-index"
    assert row.event_timestamp_raw == "2020-2-01 00:00:00"
    assert row.tp2_raw == "not-a-number"


@pytest.mark.spark
def test_parse_telemetry_types_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    incomplete = spark.createDataFrame([("0",)], ["source_index_raw"])
    with pytest.raises(SilverTelemetryError, match="missing required raw columns"):
        parse_telemetry_types(incomplete)

    conflicting = _bronze_frame(spark).withColumn("source_index", F.lit(0))
    with pytest.raises(SilverTelemetryError, match="already contains canonical columns"):
        parse_telemetry_types(conflicting)


@pytest.mark.spark
def test_split_telemetry_by_parsing_quality_reconciles_and_explains_records(
    spark: SparkSession,
) -> None:
    valid = _bronze_frame(spark, record_id="valid", comp_raw="2.0")
    invalid = _bronze_frame(
        spark,
        record_id="invalid",
        source_index_raw="not-an-index",
        event_timestamp_raw="2020-2-01 00:00:00",
        tp2_raw="not-a-number",
        tp3_raw="NaN",
        h1_raw="Infinity",
    )
    malformed = _bronze_frame(
        spark,
        record_id="malformed",
        corrupt_record="source,row,with,wrong,shape",
    )
    source = valid.unionByName(invalid).unionByName(malformed)

    split = split_telemetry_by_parsing_quality(source)
    accepted = split.accepted.collect()
    quarantined = {row.record_id: row.rejection_reasons for row in split.quarantined.collect()}

    assert len(accepted) + len(quarantined) == source.count() == 3
    assert accepted[0].record_id == "valid"
    assert accepted[0].comp == 2.0
    assert accepted[0][REJECTION_REASONS_FIELD] == []
    assert quarantined["invalid"] == [
        "invalid_source_index",
        "invalid_event_timestamp",
        "invalid_tp2",
        "invalid_tp3",
        "invalid_h1",
    ]
    assert quarantined["malformed"] == ["malformed_csv"]


@pytest.mark.spark
def test_split_telemetry_requires_bronze_control_columns(spark: SparkSession) -> None:
    without_corrupt_record = _bronze_frame(spark).drop(CORRUPT_RECORD_FIELD)

    with pytest.raises(SilverTelemetryError, match="missing required control columns"):
        split_telemetry_by_parsing_quality(without_corrupt_record)


@pytest.mark.spark
def test_digital_domain_validation_covers_every_sensor_and_preserves_parse_failures(
    spark: SparkSession,
) -> None:
    frames = [_bronze_frame(spark, record_id="valid")]
    for position, (raw_name, canonical_name) in enumerate(DIGITAL_SENSOR_COLUMNS, start=1):
        frames.append(
            _bronze_frame(
                spark,
                record_id=f"invalid-domain-{canonical_name}",
                source_index_raw=str(position * 10),
                event_timestamp_raw=f"2020-02-01 00:{position:02d}:00",
                **{raw_name: "2.0"},
            )
        )
    frames.append(
        _bronze_frame(
            spark,
            record_id="invalid-parse-comp",
            source_index_raw="90",
            event_timestamp_raw="2020-02-01 00:09:00",
            comp_raw="NaN",
        )
    )
    source = frames[0]
    for frame in frames[1:]:
        source = source.unionByName(frame)

    split = split_telemetry_by_quality(source)
    accepted = split.accepted.collect()
    quarantined = {row.record_id: row for row in split.quarantined.collect()}

    assert len(accepted) == 1
    assert len(accepted) + len(quarantined) == source.count() == 10
    assert accepted[0].record_id == "valid"
    assert all(
        isinstance(split.accepted.schema[canonical_name].dataType, ByteType)
        for _, canonical_name in DIGITAL_SENSOR_COLUMNS
    )
    for raw_name, canonical_name in DIGITAL_SENSOR_COLUMNS:
        row = quarantined[f"invalid-domain-{canonical_name}"]
        assert row[raw_name] == "2.0"
        assert row[canonical_name] is None
        assert row.rejection_reasons == [f"invalid_{canonical_name}_domain"]

    parse_failure = quarantined["invalid-parse-comp"]
    assert parse_failure.comp is None
    assert parse_failure.rejection_reasons == ["invalid_comp"]


@pytest.mark.spark
def test_digital_domain_validation_requires_parsing_annotations(spark: SparkSession) -> None:
    typed = parse_telemetry_types(_bronze_frame(spark))

    with pytest.raises(SilverTelemetryError, match="missing digital-validation columns"):
        validate_digital_sensor_domains(typed)


@pytest.mark.spark
def test_duplicate_validation_marks_all_affected_rows_but_ignores_null_keys(
    spark: SparkSession,
) -> None:
    frames = [
        _bronze_frame(spark, record_id="unique"),
        _bronze_frame(
            spark,
            record_id="source-duplicate-a",
            source_index_raw="10",
            event_timestamp_raw="2020-02-01 00:00:10",
        ),
        _bronze_frame(
            spark,
            record_id="source-duplicate-b",
            source_index_raw="10",
            event_timestamp_raw="2020-02-01 00:00:20",
        ),
        _bronze_frame(
            spark,
            record_id="timestamp-duplicate-a",
            source_index_raw="20",
            event_timestamp_raw="2020-02-01 00:00:30",
        ),
        _bronze_frame(
            spark,
            record_id="timestamp-duplicate-b",
            source_index_raw="30",
            event_timestamp_raw="2020-02-01 00:00:30",
        ),
        _bronze_frame(
            spark,
            record_id="invalid-keys-a",
            source_index_raw="invalid-a",
            event_timestamp_raw="invalid-a",
        ),
        _bronze_frame(
            spark,
            record_id="invalid-keys-b",
            source_index_raw="invalid-b",
            event_timestamp_raw="invalid-b",
        ),
    ]
    source = frames[0]
    for frame in frames[1:]:
        source = source.unionByName(frame)

    split = split_telemetry_by_quality(source)
    accepted_ids = {row.record_id for row in split.accepted.collect()}
    quarantined = {row.record_id: row.rejection_reasons for row in split.quarantined.collect()}

    assert accepted_ids == {"unique"}
    assert len(accepted_ids) + len(quarantined) == source.count() == 7
    assert quarantined["source-duplicate-a"] == ["duplicate_source_index"]
    assert quarantined["source-duplicate-b"] == ["duplicate_source_index"]
    assert quarantined["timestamp-duplicate-a"] == ["duplicate_event_timestamp"]
    assert quarantined["timestamp-duplicate-b"] == ["duplicate_event_timestamp"]
    assert quarantined["invalid-keys-a"] == [
        "invalid_source_index",
        "invalid_event_timestamp",
    ]
    assert quarantined["invalid-keys-b"] == [
        "invalid_source_index",
        "invalid_event_timestamp",
    ]


@pytest.mark.spark
def test_timestamp_sequence_flags_gaps_and_quarantines_only_out_of_order_records(
    spark: SparkSession,
) -> None:
    assert MATERIAL_GAP_SECONDS == 20
    frames = [
        _bronze_frame(spark, record_id="first"),
        _bronze_frame(
            spark,
            record_id="nominal",
            source_index_raw="10",
            event_timestamp_raw="2020-02-01 00:00:10",
        ),
        _bronze_frame(
            spark,
            record_id="jitter",
            source_index_raw="20",
            event_timestamp_raw="2020-02-01 00:00:21",
        ),
        _bronze_frame(
            spark,
            record_id="gap",
            source_index_raw="30",
            event_timestamp_raw="2020-02-01 00:00:41",
        ),
        _bronze_frame(
            spark,
            record_id="out-of-order",
            source_index_raw="40",
            event_timestamp_raw="2020-02-01 00:00:35",
        ),
        _bronze_frame(
            spark,
            record_id="recovered",
            source_index_raw="50",
            event_timestamp_raw="2020-02-01 00:00:50",
        ),
    ]
    source = frames[0]
    for frame in frames[1:]:
        source = source.unionByName(frame)

    split = split_telemetry_by_quality(source)
    accepted = {row.record_id: row for row in split.accepted.collect()}
    quarantined = {row.record_id: row for row in split.quarantined.collect()}

    assert len(accepted) + len(quarantined) == source.count() == 6
    assert set(accepted) == {"first", "nominal", "jitter", "gap", "recovered"}
    assert quarantined["out-of-order"].rejection_reasons == ["out_of_order_event_timestamp"]
    assert accepted["first"].previous_event_timestamp is None
    assert accepted["first"].interval_seconds is None
    assert accepted["first"].is_forward_gap is False
    assert accepted["nominal"].interval_seconds == 10
    assert accepted["jitter"].interval_seconds == 11
    assert accepted["jitter"].is_forward_gap is False
    assert accepted["gap"].interval_seconds == 20
    assert accepted["gap"].is_forward_gap is True
    assert quarantined["out-of-order"].interval_seconds == -6
    assert quarantined["out-of-order"].is_forward_gap is False
    assert accepted["recovered"].interval_seconds == 15


@pytest.mark.spark
def test_timestamp_sequence_does_not_bridge_duplicate_source_records(
    spark: SparkSession,
) -> None:
    frames = [
        _bronze_frame(spark, record_id="first"),
        _bronze_frame(
            spark,
            record_id="duplicate-a",
            source_index_raw="10",
            event_timestamp_raw="2020-02-01 00:00:10",
        ),
        _bronze_frame(
            spark,
            record_id="duplicate-b",
            source_index_raw="10",
            event_timestamp_raw="2020-02-01 00:00:11",
        ),
        _bronze_frame(
            spark,
            record_id="after-duplicates",
            source_index_raw="20",
            event_timestamp_raw="2020-02-01 00:00:20",
        ),
    ]
    source = frames[0]
    for frame in frames[1:]:
        source = source.unionByName(frame)

    split = split_telemetry_by_quality(source)
    accepted = {row.record_id: row for row in split.accepted.collect()}

    assert accepted["after-duplicates"].previous_source_index is None
    assert accepted["after-duplicates"].previous_event_timestamp is None
    assert accepted["after-duplicates"].interval_seconds is None
    assert accepted["after-duplicates"].is_forward_gap is False


@pytest.mark.spark
def test_timestamp_sequence_rejects_conflicting_metadata_columns(spark: SparkSession) -> None:
    parsed = split_telemetry_by_parsing_quality(_bronze_frame(spark)).accepted
    conflicting = parsed.withColumn("interval_seconds", F.lit(10))

    with pytest.raises(SilverTelemetryError, match="already contains sequence metadata columns"):
        annotate_timestamp_sequence(conflicting)
