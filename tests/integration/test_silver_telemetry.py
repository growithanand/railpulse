from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.ingestion.schemas import TELEMETRY_RAW_FIELDS
from railpulse.validation.silver_telemetry import (
    SENSOR_COLUMNS,
    SilverTelemetryError,
    parse_telemetry_types,
)


def _bronze_frame(spark: SparkSession, **overrides: str):
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
    }
    values.update(overrides)
    schema = StructType(
        [
            StructField(name, StringType(), nullable=True)
            for name in (*TELEMETRY_RAW_FIELDS, "record_id")
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
