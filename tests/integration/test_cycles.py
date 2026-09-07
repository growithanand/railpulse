from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.features.cycles import (
    CycleAggregationError,
    CycleBoundaryError,
    aggregate_loaded_cycles,
    annotate_loaded_cycle_boundaries,
    assign_loaded_cycle_segments,
)

BOUNDARY_FIELDS = (
    "record_id",
    "previous_dv_eletric",
    "is_loaded_operation",
    "is_cycle_start",
    "is_cycle_stop",
    "is_left_censored_cycle",
)
SEGMENT_FIELDS = (
    "record_id",
    "loaded_cycle_id",
    "loaded_cycle_start_record_id",
    "loaded_cycle_start_type",
)


def _expected_cycle_id(start_record_id: str) -> str:
    return hashlib.sha256(f"loaded-cycle-v1|{start_record_id}".encode()).hexdigest()


def _cycle_frame(spark: SparkSession) -> DataFrame:
    start = datetime(2020, 2, 1)
    schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_index", LongType(), nullable=False),
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("previous_source_index", LongType(), nullable=True),
            StructField("dv_eletric", ByteType(), nullable=False),
            StructField("is_forward_gap", BooleanType(), nullable=False),
            StructField(
                "rejection_reasons",
                ArrayType(StringType(), containsNull=False),
                nullable=False,
            ),
        ]
    )
    rows = [
        ("initial-active", 0, start, None, 1, False, []),
        ("continuing-active", 10, start + timedelta(seconds=10), 0, 1, False, []),
        ("observed-stop", 20, start + timedelta(seconds=20), 10, 0, False, []),
        ("continuing-inactive", 30, start + timedelta(seconds=30), 20, 0, False, []),
        ("observed-start", 40, start + timedelta(seconds=40), 30, 1, False, []),
        ("active-after-gap", 50, start + timedelta(seconds=100), 40, 1, True, []),
        (
            "stop-after-censored-segment",
            60,
            start + timedelta(seconds=110),
            50,
            0,
            False,
            [],
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_loaded_cycle_boundaries_preserve_censoring_and_do_not_use_future_rows(
    spark: SparkSession,
) -> None:
    source = _cycle_frame(spark)
    annotated = annotate_loaded_cycle_boundaries(source)
    rows = {row.record_id: row for row in annotated.collect()}

    assert rows["initial-active"].previous_dv_eletric is None
    assert rows["initial-active"].is_loaded_operation is True
    assert rows["initial-active"].is_cycle_start is False
    assert rows["initial-active"].is_left_censored_cycle is True
    assert rows["continuing-active"].is_cycle_start is False
    assert rows["continuing-active"].is_left_censored_cycle is False
    assert rows["observed-stop"].is_cycle_stop is True
    assert rows["continuing-inactive"].is_cycle_start is False
    assert rows["continuing-inactive"].is_cycle_stop is False
    assert rows["observed-start"].is_cycle_start is True
    assert rows["observed-start"].is_left_censored_cycle is False
    assert rows["active-after-gap"].is_cycle_start is False
    assert rows["active-after-gap"].is_left_censored_cycle is True
    assert rows["stop-after-censored-segment"].is_cycle_stop is True

    prefix = source.where(F.col("source_index") <= 40)
    prefix_rows = (
        annotate_loaded_cycle_boundaries(prefix)
        .select(*BOUNDARY_FIELDS)
        .orderBy("record_id")
        .collect()
    )
    full_prefix_rows = (
        annotated.where(F.col("source_index") <= 40)
        .select(*BOUNDARY_FIELDS)
        .orderBy("record_id")
        .collect()
    )
    assert prefix_rows == full_prefix_rows


@pytest.mark.spark
def test_loaded_cycle_boundaries_reject_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    source = _cycle_frame(spark)

    with pytest.raises(CycleBoundaryError, match="missing cycle-boundary columns"):
        annotate_loaded_cycle_boundaries(source.drop("previous_source_index"))

    conflicting = source.withColumn("is_cycle_start", F.lit(False))
    with pytest.raises(CycleBoundaryError, match="already contains cycle-boundary columns"):
        annotate_loaded_cycle_boundaries(conflicting)


@pytest.mark.spark
def test_loaded_cycle_segments_have_stable_ids_and_preserve_start_type(
    spark: SparkSession,
) -> None:
    boundaries = annotate_loaded_cycle_boundaries(_cycle_frame(spark))
    segmented = assign_loaded_cycle_segments(boundaries)
    rows = {row.record_id: row for row in segmented.collect()}

    initial_cycle_id = _expected_cycle_id("initial-active")
    assert rows["initial-active"].loaded_cycle_id == initial_cycle_id
    assert rows["continuing-active"].loaded_cycle_id == initial_cycle_id
    assert rows["observed-stop"].loaded_cycle_id == initial_cycle_id
    assert rows["observed-stop"].loaded_cycle_start_record_id == "initial-active"
    assert rows["observed-stop"].loaded_cycle_start_type == "left_censored"
    assert rows["continuing-inactive"].loaded_cycle_id is None

    assert rows["observed-start"].loaded_cycle_id == _expected_cycle_id("observed-start")
    assert rows["observed-start"].loaded_cycle_start_type == "observed"
    gap_cycle_id = _expected_cycle_id("active-after-gap")
    assert rows["active-after-gap"].loaded_cycle_id == gap_cycle_id
    assert rows["active-after-gap"].loaded_cycle_start_type == "left_censored"
    assert rows["stop-after-censored-segment"].loaded_cycle_id == gap_cycle_id

    prefix = boundaries.where(F.col("source_index") <= 40)
    prefix_rows = assign_loaded_cycle_segments(prefix).select(*SEGMENT_FIELDS).collect()
    full_prefix_rows = (
        segmented.where(F.col("source_index") <= 40).select(*SEGMENT_FIELDS).collect()
    )
    assert sorted(prefix_rows, key=lambda row: row.record_id) == sorted(
        full_prefix_rows,
        key=lambda row: row.record_id,
    )


@pytest.mark.spark
def test_loaded_cycle_segments_reject_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    boundaries = annotate_loaded_cycle_boundaries(_cycle_frame(spark))

    with pytest.raises(CycleBoundaryError, match="missing segment columns"):
        assign_loaded_cycle_segments(boundaries.drop("is_cycle_stop"))

    conflicting = boundaries.withColumn("loaded_cycle_id", F.lit("existing"))
    with pytest.raises(CycleBoundaryError, match="already contains cycle-segment columns"):
        assign_loaded_cycle_segments(conflicting)


@pytest.mark.spark
def test_loaded_cycle_aggregation_preserves_observed_and_censored_boundaries(
    spark: SparkSession,
) -> None:
    segmented = assign_loaded_cycle_segments(annotate_loaded_cycle_boundaries(_cycle_frame(spark)))
    cycles = {
        row.loaded_cycle_start_record_id: row
        for row in aggregate_loaded_cycles(segmented).collect()
    }

    initial = cycles["initial-active"]
    assert initial.loaded_cycle_id == _expected_cycle_id("initial-active")
    assert initial.loaded_cycle_start_type == "left_censored"
    assert initial.loaded_cycle_start_timestamp == datetime(2020, 2, 1)
    assert initial.loaded_cycle_stop_record_id == "observed-stop"
    assert initial.loaded_cycle_stop_timestamp == datetime(2020, 2, 1, 0, 0, 20)
    assert initial.loaded_observation_count == 2
    assert initial.is_right_censored is False
    assert initial.observed_duration_seconds == 20

    interrupted = cycles["observed-start"]
    assert interrupted.loaded_cycle_start_type == "observed"
    assert interrupted.loaded_cycle_stop_record_id is None
    assert interrupted.loaded_cycle_stop_timestamp is None
    assert interrupted.loaded_observation_count == 1
    assert interrupted.is_right_censored is True
    assert interrupted.observed_duration_seconds is None

    after_gap = cycles["active-after-gap"]
    assert after_gap.loaded_cycle_start_type == "left_censored"
    assert after_gap.loaded_cycle_stop_record_id == "stop-after-censored-segment"
    assert after_gap.loaded_observation_count == 1
    assert after_gap.is_right_censored is False
    assert after_gap.observed_duration_seconds == 10


@pytest.mark.spark
def test_loaded_cycle_aggregation_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    segmented = assign_loaded_cycle_segments(annotate_loaded_cycle_boundaries(_cycle_frame(spark)))

    with pytest.raises(CycleAggregationError, match="missing cycle-aggregation columns"):
        aggregate_loaded_cycles(segmented.drop("event_timestamp"))

    conflicting = segmented.withColumn("is_right_censored", F.lit(False))
    with pytest.raises(CycleAggregationError, match="already contains cycle-aggregation columns"):
        aggregate_loaded_cycles(conflicting)
