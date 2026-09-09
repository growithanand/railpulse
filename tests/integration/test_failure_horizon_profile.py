from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.features.failure_horizon_profile import (
    FAILURE_HORIZON_PROFILE_VERSION,
    FailureHorizonProfileError,
    collect_failure_horizon_profile,
)
from railpulse.features.failure_horizons import FAILURE_HORIZON_VERSION


def _cycles(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=True),
            StructField("is_right_censored", BooleanType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            ("positive", datetime(2020, 1, 1, 9, 59, 59), False),
            ("inside", datetime(2020, 1, 1, 10, 0), False),
            ("negative", datetime(2020, 1, 2, 10, 0), False),
            ("upper", datetime(2020, 1, 3, 10, 0), False),
            ("censored", datetime(2020, 1, 10, 0, 20), False),
            ("open", None, True),
        ],
        schema=schema,
    )


def _failures(spark: SparkSession):
    schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_row", LongType(), nullable=False),
            StructField("failure_start", TimestampNTZType(), nullable=False),
            StructField("failure_end", TimestampNTZType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("source_document_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            (
                "failure-1",
                1,
                datetime(2020, 1, 1, 10, 0),
                datetime(2020, 1, 1, 11, 0),
                "fixture-v1",
                "failure-sha",
                "document-sha",
                "failure-batch",
            ),
            (
                "failure-2",
                2,
                datetime(2020, 1, 3, 12, 0),
                datetime(2020, 1, 3, 13, 0),
                "fixture-v1",
                "failure-sha",
                "document-sha",
                "failure-batch",
            ),
            (
                "failure-3",
                3,
                datetime(2020, 1, 8, 12, 0),
                datetime(2020, 1, 8, 13, 0),
                "fixture-v1",
                "failure-sha",
                "document-sha",
                "failure-batch",
            ),
        ],
        schema=schema,
    )


def _telemetry(spark: SparkSession):
    schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            (datetime(2020, 1, 1), "fixture-v1", "telemetry-sha", "telemetry-batch"),
            (
                datetime(2020, 1, 10, 0, 30),
                "fixture-v1",
                "telemetry-sha",
                "telemetry-batch",
            ),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_failure_horizon_profile_reconciles_status_and_event_counts(
    spark: SparkSession,
) -> None:
    profile = collect_failure_horizon_profile(
        _cycles(spark),
        _failures(spark),
        _telemetry(spark),
        horizon_seconds=7_200,
    )
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    events = {item.failure_record_id: item for item in profile.failure_event_counts}

    assert profile.profile_version == FAILURE_HORIZON_PROFILE_VERSION
    assert profile.horizon_version == FAILURE_HORIZON_VERSION
    assert profile.horizon_seconds == 7_200
    assert profile.label_observation_end == "2020-01-10 00:30:00"
    assert profile.dataset_version == "fixture-v1"
    assert profile.telemetry_source_sha256 == "telemetry-sha"
    assert profile.telemetry_ingestion_batch_id == "telemetry-batch"
    assert profile.failure_source_sha256 == "failure-sha"
    assert profile.failure_source_document_sha256 == "document-sha"
    assert profile.failure_ingestion_batch_id == "failure-batch"
    assert profile.accepted_telemetry_record_count == 2
    assert profile.cycle_count == 6
    assert profile.accepted_failure_event_count == 3
    assert profile.matched_failure_event_count == 2
    assert statuses == {
        "positive": 2,
        "negative": 1,
        "horizon_censored": 1,
        "inside_failure_interval": 1,
        "missing_prediction_boundary": 1,
    }
    assert events["failure-1"].source_row == 1
    assert events["failure-1"].positive_cycle_count == 1
    assert events["failure-2"].positive_cycle_count == 1
    assert events["failure-3"].positive_cycle_count == 0


@pytest.mark.spark
def test_failure_horizon_profile_rejects_missing_or_empty_telemetry(
    spark: SparkSession,
) -> None:
    with pytest.raises(FailureHorizonProfileError, match="missing profile columns"):
        collect_failure_horizon_profile(
            _cycles(spark),
            _failures(spark),
            _telemetry(spark).withColumnRenamed("event_timestamp", "timestamp"),
        )

    empty = spark.createDataFrame([], _telemetry(spark).schema)
    with pytest.raises(FailureHorizonProfileError, match="requires accepted telemetry"):
        collect_failure_horizon_profile(
            _cycles(spark),
            _failures(spark),
            empty,
        )
