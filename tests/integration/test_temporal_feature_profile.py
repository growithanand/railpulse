from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.features.temporal_feature_profile import (
    MOTOR_CURRENT_FEATURE_PROFILE_VERSION,
    MotorCurrentFeatureProfileError,
    collect_motor_current_feature_profile,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_VERSION
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION


def _cycles(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("available-three", datetime(2020, 1, 1, 10, 0)),
            ("available-one", datetime(2020, 1, 1, 10, 20)),
            ("missing-observation", datetime(2020, 1, 1, 11, 0)),
            ("missing-boundary", None),
        ],
        schema=schema,
    )


def _telemetry(spark: SparkSession):
    schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("motor_current", DoubleType(), nullable=False),
            StructField("interval_seconds", LongType(), nullable=True),
            StructField("is_forward_gap", BooleanType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            (
                datetime(2020, 1, 1, 9, 45),
                9.0,
                None,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 9, 45, 1),
                2.0,
                1,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 9, 59, 59),
                4.0,
                898,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 0),
                6.0,
                1,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 5),
                8.0,
                300,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 20),
                7.0,
                900,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_motor_current_feature_profile_reconciles_status_and_support(
    spark: SparkSession,
) -> None:
    profile = collect_motor_current_feature_profile(_cycles(spark), _telemetry(spark))
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    support = profile.available_window_support
    tail = profile.low_support_tail

    assert profile.profile_version == MOTOR_CURRENT_FEATURE_PROFILE_VERSION
    assert profile.feature_version == MOTOR_CURRENT_FEATURE_VERSION
    assert profile.window_seconds == 900
    assert profile.telemetry_validation_version == TELEMETRY_VALIDATION_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.telemetry_source_sha256 == "source-sha"
    assert profile.telemetry_ingestion_batch_id == "batch-id"
    assert profile.accepted_telemetry_record_count == 6
    assert profile.cycle_count == 4
    assert statuses == {
        "available": 2,
        "missing_prediction_boundary": 1,
        "missing_prediction_observation": 1,
    }
    assert support.available_cycle_count == 2
    assert support.minimum_observation_count == 1
    assert support.p05_observation_count == 1
    assert support.median_observation_count == 1
    assert support.p95_observation_count == 3
    assert support.maximum_observation_count == 3
    assert support.minimum_observation_span_seconds == 0
    assert support.p05_observation_span_seconds == 0
    assert support.median_observation_span_seconds == 0
    assert support.p95_observation_span_seconds == 899
    assert support.maximum_observation_span_seconds == 899
    assert tail.p05_observation_count == 1
    assert tail.cycle_count_below_p05 == 0
    assert tail.cycle_count_equal_to_p05 == 1
    assert tail.cycle_count_at_or_below_p05 == 1
    assert tail.example_limit == 10
    assert len(tail.examples) == 1
    example = tail.examples[0]
    assert example.loaded_cycle_id == "available-one"
    assert example.prediction_timestamp == "2020-01-01 10:20:00"
    assert example.observation_count == 1
    assert example.first_observation_timestamp == "2020-01-01 10:20:00"
    assert example.observation_span_seconds == 0
    assert example.leading_unobserved_seconds == 900
    assert example.first_observation_follows_forward_gap is True
    assert example.preceding_interval_seconds == 900


@pytest.mark.spark
def test_motor_current_feature_profile_rejects_missing_lineage_and_duplicate_cycles(
    spark: SparkSession,
) -> None:
    with pytest.raises(MotorCurrentFeatureProfileError, match="missing feature-profile columns"):
        collect_motor_current_feature_profile(
            _cycles(spark),
            _telemetry(spark).drop("source_sha256"),
        )

    duplicate_cycles = _cycles(spark).unionByName(_cycles(spark).limit(1))
    with pytest.raises(MotorCurrentFeatureProfileError, match="unique non-null cycle IDs"):
        collect_motor_current_feature_profile(duplicate_cycles, _telemetry(spark))
