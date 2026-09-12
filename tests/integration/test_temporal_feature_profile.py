from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
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
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            (datetime(2020, 1, 1, 9, 45), 9.0, "fixture-v1", "source-sha", "batch-id"),
            (datetime(2020, 1, 1, 9, 45, 1), 2.0, "fixture-v1", "source-sha", "batch-id"),
            (datetime(2020, 1, 1, 9, 59, 59), 4.0, "fixture-v1", "source-sha", "batch-id"),
            (datetime(2020, 1, 1, 10, 0), 6.0, "fixture-v1", "source-sha", "batch-id"),
            (datetime(2020, 1, 1, 10, 5), 8.0, "fixture-v1", "source-sha", "batch-id"),
            (datetime(2020, 1, 1, 10, 20), 7.0, "fixture-v1", "source-sha", "batch-id"),
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
