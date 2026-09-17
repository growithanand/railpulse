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

from railpulse.features.eligibility_profile import (
    MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION,
    EligibilityProfileError,
    collect_motor_current_eligibility_profile,
)
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_VERSION
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
            ("eligible", datetime(2020, 1, 1, 10, 0)),
            ("material-gap", datetime(2020, 1, 1, 10, 20)),
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
                datetime(2020, 1, 1, 9, 59, 40),
                2.0,
                None,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 9, 59, 50),
                4.0,
                10,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 0),
                6.0,
                10,
                False,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 5, 1),
                8.0,
                301,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
            (
                datetime(2020, 1, 1, 10, 20),
                7.0,
                899,
                True,
                "fixture-v1",
                "source-sha",
                "batch-id",
            ),
        ],
        schema=schema,
    )


@pytest.mark.spark
def test_eligibility_profile_reconciles_status_and_reason_counts(
    spark: SparkSession,
) -> None:
    profile = collect_motor_current_eligibility_profile(_cycles(spark), _telemetry(spark))
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    reasons = {item.reason: item.cycle_count for item in profile.reason_counts}

    assert profile.profile_version == MOTOR_CURRENT_ELIGIBILITY_PROFILE_VERSION
    assert profile.eligibility_version == MOTOR_CURRENT_ELIGIBILITY_VERSION
    assert profile.feature_version == MOTOR_CURRENT_FEATURE_VERSION
    assert profile.feature_window_seconds == 900
    assert profile.minimum_excluded_gap_seconds == 20
    assert profile.telemetry_validation_version == TELEMETRY_VALIDATION_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.telemetry_source_sha256 == "source-sha"
    assert profile.telemetry_ingestion_batch_id == "batch-id"
    assert profile.accepted_telemetry_record_count == 5
    assert profile.cycle_count == 4
    assert statuses == {"eligible": 1, "ineligible": 3}
    assert reasons == {
        "feature_missing_prediction_boundary": 1,
        "feature_missing_prediction_observation": 1,
        "missing_or_invalid_coverage_context": 0,
        "material_window_gap": 1,
        "unsupported_feature_status": 0,
    }


@pytest.mark.spark
def test_eligibility_profile_rejects_missing_telemetry_lineage(
    spark: SparkSession,
) -> None:
    with pytest.raises(EligibilityProfileError, match="missing feature-profile columns"):
        collect_motor_current_eligibility_profile(
            _cycles(spark),
            _telemetry(spark).drop("source_sha256"),
        )
