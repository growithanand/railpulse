from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from railpulse.features.pressure_context import PRESSURE_CONTEXT_FEATURE_VERSION
from railpulse.features.pressure_context_profile import (
    PRESSURE_CONTEXT_PROFILE_VERSION,
    collect_pressure_context_profile,
)
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION


def _cycles(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("available", datetime(2020, 1, 1, 10, 0)),
            ("missing-observation", datetime(2020, 1, 1, 11, 0)),
            ("missing-boundary", None),
        ],
        "loaded_cycle_id string, loaded_cycle_stop_timestamp timestamp_ntz",
    )


def _telemetry(spark: SparkSession):
    return spark.createDataFrame(
        [
            (datetime(2020, 1, 1, 9, 45, 1), 9.0, 8.5),
            (datetime(2020, 1, 1, 9, 59, 59), 8.0, 7.0),
            (datetime(2020, 1, 1, 10, 0), 7.0, 6.8),
        ],
        "event_timestamp timestamp_ntz, tp3 double, reservoirs double",
    ).selectExpr(
        "*",
        "'fixture-v1' AS dataset_version",
        "'source-sha' AS source_sha256",
        "'batch-id' AS ingestion_batch_id",
    )


@pytest.mark.spark
def test_pressure_context_profile_reconciles_status_support_and_lineage(
    spark: SparkSession,
) -> None:
    profile = collect_pressure_context_profile(_cycles(spark), _telemetry(spark))
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    distribution = profile.available_distribution

    assert profile.profile_version == PRESSURE_CONTEXT_PROFILE_VERSION
    assert profile.feature_version == PRESSURE_CONTEXT_FEATURE_VERSION
    assert profile.telemetry_validation_version == TELEMETRY_VALIDATION_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.telemetry_source_sha256 == "source-sha"
    assert profile.telemetry_ingestion_batch_id == "batch-id"
    assert profile.accepted_telemetry_record_count == 3
    assert profile.cycle_count == 3
    assert statuses == {
        "available": 1,
        "missing_prediction_boundary": 1,
        "missing_prediction_observation": 1,
    }
    assert distribution.available_cycle_count == 1
    assert distribution.minimum_observation_count == 3
    assert distribution.median_observation_count == 3
    assert distribution.maximum_observation_count == 3
    assert distribution.median_mean_difference_bar == pytest.approx(1.7 / 3)
    assert distribution.median_mean_absolute_difference_bar == pytest.approx(1.7 / 3)
    assert distribution.median_maximum_absolute_difference_bar == 1.0
