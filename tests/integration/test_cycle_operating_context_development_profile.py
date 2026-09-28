from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.cycle_operating_context_development_profile import (
    CYCLE_OPERATING_CONTEXT_DEVELOPMENT_PROFILE_VERSION,
    collect_cycle_operating_context_development_summary,
)


def _features(spark: SparkSession, *, test_value: float = 10_000.0):
    return spark.createDataFrame(
        [
            ("train-p", 100.0, 90.0, 600.0, "available"),
            ("train-n", 120.0, 110.0, 800.0, "available"),
            ("validation-p", 140.0, 130.0, 1_000.0, "available"),
            ("validation-n", 160.0, 150.0, 1_200.0, "available"),
            ("test", test_value, test_value, test_value, "available"),
            ("unavailable", 1.0, 1.0, 1.0, "incomplete_previous_cycle"),
        ],
        "loaded_cycle_id string, cycle_context_current_duration_seconds double, "
        "cycle_context_previous_duration_seconds double, "
        "cycle_context_previous_idle_seconds double, cycle_context_status string",
    ).selectExpr(
        "*",
        f"'{CYCLE_OPERATING_CONTEXT_VERSION}' AS cycle_context_feature_version",
    )


def _horizons(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("train-p", datetime(2020, 5, 1), "positive"),
            ("train-n", datetime(2020, 5, 2), "negative"),
            ("validation-p", datetime(2020, 6, 1), "positive"),
            ("validation-n", datetime(2020, 6, 2), "negative"),
            ("test", datetime(2020, 7, 1), "positive"),
            ("unavailable", datetime(2020, 6, 3), "negative"),
        ],
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, failure_horizon_status string",
    )


@pytest.mark.spark
def test_cycle_context_development_profile_reconciles_and_excludes_test(
    spark: SparkSession,
) -> None:
    summary = collect_cycle_operating_context_development_summary(
        _features(spark), _horizons(spark)
    )
    groups = {
        (distribution.partition, distribution.label_status): distribution
        for distribution in summary.distributions
    }

    assert CYCLE_OPERATING_CONTEXT_DEVELOPMENT_PROFILE_VERSION
    assert summary.development_row_count == 4
    assert summary.train_row_count == 2
    assert summary.validation_row_count == 2
    assert groups[("train", "positive")].median_current_duration_seconds == 100.0
    assert groups[("train", "negative")].median_previous_idle_seconds == 800.0
    assert groups[("validation", "positive")].median_previous_duration_seconds == 130.0


@pytest.mark.spark
def test_test_values_cannot_change_cycle_context_development_profile(
    spark: SparkSession,
) -> None:
    horizons = _horizons(spark)
    original = collect_cycle_operating_context_development_summary(
        _features(spark, test_value=10_000.0), horizons
    )
    changed = collect_cycle_operating_context_development_summary(
        _features(spark, test_value=100_000.0), horizons
    )

    assert changed == original
