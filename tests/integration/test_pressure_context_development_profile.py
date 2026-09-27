from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from railpulse.features.pressure_context import PRESSURE_CONTEXT_FEATURE_VERSION
from railpulse.features.pressure_context_development_profile import (
    PRESSURE_CONTEXT_DEVELOPMENT_PROFILE_VERSION,
    collect_pressure_context_development_summary,
)


def _features(spark: SparkSession, *, test_value: float = 100.0):
    return spark.createDataFrame(
        [
            ("train-p", datetime(2020, 5, 1), 0.01, 0.01, 0.02),
            ("train-n", datetime(2020, 5, 2), 0.00, 0.002, 0.004),
            ("validation-p", datetime(2020, 6, 1), 0.02, 0.02, 0.03),
            ("validation-n", datetime(2020, 6, 2), -0.01, 0.003, 0.005),
            ("test", datetime(2020, 7, 1), test_value, test_value, test_value),
        ],
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, "
        "pressure_context_15m_mean_difference_bar double, "
        "pressure_context_15m_mean_absolute_difference_bar double, "
        "pressure_context_15m_maximum_absolute_difference_bar double",
    ).selectExpr(
        "*",
        f"'{PRESSURE_CONTEXT_FEATURE_VERSION}' AS pressure_context_15m_feature_version",
        "'available' AS pressure_context_15m_status",
    )


def _horizons(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("train-p", datetime(2020, 5, 1), "positive"),
            ("train-n", datetime(2020, 5, 2), "negative"),
            ("validation-p", datetime(2020, 6, 1), "positive"),
            ("validation-n", datetime(2020, 6, 2), "negative"),
            ("test", datetime(2020, 7, 1), "positive"),
        ],
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, failure_horizon_status string",
    )


@pytest.mark.spark
def test_pressure_development_profile_reconciles_groups_and_excludes_test(
    spark: SparkSession,
) -> None:
    summary = collect_pressure_context_development_summary(_features(spark), _horizons(spark))
    groups = {
        (distribution.partition, distribution.label_status): distribution
        for distribution in summary.distributions
    }

    assert PRESSURE_CONTEXT_DEVELOPMENT_PROFILE_VERSION
    assert summary.development_row_count == 4
    assert summary.train_row_count == 2
    assert summary.validation_row_count == 2
    assert groups[("train", "positive")].median_mean_absolute_difference_bar == 0.01
    assert groups[("train", "negative")].median_mean_absolute_difference_bar == 0.002
    assert groups[("validation", "positive")].median_maximum_absolute_difference_bar == 0.03


@pytest.mark.spark
def test_test_values_cannot_change_pressure_development_profile(spark: SparkSession) -> None:
    horizons = _horizons(spark)
    original = collect_pressure_context_development_summary(
        _features(spark, test_value=100.0), horizons
    )
    changed = collect_pressure_context_development_summary(
        _features(spark, test_value=10_000.0), horizons
    )

    assert changed == original
