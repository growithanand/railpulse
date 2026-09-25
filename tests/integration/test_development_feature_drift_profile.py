from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.models.development_feature_drift_profile import (
    DEVELOPMENT_FEATURE_DRIFT_PROFILE_VERSION,
    collect_development_feature_drift_summary,
)


def _view(spark: SparkSession, *, test_value: float = 1000.0):
    return (
        spark.createDataFrame(
            [
                ("feb-n", "negative", datetime(2020, 2, 1), 1.0),
                ("feb-p", "positive", datetime(2020, 2, 2), 2.0),
                ("may-n", "negative", datetime(2020, 5, 1), 3.0),
                ("jun-n", "negative", datetime(2020, 6, 1), 5.0),
                ("jun-p", "positive", datetime(2020, 6, 2), 4.0),
                ("test", "positive", datetime(2020, 7, 1), test_value),
            ],
            "loaded_cycle_id string, failure_horizon_status string, "
            "prediction_timestamp timestamp_ntz, "
            "motor_current_15m_mean_amperes double",
        )
        .withColumn("modeling_row_status", F.lit("trainable"))
        .withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))
    )


@pytest.mark.spark
def test_drift_profile_reconciles_month_label_groups_and_excludes_test(
    spark: SparkSession,
) -> None:
    summary = collect_development_feature_drift_summary(_view(spark))
    groups = {
        (distribution.calendar_month, distribution.label_status): distribution
        for distribution in summary.monthly_distributions
    }

    assert DEVELOPMENT_FEATURE_DRIFT_PROFILE_VERSION
    assert summary.development_row_count == 5
    assert summary.earliest_prediction_timestamp == "2020-02-01 00:00:00"
    assert summary.latest_prediction_timestamp == "2020-06-02 00:00:00"
    assert groups[("2020-02", "negative")].median_amperes == 1.0
    assert groups[("2020-06", "negative")].median_amperes == 5.0
    assert groups[("2020-06", "positive")].row_count == 1


@pytest.mark.spark
def test_test_values_cannot_change_development_drift_summary(spark: SparkSession) -> None:
    original = collect_development_feature_drift_summary(_view(spark, test_value=1_000.0))
    changed = collect_development_feature_drift_summary(_view(spark, test_value=1_000_000.0))

    assert changed == original
