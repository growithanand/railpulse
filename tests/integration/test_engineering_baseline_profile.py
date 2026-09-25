from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.models.engineering_baseline import fit_engineering_baseline
from railpulse.models.engineering_baseline_profile import (
    ENGINEERING_BASELINE_PROFILE_VERSION,
    collect_engineering_baseline_development_summary,
)


def _view(spark: SparkSession, *, test_value: float = 1_000.0):
    return (
        spark.createDataFrame(
            [
                ("train-n1", "negative", datetime(2020, 5, 1), 4.0),
                ("train-n2", "negative", datetime(2020, 5, 2), 6.0),
                ("train-n3", "negative", datetime(2020, 5, 3), 8.0),
                ("train-p", "positive", datetime(2020, 5, 4), 10.0),
                ("validation-n", "negative", datetime(2020, 6, 1), 7.0),
                ("validation-p", "positive", datetime(2020, 6, 2), 12.0),
                ("test-p", "positive", datetime(2020, 7, 1), test_value),
            ],
            "loaded_cycle_id string, failure_horizon_status string, "
            "prediction_timestamp timestamp_ntz, "
            "motor_current_15m_mean_amperes double",
        )
        .withColumn("modeling_row_status", F.lit("trainable"))
        .withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))
    )


@pytest.mark.spark
def test_development_profile_reconciles_train_validation_and_excludes_test(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    parameters = fit_engineering_baseline(view)
    summary = collect_engineering_baseline_development_summary(view, parameters)
    groups = {
        (distribution.partition, distribution.label_status): distribution
        for distribution in summary.distributions
    }

    assert ENGINEERING_BASELINE_PROFILE_VERSION
    assert summary.development_row_count == 6
    assert summary.train_row_count == 4
    assert summary.validation_row_count == 2
    assert groups[("train", "negative")].row_count == 3
    assert groups[("train", "positive")].median_score == 2.0
    assert groups[("validation", "negative")].median_score == 0.5
    assert groups[("validation", "positive")].median_score == 3.0


@pytest.mark.spark
def test_test_period_values_cannot_change_development_profile(spark: SparkSession) -> None:
    original = _view(spark, test_value=1_000.0)
    changed_test = _view(spark, test_value=1_000_000.0)

    original_parameters = fit_engineering_baseline(original)
    changed_parameters = fit_engineering_baseline(changed_test)
    original_summary = collect_engineering_baseline_development_summary(
        original, original_parameters
    )
    changed_summary = collect_engineering_baseline_development_summary(
        changed_test, changed_parameters
    )

    assert changed_parameters == original_parameters
    assert changed_summary == original_summary
