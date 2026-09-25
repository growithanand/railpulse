from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.models.engineering_baseline import (
    BASELINE_SCORE_COLUMN,
    ENGINEERING_BASELINE_VERSION,
    EngineeringBaselineError,
    fit_engineering_baseline,
    score_engineering_baseline,
)


def _view(spark: SparkSession, *, future_value: float = 100.0):
    return spark.createDataFrame(
        [
            ("train-negative-1", "trainable", "negative", datetime(2020, 5, 1), 4.0),
            ("train-negative-2", "trainable", "negative", datetime(2020, 5, 2), 6.0),
            ("train-negative-3", "trainable", "negative", datetime(2020, 5, 3), 8.0),
            ("train-positive", "trainable", "positive", datetime(2020, 5, 4), 50.0),
            ("validation", "trainable", "negative", datetime(2020, 6, 1), future_value),
            ("test", "trainable", "positive", datetime(2020, 7, 1), future_value),
            ("excluded", "excluded", "negative", datetime(2020, 5, 5), 500.0),
        ],
        "loaded_cycle_id string, modeling_row_status string, failure_horizon_status string, "
        "prediction_timestamp timestamp_ntz, motor_current_15m_mean_amperes double",
    ).withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))


@pytest.mark.spark
def test_baseline_fits_negative_training_rows_only_and_scores_robust_deviation(
    spark: SparkSession,
) -> None:
    parameters = fit_engineering_baseline(_view(spark))

    assert parameters.baseline_version == ENGINEERING_BASELINE_VERSION
    assert parameters.negative_training_row_count == 3
    assert parameters.training_median_amperes == 6.0
    assert parameters.training_median_absolute_deviation_amperes == 2.0

    scores = {
        row.loaded_cycle_id: row[BASELINE_SCORE_COLUMN]
        for row in score_engineering_baseline(_view(spark), parameters).collect()
    }
    assert scores["train-negative-2"] == 0.0
    assert scores["train-negative-1"] == 1.0
    assert scores["train-positive"] == 22.0


@pytest.mark.spark
def test_future_rows_cannot_change_training_parameters_and_invalid_scale_is_rejected(
    spark: SparkSession,
) -> None:
    original = fit_engineering_baseline(_view(spark, future_value=100.0))
    changed_future = fit_engineering_baseline(_view(spark, future_value=10_000.0))

    assert changed_future == original

    with pytest.raises(EngineeringBaselineError, match="incompatible or invalid"):
        score_engineering_baseline(
            _view(spark),
            replace(original, training_median_absolute_deviation_amperes=0.0),
        )
