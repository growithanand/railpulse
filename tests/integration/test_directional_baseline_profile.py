from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.models.directional_baseline_profile import (
    DIRECTIONAL_BASELINE_PROFILE_VERSION,
    collect_directional_baseline_comparison,
)


def _view(spark: SparkSession, *, test_value: float = 0.1):
    return (
        spark.createDataFrame(
            [
                ("train-p1", "positive", datetime(2020, 5, 1), 0.8, "failure-train"),
                ("train-p2", "positive", datetime(2020, 5, 2), 1.4, "failure-train"),
                ("train-n1", "negative", datetime(2020, 5, 3), 0.7, None),
                ("train-n2", "negative", datetime(2020, 5, 4), 3.0, None),
                ("validation-p", "positive", datetime(2020, 6, 1), 1.1, "failure-validation"),
                ("validation-n1", "negative", datetime(2020, 6, 2), 0.95, None),
                ("validation-n2", "negative", datetime(2020, 6, 3), 5.0, None),
                ("test", "positive", datetime(2020, 7, 1), test_value, "failure-test"),
            ],
            "loaded_cycle_id string, failure_horizon_status string, "
            "prediction_timestamp timestamp_ntz, "
            "motor_current_15m_mean_amperes double, matched_failure_record_id string",
        )
        .withColumn("modeling_row_status", F.lit("trainable"))
        .withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))
    )


@pytest.mark.spark
def test_directional_profile_reconciles_candidate_metrics_and_excludes_test(
    spark: SparkSession,
) -> None:
    comparison = collect_directional_baseline_comparison(_view(spark))
    candidates = {candidate.candidate_id: candidate for candidate in comparison.candidates}

    assert DIRECTIONAL_BASELINE_PROFILE_VERSION
    assert comparison.development_row_count == 7
    candidate = candidates["mean-current-at-most-1.2a"]
    partitions = {metrics.partition: metrics for metrics in candidate.partitions}

    assert partitions["train"].true_positive_count == 1
    assert partitions["train"].false_negative_count == 1
    assert partitions["train"].false_positive_count == 1
    assert partitions["train"].true_negative_count == 1
    assert partitions["train"].precision == 0.5
    assert partitions["train"].recall == 0.5
    assert partitions["validation"].true_positive_count == 1
    assert partitions["validation"].false_positive_count == 1
    assert partitions["validation"].represented_failure_event_count == 1


@pytest.mark.spark
def test_test_values_cannot_change_directional_development_comparison(
    spark: SparkSession,
) -> None:
    original = collect_directional_baseline_comparison(_view(spark, test_value=0.1))
    changed = collect_directional_baseline_comparison(_view(spark, test_value=1000.0))

    assert changed == original
