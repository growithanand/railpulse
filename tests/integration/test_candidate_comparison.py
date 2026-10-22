"""Verify the development-only logistic candidate comparison."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_comparison import (
    ModelCandidateComparisonError,
    compare_model_candidates,
)
from railpulse.models.candidate_development_data import (
    prepare_model_candidate_development_data,
)


def _view(spark: SparkSession):
    rows = []
    for partition_start, prefix, row_count in (
        (datetime(2020, 5, 1), "train", 12),
        (datetime(2020, 6, 1), "validation", 8),
        (datetime(2020, 7, 1), "test", 4),
    ):
        for index in range(row_count):
            positive = index % 2 == 0
            context_signal = 100.0 if positive else 1.0
            rows.append(
                (
                    f"{prefix}-{index}",
                    partition_start + timedelta(hours=index),
                    "positive" if positive else "negative",
                    positive,
                    float(index % 3),
                    context_signal,
                )
            )
    return spark.createDataFrame(
        rows,
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, "
        "failure_horizon_status string, failure_within_horizon boolean, "
        "motor_signal double, context_signal double",
    ).select(
        "loaded_cycle_id",
        "prediction_timestamp",
        "failure_horizon_status",
        "failure_within_horizon",
        F.lit("trainable").alias("modeling_row_status"),
        F.lit(MODELING_VIEW_V2_VERSION).alias("modeling_view_version"),
        F.lit(100).cast("long").alias("motor_current_15m_observation_count"),
        F.col("motor_signal").alias("motor_current_15m_minimum_amperes"),
        (F.col("motor_signal") + 1.0).alias("motor_current_15m_mean_amperes"),
        (F.col("motor_signal") + 2.0).alias("motor_current_15m_maximum_amperes"),
        F.lit(10).cast("long").alias("motor_current_maximum_window_gap_seconds"),
        F.col("context_signal").cast("long").alias("cycle_context_current_duration_seconds"),
        (F.col("context_signal") + 1.0)
        .cast("long")
        .alias("cycle_context_previous_duration_seconds"),
        (F.col("context_signal") + 2.0).cast("long").alias("cycle_context_previous_idle_seconds"),
    )


@pytest.mark.spark
def test_comparison_fits_both_candidates_with_validation_only_evidence(
    spark: SparkSession,
) -> None:
    development = prepare_model_candidate_development_data(_view(spark))
    comparison = compare_model_candidates(development)

    assert comparison.comparison_version == "cycle-context-logistic-candidate-comparison-v1"
    assert comparison.modeling_view_version == MODELING_VIEW_V2_VERSION
    assert comparison.split_candidate_id == "validation-2020-06_test-2020-07"
    assert comparison.estimator == "logistic-regression"
    assert comparison.selection_metric == "area-under-precision-recall-curve"
    assert comparison.max_iterations == 100
    assert comparison.regularization == 0.1
    assert comparison.elastic_net == 0.0
    assert comparison.fit_intercept is True
    assert comparison.standardization is True
    assert comparison.family == "binomial"

    results = {result.candidate_id: result for result in comparison.candidates}
    assert tuple(result.candidate_id for result in comparison.candidates) == (
        "motor-current-only",
        "motor-current-plus-cycle-context",
    )
    assert set(results) == {"motor-current-only", "motor-current-plus-cycle-context"}
    for result in results.values():
        train_counts = (
            result.train_row_count,
            result.train_positive_count,
            result.train_negative_count,
        )
        assert train_counts == (
            12,
            6,
            6,
        )
        assert (
            result.validation_row_count,
            result.validation_positive_count,
            result.validation_negative_count,
        ) == (8, 4, 4)
        assert 0.0 <= result.validation_area_under_pr <= 1.0
        assert len(result.fitted_coefficients) == len(result.feature_columns)
        assert 0 <= result.completed_iterations <= 100
    assert (
        results["motor-current-plus-cycle-context"].validation_area_under_pr
        > results["motor-current-only"].validation_area_under_pr
    )


@pytest.mark.spark
def test_comparison_rejects_incompatible_contract_metadata(spark: SparkSession) -> None:
    development = prepare_model_candidate_development_data(_view(spark))
    incompatible = replace(development, comparison_version="unsupported")

    with pytest.raises(ModelCandidateComparisonError, match="incompatible"):
        compare_model_candidates(incompatible)
