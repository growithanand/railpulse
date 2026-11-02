"""Verify sealed-test scoring with synthetic held-out rows only."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.selected_candidate_sealed_test import (
    SEALED_TEST_SCORE_COLUMNS,
    SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION,
    SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION,
    SelectedCandidateSealedTestError,
    score_selected_candidate_sealed_test,
)


def _partition_rows(
    *,
    prefix: str,
    start: datetime,
    row_count: int,
    positive_count: int,
) -> list[tuple[str, datetime, str, bool, float]]:
    rows = []
    for index in range(row_count):
        positive = index < positive_count
        signal = 20.0 + index / row_count if positive else float(index % 17)
        rows.append(
            (
                f"{prefix}-{index}",
                start + timedelta(seconds=index),
                "positive" if positive else "negative",
                positive,
                signal,
            )
        )
    return rows


def _view(spark: SparkSession):
    rows = [
        *_partition_rows(
            prefix="train",
            start=datetime(2020, 5, 1),
            row_count=6_325,
            positive_count=4,
        ),
        ("validation-ignored", datetime(2020, 6, 1), "unsupported", True, float("nan")),
        *_partition_rows(
            prefix="synthetic-test",
            start=datetime(2020, 7, 1),
            row_count=40,
            positive_count=2,
        ),
    ]
    return spark.createDataFrame(
        rows,
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, "
        "failure_horizon_status string, failure_within_horizon boolean, signal double",
    ).select(
        "loaded_cycle_id",
        "prediction_timestamp",
        "failure_horizon_status",
        "failure_within_horizon",
        F.lit("trainable").alias("modeling_row_status"),
        F.lit(MODELING_VIEW_V2_VERSION).alias("modeling_view_version"),
        F.lit(100).cast("long").alias("motor_current_15m_observation_count"),
        F.col("signal").alias("motor_current_15m_minimum_amperes"),
        (F.col("signal") + 1.0).alias("motor_current_15m_mean_amperes"),
        (F.col("signal") + 2.0).alias("motor_current_15m_maximum_amperes"),
        F.lit(10).cast("long").alias("motor_current_maximum_window_gap_seconds"),
    )


@pytest.mark.spark
def test_adapter_fits_training_and_scores_only_synthetic_test_rows(
    spark: SparkSession,
) -> None:
    result = score_selected_candidate_sealed_test(_view(spark))

    assert result.adapter_version == SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION
    assert result.scoring_version == SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION
    assert result.contract_version == "motor-current-sealed-test-evaluation-contract-v1"
    assert result.model_candidate_id == "motor-current-only"
    assert result.alert_policy_candidate_id == "p95-two-consecutive-cycles"
    assert result.feature_columns == MODEL_INPUT_COLUMNS
    assert (result.train_summary.row_count, result.train_summary.positive_count) == (6_325, 4)
    assert (result.test_summary.row_count, result.test_summary.positive_count) == (40, 2)
    assert result.scores.columns == list(SEALED_TEST_SCORE_COLUMNS)
    assert result.scores.count() == 40
    assert result.scores.where(F.col("loaded_cycle_id") == "validation-ignored").count() == 0
    assert result.scores.select("evaluation_partition").distinct().first()[0] == "test"
    assert result.scores.select("sealed_test_contract_version").distinct().first()[0] == (
        "motor-current-sealed-test-evaluation-contract-v1"
    )
    assert 0.0 <= result.test_area_under_pr <= 1.0
    assert len(result.fitted_coefficients) == len(MODEL_INPUT_COLUMNS)
    score_range = result.scores.agg(
        F.min("failure_probability").alias("minimum"),
        F.max("failure_probability").alias("maximum"),
    ).first()
    assert 0.0 <= score_range.minimum <= score_range.maximum <= 1.0


@pytest.mark.spark
def test_adapter_rejects_changed_training_population_and_cross_partition_keys(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    reduced_training = view.where(F.col("loaded_cycle_id") != "train-0")
    with pytest.raises(SelectedCandidateSealedTestError, match="training population"):
        score_selected_candidate_sealed_test(reduced_training)

    duplicate_key = view.withColumn(
        "loaded_cycle_id",
        F.when(
            F.col("loaded_cycle_id") == "synthetic-test-0",
            F.lit("train-0"),
        ).otherwise(F.col("loaded_cycle_id")),
    )
    with pytest.raises(SelectedCandidateSealedTestError, match="globally unique"):
        score_selected_candidate_sealed_test(duplicate_key)


@pytest.mark.spark
def test_adapter_rejects_invalid_test_values_and_missing_columns(spark: SparkSession) -> None:
    view = _view(spark)
    invalid_test = view.withColumn(
        "motor_current_15m_mean_amperes",
        F.when(
            F.col("loaded_cycle_id") == "synthetic-test-0",
            F.lit(float("nan")),
        ).otherwise(F.col("motor_current_15m_mean_amperes")),
    )
    with pytest.raises(SelectedCandidateSealedTestError, match="test rows contain incompatible"):
        score_selected_candidate_sealed_test(invalid_test)

    with pytest.raises(SelectedCandidateSealedTestError, match="missing sealed-test"):
        score_selected_candidate_sealed_test(view.drop("failure_horizon_status"))
