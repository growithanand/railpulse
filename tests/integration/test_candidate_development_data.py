"""Verify leakage-safe preparation of model candidate development rows."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_development_data import (
    MODEL_CANDIDATE_DEVELOPMENT_COLUMNS,
    ModelCandidateDevelopmentDataError,
    prepare_model_candidate_development_data,
)


def _view(spark: SparkSession, *, test_value: float = 50.0, test_label: bool = True):
    rows = [
        ("train-p", datetime(2020, 5, 1), "positive", True, 1.0),
        ("train-n", datetime(2020, 5, 2), "negative", False, 2.0),
        ("validation-p", datetime(2020, 6, 1), "positive", True, 3.0),
        ("validation-n", datetime(2020, 6, 2), "negative", False, 4.0),
        (
            "test",
            datetime(2020, 7, 1),
            "positive" if test_label else "negative",
            test_label,
            test_value,
        ),
    ]
    return spark.createDataFrame(
        rows,
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, "
        "failure_horizon_status string, failure_within_horizon boolean, seed double",
    ).select(
        "loaded_cycle_id",
        "prediction_timestamp",
        "failure_horizon_status",
        "failure_within_horizon",
        F.lit("trainable").alias("modeling_row_status"),
        F.lit(MODELING_VIEW_V2_VERSION).alias("modeling_view_version"),
        F.lit(100).cast("long").alias("motor_current_15m_observation_count"),
        F.col("seed").alias("motor_current_15m_minimum_amperes"),
        (F.col("seed") + 1.0).alias("motor_current_15m_mean_amperes"),
        (F.col("seed") + 2.0).alias("motor_current_15m_maximum_amperes"),
        F.lit(10).cast("long").alias("motor_current_maximum_window_gap_seconds"),
        (F.col("seed") * 10).cast("long").alias("cycle_context_current_duration_seconds"),
        (F.col("seed") * 11).cast("long").alias("cycle_context_previous_duration_seconds"),
        (F.col("seed") * 12).cast("long").alias("cycle_context_previous_idle_seconds"),
    )


@pytest.mark.spark
def test_preparation_returns_only_auditable_train_and_validation_rows(
    spark: SparkSession,
) -> None:
    prepared = prepare_model_candidate_development_data(_view(spark))

    assert prepared.comparison_version == "cycle-context-logistic-candidate-comparison-v1"
    assert prepared.modeling_view_version == MODELING_VIEW_V2_VERSION
    assert prepared.split_candidate_id == "validation-2020-06_test-2020-07"
    assert prepared.train.columns == list(MODEL_CANDIDATE_DEVELOPMENT_COLUMNS)
    assert prepared.validation.columns == list(MODEL_CANDIDATE_DEVELOPMENT_COLUMNS)
    assert {row.loaded_cycle_id for row in prepared.train.collect()} == {"train-p", "train-n"}
    assert {row.loaded_cycle_id for row in prepared.validation.collect()} == {
        "validation-p",
        "validation-n",
    }
    assert [summary.partition for summary in prepared.summaries] == ["train", "validation"]
    assert [(summary.positive_count, summary.negative_count) for summary in prepared.summaries] == [
        (1, 1),
        (1, 1),
    ]


@pytest.mark.spark
def test_test_values_cannot_change_prepared_development_data(spark: SparkSession) -> None:
    original = prepare_model_candidate_development_data(_view(spark))
    changed = prepare_model_candidate_development_data(
        _view(spark, test_value=float("nan"), test_label=False)
    )

    assert changed.summaries == original.summaries
    assert changed.train.collect() == original.train.collect()
    assert changed.validation.collect() == original.validation.collect()


@pytest.mark.spark
def test_preparation_rejects_missing_or_invalid_development_values(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    with pytest.raises(ModelCandidateDevelopmentDataError, match="missing candidate-development"):
        prepare_model_candidate_development_data(view.drop("cycle_context_previous_idle_seconds"))

    invalid_version = view.withColumn(
        "modeling_view_version",
        F.when(F.col("loaded_cycle_id") == "train-p", F.lit("unsupported")).otherwise(
            F.col("modeling_view_version")
        ),
    )
    with pytest.raises(ModelCandidateDevelopmentDataError, match="incompatible version"):
        prepare_model_candidate_development_data(invalid_version)

    invalid_feature = view.withColumn(
        "motor_current_15m_mean_amperes",
        F.when(F.col("loaded_cycle_id") == "train-p", F.lit(float("nan"))).otherwise(
            F.col("motor_current_15m_mean_amperes")
        ),
    )
    with pytest.raises(ModelCandidateDevelopmentDataError, match="incompatible version"):
        prepare_model_candidate_development_data(invalid_feature)

    invalid_label_status = view.withColumn(
        "failure_horizon_status",
        F.when(F.col("loaded_cycle_id") == "train-p", F.lit(None).cast("string")).otherwise(
            F.col("failure_horizon_status")
        ),
    )
    with pytest.raises(ModelCandidateDevelopmentDataError, match="incompatible version"):
        prepare_model_candidate_development_data(invalid_label_status)


@pytest.mark.spark
def test_preparation_requires_unique_keys_and_both_labels_in_each_partition(
    spark: SparkSession,
) -> None:
    view = _view(spark)
    duplicate = view.unionByName(view.where(F.col("loaded_cycle_id") == "train-p"))
    with pytest.raises(ModelCandidateDevelopmentDataError, match="unique cycle IDs"):
        prepare_model_candidate_development_data(duplicate)

    missing_validation_positive = view.where(F.col("loaded_cycle_id") != "validation-p")
    with pytest.raises(ModelCandidateDevelopmentDataError, match="both target labels"):
        prepare_model_candidate_development_data(missing_validation_positive)
