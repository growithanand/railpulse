"""Verify guarded validation scoring for the retained development candidate."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_development_data import prepare_model_candidate_development_data
from railpulse.models.selected_candidate_validation import (
    SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION,
    VALIDATION_SCORE_COLUMNS,
    SelectedCandidateValidationError,
    score_selected_candidate_validation,
)
from railpulse.models.selected_candidate_validation_alert_evaluation import (
    SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_ADAPTER_VERSION,
    SelectedCandidateValidationAlertEvaluationError,
    evaluate_selected_candidate_validation_alerts,
)
from railpulse.models.selected_candidate_validation_alerts import (
    SELECTED_CANDIDATE_VALIDATION_ALERT_ADAPTER_VERSION,
    SelectedCandidateValidationAlertError,
    build_selected_candidate_validation_alerts,
)
from railpulse.models.selected_candidate_validation_profile import (
    SCORE_POPULATION_ORDER,
    SELECTED_CANDIDATE_VALIDATION_PROFILE_VERSION,
)
from railpulse.models.validation_alert_policy_contract import VALIDATION_ALERT_POLICY_CANDIDATES


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
        *_partition_rows(
            prefix="validation",
            start=datetime(2020, 6, 1),
            row_count=4_730,
            positive_count=5,
        ),
        ("test", datetime(2020, 7, 1), "positive", True, float("nan")),
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
        F.lit(100).cast("long").alias("cycle_context_current_duration_seconds"),
        F.lit(90).cast("long").alias("cycle_context_previous_duration_seconds"),
        F.lit(20).cast("long").alias("cycle_context_previous_idle_seconds"),
    )


def _accepted_failures(spark: SparkSession):
    return spark.createDataFrame(
        (
            (
                "failure-april",
                datetime(2020, 4, 18),
                datetime(2020, 4, 18, 23, 59),
                [],
            ),
            (
                "failure-june",
                datetime(2020, 6, 1, 2),
                datetime(2020, 6, 1, 3),
                [],
            ),
            (
                "failure-july",
                datetime(2020, 7, 15, 14, 30),
                datetime(2020, 7, 15, 19),
                [],
            ),
        ),
        "record_id string, failure_start timestamp_ntz, failure_end timestamp_ntz, "
        "rejection_reasons array<string>",
    )


@pytest.mark.spark
def test_scoring_exposes_only_reconciled_validation_rows(spark: SparkSession) -> None:
    development = prepare_model_candidate_development_data(_view(spark))
    result = score_selected_candidate_validation(development)

    assert result.scoring_version == SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION
    assert result.selection_version == "motor-current-logistic-development-selection-v1"
    assert result.candidate_id == "motor-current-only"
    assert result.feature_columns == MODEL_INPUT_COLUMNS
    assert result.train_summary.row_count == 6_325
    assert result.validation_summary.row_count == 4_730
    assert result.scores.columns == list(VALIDATION_SCORE_COLUMNS)
    assert result.scores.count() == 4_730
    assert result.scores.where(F.col("loaded_cycle_id") == "test").count() == 0
    assert result.scores.select("evaluation_partition").distinct().first()[0] == "validation"
    score_range = result.scores.agg(
        F.min("failure_probability").alias("minimum"),
        F.max("failure_probability").alias("maximum"),
    ).first()
    assert 0.0 <= score_range.minimum <= score_range.maximum <= 1.0
    assert 0.0 <= result.validation_area_under_pr <= 1.0
    assert len(result.fitted_coefficients) == len(MODEL_INPUT_COLUMNS)

    alert_evaluation = evaluate_selected_candidate_validation_alerts(
        result,
        _accepted_failures(spark),
    )
    alert_candidates = alert_evaluation.alert_candidates
    profile = alert_candidates.profile
    assert (
        alert_evaluation.adapter_version
        == SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_ADAPTER_VERSION
    )
    assert (
        alert_evaluation.evaluation_version == "motor-current-validation-alert-event-evaluation-v1"
    )
    assert alert_evaluation.evaluated_cycle_count == 4_730
    assert alert_evaluation.operating_day_count == 1
    assert alert_evaluation.accepted_failure_event_count == 1
    assert tuple(item.policy_candidate_id for item in alert_evaluation.policy_evaluations) == tuple(
        candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
    )
    assert all(
        item.operating_day_count == 1
        and item.accepted_failure_event_count == 1
        and item.alert_episode_count
        == item.warning_episode_count
        + item.inside_failure_episode_count
        + item.false_alert_episode_count
        and item.event_evaluations[0].failure_record_id == "failure-june"
        for item in alert_evaluation.policy_evaluations
    )
    assert alert_candidates.adapter_version == SELECTED_CANDIDATE_VALIDATION_ALERT_ADAPTER_VERSION
    assert alert_candidates.evaluated_cycle_count == 4_730
    assert tuple(item.policy_candidate_id for item in alert_candidates.candidate_results) == tuple(
        candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
    )
    assert all(item.evaluated_cycle_count == 4_730 for item in alert_candidates.candidate_results)
    assert all(
        not hasattr(episode, "failure_within_horizon")
        for item in alert_candidates.candidate_results
        for episode in item.episodes
    )
    assert profile.profile_version == SELECTED_CANDIDATE_VALIDATION_PROFILE_VERSION
    assert profile.validation_row_count == 4_730
    assert profile.validation_positive_count == 5
    assert profile.validation_negative_count == 4_725
    assert profile.validation_positive_rate == pytest.approx(5 / 4_730)
    assert tuple(item.population for item in profile.score_distributions) == SCORE_POPULATION_ORDER
    assert tuple(item.row_count for item in profile.score_distributions) == (4_730, 5, 4_725)
    for distribution in profile.score_distributions:
        assert 0.0 <= distribution.minimum_score <= distribution.median_score
        assert distribution.median_score <= distribution.maximum_score <= 1.0

    changed_identity = result.scores.withColumn(
        "model_candidate_id",
        F.lit("unreviewed-candidate"),
    )
    with pytest.raises(SelectedCandidateValidationAlertError, match="identity"):
        build_selected_candidate_validation_alerts(replace(result, scores=changed_identity))

    rejected_june = _accepted_failures(spark).withColumn(
        "rejection_reasons",
        F.when(
            F.col("record_id") == "failure-june",
            F.array(F.lit("not_accepted")),
        ).otherwise(F.col("rejection_reasons")),
    )
    with pytest.raises(
        SelectedCandidateValidationAlertEvaluationError,
        match="accepted failure events",
    ):
        evaluate_selected_candidate_validation_alerts(result, rejected_june)


@pytest.mark.spark
def test_scoring_rejects_changed_population_or_test_timestamp(spark: SparkSession) -> None:
    development = prepare_model_candidate_development_data(_view(spark))
    reduced_validation = development.validation.where(F.col("loaded_cycle_id") != "validation-0")
    with pytest.raises(SelectedCandidateValidationError, match="population"):
        score_selected_candidate_validation(replace(development, validation=reduced_validation))

    test_timestamp = development.validation.withColumn(
        "prediction_timestamp",
        F.when(
            F.col("loaded_cycle_id") == "validation-0",
            F.lit(datetime(2020, 7, 1)),
        ).otherwise(F.col("prediction_timestamp")),
    )
    with pytest.raises(SelectedCandidateValidationError, match="chronological"):
        score_selected_candidate_validation(replace(development, validation=test_timestamp))


@pytest.mark.spark
def test_scoring_rejects_incompatible_contract_metadata(spark: SparkSession) -> None:
    development = prepare_model_candidate_development_data(_view(spark))

    with pytest.raises(SelectedCandidateValidationError, match="incompatible"):
        score_selected_candidate_validation(replace(development, comparison_version="unsupported"))
