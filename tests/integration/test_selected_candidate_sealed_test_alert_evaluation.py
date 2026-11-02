"""Verify selected-policy held-out evaluation with synthetic scores and events."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.selected_candidate_sealed_test import (
    SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION,
    SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION,
    SealedTestPartitionSummary,
    SelectedCandidateSealedTestScores,
)
from railpulse.models.selected_candidate_sealed_test_alert_evaluation import (
    SELECTED_CANDIDATE_SEALED_TEST_ALERT_ADAPTER_VERSION,
    SelectedCandidateSealedTestAlertEvaluationError,
    evaluate_selected_candidate_sealed_test_alerts,
)
from railpulse.models.validation_alert_policy_contract import (
    get_validation_alert_policy_candidate,
)


def _scores(spark: SparkSession) -> SelectedCandidateSealedTestScores:
    threshold = get_validation_alert_policy_candidate(
        "p95-two-consecutive-cycles"
    ).minimum_failure_probability
    rows = (
        ("test-1", datetime(2020, 7, 10, 8, 0), True, threshold * 1.01),
        ("test-2", datetime(2020, 7, 10, 8, 1), True, threshold * 1.01),
        ("test-3", datetime(2020, 7, 10, 8, 2), False, threshold / 2),
        ("test-4", datetime(2020, 7, 10, 12, 0), False, threshold / 2),
    )
    frame = spark.createDataFrame(
        rows,
        "loaded_cycle_id string, prediction_timestamp timestamp_ntz, "
        "failure_within_horizon boolean, failure_probability double",
    ).select(
        "loaded_cycle_id",
        "prediction_timestamp",
        "failure_within_horizon",
        "failure_probability",
        F.lit("test").alias("evaluation_partition"),
        F.lit("motor-current-sealed-test-evaluation-contract-v1").alias(
            "sealed_test_contract_version"
        ),
        F.lit("motor-current-logistic-development-selection-v1").alias("model_selection_version"),
        F.lit("motor-current-only").alias("model_candidate_id"),
        F.lit("motor-current-validation-alert-policy-selection-v1").alias(
            "alert_policy_selection_version"
        ),
        F.lit("p95-two-consecutive-cycles").alias("alert_policy_candidate_id"),
        F.lit("validation-2020-06_test-2020-07").alias("split_candidate_id"),
    )
    return SelectedCandidateSealedTestScores(
        adapter_version=SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION,
        scoring_version=SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION,
        contract_version="motor-current-sealed-test-evaluation-contract-v1",
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        split_candidate_id="validation-2020-06_test-2020-07",
        model_selection_version="motor-current-logistic-development-selection-v1",
        model_candidate_id="motor-current-only",
        alert_policy_selection_version="motor-current-validation-alert-policy-selection-v1",
        alert_policy_candidate_id="p95-two-consecutive-cycles",
        feature_columns=MODEL_INPUT_COLUMNS,
        train_summary=SealedTestPartitionSummary("train", 6_325, 4, 6_321),
        test_summary=SealedTestPartitionSummary("test", 4, 2, 2),
        test_area_under_pr=0.5,
        fitted_intercept=-7.0,
        fitted_coefficients=(0.1, 0.2, 0.3, 0.4, 0.5),
        completed_iterations=10,
        scores=frame,
    )


def _accepted_failures(spark: SparkSession):
    return spark.createDataFrame(
        (
            (
                "failure-june",
                datetime(2020, 6, 5, 10),
                datetime(2020, 6, 5, 11),
                [],
            ),
            (
                "failure-july",
                datetime(2020, 7, 10, 9),
                datetime(2020, 7, 10, 10),
                [],
            ),
        ),
        "record_id string, failure_start timestamp_ntz, failure_end timestamp_ntz, "
        "rejection_reasons array<string>",
    )


@pytest.mark.spark
def test_adapter_builds_selected_policy_before_synthetic_event_evaluation(
    spark: SparkSession,
) -> None:
    result = evaluate_selected_candidate_sealed_test_alerts(
        _scores(spark),
        _accepted_failures(spark),
    )

    assert result.adapter_version == SELECTED_CANDIDATE_SEALED_TEST_ALERT_ADAPTER_VERSION
    assert result.evaluated_cycle_count == 4
    assert result.operating_day_count == 1
    assert result.accepted_failure_event_count == 1
    assert result.alert_result.policy_candidate_id == "p95-two-consecutive-cycles"
    assert result.alert_result.evaluated_cycle_count == 4
    assert len(result.alert_result.episodes) == 1
    assert all(
        not hasattr(episode, "failure_within_horizon") for episode in result.alert_result.episodes
    )
    evaluation = result.policy_evaluation
    assert evaluation.detected_failure_event_count == 1
    assert evaluation.missed_failure_event_count == 0
    assert evaluation.warning_episode_count == 1
    assert evaluation.false_alert_episode_count == 0
    assert evaluation.event_evaluations[0].failure_record_id == "failure-july"
    assert evaluation.event_evaluations[0].warning_lead_time_seconds == 3_540


@pytest.mark.spark
def test_adapter_rejects_changed_score_identity_and_counts(spark: SparkSession) -> None:
    scores = _scores(spark)
    changed_identity = scores.scores.withColumn(
        "alert_policy_candidate_id",
        F.lit("p99-single-cycle"),
    )
    with pytest.raises(
        SelectedCandidateSealedTestAlertEvaluationError,
        match="score identity",
    ):
        evaluate_selected_candidate_sealed_test_alerts(
            replace(scores, scores=changed_identity),
            _accepted_failures(spark),
        )

    with pytest.raises(
        SelectedCandidateSealedTestAlertEvaluationError,
        match="score rows do not reconcile",
    ):
        evaluate_selected_candidate_sealed_test_alerts(
            replace(scores, test_summary=SealedTestPartitionSummary("test", 5, 2, 3)),
            _accepted_failures(spark),
        )


@pytest.mark.spark
def test_adapter_rejects_rejected_or_missing_synthetic_test_event(
    spark: SparkSession,
) -> None:
    failures = _accepted_failures(spark)
    rejected = failures.withColumn(
        "rejection_reasons",
        F.when(
            F.col("record_id") == "failure-july",
            F.array(F.lit("not_accepted")),
        ).otherwise(F.col("rejection_reasons")),
    )
    with pytest.raises(
        SelectedCandidateSealedTestAlertEvaluationError,
        match="accepted failure events",
    ):
        evaluate_selected_candidate_sealed_test_alerts(_scores(spark), rejected)

    without_test_event = failures.where(F.col("record_id") != "failure-july")
    with pytest.raises(
        SelectedCandidateSealedTestAlertEvaluationError,
        match="exactly one",
    ):
        evaluate_selected_candidate_sealed_test_alerts(_scores(spark), without_test_event)
