"""Construct and evaluate the frozen held-out alert policy in memory."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import PARTITION_TEST, get_selected_split_candidate
from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS, MODEL_TARGET_COLUMN
from railpulse.models.candidate_development_data import MODEL_CANDIDATE_KEY_COLUMN
from railpulse.models.development_alert_policy_selection import (
    DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
    get_selected_development_alert_policy,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
    get_development_model_selection,
)
from railpulse.models.sealed_test_evaluation_contract import (
    SEALED_TEST_EVALUATION_CONTRACT_VERSION,
    get_sealed_test_evaluation_contract,
)
from railpulse.models.selected_candidate_sealed_test import (
    SEALED_TEST_SCORE_COLUMN,
    SEALED_TEST_SCORE_COLUMNS,
    SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION,
    SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION,
    SelectedCandidateSealedTestScores,
)
from railpulse.models.validation_alert_episodes import (
    VALIDATION_ALERT_EPISODE_VERSION,
    ValidationAlertEpisodeError,
    ValidationAlertEpisodeResult,
    ValidationScorePoint,
    build_validation_alert_episodes,
)
from railpulse.models.validation_alert_evaluation import (
    VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
    ValidationAlertEvaluationError,
    ValidationAlertPolicyEvaluation,
    ValidationFailureEvent,
    evaluate_sealed_test_alert_policy,
)
from railpulse.models.validation_alert_policy_contract import VALIDATION_ALERT_POLICY_VERSION
from railpulse.validation.silver_failures import REJECTION_REASONS_FIELD

SELECTED_CANDIDATE_SEALED_TEST_ALERT_ADAPTER_VERSION = (
    "motor-current-sealed-test-alert-evaluation-adapter-v1"
)

_FAILURE_EVENT_COLUMNS = (
    "record_id",
    "failure_start",
    "failure_end",
    REJECTION_REASONS_FIELD,
)
_OPERATING_DAY_COLUMN = "_sealed_test_operating_day"


class SelectedCandidateSealedTestAlertEvaluationError(ValueError):
    """Raised when held-out scores cannot produce the frozen alert-policy evidence."""


@dataclass(frozen=True)
class SelectedCandidateSealedTestAlertEvaluation:
    """In-memory selected-policy episodes and aggregate held-out event evidence."""

    adapter_version: str
    contract_version: str
    episode_version: str
    evaluation_version: str
    evaluated_cycle_count: int
    operating_day_count: int
    accepted_failure_event_count: int
    scoring_result: SelectedCandidateSealedTestScores
    alert_result: ValidationAlertEpisodeResult
    policy_evaluation: ValidationAlertPolicyEvaluation


def _validate_scoring_result(result: SelectedCandidateSealedTestScores) -> None:
    contract = get_sealed_test_evaluation_contract()
    model_selection = get_development_model_selection()
    selected_evidence = next(
        evidence
        for evidence in model_selection.candidate_evidence
        if evidence.candidate_id == model_selection.selected_candidate_id
    )
    expected_training = (
        selected_evidence.train_row_count,
        selected_evidence.train_positive_count,
        selected_evidence.train_negative_count,
    )
    observed_training = (
        result.train_summary.row_count,
        result.train_summary.positive_count,
        result.train_summary.negative_count,
    )
    if (
        result.adapter_version != SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION
        or result.scoring_version != SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION
        or result.contract_version != SEALED_TEST_EVALUATION_CONTRACT_VERSION
        or result.contract_version != contract.contract_version
        or result.modeling_view_version != contract.modeling_view_version
        or result.split_candidate_id != contract.split_candidate_id
        or result.model_selection_version != DEVELOPMENT_MODEL_SELECTION_VERSION
        or result.model_candidate_id != DEVELOPMENT_SELECTED_CANDIDATE_ID
        or result.alert_policy_selection_version != DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION
        or result.alert_policy_candidate_id != DEVELOPMENT_SELECTED_ALERT_POLICY_ID
        or result.feature_columns != MODEL_INPUT_COLUMNS
        or result.train_summary.partition != contract.model_fit_partition
        or observed_training != expected_training
        or result.test_summary.partition != PARTITION_TEST
        or result.test_summary.row_count <= 0
        or result.test_summary.positive_count <= 0
        or result.test_summary.negative_count <= 0
        or result.test_summary.positive_count + result.test_summary.negative_count
        != result.test_summary.row_count
        or not isfinite(result.test_area_under_pr)
        or not 0.0 <= result.test_area_under_pr <= 1.0
        or not isfinite(result.fitted_intercept)
        or len(result.fitted_coefficients) != len(MODEL_INPUT_COLUMNS)
        or any(not isfinite(value) for value in result.fitted_coefficients)
        or result.completed_iterations < 0
    ):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test scoring evidence is incompatible with the frozen alert contract"
        )
    if tuple(result.scores.columns) != SEALED_TEST_SCORE_COLUMNS:
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test score columns do not match the frozen alert contract"
        )


def _collect_label_blind_points(
    result: SelectedCandidateSealedTestScores,
) -> tuple[ValidationScorePoint, ...]:
    split = get_selected_split_candidate()
    summary = result.scores.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(F.when(F.col(MODEL_TARGET_COLUMN), F.lit(1))).alias("positive_count"),
        F.min("prediction_timestamp").alias("minimum_prediction_timestamp"),
        F.count(
            F.when(
                F.col(SEALED_TEST_SCORE_COLUMN).isNull()
                | F.isnan(SEALED_TEST_SCORE_COLUMN)
                | (F.col(SEALED_TEST_SCORE_COLUMN) < 0.0)
                | (F.col(SEALED_TEST_SCORE_COLUMN) > 1.0),
                F.lit(1),
            )
        ).alias("invalid_score_count"),
    ).first()
    if (
        int(summary.row_count) != result.test_summary.row_count
        or int(summary.distinct_key_count) != result.test_summary.row_count
        or int(summary.positive_count) != result.test_summary.positive_count
        or summary.minimum_prediction_timestamp < split.test_start
        or int(summary.invalid_score_count) != 0
    ):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test score rows do not reconcile before label-blind construction"
        )

    identity_rows = (
        result.scores.select(
            "evaluation_partition",
            "sealed_test_contract_version",
            "model_selection_version",
            "model_candidate_id",
            "alert_policy_selection_version",
            "alert_policy_candidate_id",
            "split_candidate_id",
        )
        .distinct()
        .collect()
    )
    contract = get_sealed_test_evaluation_contract()
    if len(identity_rows) != 1:
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test score rows require one frozen identity"
        )
    identity = identity_rows[0]
    if (
        identity.evaluation_partition != PARTITION_TEST
        or identity.sealed_test_contract_version != contract.contract_version
        or identity.model_selection_version != DEVELOPMENT_MODEL_SELECTION_VERSION
        or identity.model_candidate_id != DEVELOPMENT_SELECTED_CANDIDATE_ID
        or identity.alert_policy_selection_version != DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION
        or identity.alert_policy_candidate_id != DEVELOPMENT_SELECTED_ALERT_POLICY_ID
        or identity.split_candidate_id != contract.split_candidate_id
    ):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test score identity does not match the frozen alert contract"
        )

    rows = (
        result.scores.select(
            MODEL_CANDIDATE_KEY_COLUMN,
            "prediction_timestamp",
            SEALED_TEST_SCORE_COLUMN,
        )
        .orderBy("prediction_timestamp", MODEL_CANDIDATE_KEY_COLUMN)
        .collect()
    )
    return tuple(
        ValidationScorePoint(
            cycle_id=row[MODEL_CANDIDATE_KEY_COLUMN],
            prediction_timestamp=row.prediction_timestamp,
            failure_probability=float(row[SEALED_TEST_SCORE_COLUMN]),
        )
        for row in rows
    )


def _collect_operating_day_count(result: SelectedCandidateSealedTestScores) -> int:
    day_rows = (
        result.scores.select(F.to_date(F.col("prediction_timestamp")).alias(_OPERATING_DAY_COLUMN))
        .distinct()
        .collect()
    )
    if not day_rows or any(row[_OPERATING_DAY_COLUMN] is None for row in day_rows):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test alert evaluation requires complete observed operating days"
        )
    return len(day_rows)


def _collect_failure_events(
    accepted_failure_events: DataFrame,
) -> tuple[ValidationFailureEvent, ...]:
    missing_columns = sorted(set(_FAILURE_EVENT_COLUMNS) - set(accepted_failure_events.columns))
    if missing_columns:
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Accepted failure events are missing sealed-test evaluation columns: "
            + ", ".join(missing_columns)
        )
    split = get_selected_split_candidate()
    rows = (
        accepted_failure_events.where(F.col("failure_start") >= F.lit(split.test_start))
        .select(*_FAILURE_EVENT_COLUMNS)
        .orderBy("failure_start", "record_id")
        .collect()
    )
    if any(
        row[REJECTION_REASONS_FIELD] is None or len(row[REJECTION_REASONS_FIELD]) != 0
        for row in rows
    ):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test alert evaluation requires accepted failure events"
        )
    try:
        return tuple(
            ValidationFailureEvent(
                failure_record_id=row.record_id,
                failure_start=row.failure_start,
                failure_end=row.failure_end,
            )
            for row in rows
        )
    except ValidationAlertEvaluationError as exc:
        raise SelectedCandidateSealedTestAlertEvaluationError(str(exc)) from exc


def evaluate_selected_candidate_sealed_test_alerts(
    result: SelectedCandidateSealedTestScores,
    accepted_failure_events: DataFrame,
) -> SelectedCandidateSealedTestAlertEvaluation:
    """Build the selected policy label-blind, then introduce held-out event evidence."""

    _validate_scoring_result(result)
    try:
        points = _collect_label_blind_points(result)
        selected_policy = get_selected_development_alert_policy()
        alert_result = build_validation_alert_episodes(points, selected_policy)
        operating_day_count = _collect_operating_day_count(result)
        failure_events = _collect_failure_events(accepted_failure_events)
        policy_evaluation = evaluate_sealed_test_alert_policy(
            alert_result,
            failure_events,
            operating_day_count=operating_day_count,
        )
    except (
        ValidationAlertEpisodeError,
        ValidationAlertEvaluationError,
    ) as exc:
        raise SelectedCandidateSealedTestAlertEvaluationError(str(exc)) from exc

    if (
        len(points) != result.test_summary.row_count
        or alert_result.episode_version != VALIDATION_ALERT_EPISODE_VERSION
        or alert_result.policy_version != VALIDATION_ALERT_POLICY_VERSION
        or alert_result.policy_candidate_id != DEVELOPMENT_SELECTED_ALERT_POLICY_ID
        or alert_result.evaluated_cycle_count != len(points)
        or policy_evaluation.evaluation_version != VALIDATION_ALERT_EVENT_EVALUATION_VERSION
        or policy_evaluation.policy_candidate_id != DEVELOPMENT_SELECTED_ALERT_POLICY_ID
        or policy_evaluation.operating_day_count != operating_day_count
        or policy_evaluation.accepted_failure_event_count != len(failure_events)
        or policy_evaluation.alert_episode_count != len(alert_result.episodes)
    ):
        raise SelectedCandidateSealedTestAlertEvaluationError(
            "Sealed-test alert evidence does not reconcile with the frozen contract"
        )

    return SelectedCandidateSealedTestAlertEvaluation(
        adapter_version=SELECTED_CANDIDATE_SEALED_TEST_ALERT_ADAPTER_VERSION,
        contract_version=SEALED_TEST_EVALUATION_CONTRACT_VERSION,
        episode_version=VALIDATION_ALERT_EPISODE_VERSION,
        evaluation_version=VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
        evaluated_cycle_count=len(points),
        operating_day_count=operating_day_count,
        accepted_failure_event_count=len(failure_events),
        scoring_result=result,
        alert_result=alert_result,
        policy_evaluation=policy_evaluation,
    )
