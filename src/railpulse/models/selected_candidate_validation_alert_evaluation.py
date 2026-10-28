"""Evaluate every guarded validation alert policy with governed June evidence."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import get_selected_split_candidate
from railpulse.evaluation.modeling_view_schema import MODEL_TIME_COLUMN
from railpulse.models.selected_candidate_validation import SelectedCandidateValidationScores
from railpulse.models.selected_candidate_validation_alerts import (
    SelectedCandidateValidationAlertCandidates,
    SelectedCandidateValidationAlertError,
    build_selected_candidate_validation_alerts,
)
from railpulse.models.validation_alert_evaluation import (
    VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
    ValidationAlertEvaluationError,
    ValidationAlertPolicyEvaluation,
    ValidationFailureEvent,
    evaluate_validation_alert_policy,
)
from railpulse.validation.silver_failures import REJECTION_REASONS_FIELD

SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_ADAPTER_VERSION = (
    "motor-current-validation-alert-evaluation-adapter-v1"
)

_FAILURE_EVENT_COLUMNS = (
    "record_id",
    "failure_start",
    "failure_end",
    REJECTION_REASONS_FIELD,
)
_OPERATING_DAY_COLUMN = "_validation_operating_day"


class SelectedCandidateValidationAlertEvaluationError(ValueError):
    """Raised when guarded validation evidence cannot evaluate every alert policy."""


@dataclass(frozen=True)
class SelectedCandidateValidationAlertEvaluation:
    """In-memory five-policy evaluation with compact guarded source evidence."""

    adapter_version: str
    evaluation_version: str
    evaluated_cycle_count: int
    operating_day_count: int
    accepted_failure_event_count: int
    alert_candidates: SelectedCandidateValidationAlertCandidates
    policy_evaluations: tuple[ValidationAlertPolicyEvaluation, ...]


def _collect_operating_day_count(result: SelectedCandidateValidationScores) -> int:
    day_rows = (
        result.scores.select(F.to_date(F.col(MODEL_TIME_COLUMN)).alias(_OPERATING_DAY_COLUMN))
        .distinct()
        .collect()
    )
    if not day_rows or any(row[_OPERATING_DAY_COLUMN] is None for row in day_rows):
        raise SelectedCandidateValidationAlertEvaluationError(
            "Validation alert evaluation requires complete observed operating days"
        )
    return len(day_rows)


def _collect_validation_failure_events(
    accepted_failure_events: DataFrame,
) -> tuple[ValidationFailureEvent, ...]:
    missing_columns = sorted(set(_FAILURE_EVENT_COLUMNS) - set(accepted_failure_events.columns))
    if missing_columns:
        raise SelectedCandidateValidationAlertEvaluationError(
            "Accepted failure events are missing validation-evaluation columns: "
            + ", ".join(missing_columns)
        )

    split = get_selected_split_candidate()
    rows = (
        accepted_failure_events.where(
            (F.col("failure_start") >= F.lit(split.validation_start))
            & (F.col("failure_start") < F.lit(split.test_start))
        )
        .select(*_FAILURE_EVENT_COLUMNS)
        .orderBy("failure_start", "record_id")
        .collect()
    )
    if any(
        row[REJECTION_REASONS_FIELD] is None or len(row[REJECTION_REASONS_FIELD]) != 0
        for row in rows
    ):
        raise SelectedCandidateValidationAlertEvaluationError(
            "Validation alert evaluation requires accepted failure events"
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
        raise SelectedCandidateValidationAlertEvaluationError(str(exc)) from exc


def evaluate_selected_candidate_validation_alerts(
    result: SelectedCandidateValidationScores,
    accepted_failure_events: DataFrame,
) -> SelectedCandidateValidationAlertEvaluation:
    """Construct alerts label-blind, then evaluate them with validation-only evidence."""

    try:
        alert_candidates = build_selected_candidate_validation_alerts(result)
        operating_day_count = _collect_operating_day_count(result)
        failure_events = _collect_validation_failure_events(accepted_failure_events)
        policy_evaluations = tuple(
            evaluate_validation_alert_policy(
                candidate_result,
                failure_events,
                operating_day_count=operating_day_count,
            )
            for candidate_result in alert_candidates.candidate_results
        )
    except (
        SelectedCandidateValidationAlertError,
        ValidationAlertEvaluationError,
    ) as exc:
        raise SelectedCandidateValidationAlertEvaluationError(str(exc)) from exc

    expected_candidate_ids = tuple(
        result.policy_candidate_id for result in alert_candidates.candidate_results
    )
    if tuple(
        item.policy_candidate_id for item in policy_evaluations
    ) != expected_candidate_ids or any(
        item.operating_day_count != operating_day_count
        or item.accepted_failure_event_count != len(failure_events)
        or item.alert_episode_count != len(candidate_result.episodes)
        for item, candidate_result in zip(
            policy_evaluations,
            alert_candidates.candidate_results,
            strict=True,
        )
    ):
        raise SelectedCandidateValidationAlertEvaluationError(
            "Validation alert policy evaluations do not reconcile with guarded candidates"
        )

    return SelectedCandidateValidationAlertEvaluation(
        adapter_version=SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_ADAPTER_VERSION,
        evaluation_version=VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
        evaluated_cycle_count=alert_candidates.evaluated_cycle_count,
        operating_day_count=operating_day_count,
        accepted_failure_event_count=len(failure_events),
        alert_candidates=alert_candidates,
        policy_evaluations=policy_evaluations,
    )
