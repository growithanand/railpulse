"""Evaluate validation alert episodes against accepted failure intervals."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from railpulse.evaluation.chronological_splits import get_selected_split_candidate
from railpulse.features.failure_horizons import DEFAULT_FAILURE_HORIZON_SECONDS
from railpulse.models.validation_alert_episodes import (
    VALIDATION_ALERT_EPISODE_VERSION,
    ValidationAlertEpisode,
    ValidationAlertEpisodeResult,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_VERSION,
    get_validation_alert_policy_candidate,
)

VALIDATION_ALERT_EVENT_EVALUATION_VERSION = "motor-current-validation-alert-event-evaluation-v1"


class ValidationAlertEvaluationError(ValueError):
    """Raised when validation alert episodes cannot be evaluated safely."""


@dataclass(frozen=True)
class ValidationFailureEvent:
    """Minimum accepted failure-event evidence allowed into validation evaluation."""

    failure_record_id: str
    failure_start: datetime
    failure_end: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.failure_record_id, str) or not self.failure_record_id.strip():
            raise ValidationAlertEvaluationError("Validation failure record ID must be nonempty")
        for name, value in (
            ("start", self.failure_start),
            ("end", self.failure_end),
        ):
            if not isinstance(value, datetime):
                raise ValidationAlertEvaluationError(
                    f"Validation failure {name} must be a datetime"
                )
            if value.tzinfo is not None:
                raise ValidationAlertEvaluationError(
                    f"Validation failure {name} must be timezone-free"
                )
        if self.failure_end < self.failure_start:
            raise ValidationAlertEvaluationError(
                "Validation failure end must not precede its start"
            )


@dataclass(frozen=True)
class ValidationFailureEventEvaluation:
    """Detection and warning evidence for one accepted validation failure event."""

    failure_record_id: str
    failure_start: datetime
    failure_end: datetime
    detected: bool
    warning_episode_indices: tuple[int, ...]
    inside_failure_episode_indices: tuple[int, ...]
    first_warning_timestamp: datetime | None
    warning_lead_time_seconds: int | None


@dataclass(frozen=True)
class ValidationAlertPolicyEvaluation:
    """Reconciled event detection and alert burden for one validation policy."""

    evaluation_version: str
    episode_version: str
    policy_version: str
    policy_candidate_id: str
    warning_horizon_seconds: int
    operating_day_count: int
    accepted_failure_event_count: int
    detected_failure_event_count: int
    missed_failure_event_count: int
    event_detection_rate: float
    alert_episode_count: int
    warning_episode_count: int
    inside_failure_episode_count: int
    false_alert_episode_count: int
    alert_episodes_per_operating_day: float
    false_alert_episodes_per_operating_day: float
    event_evaluations: tuple[ValidationFailureEventEvaluation, ...]


def _validate_operating_day_count(operating_day_count: int) -> None:
    split = get_selected_split_candidate()
    maximum_days = (split.test_start.date() - split.validation_start.date()).days
    if (
        isinstance(operating_day_count, bool)
        or not isinstance(operating_day_count, int)
        or not 1 <= operating_day_count <= maximum_days
    ):
        raise ValidationAlertEvaluationError(
            "Validation operating-day count must be a positive integer within the selected period"
        )


def _validate_events(
    events: Sequence[ValidationFailureEvent],
) -> tuple[ValidationFailureEvent, ...]:
    if len(events) != 1:
        raise ValidationAlertEvaluationError(
            "Validation alert evaluation requires exactly one accepted failure event"
        )
    ordered = tuple(
        sorted(events, key=lambda event: (event.failure_start, event.failure_record_id))
    )
    if len({event.failure_record_id for event in ordered}) != len(ordered):
        raise ValidationAlertEvaluationError("Validation failure events require unique record IDs")

    split = get_selected_split_candidate()
    horizon = timedelta(seconds=DEFAULT_FAILURE_HORIZON_SECONDS)
    for event in ordered:
        if (
            event.failure_start - horizon < split.validation_start
            or event.failure_end >= split.test_start
        ):
            raise ValidationAlertEvaluationError(
                "Validation failure event and complete warning window must stay inside validation"
            )
    return ordered


def _validate_episode_result(result: ValidationAlertEpisodeResult) -> None:
    if (
        result.episode_version != VALIDATION_ALERT_EPISODE_VERSION
        or result.policy_version != VALIDATION_ALERT_POLICY_VERSION
    ):
        raise ValidationAlertEvaluationError(
            "Validation alert evaluation requires the current episode and policy versions"
        )
    try:
        get_validation_alert_policy_candidate(result.policy_candidate_id)
    except ValueError as exc:
        raise ValidationAlertEvaluationError(str(exc)) from exc
    if result.evaluated_cycle_count <= 0:
        raise ValidationAlertEvaluationError(
            "Validation alert evaluation requires evaluated validation cycles"
        )
    if result.merged_episode_count - result.cooldown_suppressed_episode_count != len(
        result.episodes
    ):
        raise ValidationAlertEvaluationError(
            "Validation alert episode counts do not reconcile before evaluation"
        )

    split = get_selected_split_candidate()
    previous: ValidationAlertEpisode | None = None
    for expected_index, episode in enumerate(result.episodes, start=1):
        if (
            episode.episode_index != expected_index
            or episode.policy_candidate_id != result.policy_candidate_id
        ):
            raise ValidationAlertEvaluationError(
                "Validation alert episodes require ordered indices and one policy identity"
            )
        if (
            episode.start_timestamp.tzinfo is not None
            or episode.end_timestamp.tzinfo is not None
            or episode.end_timestamp < episode.start_timestamp
            or episode.start_timestamp < split.validation_start
            or episode.end_timestamp >= split.test_start
        ):
            raise ValidationAlertEvaluationError(
                "Validation alert episodes must stay ordered inside the selected validation period"
            )
        if previous is not None and episode.start_timestamp <= previous.end_timestamp:
            raise ValidationAlertEvaluationError(
                "Validation alert episodes must be non-overlapping and chronological"
            )
        previous = episode


def _event_evaluation(
    event: ValidationFailureEvent,
    episodes: tuple[ValidationAlertEpisode, ...],
) -> ValidationFailureEventEvaluation:
    window_start = event.failure_start - timedelta(seconds=DEFAULT_FAILURE_HORIZON_SECONDS)
    warning_episodes = tuple(
        episode
        for episode in episodes
        if window_start <= episode.start_timestamp < event.failure_start
    )
    inside_failure_episodes = tuple(
        episode
        for episode in episodes
        if event.failure_start <= episode.start_timestamp <= event.failure_end
    )
    first_warning = warning_episodes[0] if warning_episodes else None
    return ValidationFailureEventEvaluation(
        failure_record_id=event.failure_record_id,
        failure_start=event.failure_start,
        failure_end=event.failure_end,
        detected=first_warning is not None,
        warning_episode_indices=tuple(episode.episode_index for episode in warning_episodes),
        inside_failure_episode_indices=tuple(
            episode.episode_index for episode in inside_failure_episodes
        ),
        first_warning_timestamp=(
            first_warning.start_timestamp if first_warning is not None else None
        ),
        warning_lead_time_seconds=(
            int((event.failure_start - first_warning.start_timestamp).total_seconds())
            if first_warning is not None
            else None
        ),
    )


def evaluate_validation_alert_policy(
    result: ValidationAlertEpisodeResult,
    events: Sequence[ValidationFailureEvent],
    *,
    operating_day_count: int,
) -> ValidationAlertPolicyEvaluation:
    """Compare one label-blind policy result with validation-only failure events."""

    _validate_episode_result(result)
    _validate_operating_day_count(operating_day_count)
    validated_events = _validate_events(events)
    event_evaluations = tuple(
        _event_evaluation(event, result.episodes) for event in validated_events
    )

    warning_indices = {
        index for event in event_evaluations for index in event.warning_episode_indices
    }
    inside_failure_indices = {
        index for event in event_evaluations for index in event.inside_failure_episode_indices
    }
    if warning_indices.intersection(inside_failure_indices):
        raise ValidationAlertEvaluationError(
            "Validation alert episode classification must be mutually exclusive"
        )

    episode_indices = {episode.episode_index for episode in result.episodes}
    false_alert_indices = episode_indices - warning_indices - inside_failure_indices
    detected_count = sum(event.detected for event in event_evaluations)
    event_count = len(event_evaluations)
    alert_count = len(result.episodes)
    if len(warning_indices) + len(inside_failure_indices) + len(false_alert_indices) != alert_count:
        raise ValidationAlertEvaluationError(
            "Validation alert episode classifications do not reconcile"
        )

    return ValidationAlertPolicyEvaluation(
        evaluation_version=VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
        episode_version=result.episode_version,
        policy_version=result.policy_version,
        policy_candidate_id=result.policy_candidate_id,
        warning_horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        operating_day_count=operating_day_count,
        accepted_failure_event_count=event_count,
        detected_failure_event_count=detected_count,
        missed_failure_event_count=event_count - detected_count,
        event_detection_rate=detected_count / event_count,
        alert_episode_count=alert_count,
        warning_episode_count=len(warning_indices),
        inside_failure_episode_count=len(inside_failure_indices),
        false_alert_episode_count=len(false_alert_indices),
        alert_episodes_per_operating_day=alert_count / operating_day_count,
        false_alert_episodes_per_operating_day=(len(false_alert_indices) / operating_day_count),
        event_evaluations=event_evaluations,
    )
