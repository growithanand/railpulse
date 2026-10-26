"""Construct deterministic validation alert episodes without consulting labels."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite

from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_VERSION,
    ValidationAlertPolicyCandidate,
    get_validation_alert_policy_candidate,
)

VALIDATION_ALERT_EPISODE_VERSION = "motor-current-validation-alert-episodes-v1"


class ValidationAlertEpisodeError(ValueError):
    """Raised when validation scores cannot produce safe deterministic alert episodes."""


@dataclass(frozen=True)
class ValidationScorePoint:
    """Minimum label-free score evidence required to construct alert episodes."""

    cycle_id: str
    prediction_timestamp: datetime
    failure_probability: float

    def __post_init__(self) -> None:
        if not self.cycle_id.strip():
            raise ValidationAlertEpisodeError("Validation score cycle ID must be nonempty")
        if not isinstance(self.prediction_timestamp, datetime):
            raise ValidationAlertEpisodeError("Validation score timestamp must be a datetime")
        if self.prediction_timestamp.tzinfo is not None:
            raise ValidationAlertEpisodeError("Validation score timestamp must be timezone-free")
        if not isfinite(self.failure_probability) or not 0.0 <= self.failure_probability <= 1.0:
            raise ValidationAlertEpisodeError(
                "Validation score probability must be finite and within [0, 1]"
            )


@dataclass(frozen=True)
class ValidationAlertEpisode:
    """One chronological alert episode produced without label or failure-event access."""

    episode_index: int
    policy_candidate_id: str
    start_cycle_id: str
    start_timestamp: datetime
    end_cycle_id: str
    end_timestamp: datetime
    supporting_cycle_count: int
    alert_cycle_count: int
    source_streak_count: int
    maximum_failure_probability: float


@dataclass(frozen=True)
class ValidationAlertEpisodeResult:
    """Auditable construction summary and in-memory alert episodes for one policy."""

    episode_version: str
    policy_version: str
    policy_candidate_id: str
    evaluated_cycle_count: int
    threshold_qualifying_cycle_count: int
    raw_qualifying_streak_count: int
    persistence_qualified_streak_count: int
    merged_episode_count: int
    cooldown_suppressed_episode_count: int
    episodes: tuple[ValidationAlertEpisode, ...]


@dataclass(frozen=True)
class _EpisodeDraft:
    start: ValidationScorePoint
    end: ValidationScorePoint
    supporting_cycle_count: int
    alert_cycle_count: int
    source_streak_count: int
    maximum_failure_probability: float


def _validate_candidate(candidate: ValidationAlertPolicyCandidate) -> None:
    try:
        declared = get_validation_alert_policy_candidate(candidate.candidate_id)
    except ValueError as exc:
        raise ValidationAlertEpisodeError(str(exc)) from exc
    if candidate != declared:
        raise ValidationAlertEpisodeError(
            "Validation alert episodes require an unchanged declared policy candidate"
        )


def _ordered_unique_points(
    points: Sequence[ValidationScorePoint],
) -> tuple[ValidationScorePoint, ...]:
    if not points:
        raise ValidationAlertEpisodeError("Validation alert construction requires score points")
    cycle_ids = tuple(point.cycle_id for point in points)
    if len(cycle_ids) != len(set(cycle_ids)):
        raise ValidationAlertEpisodeError("Validation alert score points require unique cycle IDs")
    return tuple(sorted(points, key=lambda point: (point.prediction_timestamp, point.cycle_id)))


def _qualifying_streaks(
    points: tuple[ValidationScorePoint, ...],
    threshold: float,
) -> tuple[tuple[ValidationScorePoint, ...], ...]:
    streaks: list[tuple[ValidationScorePoint, ...]] = []
    current: list[ValidationScorePoint] = []
    for point in points:
        if point.failure_probability >= threshold:
            current.append(point)
        elif current:
            streaks.append(tuple(current))
            current = []
    if current:
        streaks.append(tuple(current))
    return tuple(streaks)


def _persistence_qualified_drafts(
    streaks: tuple[tuple[ValidationScorePoint, ...], ...],
    candidate: ValidationAlertPolicyCandidate,
) -> tuple[_EpisodeDraft, ...]:
    drafts = []
    for streak in streaks:
        if len(streak) < candidate.minimum_consecutive_cycles:
            continue
        trigger_index = candidate.minimum_consecutive_cycles - 1
        trigger = streak[trigger_index]
        drafts.append(
            _EpisodeDraft(
                start=trigger,
                end=streak[-1],
                supporting_cycle_count=len(streak),
                alert_cycle_count=len(streak) - trigger_index,
                source_streak_count=1,
                maximum_failure_probability=max(point.failure_probability for point in streak),
            )
        )
    return tuple(drafts)


def _merge_drafts(
    drafts: tuple[_EpisodeDraft, ...],
    *,
    gap: timedelta,
) -> tuple[_EpisodeDraft, ...]:
    merged: list[_EpisodeDraft] = []
    for draft in drafts:
        if (
            not merged
            or draft.start.prediction_timestamp > merged[-1].end.prediction_timestamp + gap
        ):
            merged.append(draft)
            continue
        previous = merged[-1]
        merged[-1] = _EpisodeDraft(
            start=previous.start,
            end=max(
                (previous.end, draft.end),
                key=lambda point: (point.prediction_timestamp, point.cycle_id),
            ),
            supporting_cycle_count=(previous.supporting_cycle_count + draft.supporting_cycle_count),
            alert_cycle_count=previous.alert_cycle_count + draft.alert_cycle_count,
            source_streak_count=previous.source_streak_count + draft.source_streak_count,
            maximum_failure_probability=max(
                previous.maximum_failure_probability,
                draft.maximum_failure_probability,
            ),
        )
    return tuple(merged)


def _apply_cooldown(
    drafts: tuple[_EpisodeDraft, ...],
    *,
    cooldown: timedelta,
) -> tuple[tuple[_EpisodeDraft, ...], int]:
    retained: list[_EpisodeDraft] = []
    suppressed_count = 0
    for draft in drafts:
        if (
            retained
            and draft.start.prediction_timestamp < retained[-1].end.prediction_timestamp + cooldown
        ):
            suppressed_count += 1
            continue
        retained.append(draft)
    return tuple(retained), suppressed_count


def build_validation_alert_episodes(
    points: Sequence[ValidationScorePoint],
    candidate: ValidationAlertPolicyCandidate,
) -> ValidationAlertEpisodeResult:
    """Build time-ordered validation episodes without reading labels or test rows."""

    _validate_candidate(candidate)
    ordered = _ordered_unique_points(points)
    streaks = _qualifying_streaks(ordered, candidate.minimum_failure_probability)
    qualified = _persistence_qualified_drafts(streaks, candidate)
    merged = _merge_drafts(
        qualified,
        gap=timedelta(minutes=candidate.merge_gap_minutes),
    )
    retained, suppressed_count = _apply_cooldown(
        merged,
        cooldown=timedelta(minutes=candidate.cooldown_minutes),
    )
    episodes = tuple(
        ValidationAlertEpisode(
            episode_index=index,
            policy_candidate_id=candidate.candidate_id,
            start_cycle_id=draft.start.cycle_id,
            start_timestamp=draft.start.prediction_timestamp,
            end_cycle_id=draft.end.cycle_id,
            end_timestamp=draft.end.prediction_timestamp,
            supporting_cycle_count=draft.supporting_cycle_count,
            alert_cycle_count=draft.alert_cycle_count,
            source_streak_count=draft.source_streak_count,
            maximum_failure_probability=draft.maximum_failure_probability,
        )
        for index, draft in enumerate(retained, start=1)
    )
    return ValidationAlertEpisodeResult(
        episode_version=VALIDATION_ALERT_EPISODE_VERSION,
        policy_version=VALIDATION_ALERT_POLICY_VERSION,
        policy_candidate_id=candidate.candidate_id,
        evaluated_cycle_count=len(ordered),
        threshold_qualifying_cycle_count=sum(len(streak) for streak in streaks),
        raw_qualifying_streak_count=len(streaks),
        persistence_qualified_streak_count=len(qualified),
        merged_episode_count=len(merged),
        cooldown_suppressed_episode_count=suppressed_count,
        episodes=episodes,
    )
