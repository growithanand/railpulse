"""Verify deterministic label-blind validation alert-episode construction."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from railpulse.models.validation_alert_episodes import (
    VALIDATION_ALERT_EPISODE_VERSION,
    ValidationAlertEpisodeError,
    ValidationScorePoint,
    build_validation_alert_episodes,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_VERSION,
    get_validation_alert_policy_candidate,
)

START = datetime(2020, 6, 1)


def _point(cycle: str, minute: int, probability: float) -> ValidationScorePoint:
    return ValidationScorePoint(cycle, START + timedelta(minutes=minute), probability)


def test_single_cycle_policy_sorts_scores_and_uses_inclusive_threshold() -> None:
    candidate = get_validation_alert_policy_candidate("p95-single-cycle")
    threshold = candidate.minimum_failure_probability
    points = (
        _point("cycle-3", 30, threshold - 0.000000001),
        _point("cycle-1", 0, threshold),
        _point("cycle-2", 10, threshold + 0.000000001),
    )

    result = build_validation_alert_episodes(points, candidate)

    assert result.episode_version == VALIDATION_ALERT_EPISODE_VERSION
    assert result.policy_version == VALIDATION_ALERT_POLICY_VERSION
    assert result.evaluated_cycle_count == 3
    assert result.threshold_qualifying_cycle_count == 2
    assert result.raw_qualifying_streak_count == 1
    assert result.persistence_qualified_streak_count == 1
    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.start_cycle_id == "cycle-1"
    assert episode.end_cycle_id == "cycle-2"
    assert episode.supporting_cycle_count == 2
    assert episode.alert_cycle_count == 2


def test_two_cycle_persistence_triggers_on_second_consecutive_cycle() -> None:
    candidate = get_validation_alert_policy_candidate("p90-two-consecutive-cycles")
    high = candidate.minimum_failure_probability
    low = high - 0.000000001
    points = (
        _point("isolated", 0, high),
        _point("break", 10, low),
        _point("support-1", 20, high),
        _point("support-2", 30, high),
        _point("support-3", 40, high),
    )

    result = build_validation_alert_episodes(points, candidate)

    assert result.threshold_qualifying_cycle_count == 4
    assert result.raw_qualifying_streak_count == 2
    assert result.persistence_qualified_streak_count == 1
    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.start_cycle_id == "support-2"
    assert episode.end_cycle_id == "support-3"
    assert episode.supporting_cycle_count == 3
    assert episode.alert_cycle_count == 2


def test_merge_and_cooldown_have_distinct_deterministic_effects() -> None:
    candidate = get_validation_alert_policy_candidate("p90-single-cycle")
    high = candidate.minimum_failure_probability
    low = high - 0.000000001
    points = (
        _point("high-1", 0, high),
        _point("low-1", 10, low),
        _point("high-2", 50, high),
        _point("low-2", 60, low),
        _point("suppressed", 140, high),
        _point("low-3", 150, low),
        _point("retained", 210, high),
    )

    result = build_validation_alert_episodes(points, candidate)

    assert result.raw_qualifying_streak_count == 4
    assert result.persistence_qualified_streak_count == 4
    assert result.merged_episode_count == 3
    assert result.cooldown_suppressed_episode_count == 1
    assert tuple(episode.start_cycle_id for episode in result.episodes) == (
        "high-1",
        "retained",
    )
    assert result.episodes[0].end_cycle_id == "high-2"
    assert result.episodes[0].source_streak_count == 2


def test_episode_construction_rejects_duplicate_or_invalid_score_points() -> None:
    candidate = get_validation_alert_policy_candidate("p99-single-cycle")
    point = _point("duplicate", 0, candidate.minimum_failure_probability)

    with pytest.raises(ValidationAlertEpisodeError, match="requires score points"):
        build_validation_alert_episodes((), candidate)
    with pytest.raises(ValidationAlertEpisodeError, match="unique cycle IDs"):
        build_validation_alert_episodes((point, point), candidate)
    with pytest.raises(ValidationAlertEpisodeError, match="timezone-free"):
        ValidationScorePoint("cycle", datetime(2020, 6, 1, tzinfo=UTC), 0.1)
    with pytest.raises(ValidationAlertEpisodeError, match=r"within \[0, 1\]"):
        ValidationScorePoint("cycle", START, float("nan"))


def test_episode_construction_rejects_changed_or_unknown_policy() -> None:
    candidate = get_validation_alert_policy_candidate("p95-single-cycle")
    point = _point("cycle", 0, candidate.minimum_failure_probability)

    with pytest.raises(ValidationAlertEpisodeError, match="unchanged"):
        build_validation_alert_episodes(
            (point,),
            replace(candidate, cooldown_minutes=121),
        )
    with pytest.raises(ValidationAlertEpisodeError, match="Unknown"):
        build_validation_alert_episodes(
            (point,),
            replace(candidate, candidate_id="not-declared"),
        )
