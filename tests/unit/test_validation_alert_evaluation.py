"""Verify validation-only event matching for constructed alert episodes."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from railpulse.models.validation_alert_episodes import (
    ValidationScorePoint,
    build_validation_alert_episodes,
)
from railpulse.models.validation_alert_evaluation import (
    VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
    ValidationAlertEvaluationError,
    ValidationFailureEvent,
    evaluate_validation_alert_policy,
)
from railpulse.models.validation_alert_policy_contract import (
    get_validation_alert_policy_candidate,
)

FAILURE_START = datetime(2020, 6, 5, 10)
FAILURE_END = datetime(2020, 6, 7, 14, 30)


def _event() -> ValidationFailureEvent:
    return ValidationFailureEvent("failure-june", FAILURE_START, FAILURE_END)


def _result(*high_timestamps: datetime):
    candidate = get_validation_alert_policy_candidate("p95-single-cycle")
    threshold = candidate.minimum_failure_probability
    points = []
    for index, timestamp in enumerate(high_timestamps, start=1):
        points.extend(
            (
                ValidationScorePoint(f"high-{index}", timestamp, threshold),
                ValidationScorePoint(
                    f"break-{index}", timestamp + timedelta(seconds=1), threshold / 2
                ),
            )
        )
    return build_validation_alert_episodes(tuple(points), candidate)


def test_evaluation_separates_warning_late_and_false_alert_episodes() -> None:
    result = _result(
        FAILURE_START - timedelta(hours=2),
        FAILURE_START,
        FAILURE_END + timedelta(hours=3),
    )

    evaluation = evaluate_validation_alert_policy(
        result,
        (_event(),),
        operating_day_count=10,
    )

    assert evaluation.evaluation_version == VALIDATION_ALERT_EVENT_EVALUATION_VERSION
    assert evaluation.warning_horizon_seconds == 7_200
    assert evaluation.accepted_failure_event_count == 1
    assert evaluation.detected_failure_event_count == 1
    assert evaluation.missed_failure_event_count == 0
    assert evaluation.event_detection_rate == 1.0
    assert evaluation.alert_episode_count == 3
    assert evaluation.warning_episode_count == 1
    assert evaluation.inside_failure_episode_count == 1
    assert evaluation.false_alert_episode_count == 1
    assert evaluation.alert_episodes_per_operating_day == pytest.approx(0.3)
    assert evaluation.false_alert_episodes_per_operating_day == pytest.approx(0.1)

    event = evaluation.event_evaluations[0]
    assert event.detected is True
    assert event.warning_episode_indices == (1,)
    assert event.inside_failure_episode_indices == (2,)
    assert event.first_warning_timestamp == FAILURE_START - timedelta(hours=2)
    assert event.warning_lead_time_seconds == 7_200


def test_evaluation_reports_a_missed_event_and_reconciles_false_alerts() -> None:
    result = _result(FAILURE_END + timedelta(hours=3))

    evaluation = evaluate_validation_alert_policy(
        result,
        (_event(),),
        operating_day_count=5,
    )

    assert evaluation.detected_failure_event_count == 0
    assert evaluation.missed_failure_event_count == 1
    assert evaluation.event_detection_rate == 0.0
    assert evaluation.warning_episode_count == 0
    assert evaluation.inside_failure_episode_count == 0
    assert evaluation.false_alert_episode_count == 1
    assert evaluation.event_evaluations[0].first_warning_timestamp is None
    assert evaluation.event_evaluations[0].warning_lead_time_seconds is None


def test_evaluation_rejects_test_events_invalid_days_and_tampered_results() -> None:
    result = _result(FAILURE_START - timedelta(hours=1))

    with pytest.raises(ValidationAlertEvaluationError, match="exactly one"):
        evaluate_validation_alert_policy(result, (), operating_day_count=10)
    with pytest.raises(ValidationAlertEvaluationError, match="operating-day"):
        evaluate_validation_alert_policy(result, (_event(),), operating_day_count=0)
    with pytest.raises(ValidationAlertEvaluationError, match="inside validation"):
        evaluate_validation_alert_policy(
            result,
            (ValidationFailureEvent("failure-july", datetime(2020, 7, 15), datetime(2020, 7, 16)),),
            operating_day_count=10,
        )
    with pytest.raises(ValidationAlertEvaluationError, match="versions"):
        evaluate_validation_alert_policy(
            replace(result, episode_version="changed"),
            (_event(),),
            operating_day_count=10,
        )
    with pytest.raises(ValidationAlertEvaluationError, match="timezone-free"):
        ValidationFailureEvent(
            "failure-june",
            FAILURE_START.replace(tzinfo=UTC),
            FAILURE_END.replace(tzinfo=UTC),
        )
