"""Route guarded Spark validation scores into label-blind alert policies."""

from __future__ import annotations

from dataclasses import dataclass

from railpulse.evaluation.modeling_view_schema import MODEL_TIME_COLUMN
from railpulse.models.candidate_development_data import MODEL_CANDIDATE_KEY_COLUMN
from railpulse.models.selected_candidate_validation import (
    VALIDATION_SCORE_COLUMN,
    SelectedCandidateValidationScores,
)
from railpulse.models.selected_candidate_validation_profile import (
    SelectedCandidateValidationProfile,
    SelectedCandidateValidationProfileError,
    collect_selected_candidate_validation_profile,
)
from railpulse.models.validation_alert_episodes import (
    ValidationAlertEpisodeError,
    ValidationAlertEpisodeResult,
    ValidationScorePoint,
    build_validation_alert_episodes,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_CANDIDATES,
    VALIDATION_ALERT_POLICY_VERSION,
)

SELECTED_CANDIDATE_VALIDATION_ALERT_ADAPTER_VERSION = "motor-current-validation-alert-adapter-v1"


class SelectedCandidateValidationAlertError(ValueError):
    """Raised when guarded validation scores cannot produce the fixed alert candidate set."""


@dataclass(frozen=True)
class SelectedCandidateValidationAlertCandidates:
    """Compact profile and in-memory episode evidence for every declared validation policy."""

    adapter_version: str
    policy_version: str
    evaluated_cycle_count: int
    profile: SelectedCandidateValidationProfile
    candidate_results: tuple[ValidationAlertEpisodeResult, ...]


def build_selected_candidate_validation_alerts(
    result: SelectedCandidateValidationScores,
) -> SelectedCandidateValidationAlertCandidates:
    """Validate Spark scores, remove labels, and construct every fixed validation policy."""

    try:
        profile = collect_selected_candidate_validation_profile(result)
        rows = (
            result.scores.select(
                MODEL_CANDIDATE_KEY_COLUMN,
                MODEL_TIME_COLUMN,
                VALIDATION_SCORE_COLUMN,
            )
            .orderBy(MODEL_TIME_COLUMN, MODEL_CANDIDATE_KEY_COLUMN)
            .collect()
        )
        points = tuple(
            ValidationScorePoint(
                cycle_id=row[MODEL_CANDIDATE_KEY_COLUMN],
                prediction_timestamp=row[MODEL_TIME_COLUMN],
                failure_probability=float(row[VALIDATION_SCORE_COLUMN]),
            )
            for row in rows
        )
        candidate_results = tuple(
            build_validation_alert_episodes(points, candidate)
            for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
        )
    except (
        SelectedCandidateValidationProfileError,
        ValidationAlertEpisodeError,
    ) as exc:
        raise SelectedCandidateValidationAlertError(str(exc)) from exc

    expected_candidate_ids = tuple(
        candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
    )
    if (
        len(points) != profile.validation_row_count
        or tuple(item.policy_candidate_id for item in candidate_results) != expected_candidate_ids
        or any(item.evaluated_cycle_count != len(points) for item in candidate_results)
    ):
        raise SelectedCandidateValidationAlertError(
            "Validation alert candidate results do not reconcile with the guarded score profile"
        )

    return SelectedCandidateValidationAlertCandidates(
        adapter_version=SELECTED_CANDIDATE_VALIDATION_ALERT_ADAPTER_VERSION,
        policy_version=VALIDATION_ALERT_POLICY_VERSION,
        evaluated_cycle_count=len(points),
        profile=profile,
        candidate_results=candidate_results,
    )
