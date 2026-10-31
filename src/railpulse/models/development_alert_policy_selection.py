"""Freeze the development-only alert-policy choice without opening the test partition."""

from __future__ import annotations

from dataclasses import dataclass

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.models.development_model_selection import DEVELOPMENT_MODEL_SELECTION_VERSION
from railpulse.models.validation_alert_episodes import VALIDATION_ALERT_EPISODE_VERSION
from railpulse.models.validation_alert_evaluation import (
    VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_CANDIDATES,
    VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION,
    VALIDATION_ALERT_POLICY_VERSION,
    ValidationAlertPolicyCandidate,
    get_validation_alert_policy_candidate,
)

DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION = "motor-current-validation-alert-policy-selection-v1"
DEVELOPMENT_ALERT_POLICY_SELECTION_SCOPE = "development-validation-only"
DEVELOPMENT_ALERT_POLICY_SELECTION_REASON = "full-event-detection-then-lowest-false-alert-burden"
DEVELOPMENT_SELECTED_ALERT_POLICY_ID = "p95-two-consecutive-cycles"
DEVELOPMENT_ALERT_POLICY_EVALUATED_CYCLE_COUNT = 4_730
DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT = 28
DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT = 1


class DevelopmentAlertPolicySelectionError(ValueError):
    """Raised when alert-policy selection evidence violates the reviewed contract."""


def _validate_count(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DevelopmentAlertPolicySelectionError(
            f"Alert-policy {name} must be a nonnegative integer"
        )


@dataclass(frozen=True)
class AlertPolicyValidationEvidence:
    """Aggregate managed validation evidence for one alert-policy candidate."""

    policy_candidate_id: str
    detected_failure_event_count: int
    missed_failure_event_count: int
    alert_episode_count: int
    warning_episode_count: int
    inside_failure_episode_count: int
    false_alert_episode_count: int
    warning_lead_time_seconds: int | None

    def __post_init__(self) -> None:
        try:
            get_validation_alert_policy_candidate(self.policy_candidate_id)
        except ValueError as exc:
            raise DevelopmentAlertPolicySelectionError(str(exc)) from exc

        for name, value in (
            ("detected failure-event count", self.detected_failure_event_count),
            ("missed failure-event count", self.missed_failure_event_count),
            ("episode count", self.alert_episode_count),
            ("warning episode count", self.warning_episode_count),
            ("inside-failure episode count", self.inside_failure_episode_count),
            ("false-alert episode count", self.false_alert_episode_count),
        ):
            _validate_count(name, value)

        if (
            self.detected_failure_event_count + self.missed_failure_event_count
            != DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT
        ):
            raise DevelopmentAlertPolicySelectionError(
                "Alert-policy failure-event counts do not reconcile"
            )
        if (
            self.warning_episode_count
            + self.inside_failure_episode_count
            + self.false_alert_episode_count
            != self.alert_episode_count
        ):
            raise DevelopmentAlertPolicySelectionError(
                "Alert-policy episode counts do not reconcile"
            )

        detected = self.detected_failure_event_count == 1
        if detected:
            if self.warning_episode_count <= 0 or (
                isinstance(self.warning_lead_time_seconds, bool)
                or not isinstance(self.warning_lead_time_seconds, int)
                or self.warning_lead_time_seconds <= 0
            ):
                raise DevelopmentAlertPolicySelectionError(
                    "Detected alert policy requires positive warning evidence"
                )
        elif self.warning_episode_count != 0 or self.warning_lead_time_seconds is not None:
            raise DevelopmentAlertPolicySelectionError(
                "Missed alert policy must not contain warning evidence"
            )

    @property
    def detected_all_failure_events(self) -> bool:
        """Return whether the policy detected the complete accepted validation event set."""

        return (
            self.detected_failure_event_count
            == DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT
            and self.missed_failure_event_count == 0
        )

    @property
    def false_alert_episodes_per_operating_day(self) -> float:
        """Return the reviewed false-alert burden using the shared operating-day count."""

        return self.false_alert_episode_count / DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT


MANAGED_VALIDATION_ALERT_POLICY_EVIDENCE = (
    AlertPolicyValidationEvidence(
        policy_candidate_id="p90-single-cycle",
        detected_failure_event_count=1,
        missed_failure_event_count=0,
        alert_episode_count=68,
        warning_episode_count=1,
        inside_failure_episode_count=0,
        false_alert_episode_count=67,
        warning_lead_time_seconds=6_984,
    ),
    AlertPolicyValidationEvidence(
        policy_candidate_id="p95-single-cycle",
        detected_failure_event_count=1,
        missed_failure_event_count=0,
        alert_episode_count=67,
        warning_episode_count=1,
        inside_failure_episode_count=0,
        false_alert_episode_count=66,
        warning_lead_time_seconds=5_606,
    ),
    AlertPolicyValidationEvidence(
        policy_candidate_id="p99-single-cycle",
        detected_failure_event_count=0,
        missed_failure_event_count=1,
        alert_episode_count=27,
        warning_episode_count=0,
        inside_failure_episode_count=0,
        false_alert_episode_count=27,
        warning_lead_time_seconds=None,
    ),
    AlertPolicyValidationEvidence(
        policy_candidate_id="p90-two-consecutive-cycles",
        detected_failure_event_count=1,
        missed_failure_event_count=0,
        alert_episode_count=59,
        warning_episode_count=1,
        inside_failure_episode_count=0,
        false_alert_episode_count=58,
        warning_lead_time_seconds=5_606,
    ),
    AlertPolicyValidationEvidence(
        policy_candidate_id="p95-two-consecutive-cycles",
        detected_failure_event_count=1,
        missed_failure_event_count=0,
        alert_episode_count=34,
        warning_episode_count=1,
        inside_failure_episode_count=0,
        false_alert_episode_count=33,
        warning_lead_time_seconds=1_661,
    ),
)


@dataclass(frozen=True)
class DevelopmentAlertPolicySelection:
    """Reviewed validation-only policy choice for the retained development model."""

    selection_version: str
    selection_scope: str
    selection_reason: str
    model_selection_version: str
    score_profile_version: str
    policy_version: str
    episode_version: str
    evaluation_version: str
    selected_policy_candidate_id: str
    sealed_partition: str
    evaluated_cycle_count: int
    operating_day_count: int
    accepted_failure_event_count: int
    candidate_evidence: tuple[AlertPolicyValidationEvidence, ...]

    def __post_init__(self) -> None:
        if (
            self.selection_version != DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION
            or self.selection_scope != DEVELOPMENT_ALERT_POLICY_SELECTION_SCOPE
            or self.selection_reason != DEVELOPMENT_ALERT_POLICY_SELECTION_REASON
            or self.model_selection_version != DEVELOPMENT_MODEL_SELECTION_VERSION
            or self.score_profile_version != VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION
            or self.policy_version != VALIDATION_ALERT_POLICY_VERSION
            or self.episode_version != VALIDATION_ALERT_EPISODE_VERSION
            or self.evaluation_version != VALIDATION_ALERT_EVENT_EVALUATION_VERSION
            or self.sealed_partition != PARTITION_TEST
            or self.evaluated_cycle_count != DEVELOPMENT_ALERT_POLICY_EVALUATED_CYCLE_COUNT
            or self.operating_day_count != DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT
            or self.accepted_failure_event_count
            != DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT
        ):
            raise DevelopmentAlertPolicySelectionError(
                "Alert-policy selection metadata does not match the reviewed contract"
            )

        expected_ids = tuple(
            candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
        )
        evidence_ids = tuple(evidence.policy_candidate_id for evidence in self.candidate_evidence)
        if evidence_ids != expected_ids:
            raise DevelopmentAlertPolicySelectionError(
                "Selection evidence must contain every alert-policy candidate in reviewed order"
            )
        if self.candidate_evidence != MANAGED_VALIDATION_ALERT_POLICY_EVIDENCE:
            raise DevelopmentAlertPolicySelectionError(
                "Selection evidence must match the reviewed managed evaluation"
            )

        eligible = tuple(
            evidence for evidence in self.candidate_evidence if evidence.detected_all_failure_events
        )
        if not eligible:
            raise DevelopmentAlertPolicySelectionError(
                "Alert-policy selection requires at least one full-detection candidate"
            )
        lowest_false_alert_count = min(evidence.false_alert_episode_count for evidence in eligible)
        leaders = tuple(
            evidence.policy_candidate_id
            for evidence in eligible
            if evidence.false_alert_episode_count == lowest_false_alert_count
        )
        if leaders != (self.selected_policy_candidate_id,):
            raise DevelopmentAlertPolicySelectionError(
                "Selected alert policy must be the unique lowest-burden full-detection candidate"
            )


DEVELOPMENT_ALERT_POLICY_SELECTION = DevelopmentAlertPolicySelection(
    selection_version=DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    selection_scope=DEVELOPMENT_ALERT_POLICY_SELECTION_SCOPE,
    selection_reason=DEVELOPMENT_ALERT_POLICY_SELECTION_REASON,
    model_selection_version=DEVELOPMENT_MODEL_SELECTION_VERSION,
    score_profile_version=VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION,
    policy_version=VALIDATION_ALERT_POLICY_VERSION,
    episode_version=VALIDATION_ALERT_EPISODE_VERSION,
    evaluation_version=VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
    selected_policy_candidate_id=DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
    sealed_partition=PARTITION_TEST,
    evaluated_cycle_count=DEVELOPMENT_ALERT_POLICY_EVALUATED_CYCLE_COUNT,
    operating_day_count=DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT,
    accepted_failure_event_count=DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT,
    candidate_evidence=MANAGED_VALIDATION_ALERT_POLICY_EVIDENCE,
)


def get_development_alert_policy_selection() -> DevelopmentAlertPolicySelection:
    """Return the reviewed development-only alert-policy selection."""

    return DEVELOPMENT_ALERT_POLICY_SELECTION


def get_selected_development_alert_policy() -> ValidationAlertPolicyCandidate:
    """Return the selected policy while preserving its frozen candidate contract."""

    return get_validation_alert_policy_candidate(
        DEVELOPMENT_ALERT_POLICY_SELECTION.selected_policy_candidate_id
    )
