"""Frozen validation-only alert-policy candidates for the retained model."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
)

VALIDATION_ALERT_POLICY_VERSION = "motor-current-validation-alert-policy-candidates-v1"
VALIDATION_ALERT_POLICY_SCOPE = "selected-candidate-validation-only"
VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION = "motor-current-validation-score-profile-v1"
VALIDATION_ALERT_POLICY_THRESHOLD_SOURCE = "managed-all-validation-score-quantiles"
VALIDATION_ALERT_POLICY_SELECTION_VERSION = DEVELOPMENT_MODEL_SELECTION_VERSION
VALIDATION_ALERT_POLICY_MODEL_CANDIDATE_ID = DEVELOPMENT_SELECTED_CANDIDATE_ID
VALIDATION_ALERT_POLICY_SEALED_PARTITION = PARTITION_TEST
VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES = 60
VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES = 120


class ValidationAlertPolicyContractError(ValueError):
    """Raised when a validation alert-policy candidate is unsafe or incomplete."""


@dataclass(frozen=True)
class ValidationAlertPolicyCandidate:
    """One fixed score threshold and temporal persistence policy for validation."""

    candidate_id: str
    threshold_source_quantile: float
    minimum_failure_probability: float
    minimum_consecutive_cycles: int
    merge_gap_minutes: int
    cooldown_minutes: int

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValidationAlertPolicyContractError("Alert-policy candidate ID must be nonempty")
        if not isfinite(self.threshold_source_quantile) or not (
            0.0 < self.threshold_source_quantile < 1.0
        ):
            raise ValidationAlertPolicyContractError(
                "Alert-policy threshold quantile must be finite and within (0, 1)"
            )
        if not isfinite(self.minimum_failure_probability) or not (
            0.0 < self.minimum_failure_probability < 1.0
        ):
            raise ValidationAlertPolicyContractError(
                "Alert-policy probability threshold must be finite and within (0, 1)"
            )
        if (
            isinstance(self.minimum_consecutive_cycles, bool)
            or not isinstance(self.minimum_consecutive_cycles, int)
            or self.minimum_consecutive_cycles <= 0
        ):
            raise ValidationAlertPolicyContractError(
                "Alert-policy persistence must be a positive integer cycle count"
            )
        for name, value in (
            ("merge gap", self.merge_gap_minutes),
            ("cooldown", self.cooldown_minutes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationAlertPolicyContractError(
                    f"Alert-policy {name} must be a nonnegative integer minute count"
                )


VALIDATION_ALERT_POLICY_CANDIDATES = (
    ValidationAlertPolicyCandidate(
        candidate_id="p90-single-cycle",
        threshold_source_quantile=0.90,
        minimum_failure_probability=0.0006357159376330079,
        minimum_consecutive_cycles=1,
        merge_gap_minutes=VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
        cooldown_minutes=VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    ),
    ValidationAlertPolicyCandidate(
        candidate_id="p95-single-cycle",
        threshold_source_quantile=0.95,
        minimum_failure_probability=0.0006360194297156563,
        minimum_consecutive_cycles=1,
        merge_gap_minutes=VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
        cooldown_minutes=VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    ),
    ValidationAlertPolicyCandidate(
        candidate_id="p99-single-cycle",
        threshold_source_quantile=0.99,
        minimum_failure_probability=0.0006361879416607152,
        minimum_consecutive_cycles=1,
        merge_gap_minutes=VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
        cooldown_minutes=VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    ),
    ValidationAlertPolicyCandidate(
        candidate_id="p90-two-consecutive-cycles",
        threshold_source_quantile=0.90,
        minimum_failure_probability=0.0006357159376330079,
        minimum_consecutive_cycles=2,
        merge_gap_minutes=VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
        cooldown_minutes=VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    ),
    ValidationAlertPolicyCandidate(
        candidate_id="p95-two-consecutive-cycles",
        threshold_source_quantile=0.95,
        minimum_failure_probability=0.0006360194297156563,
        minimum_consecutive_cycles=2,
        merge_gap_minutes=VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
        cooldown_minutes=VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    ),
)


def get_validation_alert_policy_candidate(candidate_id: str) -> ValidationAlertPolicyCandidate:
    """Return one declared validation policy by its stable identifier."""

    for candidate in VALIDATION_ALERT_POLICY_CANDIDATES:
        if candidate.candidate_id == candidate_id:
            return candidate
    raise ValidationAlertPolicyContractError(
        f"Unknown validation alert-policy candidate: {candidate_id}"
    )
