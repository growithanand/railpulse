"""Verify the retained model's validation-only alert-policy contract."""

from __future__ import annotations

import pytest

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_CANDIDATES,
    VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES,
    VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES,
    VALIDATION_ALERT_POLICY_MODEL_CANDIDATE_ID,
    VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION,
    VALIDATION_ALERT_POLICY_SEALED_PARTITION,
    VALIDATION_ALERT_POLICY_SELECTION_VERSION,
    VALIDATION_ALERT_POLICY_THRESHOLD_SOURCE,
    VALIDATION_ALERT_POLICY_VERSION,
    ValidationAlertPolicyCandidate,
    ValidationAlertPolicyContractError,
    get_validation_alert_policy_candidate,
)


def test_alert_policy_contract_freezes_development_identity_and_sealed_test() -> None:
    assert VALIDATION_ALERT_POLICY_VERSION == "motor-current-validation-alert-policy-candidates-v1"
    assert VALIDATION_ALERT_POLICY_SCORE_PROFILE_VERSION == (
        "motor-current-validation-score-profile-v1"
    )
    assert VALIDATION_ALERT_POLICY_THRESHOLD_SOURCE == "managed-all-validation-score-quantiles"
    assert VALIDATION_ALERT_POLICY_SELECTION_VERSION == DEVELOPMENT_MODEL_SELECTION_VERSION
    assert VALIDATION_ALERT_POLICY_MODEL_CANDIDATE_ID == DEVELOPMENT_SELECTED_CANDIDATE_ID
    assert VALIDATION_ALERT_POLICY_SEALED_PARTITION == PARTITION_TEST


def test_alert_policy_candidates_are_small_ordered_and_unique() -> None:
    assert tuple(candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES) == (
        "p90-single-cycle",
        "p95-single-cycle",
        "p99-single-cycle",
        "p90-two-consecutive-cycles",
        "p95-two-consecutive-cycles",
    )
    identities = {
        (
            candidate.minimum_failure_probability,
            candidate.minimum_consecutive_cycles,
            candidate.merge_gap_minutes,
            candidate.cooldown_minutes,
        )
        for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
    }
    assert len(identities) == len(VALIDATION_ALERT_POLICY_CANDIDATES)


def test_alert_policy_candidates_use_managed_quantiles_and_shared_time_rules() -> None:
    expected_thresholds = {
        0.90: 0.0006357159376330079,
        0.95: 0.0006360194297156563,
        0.99: 0.0006361879416607152,
    }
    for candidate in VALIDATION_ALERT_POLICY_CANDIDATES:
        assert candidate.minimum_failure_probability == pytest.approx(
            expected_thresholds[candidate.threshold_source_quantile]
        )
        assert candidate.minimum_consecutive_cycles in {1, 2}
        assert candidate.merge_gap_minutes == VALIDATION_ALERT_POLICY_MERGE_GAP_MINUTES == 60
        assert candidate.cooldown_minutes == VALIDATION_ALERT_POLICY_COOLDOWN_MINUTES == 120


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("threshold_source_quantile", float("nan"), "quantile"),
        ("threshold_source_quantile", 1.0, "quantile"),
        ("minimum_failure_probability", 0.0, "probability"),
        ("minimum_failure_probability", float("inf"), "probability"),
        ("minimum_consecutive_cycles", 0, "persistence"),
        ("minimum_consecutive_cycles", True, "persistence"),
        ("merge_gap_minutes", -1, "merge gap"),
        ("cooldown_minutes", -1, "cooldown"),
    ],
)
def test_alert_policy_candidate_rejects_invalid_values(
    field: str,
    value: object,
    message: str,
) -> None:
    values = {
        "candidate_id": "candidate",
        "threshold_source_quantile": 0.95,
        "minimum_failure_probability": 0.001,
        "minimum_consecutive_cycles": 1,
        "merge_gap_minutes": 120,
        "cooldown_minutes": 120,
    }
    values[field] = value

    with pytest.raises(ValidationAlertPolicyContractError, match=message):
        ValidationAlertPolicyCandidate(**values)


def test_alert_policy_candidate_rejects_blank_and_unknown_ids() -> None:
    with pytest.raises(ValidationAlertPolicyContractError, match="nonempty"):
        ValidationAlertPolicyCandidate(" ", 0.95, 0.001, 1, 120, 120)
    with pytest.raises(ValidationAlertPolicyContractError, match="Unknown"):
        get_validation_alert_policy_candidate("not-declared")

    assert (
        get_validation_alert_policy_candidate("p95-single-cycle")
        is (VALIDATION_ALERT_POLICY_CANDIDATES[1])
    )
