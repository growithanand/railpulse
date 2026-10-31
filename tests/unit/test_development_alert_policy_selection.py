"""Verify the development-only alert-policy selection contract."""

from __future__ import annotations

from dataclasses import replace

import pytest

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.models.development_alert_policy_selection import (
    DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT,
    DEVELOPMENT_ALERT_POLICY_EVALUATED_CYCLE_COUNT,
    DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT,
    DEVELOPMENT_ALERT_POLICY_SELECTION_REASON,
    DEVELOPMENT_ALERT_POLICY_SELECTION_SCOPE,
    DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
    AlertPolicyValidationEvidence,
    DevelopmentAlertPolicySelectionError,
    get_development_alert_policy_selection,
    get_selected_development_alert_policy,
)
from railpulse.models.validation_alert_policy_contract import (
    VALIDATION_ALERT_POLICY_CANDIDATES,
)


def test_selection_freezes_complete_managed_policy_evidence() -> None:
    selection = get_development_alert_policy_selection()

    assert selection.selection_version == DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION
    assert selection.selection_scope == DEVELOPMENT_ALERT_POLICY_SELECTION_SCOPE
    assert selection.selection_reason == DEVELOPMENT_ALERT_POLICY_SELECTION_REASON
    assert selection.selected_policy_candidate_id == DEVELOPMENT_SELECTED_ALERT_POLICY_ID
    assert selection.sealed_partition == PARTITION_TEST
    assert selection.evaluated_cycle_count == DEVELOPMENT_ALERT_POLICY_EVALUATED_CYCLE_COUNT
    assert selection.operating_day_count == DEVELOPMENT_ALERT_POLICY_OPERATING_DAY_COUNT
    assert (
        selection.accepted_failure_event_count
        == DEVELOPMENT_ALERT_POLICY_ACCEPTED_FAILURE_EVENT_COUNT
    )
    assert tuple(item.policy_candidate_id for item in selection.candidate_evidence) == tuple(
        candidate.candidate_id for candidate in VALIDATION_ALERT_POLICY_CANDIDATES
    )
    assert tuple(item.false_alert_episode_count for item in selection.candidate_evidence) == (
        67,
        66,
        27,
        58,
        33,
    )
    assert tuple(item.warning_lead_time_seconds for item in selection.candidate_evidence) == (
        6_984,
        5_606,
        None,
        5_606,
        1_661,
    )


def test_selection_chooses_lowest_burden_full_detection_policy() -> None:
    selection = get_development_alert_policy_selection()
    selected = get_selected_development_alert_policy()
    eligible = tuple(
        item for item in selection.candidate_evidence if item.detected_all_failure_events
    )

    assert selected.candidate_id == "p95-two-consecutive-cycles"
    assert selected.minimum_consecutive_cycles == 2
    assert selected.threshold_source_quantile == pytest.approx(0.95)
    assert min(item.false_alert_episode_count for item in eligible) == 33
    assert selection.candidate_evidence[2].false_alert_episode_count == 27
    assert not selection.candidate_evidence[2].detected_all_failure_events


def test_selection_keeps_test_sealed_and_contains_no_test_metrics() -> None:
    selection = get_development_alert_policy_selection()

    assert selection.sealed_partition == PARTITION_TEST
    assert all(
        not hasattr(item, "test_event_detection_rate")
        and not hasattr(item, "test_false_alert_episode_count")
        for item in selection.candidate_evidence
    )
    assert selection.candidate_evidence[-1].false_alert_episodes_per_operating_day == (
        pytest.approx(33 / 28)
    )


def test_policy_evidence_rejects_inconsistent_counts_and_warning_state() -> None:
    with pytest.raises(DevelopmentAlertPolicySelectionError, match="failure-event counts"):
        AlertPolicyValidationEvidence("p90-single-cycle", 1, 1, 1, 1, 0, 0, 60)
    with pytest.raises(DevelopmentAlertPolicySelectionError, match="episode counts"):
        AlertPolicyValidationEvidence("p90-single-cycle", 1, 0, 2, 1, 0, 0, 60)
    with pytest.raises(DevelopmentAlertPolicySelectionError, match="positive warning"):
        AlertPolicyValidationEvidence("p90-single-cycle", 1, 0, 1, 1, 0, 0, None)
    with pytest.raises(DevelopmentAlertPolicySelectionError, match="must not contain warning"):
        AlertPolicyValidationEvidence("p99-single-cycle", 0, 1, 1, 1, 0, 0, 60)


def test_selection_rejects_incomplete_changed_or_nonleading_evidence() -> None:
    selection = get_development_alert_policy_selection()

    with pytest.raises(DevelopmentAlertPolicySelectionError, match="every alert-policy"):
        replace(selection, candidate_evidence=selection.candidate_evidence[:-1])

    changed = replace(
        selection.candidate_evidence[0],
        alert_episode_count=67,
        false_alert_episode_count=66,
    )
    with pytest.raises(DevelopmentAlertPolicySelectionError, match="reviewed managed"):
        replace(
            selection,
            candidate_evidence=(changed, *selection.candidate_evidence[1:]),
        )

    with pytest.raises(DevelopmentAlertPolicySelectionError, match="unique lowest-burden"):
        replace(selection, selected_policy_candidate_id="p90-two-consecutive-cycles")
