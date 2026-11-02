"""Verify the one-time sealed-test evaluation contract without reading test data."""

from __future__ import annotations

from dataclasses import replace

import pytest

from railpulse.evaluation.chronological_splits import (
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    SELECTED_SPLIT_CANDIDATE_ID,
)
from railpulse.models.development_alert_policy_selection import (
    DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
)
from railpulse.models.sealed_test_evaluation_contract import (
    SEALED_TEST_AGGREGATE_METRICS,
    SEALED_TEST_EVALUATION_CONTRACT_VERSION,
    SEALED_TEST_EVALUATION_SCOPE,
    SEALED_TEST_OUTPUT_POLICY,
    SEALED_TEST_RESULT_USE,
    SealedTestEvaluationContractError,
    get_sealed_test_evaluation_contract,
)


def test_contract_freezes_selected_model_policy_and_split() -> None:
    contract = get_sealed_test_evaluation_contract()

    assert contract.contract_version == SEALED_TEST_EVALUATION_CONTRACT_VERSION
    assert contract.scope == SEALED_TEST_EVALUATION_SCOPE
    assert contract.split_candidate_id == SELECTED_SPLIT_CANDIDATE_ID
    assert contract.model_selection_version == DEVELOPMENT_MODEL_SELECTION_VERSION
    assert contract.model_candidate_id == DEVELOPMENT_SELECTED_CANDIDATE_ID
    assert contract.alert_policy_selection_version == (DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION)
    assert contract.alert_policy_candidate_id == DEVELOPMENT_SELECTED_ALERT_POLICY_ID


def test_contract_preserves_training_fit_and_validation_threshold_roles() -> None:
    contract = get_sealed_test_evaluation_contract()

    assert contract.model_fit_partition == PARTITION_TRAIN
    assert contract.threshold_source_partition == PARTITION_VALIDATION
    assert contract.score_partition == PARTITION_TEST
    assert contract.minimum_failure_probability == pytest.approx(0.0006360194297156563)
    assert contract.minimum_consecutive_cycles == 2
    assert contract.merge_gap_minutes == 60
    assert contract.cooldown_minutes == 120
    assert contract.warning_horizon_seconds == 7_200


def test_contract_requires_one_separate_approval_and_aggregate_only_output() -> None:
    contract = get_sealed_test_evaluation_contract()

    assert contract.approved_test_read_count == 1
    assert contract.requires_explicit_execution_approval
    assert not contract.contract_authorizes_execution
    assert not contract.automatic_retry_allowed
    assert contract.retry_requires_versioned_decision
    assert contract.output_policy == SEALED_TEST_OUTPUT_POLICY
    assert contract.result_use == SEALED_TEST_RESULT_USE
    assert contract.aggregate_metrics == SEALED_TEST_AGGREGATE_METRICS
    assert len(contract.aggregate_metrics) == len(set(contract.aggregate_metrics))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_fit_partition", PARTITION_VALIDATION),
        ("threshold_source_partition", PARTITION_TEST),
        ("score_partition", PARTITION_VALIDATION),
        ("model_candidate_id", "motor-current-plus-cycle-context"),
        ("alert_policy_candidate_id", "p99-single-cycle"),
        ("approved_test_read_count", 2),
        ("contract_authorizes_execution", True),
        ("automatic_retry_allowed", True),
        ("retry_requires_versioned_decision", False),
        ("output_policy", "persist-row-level-scores"),
        ("result_use", "retune-after-test"),
    ],
)
def test_contract_rejects_weakened_or_changed_boundaries(field: str, value: object) -> None:
    contract = get_sealed_test_evaluation_contract()

    with pytest.raises(SealedTestEvaluationContractError, match="reviewed contract"):
        replace(contract, **{field: value})


def test_contract_contains_no_test_results_or_execution_identity() -> None:
    contract = get_sealed_test_evaluation_contract()

    forbidden_fields = {
        "test_row_count",
        "test_positive_count",
        "test_area_under_pr",
        "test_event_detection_rate",
        "job_id",
        "run_id",
        "workspace_url",
    }
    assert forbidden_fields.isdisjoint(vars(contract))
