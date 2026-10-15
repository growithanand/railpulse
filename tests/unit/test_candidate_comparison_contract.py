"""Verify the cycle-context model comparison contract."""

from __future__ import annotations

import pytest

from railpulse.evaluation.chronological_splits import (
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
)
from railpulse.evaluation.modeling_view_schema import (
    MODEL_INPUT_COLUMNS,
    MODEL_TARGET_COLUMN,
    MODEL_TIME_COLUMN,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    CYCLE_CONTEXT_MODEL_INPUT_COLUMNS,
    MODEL_INPUT_COLUMNS_V2,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_DEVELOPMENT_PARTITIONS,
    MODEL_CANDIDATE_ESTIMATOR,
    MODEL_CANDIDATE_SEALED_PARTITION,
    MODEL_CANDIDATE_SELECTION_METRIC,
    MODEL_CANDIDATES,
    ModelCandidate,
    ModelCandidateContractError,
    get_comparison_modeling_view_version,
    get_model_candidate,
)


def test_comparison_uses_one_estimator_and_one_shared_v2_population() -> None:
    assert MODEL_CANDIDATE_COMPARISON_VERSION == ("cycle-context-logistic-candidate-comparison-v1")
    assert MODEL_CANDIDATE_ESTIMATOR == "logistic-regression"
    assert MODEL_CANDIDATE_SELECTION_METRIC == "area-under-precision-recall-curve"
    assert get_comparison_modeling_view_version() == MODELING_VIEW_V2_VERSION


def test_comparison_exposes_only_train_and_validation_and_seals_test() -> None:
    assert MODEL_CANDIDATE_DEVELOPMENT_PARTITIONS == (
        PARTITION_TRAIN,
        PARTITION_VALIDATION,
    )
    assert MODEL_CANDIDATE_SEALED_PARTITION == PARTITION_TEST
    assert PARTITION_TEST not in MODEL_CANDIDATE_DEVELOPMENT_PARTITIONS


def test_context_candidate_is_a_strict_extension_of_the_motor_current_candidate() -> None:
    assert tuple(candidate.candidate_id for candidate in MODEL_CANDIDATES) == (
        "motor-current-only",
        "motor-current-plus-cycle-context",
    )
    baseline = get_model_candidate("motor-current-only")
    context = get_model_candidate("motor-current-plus-cycle-context")

    assert baseline.feature_columns == MODEL_INPUT_COLUMNS
    assert context.feature_columns == MODEL_INPUT_COLUMNS_V2
    assert context.feature_columns[: len(baseline.feature_columns)] == baseline.feature_columns
    assert context.feature_columns[len(baseline.feature_columns) :] == (
        CYCLE_CONTEXT_MODEL_INPUT_COLUMNS
    )


def test_candidate_inputs_do_not_include_target_time_or_metadata() -> None:
    forbidden = {
        MODEL_TARGET_COLUMN,
        MODEL_TIME_COLUMN,
        "failure_horizon_status",
        "matched_failure_record_id",
        "modeling_row_status",
        "loaded_cycle_id",
    }
    for candidate in MODEL_CANDIDATES:
        assert forbidden.isdisjoint(candidate.feature_columns)
        assert len(candidate.feature_columns) == len(set(candidate.feature_columns))


def test_candidate_rejects_invalid_definitions_and_unknown_ids() -> None:
    with pytest.raises(ModelCandidateContractError, match="ID must be nonempty"):
        ModelCandidate(" ", ("feature",))
    with pytest.raises(ModelCandidateContractError, match="at least one"):
        ModelCandidate("empty", ())
    with pytest.raises(ModelCandidateContractError, match="names must be nonempty"):
        ModelCandidate("blank-feature", ("feature", " "))
    with pytest.raises(ModelCandidateContractError, match="must be unique"):
        ModelCandidate("duplicate-feature", ("feature", "feature"))
    with pytest.raises(ModelCandidateContractError, match="Unknown"):
        get_model_candidate("not-declared")
