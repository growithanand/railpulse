"""Verify the development-only model-selection contract."""

from __future__ import annotations

from dataclasses import replace

import pytest

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_SELECTION_METRIC,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_SCOPE,
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
    CandidateValidationEvidence,
    DevelopmentModelSelectionError,
    get_development_model_selection,
    get_selected_development_candidate,
)


def test_selection_freezes_the_managed_comparison_evidence() -> None:
    selection = get_development_model_selection()

    assert selection.selection_version == DEVELOPMENT_MODEL_SELECTION_VERSION
    assert selection.selection_scope == DEVELOPMENT_MODEL_SELECTION_SCOPE
    assert selection.comparison_version == MODEL_CANDIDATE_COMPARISON_VERSION
    assert selection.selection_metric == MODEL_CANDIDATE_SELECTION_METRIC
    assert selection.selected_candidate_id == DEVELOPMENT_SELECTED_CANDIDATE_ID
    assert selection.sealed_partition == PARTITION_TEST
    assert tuple(evidence.candidate_id for evidence in selection.candidate_evidence) == (
        "motor-current-only",
        "motor-current-plus-cycle-context",
    )
    assert tuple(
        evidence.validation_area_under_pr for evidence in selection.candidate_evidence
    ) == pytest.approx((0.011954226762354818, 0.008632411016820048))


def test_selection_retains_the_simpler_candidate_feature_contract() -> None:
    candidate = get_selected_development_candidate()

    assert candidate.candidate_id == "motor-current-only"
    assert candidate.feature_columns == MODEL_INPUT_COLUMNS


def test_selection_uses_one_reconciled_population_and_keeps_test_sealed() -> None:
    selection = get_development_model_selection()
    populations = {evidence.population_identity for evidence in selection.candidate_evidence}

    assert populations == {(6_325, 4, 6_321, 4_730, 5, 4_725)}
    assert selection.sealed_partition == PARTITION_TEST
    assert all(
        not hasattr(evidence, "test_area_under_pr") for evidence in selection.candidate_evidence
    )


def test_candidate_evidence_rejects_invalid_counts_and_metrics() -> None:
    with pytest.raises(DevelopmentModelSelectionError, match="does not reconcile"):
        CandidateValidationEvidence(
            "candidate",
            10,
            1,
            8,
            10,
            1,
            9,
            0.1,
        )
    with pytest.raises(DevelopmentModelSelectionError, match="within"):
        CandidateValidationEvidence(
            "candidate",
            10,
            1,
            9,
            10,
            1,
            9,
            float("nan"),
        )


def test_selection_rejects_incomplete_or_nonleading_choices() -> None:
    selection = get_development_model_selection()

    with pytest.raises(DevelopmentModelSelectionError, match="every candidate"):
        replace(selection, candidate_evidence=selection.candidate_evidence[:1])
    with pytest.raises(DevelopmentModelSelectionError, match="unique validation AUPRC leader"):
        replace(selection, selected_candidate_id="motor-current-plus-cycle-context")
    with pytest.raises(DevelopmentModelSelectionError, match="shared train and validation"):
        changed_population = replace(
            selection.candidate_evidence[1],
            validation_row_count=4_731,
            validation_negative_count=4_726,
        )
        replace(
            selection,
            candidate_evidence=(selection.candidate_evidence[0], changed_population),
        )
