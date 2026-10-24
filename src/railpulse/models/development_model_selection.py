"""Freeze the development-only model selection without opening the test partition."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from railpulse.evaluation.chronological_splits import PARTITION_TEST
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_SELECTION_METRIC,
    MODEL_CANDIDATES,
    ModelCandidate,
    get_model_candidate,
)

DEVELOPMENT_MODEL_SELECTION_VERSION = "motor-current-logistic-development-selection-v1"
DEVELOPMENT_MODEL_SELECTION_SCOPE = "alert-policy-development-only"
DEVELOPMENT_SELECTED_CANDIDATE_ID = "motor-current-only"
DEVELOPMENT_SELECTION_REASON = "highest-validation-aupr-and-lower-feature-complexity"


class DevelopmentModelSelectionError(ValueError):
    """Raised when development selection evidence violates the reviewed contract."""


@dataclass(frozen=True)
class CandidateValidationEvidence:
    """Validation-only evidence for one candidate from the managed comparison."""

    candidate_id: str
    train_row_count: int
    train_positive_count: int
    train_negative_count: int
    validation_row_count: int
    validation_positive_count: int
    validation_negative_count: int
    validation_area_under_pr: float

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise DevelopmentModelSelectionError("Candidate evidence ID must be nonempty")
        for partition, row_count, positive_count, negative_count in (
            (
                "training",
                self.train_row_count,
                self.train_positive_count,
                self.train_negative_count,
            ),
            (
                "validation",
                self.validation_row_count,
                self.validation_positive_count,
                self.validation_negative_count,
            ),
        ):
            if row_count <= 0 or positive_count <= 0 or negative_count <= 0:
                raise DevelopmentModelSelectionError(
                    f"{partition.capitalize()} evidence must contain both labels"
                )
            if positive_count + negative_count != row_count:
                raise DevelopmentModelSelectionError(
                    f"{partition.capitalize()} evidence does not reconcile"
                )
        if not isfinite(self.validation_area_under_pr) or not (
            0.0 <= self.validation_area_under_pr <= 1.0
        ):
            raise DevelopmentModelSelectionError(
                "Validation AUPRC must be finite and within [0, 1]"
            )

    @property
    def population_identity(self) -> tuple[int, int, int, int, int, int]:
        """Return the shared train/validation population used for like-for-like comparison."""

        return (
            self.train_row_count,
            self.train_positive_count,
            self.train_negative_count,
            self.validation_row_count,
            self.validation_positive_count,
            self.validation_negative_count,
        )


@dataclass(frozen=True)
class DevelopmentModelSelection:
    """Reviewed candidate choice for development-only alert-policy work."""

    selection_version: str
    selection_scope: str
    comparison_version: str
    selection_metric: str
    selection_reason: str
    selected_candidate_id: str
    sealed_partition: str
    candidate_evidence: tuple[CandidateValidationEvidence, ...]

    def __post_init__(self) -> None:
        if (
            self.selection_version != DEVELOPMENT_MODEL_SELECTION_VERSION
            or self.selection_scope != DEVELOPMENT_MODEL_SELECTION_SCOPE
            or self.comparison_version != MODEL_CANDIDATE_COMPARISON_VERSION
            or self.selection_metric != MODEL_CANDIDATE_SELECTION_METRIC
            or self.selection_reason != DEVELOPMENT_SELECTION_REASON
            or self.sealed_partition != PARTITION_TEST
        ):
            raise DevelopmentModelSelectionError(
                "Development model selection metadata does not match the reviewed contract"
            )

        declared_ids = tuple(candidate.candidate_id for candidate in MODEL_CANDIDATES)
        evidence_ids = tuple(evidence.candidate_id for evidence in self.candidate_evidence)
        if evidence_ids != declared_ids:
            raise DevelopmentModelSelectionError(
                "Selection evidence must contain every candidate in reviewed order"
            )
        if len({evidence.population_identity for evidence in self.candidate_evidence}) != 1:
            raise DevelopmentModelSelectionError(
                "Selection evidence must use one shared train and validation population"
            )
        if self.selected_candidate_id not in evidence_ids:
            raise DevelopmentModelSelectionError("Selected candidate is not declared")

        best_metric = max(evidence.validation_area_under_pr for evidence in self.candidate_evidence)
        leaders = tuple(
            evidence.candidate_id
            for evidence in self.candidate_evidence
            if evidence.validation_area_under_pr == best_metric
        )
        if leaders != (self.selected_candidate_id,):
            raise DevelopmentModelSelectionError(
                "Selected candidate must be the unique validation AUPRC leader"
            )


DEVELOPMENT_MODEL_SELECTION = DevelopmentModelSelection(
    selection_version=DEVELOPMENT_MODEL_SELECTION_VERSION,
    selection_scope=DEVELOPMENT_MODEL_SELECTION_SCOPE,
    comparison_version=MODEL_CANDIDATE_COMPARISON_VERSION,
    selection_metric=MODEL_CANDIDATE_SELECTION_METRIC,
    selection_reason=DEVELOPMENT_SELECTION_REASON,
    selected_candidate_id=DEVELOPMENT_SELECTED_CANDIDATE_ID,
    sealed_partition=PARTITION_TEST,
    candidate_evidence=(
        CandidateValidationEvidence(
            candidate_id="motor-current-only",
            train_row_count=6_325,
            train_positive_count=4,
            train_negative_count=6_321,
            validation_row_count=4_730,
            validation_positive_count=5,
            validation_negative_count=4_725,
            validation_area_under_pr=0.011954226762354818,
        ),
        CandidateValidationEvidence(
            candidate_id="motor-current-plus-cycle-context",
            train_row_count=6_325,
            train_positive_count=4,
            train_negative_count=6_321,
            validation_row_count=4_730,
            validation_positive_count=5,
            validation_negative_count=4_725,
            validation_area_under_pr=0.008632411016820048,
        ),
    ),
)


def get_development_model_selection() -> DevelopmentModelSelection:
    """Return the reviewed development-only selection and its managed-run evidence."""

    return DEVELOPMENT_MODEL_SELECTION


def get_selected_development_candidate() -> ModelCandidate:
    """Return the selected candidate while preserving its original feature contract."""

    return get_model_candidate(DEVELOPMENT_MODEL_SELECTION.selected_candidate_id)
