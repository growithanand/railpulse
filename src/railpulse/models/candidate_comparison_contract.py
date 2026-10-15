"""Frozen contract for the first cycle-context model candidate comparison."""

from __future__ import annotations

from dataclasses import dataclass

from railpulse.evaluation.chronological_splits import (
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
)
from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.evaluation.modeling_view_v2_schema import (
    MODEL_INPUT_COLUMNS_V2,
    MODELING_VIEW_V2_VERSION,
)

MODEL_CANDIDATE_COMPARISON_VERSION = "cycle-context-logistic-candidate-comparison-v1"
MODEL_CANDIDATE_ESTIMATOR = "logistic-regression"
MODEL_CANDIDATE_SELECTION_METRIC = "area-under-precision-recall-curve"
MODEL_CANDIDATE_DEVELOPMENT_PARTITIONS = (PARTITION_TRAIN, PARTITION_VALIDATION)
MODEL_CANDIDATE_SEALED_PARTITION = PARTITION_TEST


class ModelCandidateContractError(ValueError):
    """Raised when a model candidate does not have a safe input contract."""


@dataclass(frozen=True)
class ModelCandidate:
    """One named feature set evaluated with the shared estimator and population."""

    candidate_id: str
    feature_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ModelCandidateContractError("Model candidate ID must be nonempty")
        if not self.feature_columns:
            raise ModelCandidateContractError("Model candidate requires at least one feature")
        if any(not column.strip() for column in self.feature_columns):
            raise ModelCandidateContractError("Model candidate feature names must be nonempty")
        if len(self.feature_columns) != len(set(self.feature_columns)):
            raise ModelCandidateContractError("Model candidate features must be unique")


MODEL_CANDIDATES = (
    ModelCandidate(
        candidate_id="motor-current-only",
        feature_columns=MODEL_INPUT_COLUMNS,
    ),
    ModelCandidate(
        candidate_id="motor-current-plus-cycle-context",
        feature_columns=MODEL_INPUT_COLUMNS_V2,
    ),
)


def get_model_candidate(candidate_id: str) -> ModelCandidate:
    """Return one declared comparison candidate by its stable identifier."""

    for candidate in MODEL_CANDIDATES:
        if candidate.candidate_id == candidate_id:
            return candidate
    raise ModelCandidateContractError(f"Unknown model candidate: {candidate_id}")


def get_comparison_modeling_view_version() -> str:
    """Return the shared population contract used for the like-for-like comparison."""

    return MODELING_VIEW_V2_VERSION
