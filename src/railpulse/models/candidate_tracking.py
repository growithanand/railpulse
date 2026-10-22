"""Log guarded candidate-comparison evidence to MLflow without test-period data."""

from __future__ import annotations

import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import datetime
from math import isfinite
from typing import Any, Protocol

from railpulse.evaluation.chronological_splits import get_selected_split_candidate
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.models.candidate_comparison import (
    CandidateComparisonResult,
    ModelCandidateComparison,
)
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_ELASTIC_NET,
    MODEL_CANDIDATE_ESTIMATOR,
    MODEL_CANDIDATE_FAMILY,
    MODEL_CANDIDATE_FIT_INTERCEPT,
    MODEL_CANDIDATE_MAX_ITERATIONS,
    MODEL_CANDIDATE_REGULARIZATION,
    MODEL_CANDIDATE_SEALED_PARTITION,
    MODEL_CANDIDATE_SELECTION_METRIC,
    MODEL_CANDIDATE_STANDARDIZATION,
    MODEL_CANDIDATES,
)

MODEL_CANDIDATE_TRACKING_VERSION = "cycle-context-logistic-mlflow-tracking-v1"
MODEL_CANDIDATE_PARENT_RUN_NAME = "cycle-context-logistic-candidate-comparison"
MODEL_CANDIDATE_CONTRACT_ARTIFACT = "comparison_contract.json"
MODEL_CANDIDATE_EVIDENCE_ARTIFACT = "candidate_evidence.json"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class ModelCandidateTrackingError(ValueError):
    """Raised when comparison evidence cannot be logged under the reviewed contract."""


class _RunInfo(Protocol):
    run_id: str


class _ActiveRun(Protocol):
    info: _RunInfo


class MlflowTrackingApi(Protocol):
    """Small MLflow API surface used by the tracking boundary."""

    def set_experiment(self, experiment_name: str) -> Any: ...

    def start_run(
        self,
        *,
        run_name: str,
        nested: bool = False,
        tags: dict[str, str] | None = None,
    ) -> AbstractContextManager[_ActiveRun]: ...

    def log_params(self, params: dict[str, Any]) -> None: ...

    def log_metric(self, key: str, value: float) -> None: ...

    def log_dict(self, dictionary: dict[str, Any], artifact_file: str) -> None: ...


@dataclass(frozen=True)
class CandidateTrackingContext:
    """Immutable source and code identity attached to one comparison run group."""

    experiment_name: str
    code_version: str
    dataset_version: str
    feature_snapshot_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_dataset_version: str
    failure_source_sha256: str
    failure_ingestion_batch_id: str
    label_observation_end: datetime

    def __post_init__(self) -> None:
        text_values = (
            self.experiment_name,
            self.code_version,
            self.dataset_version,
            self.feature_snapshot_version,
            self.telemetry_source_sha256,
            self.telemetry_ingestion_batch_id,
            self.failure_dataset_version,
            self.failure_source_sha256,
            self.failure_ingestion_batch_id,
        )
        if any(not value.strip() for value in text_values):
            raise ModelCandidateTrackingError("Tracking context values must be nonempty")
        if (
            self.feature_snapshot_version != FEATURE_SNAPSHOT_V2_VERSION
            or self.dataset_version != self.failure_dataset_version
        ):
            raise ModelCandidateTrackingError(
                "Tracking context is incompatible with the candidate comparison contract"
            )
        provenance_values = (
            self.telemetry_source_sha256,
            self.telemetry_ingestion_batch_id,
            self.failure_source_sha256,
            self.failure_ingestion_batch_id,
        )
        if any(_SHA256_PATTERN.fullmatch(value) is None for value in provenance_values):
            raise ModelCandidateTrackingError(
                "Tracking source and ingestion identities must be lowercase SHA-256 values"
            )
        if not isinstance(self.label_observation_end, datetime):
            raise ModelCandidateTrackingError("Label observation end must be a datetime")
        if self.label_observation_end.tzinfo is not None:
            raise ModelCandidateTrackingError(
                "Label observation end must use the source's timezone-free timestamp contract"
            )


@dataclass(frozen=True)
class TrackedCandidateRun:
    """MLflow identity for one nested candidate run."""

    candidate_id: str
    run_id: str


@dataclass(frozen=True)
class TrackedCandidateComparison:
    """MLflow identities created for one parent comparison and its candidate children."""

    tracking_version: str
    experiment_name: str
    parent_run_id: str
    candidate_runs: tuple[TrackedCandidateRun, ...]


def _validate_result(
    result: CandidateComparisonResult,
    *,
    expected_candidate_id: str,
    expected_features: tuple[str, ...],
) -> None:
    if result.candidate_id != expected_candidate_id or result.feature_columns != expected_features:
        raise ModelCandidateTrackingError(
            "Candidate result does not match the declared feature set"
        )
    if (
        result.train_row_count <= 0
        or result.train_positive_count <= 0
        or result.train_negative_count <= 0
        or result.train_positive_count + result.train_negative_count != result.train_row_count
        or result.validation_row_count <= 0
        or result.validation_positive_count <= 0
        or result.validation_negative_count <= 0
        or result.validation_positive_count + result.validation_negative_count
        != result.validation_row_count
        or not isfinite(result.validation_area_under_pr)
        or not 0.0 <= result.validation_area_under_pr <= 1.0
        or not isfinite(result.fitted_intercept)
        or len(result.fitted_coefficients) != len(expected_features)
        or any(not isfinite(value) for value in result.fitted_coefficients)
        or result.completed_iterations < 0
        or result.completed_iterations > MODEL_CANDIDATE_MAX_ITERATIONS
    ):
        raise ModelCandidateTrackingError(
            f"Candidate {result.candidate_id} contains invalid comparison evidence"
        )


def _validate_comparison(comparison: ModelCandidateComparison) -> None:
    selected_split = get_selected_split_candidate()
    if (
        comparison.comparison_version != MODEL_CANDIDATE_COMPARISON_VERSION
        or comparison.modeling_view_version != MODELING_VIEW_V2_VERSION
        or comparison.split_candidate_id != selected_split.candidate_id
        or comparison.estimator != MODEL_CANDIDATE_ESTIMATOR
        or comparison.selection_metric != MODEL_CANDIDATE_SELECTION_METRIC
        or comparison.max_iterations != MODEL_CANDIDATE_MAX_ITERATIONS
        or comparison.regularization != MODEL_CANDIDATE_REGULARIZATION
        or comparison.elastic_net != MODEL_CANDIDATE_ELASTIC_NET
        or comparison.fit_intercept is not MODEL_CANDIDATE_FIT_INTERCEPT
        or comparison.standardization is not MODEL_CANDIDATE_STANDARDIZATION
        or comparison.family != MODEL_CANDIDATE_FAMILY
        or len(comparison.candidates) != len(MODEL_CANDIDATES)
    ):
        raise ModelCandidateTrackingError(
            "Candidate comparison is incompatible with the MLflow tracking contract"
        )
    for result, candidate in zip(comparison.candidates, MODEL_CANDIDATES, strict=True):
        _validate_result(
            result,
            expected_candidate_id=candidate.candidate_id,
            expected_features=candidate.feature_columns,
        )


def _load_mlflow() -> MlflowTrackingApi:
    try:
        import mlflow
    except ImportError as error:
        raise ModelCandidateTrackingError(
            "MLflow is required to track the candidate comparison"
        ) from error
    return mlflow


def _parent_tags(
    comparison: ModelCandidateComparison,
    context: CandidateTrackingContext,
) -> dict[str, str]:
    return {
        "railpulse.stage": "candidate-comparison",
        "railpulse.tracking_version": MODEL_CANDIDATE_TRACKING_VERSION,
        "railpulse.comparison_version": comparison.comparison_version,
        "railpulse.modeling_view_version": comparison.modeling_view_version,
        "railpulse.split_candidate_id": comparison.split_candidate_id,
        "railpulse.sealed_partition": MODEL_CANDIDATE_SEALED_PARTITION,
        "railpulse.code_version": context.code_version,
        "railpulse.dataset_version": context.dataset_version,
        "railpulse.feature_snapshot_version": context.feature_snapshot_version,
        "railpulse.telemetry_source_sha256": context.telemetry_source_sha256,
        "railpulse.telemetry_ingestion_batch_id": context.telemetry_ingestion_batch_id,
        "railpulse.failure_dataset_version": context.failure_dataset_version,
        "railpulse.failure_source_sha256": context.failure_source_sha256,
        "railpulse.failure_ingestion_batch_id": context.failure_ingestion_batch_id,
        "railpulse.label_observation_end": context.label_observation_end.isoformat(sep=" "),
    }


def _parent_params(comparison: ModelCandidateComparison) -> dict[str, Any]:
    return {
        "estimator": comparison.estimator,
        "selection_metric": comparison.selection_metric,
        "max_iterations": comparison.max_iterations,
        "regularization": comparison.regularization,
        "elastic_net": comparison.elastic_net,
        "fit_intercept": comparison.fit_intercept,
        "standardization": comparison.standardization,
        "family": comparison.family,
        "candidate_count": len(comparison.candidates),
    }


def _candidate_params(result: CandidateComparisonResult) -> dict[str, Any]:
    return {
        "candidate_id": result.candidate_id,
        "feature_count": len(result.feature_columns),
        "feature_columns": ",".join(result.feature_columns),
        "train_row_count": result.train_row_count,
        "train_positive_count": result.train_positive_count,
        "train_negative_count": result.train_negative_count,
        "validation_row_count": result.validation_row_count,
        "validation_positive_count": result.validation_positive_count,
        "validation_negative_count": result.validation_negative_count,
        "completed_iterations": result.completed_iterations,
    }


def track_candidate_comparison(
    comparison: ModelCandidateComparison,
    context: CandidateTrackingContext,
    *,
    mlflow_api: MlflowTrackingApi | None = None,
) -> TrackedCandidateComparison:
    """Log approved development evidence and return the created MLflow run identities."""

    _validate_comparison(comparison)
    tracking = mlflow_api if mlflow_api is not None else _load_mlflow()
    tracking.set_experiment(context.experiment_name)
    candidate_runs = []
    with tracking.start_run(
        run_name=MODEL_CANDIDATE_PARENT_RUN_NAME,
        tags=_parent_tags(comparison, context),
    ) as parent_run:
        parent_run_id = parent_run.info.run_id
        if not parent_run_id.strip():
            raise ModelCandidateTrackingError("MLflow parent run ID must be nonempty")
        tracking.log_params(_parent_params(comparison))
        tracking.log_dict(
            {
                "tracking_version": MODEL_CANDIDATE_TRACKING_VERSION,
                "comparison_version": comparison.comparison_version,
                "modeling_view_version": comparison.modeling_view_version,
                "split_candidate_id": comparison.split_candidate_id,
                "sealed_partition": MODEL_CANDIDATE_SEALED_PARTITION,
                "candidate_ids": [result.candidate_id for result in comparison.candidates],
            },
            MODEL_CANDIDATE_CONTRACT_ARTIFACT,
        )
        for result in comparison.candidates:
            with tracking.start_run(
                run_name=result.candidate_id,
                nested=True,
                tags={
                    "railpulse.stage": "candidate-comparison-child",
                    "railpulse.tracking_version": MODEL_CANDIDATE_TRACKING_VERSION,
                    "railpulse.comparison_version": comparison.comparison_version,
                    "railpulse.candidate_id": result.candidate_id,
                    "railpulse.sealed_partition": MODEL_CANDIDATE_SEALED_PARTITION,
                },
            ) as candidate_run:
                run_id = candidate_run.info.run_id
                if not run_id.strip():
                    raise ModelCandidateTrackingError("MLflow candidate run ID must be nonempty")
                tracking.log_params(_candidate_params(result))
                tracking.log_metric(
                    "validation_area_under_pr",
                    result.validation_area_under_pr,
                )
                tracking.log_dict(asdict(result), MODEL_CANDIDATE_EVIDENCE_ARTIFACT)
                candidate_runs.append(
                    TrackedCandidateRun(candidate_id=result.candidate_id, run_id=run_id)
                )
    return TrackedCandidateComparison(
        tracking_version=MODEL_CANDIDATE_TRACKING_VERSION,
        experiment_name=context.experiment_name,
        parent_run_id=parent_run_id,
        candidate_runs=tuple(candidate_runs),
    )
