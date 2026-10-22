"""Verify the guarded MLflow candidate-comparison boundary."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from railpulse.evaluation.modeling_view_schema import MODEL_INPUT_COLUMNS
from railpulse.evaluation.modeling_view_v2_schema import (
    MODEL_INPUT_COLUMNS_V2,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.models.candidate_comparison import (
    CandidateComparisonResult,
    ModelCandidateComparison,
)
from railpulse.models.candidate_tracking import (
    MODEL_CANDIDATE_CONTRACT_ARTIFACT,
    MODEL_CANDIDATE_EVIDENCE_ARTIFACT,
    MODEL_CANDIDATE_TRACKING_VERSION,
    CandidateTrackingContext,
    ModelCandidateTrackingError,
    track_candidate_comparison,
)


class FakeMlflow:
    """In-memory recorder for the narrow MLflow surface used by the boundary."""

    def __init__(self) -> None:
        self.experiment_name = ""
        self.runs: list[dict[str, Any]] = []
        self._active_run_indexes: list[int] = []

    def set_experiment(self, experiment_name: str) -> None:
        self.experiment_name = experiment_name

    @contextmanager
    def start_run(
        self,
        *,
        run_name: str,
        nested: bool = False,
        tags: dict[str, str] | None = None,
    ):
        run_id = f"run-{len(self.runs) + 1}"
        self.runs.append(
            {
                "run_id": run_id,
                "run_name": run_name,
                "nested": nested,
                "tags": dict(tags or {}),
                "params": {},
                "metrics": {},
                "artifacts": {},
            }
        )
        self._active_run_indexes.append(len(self.runs) - 1)
        try:
            yield SimpleNamespace(info=SimpleNamespace(run_id=run_id))
        finally:
            self._active_run_indexes.pop()

    def log_params(self, params: dict[str, Any]) -> None:
        self.runs[self._active_run_indexes[-1]]["params"].update(params)

    def log_metric(self, key: str, value: float) -> None:
        self.runs[self._active_run_indexes[-1]]["metrics"][key] = value

    def log_dict(self, dictionary: dict[str, Any], artifact_file: str) -> None:
        self.runs[self._active_run_indexes[-1]]["artifacts"][artifact_file] = dictionary


def _result(candidate_id: str, features: tuple[str, ...], metric: float):
    return CandidateComparisonResult(
        candidate_id=candidate_id,
        feature_columns=features,
        train_row_count=100,
        train_positive_count=4,
        train_negative_count=96,
        validation_row_count=50,
        validation_positive_count=5,
        validation_negative_count=45,
        validation_area_under_pr=metric,
        fitted_intercept=-1.5,
        fitted_coefficients=tuple(float(index) / 10 for index in range(len(features))),
        completed_iterations=8,
    )


def _comparison() -> ModelCandidateComparison:
    return ModelCandidateComparison(
        comparison_version="cycle-context-logistic-candidate-comparison-v1",
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        split_candidate_id="validation-2020-06_test-2020-07",
        estimator="logistic-regression",
        selection_metric="area-under-precision-recall-curve",
        max_iterations=100,
        regularization=0.1,
        elastic_net=0.0,
        fit_intercept=True,
        standardization=True,
        family="binomial",
        candidates=(
            _result("motor-current-only", MODEL_INPUT_COLUMNS, 0.2),
            _result("motor-current-plus-cycle-context", MODEL_INPUT_COLUMNS_V2, 0.3),
        ),
    )


def _context() -> CandidateTrackingContext:
    return CandidateTrackingContext(
        experiment_name="railpulse-candidate-comparison",
        code_version="commit-version",
        dataset_version="dataset-version",
        feature_snapshot_version=FEATURE_SNAPSHOT_V2_VERSION,
        telemetry_source_sha256="a" * 64,
        telemetry_ingestion_batch_id="b" * 64,
        failure_dataset_version="dataset-version",
        failure_source_sha256="c" * 64,
        failure_ingestion_batch_id="d" * 64,
        label_observation_end=datetime(2020, 9, 1),
    )


def test_tracking_creates_one_parent_and_two_guarded_candidate_runs() -> None:
    fake = FakeMlflow()
    tracked = track_candidate_comparison(_comparison(), _context(), mlflow_api=fake)

    assert tracked.tracking_version == MODEL_CANDIDATE_TRACKING_VERSION
    assert tracked.experiment_name == "railpulse-candidate-comparison"
    assert tracked.parent_run_id == "run-1"
    assert [(run.candidate_id, run.run_id) for run in tracked.candidate_runs] == [
        ("motor-current-only", "run-2"),
        ("motor-current-plus-cycle-context", "run-3"),
    ]
    assert fake.experiment_name == "railpulse-candidate-comparison"
    assert len(fake.runs) == 3

    parent, baseline, context = fake.runs
    assert parent["nested"] is False
    assert parent["tags"]["railpulse.sealed_partition"] == "test"
    assert parent["params"]["candidate_count"] == 2
    assert MODEL_CANDIDATE_CONTRACT_ARTIFACT in parent["artifacts"]
    for run, expected_metric in ((baseline, 0.2), (context, 0.3)):
        assert run["nested"] is True
        assert run["tags"]["railpulse.sealed_partition"] == "test"
        assert run["metrics"] == {"validation_area_under_pr": expected_metric}
        assert MODEL_CANDIDATE_EVIDENCE_ARTIFACT in run["artifacts"]
        assert all("test" not in key for key in run["metrics"])
        assert all("threshold" not in key for key in run["params"])


def test_tracking_rejects_incompatible_or_invalid_evidence_before_starting_runs() -> None:
    fake = FakeMlflow()
    incompatible = replace(_comparison(), modeling_view_version="unsupported")
    with pytest.raises(ModelCandidateTrackingError, match="incompatible"):
        track_candidate_comparison(incompatible, _context(), mlflow_api=fake)

    invalid_result = replace(_comparison().candidates[0], validation_area_under_pr=1.1)
    invalid = replace(
        _comparison(),
        candidates=(invalid_result, _comparison().candidates[1]),
    )
    with pytest.raises(ModelCandidateTrackingError, match="invalid comparison evidence"):
        track_candidate_comparison(invalid, _context(), mlflow_api=fake)
    assert fake.runs == []


def test_tracking_context_rejects_blank_or_timezone_aware_identity() -> None:
    with pytest.raises(ModelCandidateTrackingError, match="nonempty"):
        replace(_context(), code_version=" ")
    with pytest.raises(ModelCandidateTrackingError, match="must be a datetime"):
        replace(_context(), label_observation_end="2020-09-01")  # type: ignore[arg-type]
    with pytest.raises(ModelCandidateTrackingError, match="timezone-free"):
        replace(_context(), label_observation_end=datetime(2020, 9, 1, tzinfo=UTC))


def test_tracking_context_requires_fixed_snapshot_and_sha256_lineage() -> None:
    with pytest.raises(ModelCandidateTrackingError, match="incompatible"):
        replace(_context(), feature_snapshot_version="unsupported")
    with pytest.raises(ModelCandidateTrackingError, match="incompatible"):
        replace(_context(), failure_dataset_version="different-dataset")
    with pytest.raises(ModelCandidateTrackingError, match="SHA-256"):
        replace(_context(), telemetry_source_sha256="not-a-sha256")
