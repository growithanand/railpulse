from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from railpulse.evaluation.modeling_view_schema import STATUS_TRAINABLE
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION
from railpulse.jobs.gold_model_candidate_comparison import (
    ManagedGoldModelCandidateComparisonError,
    _collect_candidate_tracking_context,
)


def _identity_row(
    prediction_timestamp: datetime,
    *,
    telemetry_sha: str = "a" * 64,
) -> dict[str, object]:
    return {
        "modeling_row_status": STATUS_TRAINABLE,
        "prediction_timestamp": prediction_timestamp,
        "dataset_version": "dataset-version",
        "feature_snapshot_version": FEATURE_SNAPSHOT_V2_VERSION,
        "telemetry_source_sha256": telemetry_sha,
        "telemetry_ingestion_batch_id": "b" * 64,
        "failure_dataset_version": "dataset-version",
        "failure_source_sha256": "c" * 64,
        "failure_ingestion_batch_id": "d" * 64,
        "label_observation_end": datetime(2020, 9, 1),
    }


def test_tracking_context_uses_only_pre_test_trainable_rows(spark: SparkSession) -> None:
    view = spark.createDataFrame(
        [
            _identity_row(datetime(2020, 5, 15)),
            _identity_row(datetime(2020, 6, 15)),
            _identity_row(datetime(2020, 7, 15), telemetry_sha="e" * 64),
        ]
    )

    context = _collect_candidate_tracking_context(
        view,
        experiment_name="railpulse-candidate-comparison",
        code_version="code-version",
    )

    assert context.telemetry_source_sha256 == "a" * 64
    assert context.experiment_name == "railpulse-candidate-comparison"
    assert context.code_version == "code-version"


def test_tracking_context_rejects_mixed_development_lineage(spark: SparkSession) -> None:
    view = spark.createDataFrame(
        [
            _identity_row(datetime(2020, 5, 15)),
            _identity_row(datetime(2020, 6, 15), telemetry_sha="e" * 64),
        ]
    )

    with pytest.raises(ManagedGoldModelCandidateComparisonError, match="exactly one"):
        _collect_candidate_tracking_context(
            view,
            experiment_name="railpulse-candidate-comparison",
            code_version="code-version",
        )


def test_tracking_context_rejects_missing_boundary_columns(spark: SparkSession) -> None:
    view = spark.createDataFrame([_identity_row(datetime(2020, 5, 15))]).drop("modeling_row_status")

    with pytest.raises(ManagedGoldModelCandidateComparisonError, match="modeling_row_status"):
        _collect_candidate_tracking_context(
            view,
            experiment_name="railpulse-candidate-comparison",
            code_version="code-version",
        )
