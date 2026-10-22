"""Run the guarded model-candidate comparison from the managed modelling view."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.chronological_splits import get_selected_split_candidate
from railpulse.evaluation.modeling_view_schema import MODEL_TIME_COLUMN, STATUS_TRAINABLE
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_VIEW_V2_TABLE,
)
from railpulse.evaluation.modeling_view_v2_storage import (
    ModelingViewV2PersistenceError,
    catalog_modeling_view_v2_table,
)
from railpulse.models.candidate_comparison import (
    ModelCandidateComparison,
    ModelCandidateComparisonError,
    compare_model_candidates,
)
from railpulse.models.candidate_development_data import (
    ModelCandidateDevelopmentDataError,
    prepare_model_candidate_development_data,
)
from railpulse.models.candidate_tracking import (
    CandidateTrackingContext,
    MlflowTrackingApi,
    ModelCandidateTrackingError,
    TrackedCandidateComparison,
    track_candidate_comparison,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_MODEL_CANDIDATE_COMPARISON_JOB_CONTRACT_VERSION = (
    "databricks-gold-model-candidate-comparison-v1"
)
_TRACKING_CONTEXT_COLUMNS = (
    "dataset_version",
    "feature_snapshot_version",
    "telemetry_source_sha256",
    "telemetry_ingestion_batch_id",
    "failure_dataset_version",
    "failure_source_sha256",
    "failure_ingestion_batch_id",
    "label_observation_end",
)
_TRACKING_REQUIRED_COLUMNS = {
    "modeling_row_status",
    MODEL_TIME_COLUMN,
    *_TRACKING_CONTEXT_COLUMNS,
}


class ManagedGoldModelCandidateComparisonError(RuntimeError):
    """Raised when managed modelling rows cannot produce one tracked comparison."""


@dataclass(frozen=True)
class ManagedGoldModelCandidateComparisonResult:
    """Source, comparison, and MLflow identities for one managed comparison run."""

    contract_version: str
    source_table: str
    tracking_context: CandidateTrackingContext
    comparison: ModelCandidateComparison
    tracking: TrackedCandidateComparison


def _collect_candidate_tracking_context(
    view: DataFrame,
    *,
    experiment_name: str,
    code_version: str,
) -> CandidateTrackingContext:
    missing_columns = sorted(_TRACKING_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise ManagedGoldModelCandidateComparisonError(
            "Managed modelling view is missing tracking columns: " + ", ".join(missing_columns)
        )

    split = get_selected_split_candidate()
    development_rows = view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & F.col(MODEL_TIME_COLUMN).isNotNull()
        & (F.col(MODEL_TIME_COLUMN) < F.lit(split.test_start))
    )
    identities = development_rows.select(*_TRACKING_CONTEXT_COLUMNS).distinct().limit(2).collect()
    if len(identities) != 1:
        raise ManagedGoldModelCandidateComparisonError(
            "Managed development rows must contain exactly one complete tracking identity"
        )
    identity = identities[0]
    try:
        return CandidateTrackingContext(
            experiment_name=experiment_name,
            code_version=code_version,
            dataset_version=identity.dataset_version,
            feature_snapshot_version=identity.feature_snapshot_version,
            telemetry_source_sha256=identity.telemetry_source_sha256,
            telemetry_ingestion_batch_id=identity.telemetry_ingestion_batch_id,
            failure_dataset_version=identity.failure_dataset_version,
            failure_source_sha256=identity.failure_source_sha256,
            failure_ingestion_batch_id=identity.failure_ingestion_batch_id,
            label_observation_end=identity.label_observation_end,
        )
    except ModelCandidateTrackingError as exc:
        raise ManagedGoldModelCandidateComparisonError(str(exc)) from exc


def run_managed_gold_model_candidate_comparison(
    spark: SparkSession,
    namespace: CatalogNamespace,
    *,
    experiment_name: str,
    code_version: str,
    mlflow_api: MlflowTrackingApi | None = None,
) -> ManagedGoldModelCandidateComparisonResult:
    """Fit and track the fixed candidates without exposing the sealed test period."""

    try:
        source_table = catalog_modeling_view_v2_table(namespace, MODELING_VIEW_V2_TABLE)
        view = spark.table(source_table)
        tracking_context = _collect_candidate_tracking_context(
            view,
            experiment_name=experiment_name,
            code_version=code_version,
        )
        development_data = prepare_model_candidate_development_data(view)
        comparison = compare_model_candidates(development_data)
        tracking = track_candidate_comparison(
            comparison,
            tracking_context,
            mlflow_api=mlflow_api,
        )
    except (
        ModelingViewV2PersistenceError,
        ModelCandidateDevelopmentDataError,
        ModelCandidateComparisonError,
        ModelCandidateTrackingError,
    ) as exc:
        raise ManagedGoldModelCandidateComparisonError(str(exc)) from exc

    return ManagedGoldModelCandidateComparisonResult(
        contract_version=GOLD_MODEL_CANDIDATE_COMPARISON_JOB_CONTRACT_VERSION,
        source_table=source_table,
        tracking_context=tracking_context,
        comparison=comparison,
        tracking=tracking,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    parser.add_argument("--experiment_name", required=True)
    parser.add_argument("--code_version", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the managed Gold-to-MLflow candidate comparison."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_model_candidate_comparison(
        spark,
        namespace,
        experiment_name=args.experiment_name,
        code_version=args.code_version,
    )
    print(json.dumps(asdict(result), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
