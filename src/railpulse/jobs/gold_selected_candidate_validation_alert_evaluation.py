"""Evaluate selected-candidate alert policies from governed managed tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_TABLE
from railpulse.evaluation.modeling_view_v2_storage import (
    ModelingViewV2PersistenceError,
    catalog_modeling_view_v2_table,
)
from railpulse.models.candidate_development_data import (
    ModelCandidateDevelopmentDataError,
    prepare_model_candidate_development_data,
)
from railpulse.models.selected_candidate_validation import (
    SelectedCandidateValidationError,
    score_selected_candidate_validation,
)
from railpulse.models.selected_candidate_validation_alert_evaluation import (
    SelectedCandidateValidationAlertEvaluationError,
    evaluate_selected_candidate_validation_alerts,
)
from railpulse.models.selected_candidate_validation_profile import (
    SelectedCandidateValidationProfile,
)
from railpulse.models.validation_alert_evaluation import ValidationAlertPolicyEvaluation
from railpulse.validation.silver_storage import (
    FAILURE_ACCEPTED_TABLE,
    SilverPersistenceError,
    catalog_silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_JOB_CONTRACT_VERSION = (
    "databricks-gold-selected-candidate-validation-alert-evaluation-v1"
)


class ManagedGoldSelectedCandidateValidationAlertEvaluationError(RuntimeError):
    """Raised when governed tables cannot safely evaluate validation alert policies."""


@dataclass(frozen=True)
class ManagedGoldSelectedCandidateValidationAlertEvaluationResult:
    """Compact read-only evidence from one managed five-policy evaluation."""

    contract_version: str
    modeling_source_table: str
    failure_source_table: str
    evaluation_adapter_version: str
    evaluation_version: str
    evaluated_cycle_count: int
    operating_day_count: int
    accepted_failure_event_count: int
    profile: SelectedCandidateValidationProfile
    policy_evaluations: tuple[ValidationAlertPolicyEvaluation, ...]


def run_managed_gold_selected_candidate_validation_alert_evaluation(
    spark: SparkSession,
    namespace: CatalogNamespace,
) -> ManagedGoldSelectedCandidateValidationAlertEvaluationResult:
    """Evaluate all fixed validation policies without writing scores or opening test rows."""

    try:
        modeling_source_table = catalog_modeling_view_v2_table(
            namespace,
            MODELING_VIEW_V2_TABLE,
        )
        failure_source_table = catalog_silver_table(namespace, FAILURE_ACCEPTED_TABLE)
        view = spark.table(modeling_source_table)
        failures = spark.table(failure_source_table)
        development = prepare_model_candidate_development_data(view)
        scores = score_selected_candidate_validation(development)
        evaluation = evaluate_selected_candidate_validation_alerts(scores, failures)
    except (
        ModelingViewV2PersistenceError,
        SilverPersistenceError,
        ModelCandidateDevelopmentDataError,
        SelectedCandidateValidationError,
        SelectedCandidateValidationAlertEvaluationError,
    ) as exc:
        raise ManagedGoldSelectedCandidateValidationAlertEvaluationError(str(exc)) from exc

    return ManagedGoldSelectedCandidateValidationAlertEvaluationResult(
        contract_version=(GOLD_SELECTED_CANDIDATE_VALIDATION_ALERT_EVALUATION_JOB_CONTRACT_VERSION),
        modeling_source_table=modeling_source_table,
        failure_source_table=failure_source_table,
        evaluation_adapter_version=evaluation.adapter_version,
        evaluation_version=evaluation.evaluation_version,
        evaluated_cycle_count=evaluation.evaluated_cycle_count,
        operating_day_count=evaluation.operating_day_count,
        accepted_failure_event_count=evaluation.accepted_failure_event_count,
        profile=evaluation.alert_candidates.profile,
        policy_evaluations=evaluation.policy_evaluations,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Print one compact managed validation alert evaluation as deterministic JSON."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_selected_candidate_validation_alert_evaluation(
        spark,
        namespace,
    )
    print(json.dumps(asdict(result), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
