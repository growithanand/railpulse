"""Evaluate the frozen model and alert policy once from governed managed tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_TABLE
from railpulse.evaluation.modeling_view_v2_storage import (
    ModelingViewV2PersistenceError,
    catalog_modeling_view_v2_table,
)
from railpulse.models.sealed_test_evaluation_contract import (
    SEALED_TEST_AGGREGATE_METRICS,
    SEALED_TEST_EVALUATION_CONTRACT_VERSION,
    get_sealed_test_evaluation_contract,
)
from railpulse.models.selected_candidate_sealed_test import (
    SelectedCandidateSealedTestError,
    score_selected_candidate_sealed_test,
)
from railpulse.models.selected_candidate_sealed_test_alert_evaluation import (
    SelectedCandidateSealedTestAlertEvaluationError,
    evaluate_selected_candidate_sealed_test_alerts,
)
from railpulse.validation.silver_storage import (
    FAILURE_ACCEPTED_TABLE,
    SilverPersistenceError,
    catalog_silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_SELECTED_CANDIDATE_SEALED_TEST_EVALUATION_JOB_CONTRACT_VERSION = (
    "databricks-gold-selected-candidate-sealed-test-evaluation-v1"
)


class ManagedGoldSelectedCandidateSealedTestEvaluationError(RuntimeError):
    """Raised when a managed held-out read is unapproved or cannot reconcile safely."""


@dataclass(frozen=True)
class ManagedSealedTestAggregateMetrics:
    """Exact aggregate metric allowlist for the one-time held-out evaluation."""

    test_row_count: int
    test_positive_count: int
    test_negative_count: int
    test_area_under_precision_recall_curve: float
    operating_day_count: int
    accepted_failure_event_count: int
    detected_failure_event_count: int
    missed_failure_event_count: int
    event_detection_rate: float
    warning_lead_time_seconds: int | None
    alert_episode_count: int
    warning_episode_count: int
    inside_failure_episode_count: int
    false_alert_episode_count: int
    alert_episodes_per_operating_day: float
    false_alert_episodes_per_operating_day: float


@dataclass(frozen=True)
class ManagedGoldSelectedCandidateSealedTestEvaluationResult:
    """Compact source identity and aggregate-only evidence from one approved test read."""

    job_contract_version: str
    sealed_test_contract_version: str
    modeling_source_table: str
    failure_source_table: str
    scoring_adapter_version: str
    alert_evaluation_adapter_version: str
    model_candidate_id: str
    alert_policy_candidate_id: str
    metrics: ManagedSealedTestAggregateMetrics


def _validate_metric_allowlist() -> None:
    metric_fields = tuple(field.name for field in fields(ManagedSealedTestAggregateMetrics))
    if metric_fields != SEALED_TEST_AGGREGATE_METRICS:
        raise ManagedGoldSelectedCandidateSealedTestEvaluationError(
            "Managed sealed-test output does not match the aggregate metric allowlist"
        )


def run_managed_gold_selected_candidate_sealed_test_evaluation(
    spark: SparkSession,
    namespace: CatalogNamespace,
    *,
    sealed_test_access_approved: bool,
) -> ManagedGoldSelectedCandidateSealedTestEvaluationResult:
    """Run the read-only held-out path only after an explicit one-time approval."""

    if sealed_test_access_approved is not True:
        raise ManagedGoldSelectedCandidateSealedTestEvaluationError(
            "Managed sealed-test evaluation requires explicit test-read approval"
        )
    _validate_metric_allowlist()
    contract = get_sealed_test_evaluation_contract()
    try:
        modeling_source_table = catalog_modeling_view_v2_table(
            namespace,
            MODELING_VIEW_V2_TABLE,
        )
        failure_source_table = catalog_silver_table(namespace, FAILURE_ACCEPTED_TABLE)
        view = spark.table(modeling_source_table)
        failures = spark.table(failure_source_table)
        scores = score_selected_candidate_sealed_test(view)
        evaluation = evaluate_selected_candidate_sealed_test_alerts(scores, failures)
    except (
        ModelingViewV2PersistenceError,
        SilverPersistenceError,
        SelectedCandidateSealedTestError,
        SelectedCandidateSealedTestAlertEvaluationError,
    ) as exc:
        raise ManagedGoldSelectedCandidateSealedTestEvaluationError(str(exc)) from exc

    policy_evaluation = evaluation.policy_evaluation
    if (
        evaluation.contract_version != contract.contract_version
        or evaluation.scoring_result is not scores
        or policy_evaluation.policy_candidate_id != contract.alert_policy_candidate_id
        or len(policy_evaluation.event_evaluations) != 1
    ):
        raise ManagedGoldSelectedCandidateSealedTestEvaluationError(
            "Managed sealed-test evaluation does not match the frozen result identity"
        )
    event_evaluation = policy_evaluation.event_evaluations[0]
    metrics = ManagedSealedTestAggregateMetrics(
        test_row_count=scores.test_summary.row_count,
        test_positive_count=scores.test_summary.positive_count,
        test_negative_count=scores.test_summary.negative_count,
        test_area_under_precision_recall_curve=scores.test_area_under_pr,
        operating_day_count=evaluation.operating_day_count,
        accepted_failure_event_count=evaluation.accepted_failure_event_count,
        detected_failure_event_count=policy_evaluation.detected_failure_event_count,
        missed_failure_event_count=policy_evaluation.missed_failure_event_count,
        event_detection_rate=policy_evaluation.event_detection_rate,
        warning_lead_time_seconds=event_evaluation.warning_lead_time_seconds,
        alert_episode_count=policy_evaluation.alert_episode_count,
        warning_episode_count=policy_evaluation.warning_episode_count,
        inside_failure_episode_count=policy_evaluation.inside_failure_episode_count,
        false_alert_episode_count=policy_evaluation.false_alert_episode_count,
        alert_episodes_per_operating_day=policy_evaluation.alert_episodes_per_operating_day,
        false_alert_episodes_per_operating_day=(
            policy_evaluation.false_alert_episodes_per_operating_day
        ),
    )
    return ManagedGoldSelectedCandidateSealedTestEvaluationResult(
        job_contract_version=(GOLD_SELECTED_CANDIDATE_SEALED_TEST_EVALUATION_JOB_CONTRACT_VERSION),
        sealed_test_contract_version=SEALED_TEST_EVALUATION_CONTRACT_VERSION,
        modeling_source_table=modeling_source_table,
        failure_source_table=failure_source_table,
        scoring_adapter_version=scores.adapter_version,
        alert_evaluation_adapter_version=evaluation.adapter_version,
        model_candidate_id=scores.model_candidate_id,
        alert_policy_candidate_id=policy_evaluation.policy_candidate_id,
        metrics=metrics,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    parser.add_argument("--allow_sealed_test_read", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Print aggregate-only held-out evidence after explicit access approval."""

    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    result = run_managed_gold_selected_candidate_sealed_test_evaluation(
        spark,
        namespace,
        sealed_test_access_approved=args.allow_sealed_test_read,
    )
    print(json.dumps(asdict(result), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
