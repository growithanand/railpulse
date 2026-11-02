"""Freeze the one-time sealed-test evaluation boundary without opening test data."""

from __future__ import annotations

from dataclasses import dataclass

from railpulse.evaluation.chronological_splits import (
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    SELECTED_SPLIT_CANDIDATE_ID,
)
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.features.failure_horizons import DEFAULT_FAILURE_HORIZON_SECONDS
from railpulse.models.development_alert_policy_selection import (
    DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
    get_selected_development_alert_policy,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
)
from railpulse.models.validation_alert_episodes import VALIDATION_ALERT_EPISODE_VERSION
from railpulse.models.validation_alert_evaluation import (
    VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
)
from railpulse.models.validation_alert_policy_contract import VALIDATION_ALERT_POLICY_VERSION

SEALED_TEST_EVALUATION_CONTRACT_VERSION = "motor-current-sealed-test-evaluation-contract-v1"
SEALED_TEST_EVALUATION_SCOPE = "one-time-final-evaluation-only"
SEALED_TEST_MODEL_FIT_PARTITION = PARTITION_TRAIN
SEALED_TEST_THRESHOLD_SOURCE_PARTITION = PARTITION_VALIDATION
SEALED_TEST_SCORE_PARTITION = PARTITION_TEST
SEALED_TEST_OUTPUT_POLICY = "aggregate-only-no-row-level-persistence"
SEALED_TEST_RESULT_USE = "report-only-no-retuning"
SEALED_TEST_APPROVED_READ_COUNT = 1
SEALED_TEST_AGGREGATE_METRICS = (
    "test_row_count",
    "test_positive_count",
    "test_negative_count",
    "test_area_under_precision_recall_curve",
    "operating_day_count",
    "accepted_failure_event_count",
    "detected_failure_event_count",
    "missed_failure_event_count",
    "event_detection_rate",
    "warning_lead_time_seconds",
    "alert_episode_count",
    "warning_episode_count",
    "inside_failure_episode_count",
    "false_alert_episode_count",
    "alert_episodes_per_operating_day",
    "false_alert_episodes_per_operating_day",
)


class SealedTestEvaluationContractError(ValueError):
    """Raised when the final evaluation plan weakens the reviewed sealed-test boundary."""


@dataclass(frozen=True)
class SealedTestEvaluationContract:
    """Reviewed identities and guardrails required before any held-out test read."""

    contract_version: str
    scope: str
    modeling_view_version: str
    split_candidate_id: str
    model_selection_version: str
    model_candidate_id: str
    alert_policy_selection_version: str
    alert_policy_candidate_id: str
    alert_policy_version: str
    alert_episode_version: str
    alert_evaluation_version: str
    model_fit_partition: str
    threshold_source_partition: str
    score_partition: str
    warning_horizon_seconds: int
    minimum_failure_probability: float
    minimum_consecutive_cycles: int
    merge_gap_minutes: int
    cooldown_minutes: int
    approved_test_read_count: int
    requires_explicit_execution_approval: bool
    contract_authorizes_execution: bool
    automatic_retry_allowed: bool
    retry_requires_versioned_decision: bool
    output_policy: str
    result_use: str
    aggregate_metrics: tuple[str, ...]

    def __post_init__(self) -> None:
        selected_policy = get_selected_development_alert_policy()
        if (
            self.contract_version != SEALED_TEST_EVALUATION_CONTRACT_VERSION
            or self.scope != SEALED_TEST_EVALUATION_SCOPE
            or self.modeling_view_version != MODELING_VIEW_V2_VERSION
            or self.split_candidate_id != SELECTED_SPLIT_CANDIDATE_ID
            or self.model_selection_version != DEVELOPMENT_MODEL_SELECTION_VERSION
            or self.model_candidate_id != DEVELOPMENT_SELECTED_CANDIDATE_ID
            or self.alert_policy_selection_version != DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION
            or self.alert_policy_candidate_id != DEVELOPMENT_SELECTED_ALERT_POLICY_ID
            or self.alert_policy_version != VALIDATION_ALERT_POLICY_VERSION
            or self.alert_episode_version != VALIDATION_ALERT_EPISODE_VERSION
            or self.alert_evaluation_version != VALIDATION_ALERT_EVENT_EVALUATION_VERSION
            or self.model_fit_partition != SEALED_TEST_MODEL_FIT_PARTITION
            or self.threshold_source_partition != SEALED_TEST_THRESHOLD_SOURCE_PARTITION
            or self.score_partition != SEALED_TEST_SCORE_PARTITION
            or self.warning_horizon_seconds != DEFAULT_FAILURE_HORIZON_SECONDS
            or self.minimum_failure_probability != selected_policy.minimum_failure_probability
            or self.minimum_consecutive_cycles != selected_policy.minimum_consecutive_cycles
            or self.merge_gap_minutes != selected_policy.merge_gap_minutes
            or self.cooldown_minutes != selected_policy.cooldown_minutes
            or self.approved_test_read_count != SEALED_TEST_APPROVED_READ_COUNT
            or self.requires_explicit_execution_approval is not True
            or self.contract_authorizes_execution is not False
            or self.automatic_retry_allowed is not False
            or self.retry_requires_versioned_decision is not True
            or self.output_policy != SEALED_TEST_OUTPUT_POLICY
            or self.result_use != SEALED_TEST_RESULT_USE
            or self.aggregate_metrics != SEALED_TEST_AGGREGATE_METRICS
        ):
            raise SealedTestEvaluationContractError(
                "Sealed-test evaluation metadata does not match the reviewed contract"
            )
        if len(self.aggregate_metrics) != len(set(self.aggregate_metrics)):
            raise SealedTestEvaluationContractError("Sealed-test aggregate metrics must be unique")


_SELECTED_POLICY = get_selected_development_alert_policy()

SEALED_TEST_EVALUATION_CONTRACT = SealedTestEvaluationContract(
    contract_version=SEALED_TEST_EVALUATION_CONTRACT_VERSION,
    scope=SEALED_TEST_EVALUATION_SCOPE,
    modeling_view_version=MODELING_VIEW_V2_VERSION,
    split_candidate_id=SELECTED_SPLIT_CANDIDATE_ID,
    model_selection_version=DEVELOPMENT_MODEL_SELECTION_VERSION,
    model_candidate_id=DEVELOPMENT_SELECTED_CANDIDATE_ID,
    alert_policy_selection_version=DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    alert_policy_candidate_id=DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
    alert_policy_version=VALIDATION_ALERT_POLICY_VERSION,
    alert_episode_version=VALIDATION_ALERT_EPISODE_VERSION,
    alert_evaluation_version=VALIDATION_ALERT_EVENT_EVALUATION_VERSION,
    model_fit_partition=SEALED_TEST_MODEL_FIT_PARTITION,
    threshold_source_partition=SEALED_TEST_THRESHOLD_SOURCE_PARTITION,
    score_partition=SEALED_TEST_SCORE_PARTITION,
    warning_horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
    minimum_failure_probability=_SELECTED_POLICY.minimum_failure_probability,
    minimum_consecutive_cycles=_SELECTED_POLICY.minimum_consecutive_cycles,
    merge_gap_minutes=_SELECTED_POLICY.merge_gap_minutes,
    cooldown_minutes=_SELECTED_POLICY.cooldown_minutes,
    approved_test_read_count=SEALED_TEST_APPROVED_READ_COUNT,
    requires_explicit_execution_approval=True,
    contract_authorizes_execution=False,
    automatic_retry_allowed=False,
    retry_requires_versioned_decision=True,
    output_policy=SEALED_TEST_OUTPUT_POLICY,
    result_use=SEALED_TEST_RESULT_USE,
    aggregate_metrics=SEALED_TEST_AGGREGATE_METRICS,
)


def get_sealed_test_evaluation_contract() -> SealedTestEvaluationContract:
    """Return the plan that must be satisfied before any test-period evaluation."""

    return SEALED_TEST_EVALUATION_CONTRACT
