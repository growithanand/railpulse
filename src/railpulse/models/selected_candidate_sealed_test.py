"""Score held-out rows under the frozen final-evaluation contract."""

from __future__ import annotations

from dataclasses import dataclass
from math import inf, isfinite

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import (
    PARTITION_TEST,
    PARTITION_TRAIN,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_schema import (
    MODEL_INPUT_COLUMNS,
    MODEL_TARGET_COLUMN,
    MODEL_TIME_COLUMN,
    STATUS_TRAINABLE,
)
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_ELASTIC_NET,
    MODEL_CANDIDATE_FAMILY,
    MODEL_CANDIDATE_FIT_INTERCEPT,
    MODEL_CANDIDATE_MAX_ITERATIONS,
    MODEL_CANDIDATE_REGULARIZATION,
    MODEL_CANDIDATE_STANDARDIZATION,
)
from railpulse.models.candidate_development_data import MODEL_CANDIDATE_KEY_COLUMN
from railpulse.models.development_alert_policy_selection import (
    DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    DEVELOPMENT_SELECTED_CANDIDATE_ID,
    CandidateValidationEvidence,
    get_development_model_selection,
    get_selected_development_candidate,
)
from railpulse.models.sealed_test_evaluation_contract import (
    SEALED_TEST_EVALUATION_CONTRACT_VERSION,
    get_sealed_test_evaluation_contract,
)

SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION = "motor-current-sealed-test-scoring-adapter-v1"
SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION = "motor-current-sealed-test-scores-v1"
SEALED_TEST_SCORE_COLUMN = "failure_probability"
SEALED_TEST_SCORE_PARTITION_COLUMN = "evaluation_partition"
SEALED_TEST_SCORE_CONTRACT_COLUMN = "sealed_test_contract_version"
SEALED_TEST_SCORE_MODEL_SELECTION_COLUMN = "model_selection_version"
SEALED_TEST_SCORE_MODEL_CANDIDATE_COLUMN = "model_candidate_id"
SEALED_TEST_SCORE_POLICY_SELECTION_COLUMN = "alert_policy_selection_version"
SEALED_TEST_SCORE_POLICY_CANDIDATE_COLUMN = "alert_policy_candidate_id"
SEALED_TEST_SCORE_SPLIT_COLUMN = "split_candidate_id"
SEALED_TEST_SCORE_COLUMNS = (
    MODEL_CANDIDATE_KEY_COLUMN,
    MODEL_TIME_COLUMN,
    MODEL_TARGET_COLUMN,
    SEALED_TEST_SCORE_COLUMN,
    SEALED_TEST_SCORE_PARTITION_COLUMN,
    SEALED_TEST_SCORE_CONTRACT_COLUMN,
    SEALED_TEST_SCORE_MODEL_SELECTION_COLUMN,
    SEALED_TEST_SCORE_MODEL_CANDIDATE_COLUMN,
    SEALED_TEST_SCORE_POLICY_SELECTION_COLUMN,
    SEALED_TEST_SCORE_POLICY_CANDIDATE_COLUMN,
    SEALED_TEST_SCORE_SPLIT_COLUMN,
)

_SELECTED_INPUT_COLUMNS = (
    MODEL_CANDIDATE_KEY_COLUMN,
    MODEL_TIME_COLUMN,
    MODEL_TARGET_COLUMN,
    *MODEL_INPUT_COLUMNS,
)
_REQUIRED_COLUMNS = {
    "failure_horizon_status",
    "modeling_row_status",
    "modeling_view_version",
    *_SELECTED_INPUT_COLUMNS,
}
_FEATURE_VECTOR_COLUMN = "_sealed_test_features"
_LABEL_COLUMN = "_sealed_test_label"
_RAW_PREDICTION_COLUMN = "_sealed_test_raw_prediction"
_PROBABILITY_COLUMN = "_sealed_test_probability"
_PREDICTION_COLUMN = "_sealed_test_prediction"


class SelectedCandidateSealedTestError(ValueError):
    """Raised when held-out scoring violates the frozen final-evaluation contract."""


@dataclass(frozen=True)
class SealedTestPartitionSummary:
    """Aggregate label counts for one guarded final-evaluation partition."""

    partition: str
    row_count: int
    positive_count: int
    negative_count: int


@dataclass(frozen=True)
class SelectedCandidateSealedTestScores:
    """In-memory held-out scores that must not be persisted or returned by a job."""

    adapter_version: str
    scoring_version: str
    contract_version: str
    modeling_view_version: str
    split_candidate_id: str
    model_selection_version: str
    model_candidate_id: str
    alert_policy_selection_version: str
    alert_policy_candidate_id: str
    feature_columns: tuple[str, ...]
    train_summary: SealedTestPartitionSummary
    test_summary: SealedTestPartitionSummary
    test_area_under_pr: float
    fitted_intercept: float
    fitted_coefficients: tuple[float, ...]
    completed_iterations: int
    scores: DataFrame


def _invalid_numeric(column: str) -> Column:
    value = F.col(column).cast("double")
    return value.isNull() | F.isnan(value) | value.isin(inf, -inf)


def _selected_training_evidence() -> CandidateValidationEvidence:
    selection = get_development_model_selection()
    return next(
        evidence
        for evidence in selection.candidate_evidence
        if evidence.candidate_id == selection.selected_candidate_id
    )


def _partition_rows(view: DataFrame, *, partition: str) -> DataFrame:
    split = get_selected_split_candidate()
    if partition == PARTITION_TRAIN:
        boundary = F.col(MODEL_TIME_COLUMN) < F.lit(split.validation_start)
    elif partition == PARTITION_TEST:
        boundary = F.col(MODEL_TIME_COLUMN) >= F.lit(split.test_start)
    else:
        raise SelectedCandidateSealedTestError(
            f"Unsupported sealed-test preparation partition: {partition}"
        )
    return view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & F.col(MODEL_TIME_COLUMN).isNotNull()
        & boundary
    )


def _summarize_and_validate(
    frame: DataFrame,
    *,
    partition: str,
) -> tuple[DataFrame, SealedTestPartitionSummary]:
    target_disagrees = (
        (F.col("failure_horizon_status") == STATUS_POSITIVE)
        & (F.col(MODEL_TARGET_COLUMN) != F.lit(True))
    ) | (
        (F.col("failure_horizon_status") == STATUS_NEGATIVE)
        & (F.col(MODEL_TARGET_COLUMN) != F.lit(False))
    )
    invalid_features = F.lit(False)
    for column in MODEL_INPUT_COLUMNS:
        invalid_features = invalid_features | _invalid_numeric(column)
    invalid = frame.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_V2_VERSION))
        | F.col(MODEL_CANDIDATE_KEY_COLUMN).isNull()
        | (F.length(F.col(MODEL_CANDIDATE_KEY_COLUMN)) == 0)
        | F.col(MODEL_TARGET_COLUMN).isNull()
        | ~F.coalesce(
            F.col("failure_horizon_status").isin(STATUS_POSITIVE, STATUS_NEGATIVE),
            F.lit(False),
        )
        | target_disagrees
        | invalid_features
    ).limit(1)
    if invalid.count():
        raise SelectedCandidateSealedTestError(
            f"Sealed-test {partition} rows contain incompatible version, label, key, or feature "
            "values"
        )

    safe = frame.select(*_SELECTED_INPUT_COLUMNS)
    row = safe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(F.when(F.col(MODEL_TARGET_COLUMN), F.lit(1))).alias("positive_count"),
        F.min(MODEL_TIME_COLUMN).alias("minimum_prediction_timestamp"),
        F.max(MODEL_TIME_COLUMN).alias("maximum_prediction_timestamp"),
    ).first()
    row_count = int(row.row_count)
    positive_count = int(row.positive_count)
    negative_count = row_count - positive_count
    if row_count <= 0 or positive_count <= 0 or negative_count <= 0:
        raise SelectedCandidateSealedTestError(
            f"Sealed-test {partition} rows require both target labels"
        )
    if int(row.distinct_key_count) != row_count:
        raise SelectedCandidateSealedTestError(
            f"Sealed-test {partition} rows require unique cycle IDs"
        )

    split = get_selected_split_candidate()
    if partition == PARTITION_TRAIN:
        valid_boundary = row.maximum_prediction_timestamp < split.validation_start
    else:
        valid_boundary = row.minimum_prediction_timestamp >= split.test_start
    if not valid_boundary:
        raise SelectedCandidateSealedTestError(
            f"Sealed-test {partition} rows violate chronological boundaries"
        )
    return safe, SealedTestPartitionSummary(
        partition=partition,
        row_count=row_count,
        positive_count=positive_count,
        negative_count=negative_count,
    )


def _validate_training_population(summary: SealedTestPartitionSummary) -> None:
    evidence = _selected_training_evidence()
    observed = (summary.row_count, summary.positive_count, summary.negative_count)
    expected = (
        evidence.train_row_count,
        evidence.train_positive_count,
        evidence.train_negative_count,
    )
    if observed != expected:
        raise SelectedCandidateSealedTestError(
            "Sealed-test training population does not match frozen model-selection evidence: "
            f"observed={observed}, expected={expected}"
        )


def score_selected_candidate_sealed_test(view: DataFrame) -> SelectedCandidateSealedTestScores:
    """Fit on frozen training rows and score only held-out rows in memory."""

    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise SelectedCandidateSealedTestError(
            "Modelling view is missing sealed-test scoring columns: " + ", ".join(missing_columns)
        )

    contract = get_sealed_test_evaluation_contract()
    selection = get_development_model_selection()
    candidate = get_selected_development_candidate()
    train, train_summary = _summarize_and_validate(
        _partition_rows(view, partition=PARTITION_TRAIN),
        partition=PARTITION_TRAIN,
    )
    test, test_summary = _summarize_and_validate(
        _partition_rows(view, partition=PARTITION_TEST),
        partition=PARTITION_TEST,
    )
    _validate_training_population(train_summary)

    combined_keys = train.select(MODEL_CANDIDATE_KEY_COLUMN).unionByName(
        test.select(MODEL_CANDIDATE_KEY_COLUMN)
    )
    key_summary = combined_keys.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
    ).first()
    if int(key_summary.row_count) != int(key_summary.distinct_key_count):
        raise SelectedCandidateSealedTestError(
            "Sealed-test training and test rows require globally unique cycle IDs"
        )

    assembler = VectorAssembler(
        inputCols=list(candidate.feature_columns),
        outputCol=_FEATURE_VECTOR_COLUMN,
        handleInvalid="error",
    )
    training = assembler.transform(
        train.withColumn(_LABEL_COLUMN, F.col(MODEL_TARGET_COLUMN).cast("double"))
    )
    held_out = assembler.transform(
        test.withColumn(_LABEL_COLUMN, F.col(MODEL_TARGET_COLUMN).cast("double"))
    )
    estimator = LogisticRegression(
        featuresCol=_FEATURE_VECTOR_COLUMN,
        labelCol=_LABEL_COLUMN,
        rawPredictionCol=_RAW_PREDICTION_COLUMN,
        probabilityCol=_PROBABILITY_COLUMN,
        predictionCol=_PREDICTION_COLUMN,
        maxIter=MODEL_CANDIDATE_MAX_ITERATIONS,
        regParam=MODEL_CANDIDATE_REGULARIZATION,
        elasticNetParam=MODEL_CANDIDATE_ELASTIC_NET,
        fitIntercept=MODEL_CANDIDATE_FIT_INTERCEPT,
        standardization=MODEL_CANDIDATE_STANDARDIZATION,
        family=MODEL_CANDIDATE_FAMILY,
    )
    model = estimator.fit(training)
    scored_test = model.transform(held_out)
    metric = float(
        BinaryClassificationEvaluator(
            labelCol=_LABEL_COLUMN,
            rawPredictionCol=_RAW_PREDICTION_COLUMN,
            metricName="areaUnderPR",
        ).evaluate(scored_test)
    )
    scores = scored_test.select(
        MODEL_CANDIDATE_KEY_COLUMN,
        MODEL_TIME_COLUMN,
        MODEL_TARGET_COLUMN,
        vector_to_array(F.col(_PROBABILITY_COLUMN))[1].alias(SEALED_TEST_SCORE_COLUMN),
        F.lit(PARTITION_TEST).alias(SEALED_TEST_SCORE_PARTITION_COLUMN),
        F.lit(contract.contract_version).alias(SEALED_TEST_SCORE_CONTRACT_COLUMN),
        F.lit(selection.selection_version).alias(SEALED_TEST_SCORE_MODEL_SELECTION_COLUMN),
        F.lit(candidate.candidate_id).alias(SEALED_TEST_SCORE_MODEL_CANDIDATE_COLUMN),
        F.lit(DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION).alias(
            SEALED_TEST_SCORE_POLICY_SELECTION_COLUMN
        ),
        F.lit(DEVELOPMENT_SELECTED_ALERT_POLICY_ID).alias(
            SEALED_TEST_SCORE_POLICY_CANDIDATE_COLUMN
        ),
        F.lit(contract.split_candidate_id).alias(SEALED_TEST_SCORE_SPLIT_COLUMN),
    )
    score_summary = scores.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(
            F.when(
                F.col(SEALED_TEST_SCORE_COLUMN).isNull()
                | F.isnan(SEALED_TEST_SCORE_COLUMN)
                | (F.col(SEALED_TEST_SCORE_COLUMN) < 0.0)
                | (F.col(SEALED_TEST_SCORE_COLUMN) > 1.0),
                F.lit(1),
            )
        ).alias("invalid_score_count"),
    ).first()
    if (
        int(score_summary.row_count) != test_summary.row_count
        or int(score_summary.distinct_key_count) != test_summary.row_count
        or int(score_summary.invalid_score_count) != 0
    ):
        raise SelectedCandidateSealedTestError(
            "Sealed-test scores do not reconcile with the guarded test population"
        )

    coefficients = tuple(float(value) for value in model.coefficients)
    intercept = float(model.intercept)
    iterations = int(model.summary.totalIterations)
    if (
        not isfinite(metric)
        or not 0.0 <= metric <= 1.0
        or not isfinite(intercept)
        or len(coefficients) != len(candidate.feature_columns)
        or any(not isfinite(value) for value in coefficients)
        or iterations < 0
        or iterations > MODEL_CANDIDATE_MAX_ITERATIONS
    ):
        raise SelectedCandidateSealedTestError(
            "Selected candidate produced invalid sealed-test scoring evidence"
        )

    return SelectedCandidateSealedTestScores(
        adapter_version=SELECTED_CANDIDATE_SEALED_TEST_ADAPTER_VERSION,
        scoring_version=SELECTED_CANDIDATE_SEALED_TEST_SCORE_VERSION,
        contract_version=SEALED_TEST_EVALUATION_CONTRACT_VERSION,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        split_candidate_id=contract.split_candidate_id,
        model_selection_version=DEVELOPMENT_MODEL_SELECTION_VERSION,
        model_candidate_id=DEVELOPMENT_SELECTED_CANDIDATE_ID,
        alert_policy_selection_version=DEVELOPMENT_ALERT_POLICY_SELECTION_VERSION,
        alert_policy_candidate_id=DEVELOPMENT_SELECTED_ALERT_POLICY_ID,
        feature_columns=candidate.feature_columns,
        train_summary=train_summary,
        test_summary=test_summary,
        test_area_under_pr=metric,
        fitted_intercept=intercept,
        fitted_coefficients=coefficients,
        completed_iterations=iterations,
        scores=scores,
    )
