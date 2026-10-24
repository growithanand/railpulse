"""Fit the selected development candidate and expose validation-only cycle scores."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import (
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_schema import MODEL_TARGET_COLUMN, MODEL_TIME_COLUMN
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_ELASTIC_NET,
    MODEL_CANDIDATE_FAMILY,
    MODEL_CANDIDATE_FIT_INTERCEPT,
    MODEL_CANDIDATE_MAX_ITERATIONS,
    MODEL_CANDIDATE_REGULARIZATION,
    MODEL_CANDIDATE_STANDARDIZATION,
)
from railpulse.models.candidate_development_data import (
    MODEL_CANDIDATE_DEVELOPMENT_COLUMNS,
    MODEL_CANDIDATE_KEY_COLUMN,
    DevelopmentPartitionSummary,
    ModelCandidateDevelopmentData,
)
from railpulse.models.development_model_selection import (
    DEVELOPMENT_MODEL_SELECTION_VERSION,
    CandidateValidationEvidence,
    get_development_model_selection,
    get_selected_development_candidate,
)

SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION = "motor-current-validation-scores-v1"
VALIDATION_SCORE_COLUMN = "failure_probability"
VALIDATION_SCORE_PARTITION_COLUMN = "evaluation_partition"
VALIDATION_SCORE_SELECTION_VERSION_COLUMN = "model_selection_version"
VALIDATION_SCORE_CANDIDATE_COLUMN = "model_candidate_id"
VALIDATION_SCORE_SPLIT_COLUMN = "split_candidate_id"
VALIDATION_SCORE_COLUMNS = (
    MODEL_CANDIDATE_KEY_COLUMN,
    MODEL_TIME_COLUMN,
    MODEL_TARGET_COLUMN,
    VALIDATION_SCORE_COLUMN,
    VALIDATION_SCORE_PARTITION_COLUMN,
    VALIDATION_SCORE_SELECTION_VERSION_COLUMN,
    VALIDATION_SCORE_CANDIDATE_COLUMN,
    VALIDATION_SCORE_SPLIT_COLUMN,
)

_FEATURE_VECTOR_COLUMN = "_selected_candidate_features"
_LABEL_COLUMN = "_selected_candidate_label"
_RAW_PREDICTION_COLUMN = "_selected_candidate_raw_prediction"
_PROBABILITY_COLUMN = "_selected_candidate_probability"
_PREDICTION_COLUMN = "_selected_candidate_prediction"


class SelectedCandidateValidationError(ValueError):
    """Raised when selected-candidate scoring violates the reviewed development boundary."""


@dataclass(frozen=True)
class SelectedCandidateValidationScores:
    """Auditable validation scores with the test partition structurally unavailable."""

    scoring_version: str
    selection_version: str
    comparison_version: str
    modeling_view_version: str
    split_candidate_id: str
    candidate_id: str
    feature_columns: tuple[str, ...]
    train_summary: DevelopmentPartitionSummary
    validation_summary: DevelopmentPartitionSummary
    validation_area_under_pr: float
    fitted_intercept: float
    fitted_coefficients: tuple[float, ...]
    completed_iterations: int
    scores: DataFrame


def _summaries_by_partition(
    summaries: tuple[DevelopmentPartitionSummary, ...],
) -> dict[str, DevelopmentPartitionSummary]:
    by_partition = {summary.partition: summary for summary in summaries}
    if len(by_partition) != len(summaries) or set(by_partition) != {
        PARTITION_TRAIN,
        PARTITION_VALIDATION,
    }:
        raise SelectedCandidateValidationError(
            "Selected-candidate scoring requires one train and one validation summary"
        )
    return by_partition


def _selected_evidence() -> CandidateValidationEvidence:
    selection = get_development_model_selection()
    return next(
        evidence
        for evidence in selection.candidate_evidence
        if evidence.candidate_id == selection.selected_candidate_id
    )


def _expected_population(
    evidence: CandidateValidationEvidence,
    partition: str,
) -> tuple[int, int, int]:
    if partition == PARTITION_TRAIN:
        return (
            evidence.train_row_count,
            evidence.train_positive_count,
            evidence.train_negative_count,
        )
    return (
        evidence.validation_row_count,
        evidence.validation_positive_count,
        evidence.validation_negative_count,
    )


def _validate_partition(
    frame: DataFrame,
    *,
    partition: str,
    summary: DevelopmentPartitionSummary,
    evidence: CandidateValidationEvidence,
) -> None:
    split = get_selected_split_candidate()
    row = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(F.when(F.col(MODEL_TARGET_COLUMN), F.lit(1))).alias("positive_count"),
        F.min(MODEL_TIME_COLUMN).alias("minimum_prediction_timestamp"),
        F.max(MODEL_TIME_COLUMN).alias("maximum_prediction_timestamp"),
    ).first()
    observed = (
        int(row.row_count),
        int(row.positive_count),
        int(row.row_count) - int(row.positive_count),
    )
    declared = (summary.row_count, summary.positive_count, summary.negative_count)
    if observed != declared or observed != _expected_population(evidence, partition):
        raise SelectedCandidateValidationError(
            f"Selected-candidate {partition} population does not match managed selection evidence"
        )
    if int(row.distinct_key_count) != int(row.row_count):
        raise SelectedCandidateValidationError(
            f"Selected-candidate {partition} rows require unique cycle IDs"
        )

    minimum_timestamp = row.minimum_prediction_timestamp
    maximum_timestamp = row.maximum_prediction_timestamp
    if partition == PARTITION_TRAIN:
        valid_boundary = maximum_timestamp < split.validation_start
    else:
        valid_boundary = (
            minimum_timestamp >= split.validation_start and maximum_timestamp < split.test_start
        )
    if not valid_boundary:
        raise SelectedCandidateValidationError(
            f"Selected-candidate {partition} rows violate chronological boundaries"
        )


def _validate_development_data(
    data: ModelCandidateDevelopmentData,
) -> tuple[DevelopmentPartitionSummary, DevelopmentPartitionSummary]:
    selection = get_development_model_selection()
    split = get_selected_split_candidate()
    if (
        data.comparison_version != MODEL_CANDIDATE_COMPARISON_VERSION
        or data.comparison_version != selection.comparison_version
        or data.modeling_view_version != MODELING_VIEW_V2_VERSION
        or data.split_candidate_id != split.candidate_id
        or tuple(data.train.columns) != MODEL_CANDIDATE_DEVELOPMENT_COLUMNS
        or tuple(data.validation.columns) != MODEL_CANDIDATE_DEVELOPMENT_COLUMNS
    ):
        raise SelectedCandidateValidationError(
            "Candidate development data is incompatible with selected scoring contract"
        )

    summaries = _summaries_by_partition(data.summaries)
    evidence = _selected_evidence()
    train_summary = summaries[PARTITION_TRAIN]
    validation_summary = summaries[PARTITION_VALIDATION]
    _validate_partition(
        data.train,
        partition=PARTITION_TRAIN,
        summary=train_summary,
        evidence=evidence,
    )
    _validate_partition(
        data.validation,
        partition=PARTITION_VALIDATION,
        summary=validation_summary,
        evidence=evidence,
    )
    return train_summary, validation_summary


def score_selected_candidate_validation(
    data: ModelCandidateDevelopmentData,
) -> SelectedCandidateValidationScores:
    """Fit the retained candidate on training rows and return guarded validation scores."""

    train_summary, validation_summary = _validate_development_data(data)
    selection = get_development_model_selection()
    candidate = get_selected_development_candidate()
    assembler = VectorAssembler(
        inputCols=list(candidate.feature_columns),
        outputCol=_FEATURE_VECTOR_COLUMN,
        handleInvalid="error",
    )
    training = assembler.transform(
        data.train.withColumn(_LABEL_COLUMN, F.col(MODEL_TARGET_COLUMN).cast("double"))
    )
    validation = assembler.transform(
        data.validation.withColumn(_LABEL_COLUMN, F.col(MODEL_TARGET_COLUMN).cast("double"))
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
    scored_validation = model.transform(validation)
    metric = float(
        BinaryClassificationEvaluator(
            labelCol=_LABEL_COLUMN,
            rawPredictionCol=_RAW_PREDICTION_COLUMN,
            metricName="areaUnderPR",
        ).evaluate(scored_validation)
    )
    scores = scored_validation.select(
        MODEL_CANDIDATE_KEY_COLUMN,
        MODEL_TIME_COLUMN,
        MODEL_TARGET_COLUMN,
        vector_to_array(F.col(_PROBABILITY_COLUMN))[1].alias(VALIDATION_SCORE_COLUMN),
        F.lit(PARTITION_VALIDATION).alias(VALIDATION_SCORE_PARTITION_COLUMN),
        F.lit(DEVELOPMENT_MODEL_SELECTION_VERSION).alias(VALIDATION_SCORE_SELECTION_VERSION_COLUMN),
        F.lit(candidate.candidate_id).alias(VALIDATION_SCORE_CANDIDATE_COLUMN),
        F.lit(data.split_candidate_id).alias(VALIDATION_SCORE_SPLIT_COLUMN),
    )
    score_summary = scores.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(
            F.when(
                F.col(VALIDATION_SCORE_COLUMN).isNull()
                | F.isnan(VALIDATION_SCORE_COLUMN)
                | (F.col(VALIDATION_SCORE_COLUMN) < 0.0)
                | (F.col(VALIDATION_SCORE_COLUMN) > 1.0),
                F.lit(1),
            )
        ).alias("invalid_score_count"),
    ).first()
    if (
        int(score_summary.row_count) != validation_summary.row_count
        or int(score_summary.distinct_key_count) != validation_summary.row_count
        or int(score_summary.invalid_score_count) != 0
    ):
        raise SelectedCandidateValidationError(
            "Validation scores do not reconcile with selected-candidate population"
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
        raise SelectedCandidateValidationError(
            "Selected candidate produced invalid validation-scoring evidence"
        )

    return SelectedCandidateValidationScores(
        scoring_version=SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION,
        selection_version=selection.selection_version,
        comparison_version=data.comparison_version,
        modeling_view_version=data.modeling_view_version,
        split_candidate_id=data.split_candidate_id,
        candidate_id=candidate.candidate_id,
        feature_columns=candidate.feature_columns,
        train_summary=train_summary,
        validation_summary=validation_summary,
        validation_area_under_pr=metric,
        fitted_intercept=intercept,
        fitted_coefficients=coefficients,
        completed_iterations=iterations,
        scores=scores,
    )
