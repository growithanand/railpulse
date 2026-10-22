"""Fit and compare the frozen model candidates without reading the sealed test period."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import (
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_schema import MODEL_TARGET_COLUMN
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_comparison_contract import (
    MODEL_CANDIDATE_COMPARISON_VERSION,
    MODEL_CANDIDATE_ELASTIC_NET,
    MODEL_CANDIDATE_ESTIMATOR,
    MODEL_CANDIDATE_FAMILY,
    MODEL_CANDIDATE_FIT_INTERCEPT,
    MODEL_CANDIDATE_MAX_ITERATIONS,
    MODEL_CANDIDATE_REGULARIZATION,
    MODEL_CANDIDATE_SELECTION_METRIC,
    MODEL_CANDIDATE_STANDARDIZATION,
    MODEL_CANDIDATES,
    ModelCandidate,
)
from railpulse.models.candidate_development_data import (
    MODEL_CANDIDATE_DEVELOPMENT_COLUMNS,
    DevelopmentPartitionSummary,
    ModelCandidateDevelopmentData,
)

_FEATURE_VECTOR_COLUMN = "_candidate_features"
_LABEL_COLUMN = "_candidate_label"
_RAW_PREDICTION_COLUMN = "_candidate_raw_prediction"
_PROBABILITY_COLUMN = "_candidate_probability"
_PREDICTION_COLUMN = "_candidate_prediction"


class ModelCandidateComparisonError(ValueError):
    """Raised when candidate fitting or validation evidence violates the frozen contract."""


@dataclass(frozen=True)
class CandidateComparisonResult:
    """Auditable development-only evidence for one fitted feature candidate."""

    candidate_id: str
    feature_columns: tuple[str, ...]
    train_row_count: int
    train_positive_count: int
    train_negative_count: int
    validation_row_count: int
    validation_positive_count: int
    validation_negative_count: int
    validation_area_under_pr: float
    fitted_intercept: float
    fitted_coefficients: tuple[float, ...]
    completed_iterations: int


@dataclass(frozen=True)
class ModelCandidateComparison:
    """Like-for-like candidate evidence with the test partition structurally unavailable."""

    comparison_version: str
    modeling_view_version: str
    split_candidate_id: str
    estimator: str
    selection_metric: str
    max_iterations: int
    regularization: float
    elastic_net: float
    fit_intercept: bool
    standardization: bool
    family: str
    candidates: tuple[CandidateComparisonResult, ...]


def _summary_by_partition(
    summaries: tuple[DevelopmentPartitionSummary, ...],
) -> dict[str, DevelopmentPartitionSummary]:
    by_partition = {summary.partition: summary for summary in summaries}
    if len(by_partition) != len(summaries) or set(by_partition) != {
        PARTITION_TRAIN,
        PARTITION_VALIDATION,
    }:
        raise ModelCandidateComparisonError(
            "Candidate comparison requires one train and one validation summary"
        )
    return by_partition


def _validate_development_data(data: ModelCandidateDevelopmentData) -> None:
    selected_split = get_selected_split_candidate()
    if (
        data.comparison_version != MODEL_CANDIDATE_COMPARISON_VERSION
        or data.modeling_view_version != MODELING_VIEW_V2_VERSION
        or data.split_candidate_id != selected_split.candidate_id
    ):
        raise ModelCandidateComparisonError(
            "Candidate development data is incompatible with the comparison contract"
        )
    expected_columns = list(MODEL_CANDIDATE_DEVELOPMENT_COLUMNS)
    if data.train.columns != expected_columns or data.validation.columns != expected_columns:
        raise ModelCandidateComparisonError(
            "Candidate development frames must expose only the reviewed columns"
        )
    _summary_by_partition(data.summaries)


def _assemble(frame: DataFrame, candidate: ModelCandidate) -> DataFrame:
    assembler = VectorAssembler(
        inputCols=list(candidate.feature_columns),
        outputCol=_FEATURE_VECTOR_COLUMN,
        handleInvalid="error",
    )
    return assembler.transform(
        frame.withColumn(_LABEL_COLUMN, F.col(MODEL_TARGET_COLUMN).cast("double"))
    )


def _fit_candidate(
    data: ModelCandidateDevelopmentData,
    candidate: ModelCandidate,
    summaries: dict[str, DevelopmentPartitionSummary],
) -> CandidateComparisonResult:
    training = _assemble(data.train, candidate)
    validation = _assemble(data.validation, candidate)
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
    metric = BinaryClassificationEvaluator(
        labelCol=_LABEL_COLUMN,
        rawPredictionCol=_RAW_PREDICTION_COLUMN,
        metricName="areaUnderPR",
    ).evaluate(scored_validation)
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
        raise ModelCandidateComparisonError(
            f"Candidate {candidate.candidate_id} produced invalid fitting evidence"
        )

    train_summary = summaries[PARTITION_TRAIN]
    validation_summary = summaries[PARTITION_VALIDATION]
    return CandidateComparisonResult(
        candidate_id=candidate.candidate_id,
        feature_columns=candidate.feature_columns,
        train_row_count=train_summary.row_count,
        train_positive_count=train_summary.positive_count,
        train_negative_count=train_summary.negative_count,
        validation_row_count=validation_summary.row_count,
        validation_positive_count=validation_summary.positive_count,
        validation_negative_count=validation_summary.negative_count,
        validation_area_under_pr=float(metric),
        fitted_intercept=intercept,
        fitted_coefficients=coefficients,
        completed_iterations=iterations,
    )


def compare_model_candidates(
    data: ModelCandidateDevelopmentData,
) -> ModelCandidateComparison:
    """Fit both reviewed candidates and return validation-only comparison evidence."""

    _validate_development_data(data)
    summaries = _summary_by_partition(data.summaries)
    results = tuple(_fit_candidate(data, candidate, summaries) for candidate in MODEL_CANDIDATES)
    return ModelCandidateComparison(
        comparison_version=MODEL_CANDIDATE_COMPARISON_VERSION,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        split_candidate_id=data.split_candidate_id,
        estimator=MODEL_CANDIDATE_ESTIMATOR,
        selection_metric=MODEL_CANDIDATE_SELECTION_METRIC,
        max_iterations=MODEL_CANDIDATE_MAX_ITERATIONS,
        regularization=MODEL_CANDIDATE_REGULARIZATION,
        elastic_net=MODEL_CANDIDATE_ELASTIC_NET,
        fit_intercept=MODEL_CANDIDATE_FIT_INTERCEPT,
        standardization=MODEL_CANDIDATE_STANDARDIZATION,
        family=MODEL_CANDIDATE_FAMILY,
        candidates=results,
    )
