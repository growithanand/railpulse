"""Prepare leakage-safe train and validation data for model candidates."""

from __future__ import annotations

from dataclasses import dataclass
from math import inf

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import (
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_schema import (
    MODEL_TARGET_COLUMN,
    MODEL_TIME_COLUMN,
    STATUS_TRAINABLE,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    MODEL_INPUT_COLUMNS_V2,
    MODELING_VIEW_V2_VERSION,
)
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.models.candidate_comparison_contract import MODEL_CANDIDATE_COMPARISON_VERSION

MODEL_CANDIDATE_KEY_COLUMN = "loaded_cycle_id"
MODEL_CANDIDATE_AUDIT_COLUMNS = (
    MODEL_CANDIDATE_KEY_COLUMN,
    MODEL_TIME_COLUMN,
    MODEL_TARGET_COLUMN,
)
MODEL_CANDIDATE_DEVELOPMENT_COLUMNS = (
    *MODEL_CANDIDATE_AUDIT_COLUMNS,
    *MODEL_INPUT_COLUMNS_V2,
)

_REQUIRED_COLUMNS = {
    "failure_horizon_status",
    "modeling_row_status",
    "modeling_view_version",
    *MODEL_CANDIDATE_DEVELOPMENT_COLUMNS,
}


class ModelCandidateDevelopmentDataError(ValueError):
    """Raised when development data violates the frozen comparison contract."""


@dataclass(frozen=True)
class DevelopmentPartitionSummary:
    """Label counts for one chronological development partition."""

    partition: str
    row_count: int
    positive_count: int
    negative_count: int


@dataclass(frozen=True)
class ModelCandidateDevelopmentData:
    """Validated train and validation frames with no test rows or extra metadata."""

    comparison_version: str
    modeling_view_version: str
    split_candidate_id: str
    train: DataFrame
    validation: DataFrame
    summaries: tuple[DevelopmentPartitionSummary, ...]


def _invalid_numeric(column: str) -> Column:
    value = F.col(column).cast("double")
    return value.isNull() | F.isnan(value) | value.isin(inf, -inf)


def _summarize(frame: DataFrame, *, partition: str) -> DevelopmentPartitionSummary:
    row = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(F.when(F.col(MODEL_TARGET_COLUMN), F.lit(1))).alias("positive_count"),
    ).first()
    row_count = int(row.row_count)
    positive_count = int(row.positive_count)
    negative_count = row_count - positive_count
    if int(row.distinct_key_count) != row_count:
        raise ModelCandidateDevelopmentDataError(
            f"Model candidate {partition} rows require unique cycle IDs"
        )
    if positive_count <= 0 or negative_count <= 0:
        raise ModelCandidateDevelopmentDataError(
            f"Model candidate {partition} rows require both target labels"
        )
    return DevelopmentPartitionSummary(
        partition=partition,
        row_count=row_count,
        positive_count=positive_count,
        negative_count=negative_count,
    )


def prepare_model_candidate_development_data(view: DataFrame) -> ModelCandidateDevelopmentData:
    """Return validated train and validation frames while leaving test rows untouched."""

    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise ModelCandidateDevelopmentDataError(
            "Modelling view is missing candidate-development columns: " + ", ".join(missing_columns)
        )

    split = get_selected_split_candidate()
    development = view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & F.col(MODEL_TIME_COLUMN).isNotNull()
        & (F.col(MODEL_TIME_COLUMN) < F.lit(split.test_start))
    )
    target_disagrees = (
        (F.col("failure_horizon_status") == STATUS_POSITIVE)
        & (F.col(MODEL_TARGET_COLUMN) != F.lit(True))
    ) | (
        (F.col("failure_horizon_status") == STATUS_NEGATIVE)
        & (F.col(MODEL_TARGET_COLUMN) != F.lit(False))
    )
    invalid_features = F.lit(False)
    for column in MODEL_INPUT_COLUMNS_V2:
        invalid_features = invalid_features | _invalid_numeric(column)
    invalid = development.where(
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
        raise ModelCandidateDevelopmentDataError(
            "Development rows contain incompatible version, label, key, or feature values"
        )

    safe_development = development.select(*MODEL_CANDIDATE_DEVELOPMENT_COLUMNS)
    train = safe_development.where(F.col(MODEL_TIME_COLUMN) < F.lit(split.validation_start))
    validation = safe_development.where(F.col(MODEL_TIME_COLUMN) >= F.lit(split.validation_start))
    summaries = (
        _summarize(train, partition=PARTITION_TRAIN),
        _summarize(validation, partition=PARTITION_VALIDATION),
    )
    return ModelCandidateDevelopmentData(
        comparison_version=MODEL_CANDIDATE_COMPARISON_VERSION,
        modeling_view_version=MODELING_VIEW_V2_VERSION,
        split_candidate_id=split.candidate_id,
        train=train,
        validation=validation,
        summaries=summaries,
    )
