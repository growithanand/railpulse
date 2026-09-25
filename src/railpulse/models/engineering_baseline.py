"""Interpretable training-only motor-current deviation baseline."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.chronological_splits import get_selected_split_candidate
from railpulse.evaluation.modeling_view_schema import MODELING_VIEW_VERSION, STATUS_TRAINABLE
from railpulse.features.failure_horizons import STATUS_NEGATIVE

ENGINEERING_BASELINE_VERSION = "motor-current-robust-deviation-v1"
BASELINE_FEATURE_COLUMN = "motor_current_15m_mean_amperes"
BASELINE_SCORE_COLUMN = "motor_current_robust_deviation_score"

_REQUIRED_COLUMNS = {
    "loaded_cycle_id",
    "modeling_view_version",
    "modeling_row_status",
    "failure_horizon_status",
    "prediction_timestamp",
    BASELINE_FEATURE_COLUMN,
}


class EngineeringBaselineError(ValueError):
    """Raised when the engineering baseline cannot be fitted or scored safely."""


@dataclass(frozen=True)
class EngineeringBaselineParameters:
    """Training-derived center and scale for the robust deviation score."""

    baseline_version: str
    modeling_view_version: str
    feature_column: str
    negative_training_row_count: int
    training_median_amperes: float
    training_median_absolute_deviation_amperes: float


def _validate_required_columns(view: DataFrame) -> None:
    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise EngineeringBaselineError(
            "Modelling view is missing baseline columns: " + ", ".join(missing_columns)
        )


def _exact_median(frame: DataFrame, column: str) -> float:
    value = frame.agg(F.percentile(F.col(column), F.lit(0.5)).alias("median")).first().median
    if value is None or not isfinite(float(value)):
        raise EngineeringBaselineError(f"Cannot derive a finite median for {column}")
    return float(value)


def fit_engineering_baseline(view: DataFrame) -> EngineeringBaselineParameters:
    """Fit robust center and scale using negative rows from the selected train period only."""

    _validate_required_columns(view)
    validation_start = get_selected_split_candidate().validation_start
    reference = view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & (F.col("prediction_timestamp") < F.lit(validation_start))
        & (F.col("failure_horizon_status") == STATUS_NEGATIVE)
    )
    invalid = reference.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
        | F.col("loaded_cycle_id").isNull()
        | F.col(BASELINE_FEATURE_COLUMN).isNull()
        | F.isnan(BASELINE_FEATURE_COLUMN)
    ).limit(1)
    if invalid.count():
        raise EngineeringBaselineError("Negative training rows contain invalid baseline values")

    population = reference.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
    ).first()
    row_count = int(population.row_count)
    if row_count < 2:
        raise EngineeringBaselineError(
            "Baseline fitting requires at least two negative training rows"
        )
    if int(population.distinct_cycle_count) != row_count:
        raise EngineeringBaselineError("Negative training rows require unique cycle IDs")

    median = _exact_median(reference, BASELINE_FEATURE_COLUMN)
    deviations = reference.select(
        F.abs(F.col(BASELINE_FEATURE_COLUMN) - F.lit(median)).alias("absolute_deviation")
    )
    median_absolute_deviation = _exact_median(deviations, "absolute_deviation")
    if median_absolute_deviation <= 0:
        raise EngineeringBaselineError(
            "Baseline fitting requires a positive median absolute deviation"
        )

    return EngineeringBaselineParameters(
        baseline_version=ENGINEERING_BASELINE_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        feature_column=BASELINE_FEATURE_COLUMN,
        negative_training_row_count=row_count,
        training_median_amperes=median,
        training_median_absolute_deviation_amperes=median_absolute_deviation,
    )


def score_engineering_baseline(
    view: DataFrame,
    parameters: EngineeringBaselineParameters,
) -> DataFrame:
    """Attach an absolute robust-deviation score without applying an alert threshold."""

    _validate_required_columns(view)
    if (
        parameters.baseline_version != ENGINEERING_BASELINE_VERSION
        or parameters.modeling_view_version != MODELING_VIEW_VERSION
        or parameters.feature_column != BASELINE_FEATURE_COLUMN
        or parameters.negative_training_row_count < 2
        or not isfinite(parameters.training_median_amperes)
        or not isfinite(parameters.training_median_absolute_deviation_amperes)
        or parameters.training_median_absolute_deviation_amperes <= 0
    ):
        raise EngineeringBaselineError("Baseline parameters are incompatible or invalid")

    return view.withColumn(
        BASELINE_SCORE_COLUMN,
        F.when(
            F.col(BASELINE_FEATURE_COLUMN).isNotNull() & ~F.isnan(BASELINE_FEATURE_COLUMN),
            F.abs(F.col(BASELINE_FEATURE_COLUMN) - F.lit(parameters.training_median_amperes))
            / F.lit(parameters.training_median_absolute_deviation_amperes),
        ).cast("double"),
    )
