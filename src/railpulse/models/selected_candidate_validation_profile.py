"""Summarize selected-candidate validation scores without persisting row-level output."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from itertools import pairwise
from math import isfinite
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.chronological_splits import (
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_schema import MODEL_TARGET_COLUMN, MODEL_TIME_COLUMN
from railpulse.evaluation.modeling_view_v2_profile import (
    build_full_source_modeling_view_v2_inputs,
)
from railpulse.evaluation.modeling_view_v2_schema import MODELING_VIEW_V2_VERSION
from railpulse.models.candidate_comparison_contract import MODEL_CANDIDATE_COMPARISON_VERSION
from railpulse.models.candidate_development_data import (
    MODEL_CANDIDATE_KEY_COLUMN,
    prepare_model_candidate_development_data,
)
from railpulse.models.development_model_selection import (
    get_development_model_selection,
    get_selected_development_candidate,
)
from railpulse.models.selected_candidate_validation import (
    SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION,
    VALIDATION_SCORE_CANDIDATE_COLUMN,
    VALIDATION_SCORE_COLUMN,
    VALIDATION_SCORE_COLUMNS,
    VALIDATION_SCORE_PARTITION_COLUMN,
    VALIDATION_SCORE_SELECTION_VERSION_COLUMN,
    VALIDATION_SCORE_SPLIT_COLUMN,
    SelectedCandidateValidationError,
    SelectedCandidateValidationScores,
    score_selected_candidate_validation,
)
from railpulse.spark import create_local_spark_session

SELECTED_CANDIDATE_VALIDATION_PROFILE_VERSION = "motor-current-validation-score-profile-v1"
SCORE_POPULATION_ALL = "all"
SCORE_POPULATION_POSITIVE = "positive"
SCORE_POPULATION_NEGATIVE = "negative"
SCORE_POPULATION_ORDER = (
    SCORE_POPULATION_ALL,
    SCORE_POPULATION_POSITIVE,
    SCORE_POPULATION_NEGATIVE,
)


class SelectedCandidateValidationProfileError(ValueError):
    """Raised when validation scores cannot produce a reconciled compact profile."""


@dataclass(frozen=True)
class ValidationScoreDistribution:
    """Exact probability distribution for one validation-label population."""

    population: str
    row_count: int
    minimum_score: float
    p01_score: float
    p05_score: float
    p10_score: float
    p25_score: float
    median_score: float
    mean_score: float
    p75_score: float
    p90_score: float
    p95_score: float
    p99_score: float
    maximum_score: float


@dataclass(frozen=True)
class SelectedCandidateValidationProfile:
    """Compact full-source evidence with no row-level scores or test-period metrics."""

    profile_version: str
    scoring_version: str
    selection_version: str
    comparison_version: str
    modeling_view_version: str
    split_candidate_id: str
    candidate_id: str
    feature_columns: tuple[str, ...]
    validation_row_count: int
    validation_positive_count: int
    validation_negative_count: int
    validation_positive_rate: float
    validation_area_under_pr: float
    fitted_intercept: float
    fitted_coefficients: tuple[float, ...]
    completed_iterations: int
    score_distributions: tuple[ValidationScoreDistribution, ...]


def _percentile(column: str, probability: float, alias: str):
    return F.percentile(F.col(column), F.lit(probability)).alias(alias)


def _collect_distribution(
    scores: DataFrame,
    *,
    population: str,
) -> ValidationScoreDistribution:
    if population == SCORE_POPULATION_POSITIVE:
        selected = scores.where(F.col(MODEL_TARGET_COLUMN))
    elif population == SCORE_POPULATION_NEGATIVE:
        selected = scores.where(~F.col(MODEL_TARGET_COLUMN))
    elif population == SCORE_POPULATION_ALL:
        selected = scores
    else:
        raise SelectedCandidateValidationProfileError(
            f"Unsupported validation score population: {population}"
        )

    row = selected.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.min(VALIDATION_SCORE_COLUMN).alias("minimum_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.01, "p01_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.05, "p05_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.10, "p10_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.25, "p25_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.50, "median_score"),
        F.avg(VALIDATION_SCORE_COLUMN).alias("mean_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.75, "p75_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.90, "p90_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.95, "p95_score"),
        _percentile(VALIDATION_SCORE_COLUMN, 0.99, "p99_score"),
        F.max(VALIDATION_SCORE_COLUMN).alias("maximum_score"),
    ).first()
    values = tuple(
        float(row[field])
        for field in (
            "minimum_score",
            "p01_score",
            "p05_score",
            "p10_score",
            "p25_score",
            "median_score",
            "mean_score",
            "p75_score",
            "p90_score",
            "p95_score",
            "p99_score",
            "maximum_score",
        )
    )
    if int(row.row_count) <= 0 or any(
        not isfinite(value) or not 0.0 <= value <= 1.0 for value in values
    ):
        raise SelectedCandidateValidationProfileError(
            f"Validation {population} score distribution is empty or invalid"
        )
    ordered = (
        values[0],
        values[1],
        values[2],
        values[3],
        values[4],
        values[5],
        values[7],
        values[8],
        values[9],
        values[10],
        values[11],
    )
    if any(left > right for left, right in pairwise(ordered)):
        raise SelectedCandidateValidationProfileError(
            f"Validation {population} score percentiles are not ordered"
        )
    return ValidationScoreDistribution(
        population=population,
        row_count=int(row.row_count),
        minimum_score=values[0],
        p01_score=values[1],
        p05_score=values[2],
        p10_score=values[3],
        p25_score=values[4],
        median_score=values[5],
        mean_score=values[6],
        p75_score=values[7],
        p90_score=values[8],
        p95_score=values[9],
        p99_score=values[10],
        maximum_score=values[11],
    )


def _validate_score_frame(result: SelectedCandidateValidationScores) -> None:
    scores = result.scores
    if tuple(scores.columns) != VALIDATION_SCORE_COLUMNS:
        raise SelectedCandidateValidationProfileError(
            "Validation score frame does not expose the reviewed columns"
        )
    split = result.split_candidate_id
    expected_literals = {
        VALIDATION_SCORE_PARTITION_COLUMN: PARTITION_VALIDATION,
        VALIDATION_SCORE_SELECTION_VERSION_COLUMN: result.selection_version,
        VALIDATION_SCORE_CANDIDATE_COLUMN: result.candidate_id,
        VALIDATION_SCORE_SPLIT_COLUMN: split,
    }
    literal_checks = []
    for column in expected_literals:
        literal_checks.extend(
            [
                F.min(column).alias(f"minimum_{column}"),
                F.max(column).alias(f"maximum_{column}"),
                F.countDistinct(column).alias(f"distinct_{column}"),
            ]
        )
    row = scores.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(MODEL_CANDIDATE_KEY_COLUMN).alias("distinct_key_count"),
        F.count(F.when(F.col(MODEL_TARGET_COLUMN), F.lit(1))).alias("positive_count"),
        F.count(
            F.when(
                F.col(MODEL_CANDIDATE_KEY_COLUMN).isNull()
                | F.col(MODEL_TIME_COLUMN).isNull()
                | F.col(MODEL_TARGET_COLUMN).isNull()
                | F.col(VALIDATION_SCORE_COLUMN).isNull()
                | F.isnan(VALIDATION_SCORE_COLUMN)
                | (F.col(VALIDATION_SCORE_COLUMN) < 0.0)
                | (F.col(VALIDATION_SCORE_COLUMN) > 1.0),
                F.lit(1),
            )
        ).alias("invalid_row_count"),
        *literal_checks,
    ).first()
    expected_count = result.validation_summary.row_count
    expected_positive = result.validation_summary.positive_count
    if (
        int(row.row_count) != expected_count
        or int(row.distinct_key_count) != expected_count
        or int(row.positive_count) != expected_positive
        or int(row.invalid_row_count) != 0
    ):
        raise SelectedCandidateValidationProfileError(
            "Validation score rows do not reconcile with scoring evidence"
        )
    for column, expected in expected_literals.items():
        if (
            int(row[f"distinct_{column}"]) != 1
            or row[f"minimum_{column}"] != expected
            or row[f"maximum_{column}"] != expected
        ):
            raise SelectedCandidateValidationProfileError(
                f"Validation score identity column {column} is incompatible"
            )


def _validate_profile_metadata(result: SelectedCandidateValidationScores) -> None:
    selection = get_development_model_selection()
    candidate = get_selected_development_candidate()
    split = get_selected_split_candidate()
    evidence = next(
        item
        for item in selection.candidate_evidence
        if item.candidate_id == selection.selected_candidate_id
    )
    expected_summaries = {
        PARTITION_TRAIN: (
            evidence.train_row_count,
            evidence.train_positive_count,
            evidence.train_negative_count,
        ),
        PARTITION_VALIDATION: (
            evidence.validation_row_count,
            evidence.validation_positive_count,
            evidence.validation_negative_count,
        ),
    }
    actual_summaries = {
        result.train_summary.partition: (
            result.train_summary.row_count,
            result.train_summary.positive_count,
            result.train_summary.negative_count,
        ),
        result.validation_summary.partition: (
            result.validation_summary.row_count,
            result.validation_summary.positive_count,
            result.validation_summary.negative_count,
        ),
    }
    if (
        result.scoring_version != SELECTED_CANDIDATE_VALIDATION_SCORE_VERSION
        or result.selection_version != selection.selection_version
        or result.comparison_version != MODEL_CANDIDATE_COMPARISON_VERSION
        or result.comparison_version != selection.comparison_version
        or result.modeling_view_version != MODELING_VIEW_V2_VERSION
        or result.split_candidate_id != split.candidate_id
        or result.candidate_id != candidate.candidate_id
        or result.feature_columns != candidate.feature_columns
        or actual_summaries != expected_summaries
    ):
        raise SelectedCandidateValidationProfileError(
            "Validation score metadata does not match the reviewed selection evidence"
        )


def collect_selected_candidate_validation_profile(
    result: SelectedCandidateValidationScores,
) -> SelectedCandidateValidationProfile:
    """Collect compact validation evidence without retaining cycle-level rows."""

    _validate_profile_metadata(result)
    _validate_score_frame(result)
    distributions = tuple(
        _collect_distribution(result.scores, population=population)
        for population in SCORE_POPULATION_ORDER
    )
    counts = {distribution.population: distribution.row_count for distribution in distributions}
    if (
        counts[SCORE_POPULATION_ALL] != result.validation_summary.row_count
        or counts[SCORE_POPULATION_POSITIVE] != result.validation_summary.positive_count
        or counts[SCORE_POPULATION_NEGATIVE] != result.validation_summary.negative_count
        or counts[SCORE_POPULATION_POSITIVE] + counts[SCORE_POPULATION_NEGATIVE]
        != counts[SCORE_POPULATION_ALL]
    ):
        raise SelectedCandidateValidationProfileError(
            "Validation score distributions do not reconcile"
        )
    positive_rate = result.validation_summary.positive_count / result.validation_summary.row_count
    return SelectedCandidateValidationProfile(
        profile_version=SELECTED_CANDIDATE_VALIDATION_PROFILE_VERSION,
        scoring_version=result.scoring_version,
        selection_version=result.selection_version,
        comparison_version=result.comparison_version,
        modeling_view_version=result.modeling_view_version,
        split_candidate_id=result.split_candidate_id,
        candidate_id=result.candidate_id,
        feature_columns=result.feature_columns,
        validation_row_count=result.validation_summary.row_count,
        validation_positive_count=result.validation_summary.positive_count,
        validation_negative_count=result.validation_summary.negative_count,
        validation_positive_rate=positive_rate,
        validation_area_under_pr=result.validation_area_under_pr,
        fitted_intercept=result.fitted_intercept,
        fitted_coefficients=result.fitted_coefficients,
        completed_iterations=result.completed_iterations,
        score_distributions=distributions,
    )


def profile_full_source_selected_candidate_validation(
    spark: SparkSession,
    config: RailPulseConfig,
) -> SelectedCandidateValidationProfile:
    """Build the local modelling view and summarize validation scores without writing."""

    inputs = build_full_source_modeling_view_v2_inputs(spark, config)
    development = prepare_model_candidate_development_data(inputs.view)
    try:
        result = score_selected_candidate_validation(development)
    except SelectedCandidateValidationError as error:
        summaries = tuple(
            (
                summary.partition,
                summary.row_count,
                summary.positive_count,
                summary.negative_count,
            )
            for summary in development.summaries
        )
        raise SelectedCandidateValidationProfileError(
            "Local development population does not match managed comparison evidence: "
            f"summaries={summaries}"
        ) from error
    return collect_selected_candidate_validation_profile(result)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source validation profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session(
        "railpulse-selected-candidate-validation-profile",
        master=args.master,
    )
    try:
        profile = profile_full_source_selected_candidate_validation(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
