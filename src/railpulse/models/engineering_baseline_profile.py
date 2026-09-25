"""Profile the engineering baseline on train and validation periods only."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.chronological_splits import (
    CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_profile import build_full_source_modeling_view_inputs
from railpulse.evaluation.modeling_view_schema import STATUS_TRAINABLE
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.models.engineering_baseline import (
    BASELINE_SCORE_COLUMN,
    ENGINEERING_BASELINE_VERSION,
    EngineeringBaselineParameters,
    fit_engineering_baseline,
    score_engineering_baseline,
)
from railpulse.spark import create_local_spark_session

ENGINEERING_BASELINE_PROFILE_VERSION = "motor-current-robust-deviation-profile-v1"
_DEVELOPMENT_PARTITIONS = (PARTITION_TRAIN, PARTITION_VALIDATION)
_LABEL_ORDER = (STATUS_POSITIVE, STATUS_NEGATIVE)


class EngineeringBaselineProfileError(RuntimeError):
    """Raised when development-period baseline evidence cannot be reconciled."""


@dataclass(frozen=True)
class BaselineScoreDistribution:
    """Score distribution for one development period and label."""

    partition: str
    label_status: str
    row_count: int
    minimum_score: float
    median_score: float
    p90_score: float
    p95_score: float
    maximum_score: float


@dataclass(frozen=True)
class EngineeringBaselineDevelopmentSummary:
    """Reconciled train-and-validation score evidence."""

    development_row_count: int
    train_row_count: int
    validation_row_count: int
    distributions: tuple[BaselineScoreDistribution, ...]


@dataclass(frozen=True)
class FullSourceEngineeringBaselineProfile:
    """Source-bound baseline fit and development-only score profile."""

    profile_version: str
    baseline_version: str
    split_selection_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_ingestion_batch_id: str
    parameters: EngineeringBaselineParameters
    development_summary: EngineeringBaselineDevelopmentSummary


def collect_engineering_baseline_development_summary(
    view: DataFrame,
    parameters: EngineeringBaselineParameters,
) -> EngineeringBaselineDevelopmentSummary:
    """Score and summarize train/validation rows while leaving test rows untouched."""

    selected = get_selected_split_candidate()
    development = view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & (F.col("prediction_timestamp") < F.lit(selected.test_start))
    ).withColumn(
        "_development_partition",
        F.when(
            F.col("prediction_timestamp") < F.lit(selected.validation_start),
            F.lit(PARTITION_TRAIN),
        ).otherwise(F.lit(PARTITION_VALIDATION)),
    )
    scored = score_engineering_baseline(development, parameters)
    invalid = scored.where(
        F.col(BASELINE_SCORE_COLUMN).isNull()
        | F.isnan(BASELINE_SCORE_COLUMN)
        | (F.col(BASELINE_SCORE_COLUMN) < 0)
        | ~F.col("failure_horizon_status").isin(*_LABEL_ORDER)
    ).limit(1)
    if invalid.count():
        raise EngineeringBaselineProfileError(
            "Development rows contain invalid baseline scores or labels"
        )

    rows = (
        scored.groupBy("_development_partition", "failure_horizon_status")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.min(BASELINE_SCORE_COLUMN).alias("minimum_score"),
            F.percentile(F.col(BASELINE_SCORE_COLUMN), F.lit(0.5)).alias("median_score"),
            F.percentile(F.col(BASELINE_SCORE_COLUMN), F.lit(0.9)).alias("p90_score"),
            F.percentile(F.col(BASELINE_SCORE_COLUMN), F.lit(0.95)).alias("p95_score"),
            F.max(BASELINE_SCORE_COLUMN).alias("maximum_score"),
        )
        .collect()
    )
    by_group = {(row._development_partition, row.failure_horizon_status): row for row in rows}
    distributions = []
    for partition in _DEVELOPMENT_PARTITIONS:
        for label_status in _LABEL_ORDER:
            row = by_group.get((partition, label_status))
            if row is None:
                raise EngineeringBaselineProfileError(
                    f"Development profile requires {label_status} rows in {partition}"
                )
            distributions.append(
                BaselineScoreDistribution(
                    partition=partition,
                    label_status=label_status,
                    row_count=int(row.row_count),
                    minimum_score=float(row.minimum_score),
                    median_score=float(row.median_score),
                    p90_score=float(row.p90_score),
                    p95_score=float(row.p95_score),
                    maximum_score=float(row.maximum_score),
                )
            )

    counts = {
        partition: sum(
            distribution.row_count
            for distribution in distributions
            if distribution.partition == partition
        )
        for partition in _DEVELOPMENT_PARTITIONS
    }
    development_count = scored.count()
    if sum(counts.values()) != development_count:
        raise EngineeringBaselineProfileError("Development score groups do not reconcile")
    return EngineeringBaselineDevelopmentSummary(
        development_row_count=development_count,
        train_row_count=counts[PARTITION_TRAIN],
        validation_row_count=counts[PARTITION_VALIDATION],
        distributions=tuple(distributions),
    )


def profile_full_source_engineering_baseline(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceEngineeringBaselineProfile:
    """Fit on full-source training rows and profile development rows without writing."""

    inputs = build_full_source_modeling_view_inputs(spark, config)
    parameters = fit_engineering_baseline(inputs.view)
    summary = collect_engineering_baseline_development_summary(inputs.view, parameters)
    return FullSourceEngineeringBaselineProfile(
        profile_version=ENGINEERING_BASELINE_PROFILE_VERSION,
        baseline_version=ENGINEERING_BASELINE_VERSION,
        split_selection_version=CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        parameters=parameters,
        development_summary=summary,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source engineering-baseline development profile."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-engineering-baseline-profile", master=args.master)
    try:
        profile = profile_full_source_engineering_baseline(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
