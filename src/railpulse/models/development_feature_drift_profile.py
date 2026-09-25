"""Profile development-period motor-current drift without inspecting test rows."""

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
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_profile import build_full_source_modeling_view_inputs
from railpulse.evaluation.modeling_view_schema import MODELING_VIEW_VERSION, STATUS_TRAINABLE
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.models.engineering_baseline import BASELINE_FEATURE_COLUMN
from railpulse.spark import create_local_spark_session

DEVELOPMENT_FEATURE_DRIFT_PROFILE_VERSION = "motor-current-development-drift-profile-v1"
_LABEL_ORDER = (STATUS_POSITIVE, STATUS_NEGATIVE)
_REQUIRED_COLUMNS = {
    "loaded_cycle_id",
    "modeling_view_version",
    "modeling_row_status",
    "failure_horizon_status",
    "prediction_timestamp",
    BASELINE_FEATURE_COLUMN,
}


class DevelopmentFeatureDriftProfileError(RuntimeError):
    """Raised when development feature-drift evidence cannot be reconciled."""


@dataclass(frozen=True)
class MonthlyFeatureDistribution:
    """Monthly feature distribution for one observed label status."""

    calendar_month: str
    label_status: str
    row_count: int
    minimum_amperes: float
    p10_amperes: float
    median_amperes: float
    p90_amperes: float
    maximum_amperes: float


@dataclass(frozen=True)
class DevelopmentFeatureDriftSummary:
    """Reconciled monthly distributions over train and validation rows."""

    development_row_count: int
    earliest_prediction_timestamp: str
    latest_prediction_timestamp: str
    monthly_distributions: tuple[MonthlyFeatureDistribution, ...]


@dataclass(frozen=True)
class FullSourceDevelopmentFeatureDriftProfile:
    """Source-bound development-only motor-current drift profile."""

    profile_version: str
    split_selection_version: str
    modeling_view_version: str
    feature_column: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_ingestion_batch_id: str
    summary: DevelopmentFeatureDriftSummary


def _timestamp_text(value: object) -> str:
    return value.isoformat(sep=" ", timespec="seconds")  # type: ignore[union-attr]


def collect_development_feature_drift_summary(view: DataFrame) -> DevelopmentFeatureDriftSummary:
    """Summarize monthly train/validation feature distributions and exclude test rows."""

    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise DevelopmentFeatureDriftProfileError(
            "Modelling view is missing drift-profile columns: " + ", ".join(missing_columns)
        )
    test_start = get_selected_split_candidate().test_start
    development = view.where(
        (F.col("modeling_row_status") == STATUS_TRAINABLE)
        & (F.col("prediction_timestamp") < F.lit(test_start))
    )
    invalid = development.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
        | F.col("loaded_cycle_id").isNull()
        | F.col("prediction_timestamp").isNull()
        | ~F.col("failure_horizon_status").isin(*_LABEL_ORDER)
        | F.col(BASELINE_FEATURE_COLUMN).isNull()
        | F.isnan(BASELINE_FEATURE_COLUMN)
    ).limit(1)
    if invalid.count():
        raise DevelopmentFeatureDriftProfileError(
            "Development rows contain invalid feature or label values"
        )

    population = development.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
        F.min("prediction_timestamp").alias("earliest_prediction_timestamp"),
        F.max("prediction_timestamp").alias("latest_prediction_timestamp"),
    ).first()
    row_count = int(population.row_count)
    if row_count <= 0:
        raise DevelopmentFeatureDriftProfileError("Drift profiling requires development rows")
    if int(population.distinct_cycle_count) != row_count:
        raise DevelopmentFeatureDriftProfileError("Development rows require unique cycle IDs")

    monthly_rows = (
        development.withColumn("calendar_month", F.date_format("prediction_timestamp", "yyyy-MM"))
        .groupBy("calendar_month", "failure_horizon_status")
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.min(BASELINE_FEATURE_COLUMN).alias("minimum_amperes"),
            F.percentile(F.col(BASELINE_FEATURE_COLUMN), F.lit(0.1)).alias("p10_amperes"),
            F.percentile(F.col(BASELINE_FEATURE_COLUMN), F.lit(0.5)).alias("median_amperes"),
            F.percentile(F.col(BASELINE_FEATURE_COLUMN), F.lit(0.9)).alias("p90_amperes"),
            F.max(BASELINE_FEATURE_COLUMN).alias("maximum_amperes"),
        )
        .orderBy("calendar_month", "failure_horizon_status")
        .collect()
    )
    if sum(int(row.row_count) for row in monthly_rows) != row_count:
        raise DevelopmentFeatureDriftProfileError("Monthly feature groups do not reconcile")

    return DevelopmentFeatureDriftSummary(
        development_row_count=row_count,
        earliest_prediction_timestamp=_timestamp_text(population.earliest_prediction_timestamp),
        latest_prediction_timestamp=_timestamp_text(population.latest_prediction_timestamp),
        monthly_distributions=tuple(
            MonthlyFeatureDistribution(
                calendar_month=row.calendar_month,
                label_status=row.failure_horizon_status,
                row_count=int(row.row_count),
                minimum_amperes=float(row.minimum_amperes),
                p10_amperes=float(row.p10_amperes),
                median_amperes=float(row.median_amperes),
                p90_amperes=float(row.p90_amperes),
                maximum_amperes=float(row.maximum_amperes),
            )
            for row in monthly_rows
        ),
    )


def profile_full_source_development_feature_drift(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceDevelopmentFeatureDriftProfile:
    """Build and profile full-source development rows without writing data."""

    inputs = build_full_source_modeling_view_inputs(spark, config)
    summary = collect_development_feature_drift_summary(inputs.view)
    return FullSourceDevelopmentFeatureDriftProfile(
        profile_version=DEVELOPMENT_FEATURE_DRIFT_PROFILE_VERSION,
        split_selection_version=CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        feature_column=BASELINE_FEATURE_COLUMN,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        summary=summary,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source development feature-drift profile."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-development-feature-drift", master=args.master)
    try:
        profile = profile_full_source_development_feature_drift(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
