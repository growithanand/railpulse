"""Compare low-current baseline candidates on train and validation data only."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.chronological_splits import (
    CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    get_selected_split_candidate,
)
from railpulse.evaluation.modeling_view_profile import build_full_source_modeling_view_inputs
from railpulse.evaluation.modeling_view_schema import MODELING_VIEW_VERSION, STATUS_TRAINABLE
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.models.directional_baseline import (
    DIRECTIONAL_BASELINE_VERSION,
    DIRECTIONAL_THRESHOLD_CANDIDATES,
)
from railpulse.models.engineering_baseline import BASELINE_FEATURE_COLUMN
from railpulse.spark import create_local_spark_session

DIRECTIONAL_BASELINE_PROFILE_VERSION = "motor-current-low-directional-profile-v1"
_PARTITION_ORDER = (PARTITION_TRAIN, PARTITION_VALIDATION)
_REQUIRED_COLUMNS = {
    "loaded_cycle_id",
    "modeling_view_version",
    "modeling_row_status",
    "failure_horizon_status",
    "prediction_timestamp",
    "matched_failure_record_id",
    BASELINE_FEATURE_COLUMN,
}


class DirectionalBaselineProfileError(RuntimeError):
    """Raised when directional candidate metrics cannot be reconciled."""


@dataclass(frozen=True)
class DirectionalPartitionMetrics:
    """Cycle-level metrics for one candidate in one development period."""

    partition: str
    positive_count: int
    negative_count: int
    true_positive_count: int
    false_negative_count: int
    false_positive_count: int
    true_negative_count: int
    precision: float | None
    recall: float
    false_positive_rate: float
    represented_failure_event_count: int


@dataclass(frozen=True)
class DirectionalCandidateMetrics:
    """Training and validation evidence for one fixed threshold candidate."""

    candidate_id: str
    maximum_mean_current_amperes: float
    partitions: tuple[DirectionalPartitionMetrics, ...]


@dataclass(frozen=True)
class DirectionalBaselineComparison:
    """Reconciled comparison over the development population."""

    development_row_count: int
    candidates: tuple[DirectionalCandidateMetrics, ...]


@dataclass(frozen=True)
class FullSourceDirectionalBaselineProfile:
    """Source-bound development-only directional baseline comparison."""

    profile_version: str
    baseline_version: str
    split_selection_version: str
    modeling_view_version: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_ingestion_batch_id: str
    comparison: DirectionalBaselineComparison


def _count_when(condition: Column, alias: str) -> Column:
    return F.count(F.when(condition, F.lit(1))).alias(alias)


def collect_directional_baseline_comparison(view: DataFrame) -> DirectionalBaselineComparison:
    """Compare fixed low-current rules without scoring the sealed test period."""

    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise DirectionalBaselineProfileError(
            "Modelling view is missing directional-profile columns: " + ", ".join(missing_columns)
        )

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
    invalid = development.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
        | F.col("loaded_cycle_id").isNull()
        | F.col("prediction_timestamp").isNull()
        | ~F.col("failure_horizon_status").isin(STATUS_POSITIVE, STATUS_NEGATIVE)
        | F.col(BASELINE_FEATURE_COLUMN).isNull()
        | F.isnan(BASELINE_FEATURE_COLUMN)
        | (
            (F.col("failure_horizon_status") == STATUS_POSITIVE)
            & F.col("matched_failure_record_id").isNull()
        )
    ).limit(1)
    if invalid.count():
        raise DirectionalBaselineProfileError(
            "Development rows contain invalid directional-profile values"
        )

    population = development.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("loaded_cycle_id").alias("distinct_cycle_count"),
    ).first()
    row_count = int(population.row_count)
    if row_count <= 0:
        raise DirectionalBaselineProfileError("Directional profiling requires development rows")
    if int(population.distinct_cycle_count) != row_count:
        raise DirectionalBaselineProfileError("Development rows require unique cycle IDs")

    aggregations: list[Column] = []
    for partition in _PARTITION_ORDER:
        in_partition = F.col("_development_partition") == partition
        positive = F.col("failure_horizon_status") == STATUS_POSITIVE
        negative = F.col("failure_horizon_status") == STATUS_NEGATIVE
        aggregations.extend(
            (
                _count_when(in_partition & positive, f"{partition}_positive"),
                _count_when(in_partition & negative, f"{partition}_negative"),
            )
        )
        for index, candidate in enumerate(DIRECTIONAL_THRESHOLD_CANDIDATES):
            alert = F.col(BASELINE_FEATURE_COLUMN) <= F.lit(candidate.maximum_mean_current_amperes)
            prefix = f"candidate_{index}_{partition}"
            aggregations.extend(
                (
                    _count_when(in_partition & positive & alert, f"{prefix}_tp"),
                    _count_when(in_partition & negative & alert, f"{prefix}_fp"),
                    F.countDistinct(
                        F.when(
                            in_partition & positive & alert,
                            F.col("matched_failure_record_id"),
                        )
                    ).alias(f"{prefix}_events"),
                )
            )
    summary = development.agg(*aggregations).first()

    candidates = []
    for index, candidate in enumerate(DIRECTIONAL_THRESHOLD_CANDIDATES):
        partitions = []
        for partition in _PARTITION_ORDER:
            positive_count = int(summary[f"{partition}_positive"])
            negative_count = int(summary[f"{partition}_negative"])
            if positive_count <= 0 or negative_count <= 0:
                raise DirectionalBaselineProfileError(
                    f"Directional profiling requires both labels in {partition}"
                )
            prefix = f"candidate_{index}_{partition}"
            true_positive = int(summary[f"{prefix}_tp"])
            false_positive = int(summary[f"{prefix}_fp"])
            false_negative = positive_count - true_positive
            true_negative = negative_count - false_positive
            alert_count = true_positive + false_positive
            partitions.append(
                DirectionalPartitionMetrics(
                    partition=partition,
                    positive_count=positive_count,
                    negative_count=negative_count,
                    true_positive_count=true_positive,
                    false_negative_count=false_negative,
                    false_positive_count=false_positive,
                    true_negative_count=true_negative,
                    precision=(true_positive / alert_count if alert_count else None),
                    recall=true_positive / positive_count,
                    false_positive_rate=false_positive / negative_count,
                    represented_failure_event_count=int(summary[f"{prefix}_events"]),
                )
            )
        if (
            sum(metrics.positive_count + metrics.negative_count for metrics in partitions)
            != row_count
        ):
            raise DirectionalBaselineProfileError(
                f"Candidate {candidate.candidate_id} does not reconcile development rows"
            )
        candidates.append(
            DirectionalCandidateMetrics(
                candidate_id=candidate.candidate_id,
                maximum_mean_current_amperes=candidate.maximum_mean_current_amperes,
                partitions=tuple(partitions),
            )
        )

    return DirectionalBaselineComparison(
        development_row_count=row_count,
        candidates=tuple(candidates),
    )


def profile_full_source_directional_baselines(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceDirectionalBaselineProfile:
    """Build and compare full-source development candidates without writing."""

    inputs = build_full_source_modeling_view_inputs(spark, config)
    comparison = collect_directional_baseline_comparison(inputs.view)
    return FullSourceDirectionalBaselineProfile(
        profile_version=DIRECTIONAL_BASELINE_PROFILE_VERSION,
        baseline_version=DIRECTIONAL_BASELINE_VERSION,
        split_selection_version=CHRONOLOGICAL_SPLIT_SELECTION_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        comparison=comparison,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source directional baseline comparison."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-directional-baseline-profile", master=args.master)
    try:
        profile = profile_full_source_directional_baselines(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
