"""Compare fixed chronological split candidates without fitting a model."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.chronological_splits import (
    CHRONOLOGICAL_SPLIT_VERSION,
    PARTITION_TEST,
    PARTITION_TRAIN,
    PARTITION_VALIDATION,
    SPLIT_CANDIDATES,
)
from railpulse.evaluation.modeling_view_profile import (
    FullSourceModelingViewInputs,
    build_full_source_modeling_view_inputs,
)
from railpulse.evaluation.modeling_view_schema import (
    MODELING_VIEW_VERSION,
    STATUS_TRAINABLE,
)
from railpulse.features.failure_horizons import STATUS_NEGATIVE, STATUS_POSITIVE
from railpulse.spark import create_local_spark_session

CHRONOLOGICAL_SPLIT_PROFILE_VERSION = "calendar-chronological-split-profile-v1"
PARTITION_ORDER = (PARTITION_TRAIN, PARTITION_VALIDATION, PARTITION_TEST)


class ChronologicalSplitProfileError(RuntimeError):
    """Raised when split-comparison evidence cannot be reconciled."""


@dataclass(frozen=True)
class SplitPeriodCount:
    """Label, time, and event coverage for one chronological period."""

    partition: str
    row_count: int
    positive_count: int
    negative_count: int
    earliest_prediction_timestamp: str
    latest_prediction_timestamp: str
    prediction_span_seconds: int
    represented_failure_event_count: int
    represented_failure_record_ids: tuple[str, ...]


@dataclass(frozen=True)
class SplitCandidateCount:
    """Reconciled evidence for one declared candidate."""

    candidate_id: str
    validation_start: str
    test_start: str
    periods: tuple[SplitPeriodCount, ...]


@dataclass(frozen=True)
class ChronologicalSplitComparison:
    """Candidate comparison over the same trainable modelling-view population."""

    profile_version: str
    split_version: str
    modeling_view_version: str
    trainable_row_count: int
    accepted_failure_event_count: int
    candidates: tuple[SplitCandidateCount, ...]


@dataclass(frozen=True)
class FullSourceChronologicalSplitProfile:
    """Source-bound result of the read-only chronological split comparison."""

    profile_version: str
    split_version: str
    modeling_view_version: str
    label_observation_end: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    accepted_failure_event_count: int
    comparison: ChronologicalSplitComparison


_REQUIRED_COLUMNS = {
    "loaded_cycle_id",
    "modeling_view_version",
    "modeling_row_status",
    "failure_horizon_status",
    "prediction_timestamp",
    "matched_failure_record_id",
}


def _timestamp_text(value: object) -> str:
    return value.isoformat(sep=" ", timespec="seconds")  # type: ignore[union-attr]


def _partition_expression(validation_start: object, test_start: object) -> Column:
    return (
        F.when(F.col("prediction_timestamp") < F.lit(validation_start), F.lit(PARTITION_TRAIN))
        .when(F.col("prediction_timestamp") < F.lit(test_start), F.lit(PARTITION_VALIDATION))
        .otherwise(F.lit(PARTITION_TEST))
    )


def collect_chronological_split_comparison(
    view: DataFrame,
    failure_rows: tuple[Row, ...],
) -> ChronologicalSplitComparison:
    """Compare every declared calendar candidate over trainable modelling rows."""

    missing_columns = sorted(_REQUIRED_COLUMNS - set(view.columns))
    if missing_columns:
        raise ChronologicalSplitProfileError(
            "Modelling view is missing split-profile columns: " + ", ".join(missing_columns)
        )

    trainable = view.where(F.col("modeling_row_status") == STATUS_TRAINABLE)
    invalid = trainable.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
        | F.col("loaded_cycle_id").isNull()
        | F.col("prediction_timestamp").isNull()
        | ~F.col("failure_horizon_status").isin(STATUS_POSITIVE, STATUS_NEGATIVE)
    ).limit(1)
    if invalid.count():
        raise ChronologicalSplitProfileError("Trainable split-profile rows have invalid semantics")

    population = trainable.agg(
        F.count(F.lit(1)).cast(LongType()).alias("row_count"),
        F.countDistinct("loaded_cycle_id").cast(LongType()).alias("distinct_cycle_count"),
    ).first()
    row_count = int(population.row_count)
    if row_count <= 0:
        raise ChronologicalSplitProfileError("Split profiling requires trainable rows")
    if int(population.distinct_cycle_count) != row_count:
        raise ChronologicalSplitProfileError(
            "Trainable split-profile rows require unique cycle IDs"
        )

    accepted_ids = {str(row.record_id) for row in failure_rows}
    if len(accepted_ids) != len(failure_rows):
        raise ChronologicalSplitProfileError("Accepted failure events require unique record IDs")
    unknown_event = trainable.where(
        (F.col("failure_horizon_status") == STATUS_POSITIVE)
        & ~F.col("matched_failure_record_id").isin(*sorted(accepted_ids))
    ).limit(1)
    if unknown_event.count():
        raise ChronologicalSplitProfileError(
            "A positive trainable row references an unknown accepted failure event"
        )

    candidates = []
    for candidate in SPLIT_CANDIDATES:
        partitioned = trainable.withColumn(
            "_split_partition",
            _partition_expression(candidate.validation_start, candidate.test_start),
        )
        summaries = {
            row._split_partition: row
            for row in partitioned.groupBy("_split_partition")
            .agg(
                F.count(F.lit(1)).cast(LongType()).alias("row_count"),
                F.count(F.when(F.col("failure_horizon_status") == STATUS_POSITIVE, 1))
                .cast(LongType())
                .alias("positive_count"),
                F.count(F.when(F.col("failure_horizon_status") == STATUS_NEGATIVE, 1))
                .cast(LongType())
                .alias("negative_count"),
                F.min("prediction_timestamp").alias("earliest_prediction_timestamp"),
                F.max("prediction_timestamp").alias("latest_prediction_timestamp"),
                F.sort_array(
                    F.collect_set(
                        F.when(
                            F.col("failure_horizon_status") == STATUS_POSITIVE,
                            F.col("matched_failure_record_id"),
                        )
                    )
                ).alias("represented_failure_record_ids"),
            )
            .collect()
        }
        periods = []
        for partition in PARTITION_ORDER:
            summary = summaries.get(partition)
            if summary is None:
                raise ChronologicalSplitProfileError(
                    f"Candidate {candidate.candidate_id} has an empty {partition} period"
                )
            period_rows = int(summary.row_count)
            positive_count = int(summary.positive_count)
            negative_count = int(summary.negative_count)
            if positive_count + negative_count != period_rows:
                raise ChronologicalSplitProfileError(
                    f"Candidate {candidate.candidate_id} does not reconcile {partition} labels"
                )
            earliest = summary.earliest_prediction_timestamp
            latest = summary.latest_prediction_timestamp
            represented_ids = tuple(summary.represented_failure_record_ids)
            periods.append(
                SplitPeriodCount(
                    partition=partition,
                    row_count=period_rows,
                    positive_count=positive_count,
                    negative_count=negative_count,
                    earliest_prediction_timestamp=_timestamp_text(earliest),
                    latest_prediction_timestamp=_timestamp_text(latest),
                    prediction_span_seconds=int((latest - earliest).total_seconds()),
                    represented_failure_event_count=len(represented_ids),
                    represented_failure_record_ids=represented_ids,
                )
            )
        if sum(period.row_count for period in periods) != row_count:
            raise ChronologicalSplitProfileError(
                f"Candidate {candidate.candidate_id} does not reconcile the trainable population"
            )
        candidates.append(
            SplitCandidateCount(
                candidate_id=candidate.candidate_id,
                validation_start=_timestamp_text(candidate.validation_start),
                test_start=_timestamp_text(candidate.test_start),
                periods=tuple(periods),
            )
        )

    return ChronologicalSplitComparison(
        profile_version=CHRONOLOGICAL_SPLIT_PROFILE_VERSION,
        split_version=CHRONOLOGICAL_SPLIT_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        trainable_row_count=row_count,
        accepted_failure_event_count=len(failure_rows),
        candidates=tuple(candidates),
    )


def build_full_source_chronological_split_profile(
    inputs: FullSourceModelingViewInputs,
) -> FullSourceChronologicalSplitProfile:
    """Attach full-source lineage to a reconciled split comparison."""

    comparison = collect_chronological_split_comparison(inputs.view, inputs.failure_rows)
    return FullSourceChronologicalSplitProfile(
        profile_version=CHRONOLOGICAL_SPLIT_PROFILE_VERSION,
        split_version=CHRONOLOGICAL_SPLIT_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        label_observation_end=inputs.label_observation_end,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_source_document_sha256=inputs.failure_source_document_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        accepted_telemetry_record_count=inputs.accepted_telemetry_record_count,
        accepted_failure_event_count=len(inputs.failure_rows),
        comparison=comparison,
    )


def profile_full_source_chronological_splits(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceChronologicalSplitProfile:
    """Rebuild the modelling view and compare calendar candidates without writing."""

    inputs = build_full_source_modeling_view_inputs(spark, config)
    return build_full_source_chronological_split_profile(inputs)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source split comparison and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-chronological-split-profile", master=args.master)
    try:
        profile = profile_full_source_chronological_splits(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
