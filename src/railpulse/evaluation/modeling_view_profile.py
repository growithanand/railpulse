"""Profile the full-source chronological modelling view without writing data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.config import RailPulseConfig, load_config
from railpulse.evaluation.modeling_view import build_modeling_view
from railpulse.evaluation.modeling_view_schema import (
    MODELING_EXCLUSION_REASON_ORDER,
    MODELING_VIEW_VERSION,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.failure_horizon_profile import (
    FailureHorizonProfileError,
    _collect_failure_events,
    _collect_observation_boundary,
    _timestamp_text,
)
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
    assign_cycle_failure_horizons,
)
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.ingestion.bronze import FAILURE_TABLE, TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_failures import split_failure_events_by_quality
from railpulse.validation.silver_telemetry import split_telemetry_by_quality

MODELING_VIEW_PROFILE_VERSION = "motor-current-failure-modeling-view-profile-v1"
MODELING_ROW_STATUS_ORDER = (STATUS_TRAINABLE, STATUS_EXCLUDED)
MODELING_LABEL_STATUS_ORDER = (STATUS_POSITIVE, STATUS_NEGATIVE)


class ModelingViewProfileError(RuntimeError):
    """Raised when modelling-view profile counts cannot be reconciled."""


@dataclass(frozen=True)
class ModelingRowStatusCount:
    """Cycle count for one modelling-row status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class ModelingExclusionReasonCount:
    """Excluded-cycle count carrying one explicit reason."""

    reason: str
    cycle_count: int


@dataclass(frozen=True)
class ModelingLabelCount:
    """Trainable-cycle count for one observed label status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class ModelingFailureEventCount:
    """Trainable positive-cycle coverage for one accepted failure event."""

    failure_record_id: str
    source_row: int
    failure_start: str
    failure_end: str
    trainable_positive_cycle_count: int


@dataclass(frozen=True)
class ModelingViewSummary:
    """Reconciled analytical summary of a modelling-view DataFrame."""

    cycle_count: int
    earliest_trainable_prediction_timestamp: str
    latest_trainable_prediction_timestamp: str
    status_counts: tuple[ModelingRowStatusCount, ...]
    exclusion_reason_counts: tuple[ModelingExclusionReasonCount, ...]
    trainable_label_counts: tuple[ModelingLabelCount, ...]
    failure_event_counts: tuple[ModelingFailureEventCount, ...]


@dataclass(frozen=True)
class FullSourceModelingViewProfile:
    """Source-bound profile of the complete chronological modelling view."""

    profile_version: str
    modeling_view_version: str
    feature_snapshot_version: str
    failure_horizon_version: str
    failure_horizon_seconds: int
    label_observation_end: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    accepted_failure_event_count: int
    matched_trainable_failure_event_count: int
    summary: ModelingViewSummary


@dataclass(frozen=True)
class FullSourceModelingViewInputs:
    """Rebuilt modelling view and source metadata shared by read-only profiles."""

    view: DataFrame
    failure_rows: tuple[Row, ...]
    label_observation_end: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int


_PROFILE_COLUMNS = {
    "loaded_cycle_id",
    "modeling_view_version",
    "modeling_row_status",
    "modeling_exclusion_reasons",
    "failure_horizon_status",
    "failure_within_horizon",
    "prediction_timestamp",
    "matched_failure_record_id",
}


def collect_modeling_view_summary(
    view: DataFrame,
    failure_rows: tuple[Row, ...],
) -> ModelingViewSummary:
    """Reconcile row eligibility, observed labels, time coverage, and failure events."""

    missing_columns = sorted(_PROFILE_COLUMNS - set(view.columns))
    if missing_columns:
        raise ModelingViewProfileError(
            "Modelling view is missing profile columns: " + ", ".join(missing_columns)
        )
    status_rows = view.groupBy("modeling_row_status").count().collect()
    status_by_name = {row.modeling_row_status: int(row["count"]) for row in status_rows}
    unexpected_statuses = set(status_by_name) - set(MODELING_ROW_STATUS_ORDER)
    if unexpected_statuses:
        raise ModelingViewProfileError(
            "Modelling view contains unexpected row statuses: "
            + ", ".join(sorted(str(status) for status in unexpected_statuses))
        )
    cycle_count = sum(status_by_name.values())
    if cycle_count <= 0:
        raise ModelingViewProfileError("Modelling-view profiling requires cycle rows")

    trainable = F.col("modeling_row_status") == STATUS_TRAINABLE
    excluded = F.col("modeling_row_status") == STATUS_EXCLUDED
    reasons = F.col("modeling_exclusion_reasons")
    invalid_row_semantics = view.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_VERSION))
        | (trainable & ((F.size(reasons) != 0) | F.col("prediction_timestamp").isNull()))
        | (excluded & (F.size(reasons) == 0))
    ).limit(1)
    if invalid_row_semantics.count():
        raise ModelingViewProfileError("Modelling-view row status semantics do not reconcile")

    reason_rows = (
        view.where(excluded)
        .select(F.explode("modeling_exclusion_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    reasons_by_name = {row.reason: int(row["count"]) for row in reason_rows}
    unexpected_reasons = set(reasons_by_name) - set(MODELING_EXCLUSION_REASON_ORDER)
    if unexpected_reasons:
        raise ModelingViewProfileError(
            "Modelling view contains unexpected exclusion reasons: "
            + ", ".join(sorted(str(reason) for reason in unexpected_reasons))
        )

    label_rows = view.where(trainable).groupBy("failure_horizon_status").count().collect()
    labels_by_name = {row.failure_horizon_status: int(row["count"]) for row in label_rows}
    unexpected_labels = set(labels_by_name) - set(MODELING_LABEL_STATUS_ORDER)
    if unexpected_labels:
        raise ModelingViewProfileError(
            "Trainable rows contain unexpected label statuses: "
            + ", ".join(sorted(str(status) for status in unexpected_labels))
        )
    if sum(labels_by_name.values()) != status_by_name.get(STATUS_TRAINABLE, 0):
        raise ModelingViewProfileError("Trainable labels do not reconcile with trainable rows")

    time_summary = (
        view.where(trainable)
        .agg(
            F.min("prediction_timestamp").alias("minimum_prediction_timestamp"),
            F.max("prediction_timestamp").alias("maximum_prediction_timestamp"),
        )
        .first()
    )
    if (
        time_summary.minimum_prediction_timestamp is None
        or time_summary.maximum_prediction_timestamp is None
    ):
        raise ModelingViewProfileError("Trainable rows require a prediction-time range")

    positive_rows = (
        view.where(trainable & (F.col("failure_horizon_status") == STATUS_POSITIVE))
        .groupBy("matched_failure_record_id")
        .agg(F.count(F.lit(1)).cast(LongType()).alias("cycle_count"))
        .collect()
    )
    positive_by_event = {
        row.matched_failure_record_id: int(row.cycle_count) for row in positive_rows
    }
    accepted_ids = {row.record_id for row in failure_rows}
    if not set(positive_by_event).issubset(accepted_ids):
        raise ModelingViewProfileError(
            "A trainable positive row references an unknown accepted failure event"
        )
    if sum(positive_by_event.values()) != labels_by_name.get(STATUS_POSITIVE, 0):
        raise ModelingViewProfileError(
            "Trainable positive labels do not reconcile with failure-event counts"
        )

    return ModelingViewSummary(
        cycle_count=cycle_count,
        earliest_trainable_prediction_timestamp=_timestamp_text(
            time_summary.minimum_prediction_timestamp
        ),
        latest_trainable_prediction_timestamp=_timestamp_text(
            time_summary.maximum_prediction_timestamp
        ),
        status_counts=tuple(
            ModelingRowStatusCount(status=status, cycle_count=status_by_name.get(status, 0))
            for status in MODELING_ROW_STATUS_ORDER
        ),
        exclusion_reason_counts=tuple(
            ModelingExclusionReasonCount(
                reason=reason,
                cycle_count=reasons_by_name.get(reason, 0),
            )
            for reason in MODELING_EXCLUSION_REASON_ORDER
        ),
        trainable_label_counts=tuple(
            ModelingLabelCount(status=status, cycle_count=labels_by_name.get(status, 0))
            for status in MODELING_LABEL_STATUS_ORDER
        ),
        failure_event_counts=tuple(
            ModelingFailureEventCount(
                failure_record_id=str(row.record_id),
                source_row=int(row.source_row),
                failure_start=_timestamp_text(row.failure_start),
                failure_end=_timestamp_text(row.failure_end),
                trainable_positive_cycle_count=positive_by_event.get(row.record_id, 0),
            )
            for row in failure_rows
        ),
    )


def build_full_source_modeling_view_inputs(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceModelingViewInputs:
    """Rebuild the full-source modelling view once for read-only downstream profiles."""

    telemetry_bronze = spark.read.format("delta").load(
        str(bronze_table_path(config, TELEMETRY_TABLE))
    )
    failure_bronze = spark.read.format("delta").load(str(bronze_table_path(config, FAILURE_TABLE)))
    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    snapshots = spark.read.format("delta").load(
        str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE))
    )
    telemetry = split_telemetry_by_quality(telemetry_bronze).accepted
    failures = split_failure_events_by_quality(failure_bronze).accepted
    try:
        (
            telemetry_count,
            observation_end,
            dataset_version,
            telemetry_source_sha256,
            telemetry_ingestion_batch_id,
        ) = _collect_observation_boundary(telemetry)
        failure_rows, failure_lineage = _collect_failure_events(failures)
    except FailureHorizonProfileError as exc:
        raise ModelingViewProfileError(str(exc)) from exc
    (
        failure_dataset_version,
        failure_source_sha256,
        failure_source_document_sha256,
        failure_ingestion_batch_id,
    ) = failure_lineage
    if dataset_version != config.dataset_version or failure_dataset_version != dataset_version:
        raise ModelingViewProfileError(
            "Telemetry, failure-event, and configured dataset versions must match"
        )

    horizons = assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        observation_end=observation_end,
    )
    view = build_modeling_view(
        snapshots,
        horizons,
        failure_dataset_version=failure_dataset_version,
        failure_source_sha256=failure_source_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
    )
    return FullSourceModelingViewInputs(
        view=view,
        failure_rows=failure_rows,
        label_observation_end=_timestamp_text(observation_end),
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
        failure_source_sha256=failure_source_sha256,
        failure_source_document_sha256=failure_source_document_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
        accepted_telemetry_record_count=telemetry_count,
    )


def profile_full_source_modeling_view(
    spark: SparkSession,
    config: RailPulseConfig,
) -> FullSourceModelingViewProfile:
    """Rebuild labels, join materialized snapshots, and profile without writing."""

    inputs = build_full_source_modeling_view_inputs(spark, config)
    summary = collect_modeling_view_summary(inputs.view, inputs.failure_rows)
    matched_event_count = sum(
        event.trainable_positive_cycle_count > 0 for event in summary.failure_event_counts
    )
    return FullSourceModelingViewProfile(
        profile_version=MODELING_VIEW_PROFILE_VERSION,
        modeling_view_version=MODELING_VIEW_VERSION,
        feature_snapshot_version=FEATURE_SNAPSHOT_VERSION,
        failure_horizon_version=FAILURE_HORIZON_VERSION,
        failure_horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        label_observation_end=inputs.label_observation_end,
        dataset_version=inputs.dataset_version,
        telemetry_source_sha256=inputs.telemetry_source_sha256,
        telemetry_ingestion_batch_id=inputs.telemetry_ingestion_batch_id,
        failure_source_sha256=inputs.failure_source_sha256,
        failure_source_document_sha256=inputs.failure_source_document_sha256,
        failure_ingestion_batch_id=inputs.failure_ingestion_batch_id,
        accepted_telemetry_record_count=inputs.accepted_telemetry_record_count,
        accepted_failure_event_count=len(inputs.failure_rows),
        matched_trainable_failure_event_count=matched_event_count,
        summary=summary,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source modelling-view profile and print JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-modeling-view-profile", master=args.master)
    try:
        profile = profile_full_source_modeling_view(spark, config)
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
