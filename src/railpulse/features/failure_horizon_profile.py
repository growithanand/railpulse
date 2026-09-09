"""Profile full-source cycle-to-failure horizons without writing data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from railpulse.config import RailPulseConfig, load_config
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
    assign_cycle_failure_horizons,
)
from railpulse.ingestion.bronze import FAILURE_TABLE, TELEMETRY_TABLE, bronze_table_path
from railpulse.spark import create_local_spark_session
from railpulse.validation.silver_failures import split_failure_events_by_quality
from railpulse.validation.silver_telemetry import split_telemetry_by_quality

FAILURE_HORIZON_PROFILE_VERSION = "cycle-failure-horizon-profile-v1"
FAILURE_HORIZON_STATUS_ORDER = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)


class FailureHorizonProfileError(RuntimeError):
    """Raised when a failure-horizon profile cannot be reconciled."""


@dataclass(frozen=True)
class FailureHorizonStatusCount:
    """Cycle count for one explicit failure-horizon status."""

    status: str
    cycle_count: int


@dataclass(frozen=True)
class FailureEventHorizonCount:
    """Positive-cycle count matched to one accepted failure event."""

    failure_record_id: str
    source_row: int
    failure_start: str
    failure_end: str
    positive_cycle_count: int


@dataclass(frozen=True)
class FullSourceFailureHorizonProfile:
    """Reconciled read-only profile of cycle failure-horizon labels."""

    profile_version: str
    horizon_version: str
    horizon_seconds: int
    label_observation_end: str
    dataset_version: str
    telemetry_source_sha256: str
    telemetry_ingestion_batch_id: str
    failure_source_sha256: str
    failure_source_document_sha256: str
    failure_ingestion_batch_id: str
    accepted_telemetry_record_count: int
    cycle_count: int
    accepted_failure_event_count: int
    matched_failure_event_count: int
    status_counts: tuple[FailureHorizonStatusCount, ...]
    failure_event_counts: tuple[FailureEventHorizonCount, ...]


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="seconds")


def _collect_observation_boundary(
    telemetry: DataFrame,
) -> tuple[int, datetime, str, str, str]:
    required_columns = {
        "event_timestamp",
        "dataset_version",
        "source_sha256",
        "ingestion_batch_id",
    }
    missing_columns = sorted(required_columns - set(telemetry.columns))
    if missing_columns:
        raise FailureHorizonProfileError(
            "Accepted telemetry is missing profile columns: " + ", ".join(missing_columns)
        )
    summary = telemetry.agg(
        F.count(F.lit(1)).cast(LongType()).alias("record_count"),
        F.max("event_timestamp").alias("observation_end"),
        F.min("dataset_version").alias("minimum_dataset_version"),
        F.max("dataset_version").alias("maximum_dataset_version"),
        F.min("source_sha256").alias("minimum_source_sha256"),
        F.max("source_sha256").alias("maximum_source_sha256"),
        F.min("ingestion_batch_id").alias("minimum_ingestion_batch_id"),
        F.max("ingestion_batch_id").alias("maximum_ingestion_batch_id"),
    ).first()
    record_count = int(summary.record_count)
    if record_count <= 0 or summary.observation_end is None:
        raise FailureHorizonProfileError(
            "Failure-horizon profiling requires accepted telemetry timestamps"
        )
    lineage_pairs = (
        (summary.minimum_dataset_version, summary.maximum_dataset_version),
        (summary.minimum_source_sha256, summary.maximum_source_sha256),
        (summary.minimum_ingestion_batch_id, summary.maximum_ingestion_batch_id),
    )
    if any(minimum is None or minimum != maximum for minimum, maximum in lineage_pairs):
        raise FailureHorizonProfileError(
            "Failure-horizon profiling requires one complete telemetry lineage"
        )
    return (
        record_count,
        summary.observation_end,
        str(summary.minimum_dataset_version),
        str(summary.minimum_source_sha256),
        str(summary.minimum_ingestion_batch_id),
    )


def _collect_failure_events(failures: DataFrame) -> tuple[tuple[Row, ...], tuple[str, ...]]:
    required_columns = {
        "record_id",
        "source_row",
        "failure_start",
        "failure_end",
        "dataset_version",
        "source_sha256",
        "source_document_sha256",
        "ingestion_batch_id",
    }
    missing_columns = sorted(required_columns - set(failures.columns))
    if missing_columns:
        raise FailureHorizonProfileError(
            "Accepted failure events are missing profile columns: " + ", ".join(missing_columns)
        )
    rows = tuple(
        failures.select(*sorted(required_columns)).orderBy("source_row", "record_id").collect()
    )
    if not rows:
        raise FailureHorizonProfileError(
            "Failure-horizon profiling requires accepted failure events"
        )
    record_ids = [row.record_id for row in rows]
    source_rows = [row.source_row for row in rows]
    if len(set(record_ids)) != len(record_ids) or any(not record_id for record_id in record_ids):
        raise FailureHorizonProfileError("Accepted failure events require unique record IDs")
    if len(set(source_rows)) != len(source_rows) or any(
        source_row is None for source_row in source_rows
    ):
        raise FailureHorizonProfileError("Accepted failure events require unique source rows")
    lineage_columns = (
        "dataset_version",
        "source_sha256",
        "source_document_sha256",
        "ingestion_batch_id",
    )
    lineages = {tuple(row[column] for column in lineage_columns) for row in rows}
    if len(lineages) != 1:
        raise FailureHorizonProfileError(
            "Failure-horizon profiling requires one failure-event lineage"
        )
    lineage = next(iter(lineages))
    if any(not isinstance(value, str) or not value.strip() for value in lineage):
        raise FailureHorizonProfileError(
            "Failure-horizon profiling requires complete failure-event lineage"
        )
    return rows, lineage


def collect_failure_horizon_profile(
    cycles: DataFrame,
    failures: DataFrame,
    telemetry: DataFrame,
    *,
    horizon_seconds: int = DEFAULT_FAILURE_HORIZON_SECONDS,
) -> FullSourceFailureHorizonProfile:
    """Apply and reconcile failure horizons against accepted source snapshots."""

    (
        telemetry_count,
        observation_end,
        dataset_version,
        telemetry_source_sha256,
        telemetry_ingestion_batch_id,
    ) = _collect_observation_boundary(telemetry)
    failure_rows, failure_lineage = _collect_failure_events(failures)
    (
        failure_dataset_version,
        failure_source_sha256,
        failure_source_document_sha256,
        failure_ingestion_batch_id,
    ) = failure_lineage
    if failure_dataset_version != dataset_version:
        raise FailureHorizonProfileError(
            "Telemetry and failure-event dataset versions do not match"
        )
    labeled = assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=horizon_seconds,
        observation_end=observation_end,
    )

    status_rows = labeled.groupBy("failure_horizon_status").count().collect()
    status_counts_by_name = {row.failure_horizon_status: int(row["count"]) for row in status_rows}
    unexpected_statuses = set(status_counts_by_name) - set(FAILURE_HORIZON_STATUS_ORDER)
    if unexpected_statuses:
        raise FailureHorizonProfileError(
            "Failure-horizon output contains unexpected statuses: "
            + ", ".join(sorted(str(status) for status in unexpected_statuses))
        )
    cycle_count = sum(status_counts_by_name.values())
    if cycle_count <= 0:
        raise FailureHorizonProfileError("Failure-horizon profiling requires Gold cycles")

    positive = F.col("failure_horizon_status") == STATUS_POSITIVE
    negative = F.col("failure_horizon_status") == STATUS_NEGATIVE
    unknown = ~F.col("failure_horizon_status").isin(STATUS_POSITIVE, STATUS_NEGATIVE)
    invalid_semantics = labeled.where(
        (
            positive
            & (
                ~F.col("failure_within_horizon").eqNullSafe(F.lit(True))
                | F.col("matched_failure_record_id").isNull()
                | F.col("seconds_to_failure").isNull()
                | (F.col("seconds_to_failure") <= 0)
                | (F.col("seconds_to_failure") > F.col("failure_horizon_seconds"))
            )
        )
        | (
            negative
            & (
                ~F.col("failure_within_horizon").eqNullSafe(F.lit(False))
                | F.col("matched_failure_record_id").isNotNull()
                | F.col("seconds_to_failure").isNotNull()
            )
        )
        | (
            unknown
            & (
                F.col("failure_within_horizon").isNotNull()
                | F.col("matched_failure_record_id").isNotNull()
                | F.col("seconds_to_failure").isNotNull()
            )
        )
    ).limit(1)
    if invalid_semantics.count():
        raise FailureHorizonProfileError("Failure-horizon target semantics do not reconcile")

    positive_rows = (
        labeled.where(positive)
        .groupBy("matched_failure_record_id")
        .agg(F.count(F.lit(1)).cast(LongType()).alias("positive_cycle_count"))
        .collect()
    )
    positive_counts = {
        row.matched_failure_record_id: int(row.positive_cycle_count) for row in positive_rows
    }
    accepted_record_ids = {row.record_id for row in failure_rows}
    if not set(positive_counts).issubset(accepted_record_ids):
        raise FailureHorizonProfileError(
            "A positive horizon references an unknown accepted failure event"
        )
    if sum(positive_counts.values()) != status_counts_by_name.get(STATUS_POSITIVE, 0):
        raise FailureHorizonProfileError("Positive cycle and failure-event counts do not reconcile")

    status_counts = tuple(
        FailureHorizonStatusCount(
            status=status,
            cycle_count=status_counts_by_name.get(status, 0),
        )
        for status in FAILURE_HORIZON_STATUS_ORDER
    )
    event_counts = tuple(
        FailureEventHorizonCount(
            failure_record_id=str(row.record_id),
            source_row=int(row.source_row),
            failure_start=_timestamp_text(row.failure_start),
            failure_end=_timestamp_text(row.failure_end),
            positive_cycle_count=positive_counts.get(row.record_id, 0),
        )
        for row in failure_rows
    )
    return FullSourceFailureHorizonProfile(
        profile_version=FAILURE_HORIZON_PROFILE_VERSION,
        horizon_version=FAILURE_HORIZON_VERSION,
        horizon_seconds=horizon_seconds,
        label_observation_end=_timestamp_text(observation_end),
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
        failure_source_sha256=failure_source_sha256,
        failure_source_document_sha256=failure_source_document_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
        accepted_telemetry_record_count=telemetry_count,
        cycle_count=cycle_count,
        accepted_failure_event_count=len(failure_rows),
        matched_failure_event_count=sum(event.positive_cycle_count > 0 for event in event_counts),
        status_counts=status_counts,
        failure_event_counts=event_counts,
    )


def profile_full_source_failure_horizons(
    spark: SparkSession,
    config: RailPulseConfig,
    *,
    horizon_seconds: int = DEFAULT_FAILURE_HORIZON_SECONDS,
) -> FullSourceFailureHorizonProfile:
    """Rebuild accepted Silver views and profile materialized Gold cycles without writing."""

    telemetry_bronze = spark.read.format("delta").load(
        str(bronze_table_path(config, TELEMETRY_TABLE))
    )
    failure_bronze = spark.read.format("delta").load(str(bronze_table_path(config, FAILURE_TABLE)))
    cycles = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    telemetry_split = split_telemetry_by_quality(telemetry_bronze)
    failure_split = split_failure_events_by_quality(failure_bronze)
    profile = collect_failure_horizon_profile(
        cycles,
        failure_split.accepted,
        telemetry_split.accepted,
        horizon_seconds=horizon_seconds,
    )
    if profile.dataset_version != config.dataset_version:
        raise FailureHorizonProfileError(
            "Profile dataset version does not match the configured dataset version"
        )
    return profile


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--master", default="local[4]")
    parser.add_argument(
        "--horizon-seconds",
        type=int,
        default=DEFAULT_FAILURE_HORIZON_SECONDS,
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only full-source horizon profile and print deterministic JSON."""

    args = _parse_args(argv)
    config = load_config(args.config, project_root=args.project_root)
    spark = create_local_spark_session("railpulse-failure-horizon-profile", master=args.master)
    try:
        profile = profile_full_source_failure_horizons(
            spark,
            config,
            horizon_seconds=args.horizon_seconds,
        )
    finally:
        spark.stop()
    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
