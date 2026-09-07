"""Persist validated records to separate idempotent Silver Delta tables."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from railpulse.config import RailPulseConfig
from railpulse.ingestion.bronze import RECORD_ID_FIELD
from railpulse.validation.silver_failures import FailureQualitySplit
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualityMetrics,
    TelemetryQualitySplit,
    collect_telemetry_quality_metrics,
)

TELEMETRY_ACCEPTED_TABLE = "telemetry_accepted"
TELEMETRY_QUARANTINE_TABLE = "telemetry_quarantine"
FAILURE_ACCEPTED_TABLE = "failure_events_accepted"
FAILURE_QUARANTINE_TABLE = "failure_events_quarantine"
TELEMETRY_QUALITY_TABLE = "telemetry_quality_metrics"
TELEMETRY_VALIDATION_VERSION = "telemetry-validation-v2"
QUALITY_BATCH_ID_FIELD = "quality_batch_id"
TELEMETRY_LINEAGE_COLUMNS = ("dataset_version", "source_sha256", "ingestion_batch_id")


class SilverPersistenceError(RuntimeError):
    """Raised when validated records cannot be reconciled or persisted safely."""


class SilverTelemetryPersistenceError(SilverPersistenceError):
    """Raised when validated telemetry cannot be reconciled or persisted safely."""


class SilverFailurePersistenceError(SilverPersistenceError):
    """Raised when validated failure events cannot be reconciled or persisted safely."""


@dataclass(frozen=True)
class SilverTableWriteResult:
    """Count reconciliation for one path-backed Silver table write."""

    table_name: str
    target_path: str
    source_record_count: int
    inserted_record_count: int
    target_record_count_before: int
    target_record_count_after: int


@dataclass(frozen=True)
class TelemetryPersistenceResult:
    """Reconciled write results for accepted and quarantined telemetry."""

    total_record_count: int
    accepted: SilverTableWriteResult
    quarantined: SilverTableWriteResult


@dataclass(frozen=True)
class FailurePersistenceResult:
    """Reconciled write results for accepted and quarantined failure events."""

    total_record_count: int
    accepted: SilverTableWriteResult
    quarantined: SilverTableWriteResult


@dataclass(frozen=True)
class TelemetryQualityPersistenceResult:
    """Persisted telemetry quality metrics and their stable identity."""

    quality_batch_id: str
    validation_version: str
    metrics: TelemetryQualityMetrics
    write: SilverTableWriteResult


def silver_table_path(config: RailPulseConfig, table_name: str) -> Path:
    """Resolve a local path corresponding to a logical Silver table."""

    return config.paths.delta / config.schemas.silver / table_name


def telemetry_quality_batch_id(
    *,
    dataset_version: str,
    source_sha256: str,
    ingestion_batch_id: str,
    validation_version: str = TELEMETRY_VALIDATION_VERSION,
) -> str:
    """Return a stable identity for one source batch and validation contract."""

    material = "|".join(
        (
            TELEMETRY_QUALITY_TABLE,
            validation_version,
            dataset_version,
            source_sha256.lower(),
            ingestion_batch_id,
        )
    )
    return hashlib.sha256(material.encode()).hexdigest()


def _assert_validated_records_are_safe(
    frame: DataFrame,
    *,
    record_label: str,
    error_type: type[SilverPersistenceError],
) -> int:
    required_columns = {RECORD_ID_FIELD, REJECTION_REASONS_FIELD}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise error_type(
            f"{record_label} is missing persistence columns: " + ", ".join(missing_columns)
        )

    cached = frame.select(*sorted(required_columns)).persist(StorageLevel.DISK_ONLY)
    try:
        source_count = cached.count()
        if source_count == 0:
            raise error_type(f"{record_label} contains no records")

        missing_record_id = cached.where(
            F.col(RECORD_ID_FIELD).isNull() | (F.length(F.col(RECORD_ID_FIELD)) == 0)
        ).limit(1)
        if missing_record_id.count():
            raise error_type(f"{record_label} contains a missing record_id")

        missing_reasons = cached.where(F.col(REJECTION_REASONS_FIELD).isNull()).limit(1)
        if missing_reasons.count():
            raise error_type(f"{record_label} contains null rejection_reasons")

        duplicate = (
            cached.groupBy(RECORD_ID_FIELD)
            .count()
            .where(F.col("count") > 1)
            .select(RECORD_ID_FIELD, "count")
            .limit(1)
            .collect()
        )
        if duplicate:
            record = duplicate[0]
            raise error_type(
                f"{record_label} contains duplicate record_id {record[RECORD_ID_FIELD]} "
                f"({record['count']} records)"
            )
        return source_count
    finally:
        cached.unpersist()


def _merge_silver_records(
    frame: DataFrame,
    target_path: Path,
    *,
    table_name: str,
    error_type: type[SilverPersistenceError],
    key_field: str = RECORD_ID_FIELD,
) -> SilverTableWriteResult:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path_string = str(target_path)
    cached = frame.persist(StorageLevel.DISK_ONLY)

    try:
        source_count = cached.count()
        target_exists = DeltaTable.isDeltaTable(frame.sparkSession, target_path_string)
        before_count = (
            frame.sparkSession.read.format("delta").load(target_path_string).count()
            if target_exists
            else 0
        )

        if target_exists:
            if source_count:
                (
                    DeltaTable.forPath(frame.sparkSession, target_path_string)
                    .alias("target")
                    .merge(
                        cached.alias("source"),
                        f"target.{key_field} = source.{key_field}",
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
        else:
            cached.write.format("delta").mode("errorifexists").save(target_path_string)

        target = frame.sparkSession.read.format("delta").load(target_path_string)
        after_count = target.count()
        inserted_count = after_count - before_count
        unmatched_count = (
            cached.select(key_field)
            .join(target.select(key_field), on=key_field, how="left_anti")
            .limit(1)
            .count()
        )
        if inserted_count < 0 or inserted_count > source_count or unmatched_count:
            raise error_type(
                f"{table_name} reconciliation failed: source={source_count}, "
                f"unmatched={unmatched_count}, before={before_count}, after={after_count}"
            )

        return SilverTableWriteResult(
            table_name=table_name,
            target_path=target_path_string,
            source_record_count=source_count,
            inserted_record_count=inserted_count,
            target_record_count_before=before_count,
            target_record_count_after=after_count,
        )
    finally:
        cached.unpersist()


def _persist_quality_records(
    all_records: DataFrame,
    config: RailPulseConfig,
    *,
    record_label: str,
    accepted_table: str,
    quarantine_table: str,
    error_type: type[SilverPersistenceError],
) -> tuple[int, SilverTableWriteResult, SilverTableWriteResult]:
    """Persist one complete validated frame and reconcile its quality outputs."""

    validated = all_records.persist(StorageLevel.DISK_ONLY)
    try:
        total_count = _assert_validated_records_are_safe(
            validated,
            record_label=record_label,
            error_type=error_type,
        )
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        accepted = validated.where(reason_count == 0)
        quarantined = validated.where(reason_count > 0)

        accepted_result = _merge_silver_records(
            accepted,
            silver_table_path(config, accepted_table),
            table_name=f"{config.schemas.silver}.{accepted_table}",
            error_type=error_type,
        )
        quarantined_result = _merge_silver_records(
            quarantined,
            silver_table_path(config, quarantine_table),
            table_name=f"{config.schemas.silver}.{quarantine_table}",
            error_type=error_type,
        )

        classified_count = (
            accepted_result.source_record_count + quarantined_result.source_record_count
        )
        if classified_count != total_count:
            raise error_type(
                f"{record_label} does not reconcile: "
                f"total={total_count}, classified={classified_count}"
            )
        return total_count, accepted_result, quarantined_result
    finally:
        validated.unpersist()


def persist_telemetry_quality_split(
    split: TelemetryQualitySplit,
    config: RailPulseConfig,
) -> TelemetryPersistenceResult:
    """Persist one complete telemetry quality split and reconcile both outputs."""

    total_count, accepted_result, quarantined_result = _persist_quality_records(
        split.all_records,
        config,
        record_label="Validated telemetry",
        accepted_table=TELEMETRY_ACCEPTED_TABLE,
        quarantine_table=TELEMETRY_QUARANTINE_TABLE,
        error_type=SilverTelemetryPersistenceError,
    )
    return TelemetryPersistenceResult(
        total_record_count=total_count,
        accepted=accepted_result,
        quarantined=quarantined_result,
    )


def persist_failure_quality_split(
    split: FailureQualitySplit,
    config: RailPulseConfig,
) -> FailurePersistenceResult:
    """Persist one complete failure-event quality split and reconcile both outputs."""

    total_count, accepted_result, quarantined_result = _persist_quality_records(
        split.all_records,
        config,
        record_label="Validated failure-event data",
        accepted_table=FAILURE_ACCEPTED_TABLE,
        quarantine_table=FAILURE_QUARANTINE_TABLE,
        error_type=SilverFailurePersistenceError,
    )
    return FailurePersistenceResult(
        total_record_count=total_count,
        accepted=accepted_result,
        quarantined=quarantined_result,
    )


def _single_telemetry_lineage(frame: DataFrame) -> tuple[str, str, str]:
    missing_columns = sorted(set(TELEMETRY_LINEAGE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryPersistenceError(
            "Validated telemetry is missing quality-lineage columns: " + ", ".join(missing_columns)
        )

    lineage_rows = frame.select(*TELEMETRY_LINEAGE_COLUMNS).distinct().limit(2).collect()
    if len(lineage_rows) != 1:
        raise SilverTelemetryPersistenceError(
            "Telemetry quality metrics require exactly one source lineage"
        )
    lineage = tuple(lineage_rows[0][column] for column in TELEMETRY_LINEAGE_COLUMNS)
    if any(not isinstance(value, str) or not value.strip() for value in lineage):
        raise SilverTelemetryPersistenceError(
            "Telemetry quality metrics require complete source lineage"
        )
    return lineage


def _telemetry_quality_schema() -> StructType:
    return StructType(
        [
            StructField(QUALITY_BATCH_ID_FIELD, StringType(), nullable=False),
            StructField("validation_version", StringType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
            StructField("source_sha256", StringType(), nullable=False),
            StructField("ingestion_batch_id", StringType(), nullable=False),
            StructField("total_record_count", LongType(), nullable=False),
            StructField("accepted_record_count", LongType(), nullable=False),
            StructField("quarantined_record_count", LongType(), nullable=False),
            StructField("forward_gap_count", LongType(), nullable=False),
            StructField(
                "rejection_reason_counts",
                MapType(StringType(), LongType(), valueContainsNull=False),
                nullable=False,
            ),
        ]
    )


def persist_telemetry_quality_metrics(
    split: TelemetryQualitySplit,
    config: RailPulseConfig,
) -> TelemetryQualityPersistenceResult:
    """Collect and persist one deterministic telemetry quality summary."""

    dataset_version, source_sha256, ingestion_batch_id = _single_telemetry_lineage(
        split.all_records
    )
    metrics = collect_telemetry_quality_metrics(split)
    quality_batch_id = telemetry_quality_batch_id(
        dataset_version=dataset_version,
        source_sha256=source_sha256,
        ingestion_batch_id=ingestion_batch_id,
    )
    row = {
        QUALITY_BATCH_ID_FIELD: quality_batch_id,
        "validation_version": TELEMETRY_VALIDATION_VERSION,
        "dataset_version": dataset_version,
        "source_sha256": source_sha256,
        "ingestion_batch_id": ingestion_batch_id,
        "total_record_count": metrics.total_record_count,
        "accepted_record_count": metrics.accepted_record_count,
        "quarantined_record_count": metrics.quarantined_record_count,
        "forward_gap_count": metrics.forward_gap_count,
        "rejection_reason_counts": metrics.rejection_reason_counts,
    }
    summary = split.all_records.sparkSession.createDataFrame(
        [row],
        schema=_telemetry_quality_schema(),
    )
    write = _merge_silver_records(
        summary,
        silver_table_path(config, TELEMETRY_QUALITY_TABLE),
        table_name=f"{config.schemas.silver}.{TELEMETRY_QUALITY_TABLE}",
        error_type=SilverTelemetryPersistenceError,
        key_field=QUALITY_BATCH_ID_FIELD,
    )
    return TelemetryQualityPersistenceResult(
        quality_batch_id=quality_batch_id,
        validation_version=TELEMETRY_VALIDATION_VERSION,
        metrics=metrics,
        write=write,
    )
