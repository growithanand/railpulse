"""Persist validated telemetry to separate idempotent Silver Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig
from railpulse.ingestion.bronze import RECORD_ID_FIELD
from railpulse.validation.silver_telemetry import (
    REJECTION_REASONS_FIELD,
    TelemetryQualitySplit,
)

TELEMETRY_ACCEPTED_TABLE = "telemetry_accepted"
TELEMETRY_QUARANTINE_TABLE = "telemetry_quarantine"


class SilverTelemetryPersistenceError(RuntimeError):
    """Raised when validated telemetry cannot be reconciled or persisted safely."""


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


def silver_table_path(config: RailPulseConfig, table_name: str) -> Path:
    """Resolve a local path corresponding to a logical Silver table."""

    return config.paths.delta / config.schemas.silver / table_name


def _assert_validated_records_are_safe(frame: DataFrame) -> int:
    required_columns = {RECORD_ID_FIELD, REJECTION_REASONS_FIELD}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise SilverTelemetryPersistenceError(
            "Validated telemetry is missing persistence columns: " + ", ".join(missing_columns)
        )

    cached = frame.select(*sorted(required_columns)).persist(StorageLevel.DISK_ONLY)
    try:
        source_count = cached.count()
        if source_count == 0:
            raise SilverTelemetryPersistenceError("Validated telemetry contains no records")

        missing_record_id = cached.where(
            F.col(RECORD_ID_FIELD).isNull() | (F.length(F.col(RECORD_ID_FIELD)) == 0)
        ).limit(1)
        if missing_record_id.count():
            raise SilverTelemetryPersistenceError(
                "Validated telemetry contains a missing record_id"
            )

        missing_reasons = cached.where(F.col(REJECTION_REASONS_FIELD).isNull()).limit(1)
        if missing_reasons.count():
            raise SilverTelemetryPersistenceError(
                "Validated telemetry contains null rejection_reasons"
            )

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
            raise SilverTelemetryPersistenceError(
                f"Validated telemetry contains duplicate record_id {record[RECORD_ID_FIELD]} "
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
                        f"target.{RECORD_ID_FIELD} = source.{RECORD_ID_FIELD}",
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
            cached.select(RECORD_ID_FIELD)
            .join(target.select(RECORD_ID_FIELD), on=RECORD_ID_FIELD, how="left_anti")
            .limit(1)
            .count()
        )
        if inserted_count < 0 or inserted_count > source_count or unmatched_count:
            raise SilverTelemetryPersistenceError(
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


def persist_telemetry_quality_split(
    split: TelemetryQualitySplit,
    config: RailPulseConfig,
) -> TelemetryPersistenceResult:
    """Persist one complete quality split and reconcile both Silver outputs."""

    validated = split.all_records.persist(StorageLevel.DISK_ONLY)
    try:
        total_count = _assert_validated_records_are_safe(validated)
        reason_count = F.size(F.col(REJECTION_REASONS_FIELD))
        accepted = validated.where(reason_count == 0)
        quarantined = validated.where(reason_count > 0)

        accepted_result = _merge_silver_records(
            accepted,
            silver_table_path(config, TELEMETRY_ACCEPTED_TABLE),
            table_name=f"{config.schemas.silver}.{TELEMETRY_ACCEPTED_TABLE}",
        )
        quarantined_result = _merge_silver_records(
            quarantined,
            silver_table_path(config, TELEMETRY_QUARANTINE_TABLE),
            table_name=f"{config.schemas.silver}.{TELEMETRY_QUARANTINE_TABLE}",
        )

        classified_count = (
            accepted_result.source_record_count + quarantined_result.source_record_count
        )
        if classified_count != total_count:
            raise SilverTelemetryPersistenceError(
                "Silver telemetry outputs do not reconcile: "
                f"total={total_count}, classified={classified_count}"
            )
        return TelemetryPersistenceResult(
            total_record_count=total_count,
            accepted=accepted_result,
            quarantined=quarantined_result,
        )
    finally:
        validated.unpersist()
