"""Persist immutable Gold feature snapshots with idempotent Delta semantics."""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig
from railpulse.features.cycle_storage import gold_table_path
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_VERSION
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_KEY_COLUMN,
    FEATURE_SNAPSHOT_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_VERSION


class FeatureSnapshotPersistenceError(RuntimeError):
    """Raised when Gold feature snapshots cannot be persisted safely."""


@dataclass(frozen=True)
class FeatureSnapshotWriteResult:
    """Logical changes and count reconciliation for one snapshot merge."""

    table_name: str
    target_path: str
    source_snapshot_count: int
    inserted_snapshot_count: int
    unchanged_snapshot_count: int
    target_snapshot_count_before: int
    target_snapshot_count_after: int


def _all_equal(columns: tuple[str, ...]) -> Column:
    condition = F.lit(True)
    for column_name in columns:
        condition = condition & F.col(f"source.{column_name}").eqNullSafe(
            F.col(f"target.{column_name}")
        )
    return condition


def _assert_compatible_schemas(source: DataFrame, target: DataFrame) -> None:
    target_columns = set(target.columns)
    missing_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - target_columns)
    if missing_columns:
        raise FeatureSnapshotPersistenceError(
            "Gold feature-snapshot target is missing contract columns: "
            + ", ".join(missing_columns)
        )

    source_types = {field.name: field.dataType for field in source.schema}
    target_types = {field.name: field.dataType for field in target.schema}
    mismatches = [
        column_name
        for column_name in FEATURE_SNAPSHOT_COLUMNS
        if source_types[column_name] != target_types[column_name]
    ]
    if mismatches:
        raise FeatureSnapshotPersistenceError(
            "Gold feature-snapshot source and target types differ for: " + ", ".join(mismatches)
        )


def _assert_snapshot_rows_are_valid(frame: DataFrame, *, label: str) -> int:
    missing_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise FeatureSnapshotPersistenceError(
            f"{label} is missing feature-snapshot columns: " + ", ".join(missing_columns)
        )

    row_count = frame.count()
    invalid_key = frame.where(
        F.col(FEATURE_SNAPSHOT_KEY_COLUMN).isNull() | (F.length(FEATURE_SNAPSHOT_KEY_COLUMN) == 0)
    ).limit(1)
    if invalid_key.count():
        raise FeatureSnapshotPersistenceError(f"{label} contains an invalid snapshot key")

    duplicate = (
        frame.groupBy(FEATURE_SNAPSHOT_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(FEATURE_SNAPSHOT_KEY_COLUMN, "count")
        .limit(1)
        .collect()
    )
    if duplicate:
        row = duplicate[0]
        raise FeatureSnapshotPersistenceError(
            f"{label} contains duplicate {FEATURE_SNAPSHOT_KEY_COLUMN} {row[0]} "
            f"({row['count']} records)"
        )

    invalid_contract = frame.where(
        F.col("feature_snapshot_version").isNull()
        | (F.col("feature_snapshot_version") != FEATURE_SNAPSHOT_VERSION)
        | F.col("motor_current_15m_feature_version").isNull()
        | (F.col("motor_current_15m_feature_version") != MOTOR_CURRENT_FEATURE_VERSION)
        | F.col("motor_current_eligibility_version").isNull()
        | (F.col("motor_current_eligibility_version") != MOTOR_CURRENT_ELIGIBILITY_VERSION)
    ).limit(1)
    if invalid_contract.count():
        raise FeatureSnapshotPersistenceError(f"{label} contains unsupported contract versions")

    invalid_lineage = F.lit(False)
    for column_name in FEATURE_SNAPSHOT_LINEAGE_COLUMNS[1:]:
        invalid_lineage = (
            invalid_lineage
            | F.col(column_name).isNull()
            | (F.length(F.trim(F.col(column_name))) == 0)
        )
    if frame.where(invalid_lineage).limit(1).count():
        raise FeatureSnapshotPersistenceError(f"{label} contains incomplete source lineage")
    return row_count


def persist_feature_snapshots(
    snapshots: DataFrame,
    config: RailPulseConfig,
) -> FeatureSnapshotWriteResult:
    """Insert new snapshot keys and reject changes to existing immutable rows."""

    missing_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - set(snapshots.columns))
    if missing_columns:
        raise FeatureSnapshotPersistenceError(
            "Feature snapshots are missing Gold persistence columns: " + ", ".join(missing_columns)
        )

    target_path = gold_table_path(config, FEATURE_SNAPSHOTS_TABLE)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path_string = str(target_path)
    source = snapshots.select(*FEATURE_SNAPSHOT_COLUMNS).persist(StorageLevel.DISK_ONLY)
    matched = None
    try:
        source_count = _assert_snapshot_rows_are_valid(source, label="Feature-snapshot source")
        target_exists = DeltaTable.isDeltaTable(snapshots.sparkSession, target_path_string)
        before_count = 0
        inserted_count = source_count

        if target_exists:
            target_frame = snapshots.sparkSession.read.format("delta").load(target_path_string)
            _assert_compatible_schemas(source, target_frame)
            target = target_frame.select(*FEATURE_SNAPSHOT_COLUMNS).persist(StorageLevel.DISK_ONLY)
            try:
                before_count = _assert_snapshot_rows_are_valid(
                    target, label="Gold feature-snapshot target"
                )
                matched = (
                    source.alias("source")
                    .join(
                        target.alias("target"),
                        F.col(f"source.{FEATURE_SNAPSHOT_KEY_COLUMN}")
                        == F.col(f"target.{FEATURE_SNAPSHOT_KEY_COLUMN}"),
                        how="inner",
                    )
                    .persist(StorageLevel.DISK_ONLY)
                )
                conflict = (
                    matched.where(~_all_equal(FEATURE_SNAPSHOT_COLUMNS))
                    .select(F.col(f"source.{FEATURE_SNAPSHOT_KEY_COLUMN}").alias("key"))
                    .limit(1)
                    .first()
                )
                if conflict is not None:
                    raise FeatureSnapshotPersistenceError(
                        f"Feature snapshot {conflict.key} conflicts with its immutable target row"
                    )
                inserted_count = source_count - matched.count()
            finally:
                target.unpersist()

        if target_exists:
            if source_count:
                (
                    DeltaTable.forPath(snapshots.sparkSession, target_path_string)
                    .alias("target")
                    .merge(
                        source.alias("source"),
                        f"target.{FEATURE_SNAPSHOT_KEY_COLUMN} = "
                        f"source.{FEATURE_SNAPSHOT_KEY_COLUMN}",
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
        else:
            source.write.format("delta").mode("errorifexists").save(target_path_string)

        final_target = snapshots.sparkSession.read.format("delta").load(target_path_string)
        after_count = final_target.count()
        unreconciled = (
            source.alias("source")
            .join(
                final_target.alias("target"),
                F.col(f"source.{FEATURE_SNAPSHOT_KEY_COLUMN}")
                == F.col(f"target.{FEATURE_SNAPSHOT_KEY_COLUMN}"),
                how="left",
            )
            .where(
                F.col(f"target.{FEATURE_SNAPSHOT_KEY_COLUMN}").isNull()
                | ~_all_equal(FEATURE_SNAPSHOT_COLUMNS)
            )
            .limit(1)
            .count()
        )
        if after_count != before_count + inserted_count or unreconciled:
            raise FeatureSnapshotPersistenceError(
                f"gold.{FEATURE_SNAPSHOTS_TABLE} reconciliation failed: "
                f"source={source_count}, inserted={inserted_count}, "
                f"before={before_count}, after={after_count}"
            )

        return FeatureSnapshotWriteResult(
            table_name=f"{config.schemas.gold}.{FEATURE_SNAPSHOTS_TABLE}",
            target_path=target_path_string,
            source_snapshot_count=source_count,
            inserted_snapshot_count=inserted_count,
            unchanged_snapshot_count=source_count - inserted_count,
            target_snapshot_count_before=before_count,
            target_snapshot_count_after=after_count,
        )
    finally:
        if matched is not None:
            matched.unpersist()
        source.unpersist()
