"""Persist loaded-cycle aggregates with monotonic Gold update semantics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig
from railpulse.features.cycles import CYCLE_AGGREGATION_COLUMNS, LOADED_CYCLE_ID_VERSION

LOADED_CYCLES_TABLE = "loaded_cycles"
GOLD_CYCLE_COLUMNS = (
    "loaded_cycle_id",
    "loaded_cycle_start_record_id",
    "loaded_cycle_start_type",
    *CYCLE_AGGREGATION_COLUMNS,
)
IMMUTABLE_CYCLE_COLUMNS = (
    "loaded_cycle_id",
    "loaded_cycle_start_record_id",
    "loaded_cycle_start_type",
    "loaded_cycle_start_timestamp",
)


class GoldCyclePersistenceError(RuntimeError):
    """Raised when loaded-cycle aggregates cannot be persisted safely."""


@dataclass(frozen=True)
class GoldCycleWriteResult:
    """Logical changes and count reconciliation for one Gold cycle merge."""

    table_name: str
    target_path: str
    source_cycle_count: int
    inserted_cycle_count: int
    updated_cycle_count: int
    unchanged_cycle_count: int
    target_cycle_count_before: int
    target_cycle_count_after: int


def gold_table_path(config: RailPulseConfig, table_name: str) -> Path:
    """Resolve a local path corresponding to a logical Gold table."""

    return config.paths.delta / config.schemas.gold / table_name


def _all_equal(columns: tuple[str, ...]) -> Column:
    condition = F.lit(True)
    for column_name in columns:
        condition = condition & F.col(f"source.{column_name}").eqNullSafe(
            F.col(f"target.{column_name}")
        )
    return condition


def _assert_cycle_rows_are_valid(frame: DataFrame, *, label: str) -> int:
    missing_columns = sorted(set(GOLD_CYCLE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise GoldCyclePersistenceError(
            f"{label} is missing Gold cycle columns: " + ", ".join(missing_columns)
        )

    source_count = frame.count()
    duplicate = (
        frame.groupBy("loaded_cycle_id")
        .count()
        .where(F.col("count") > 1)
        .select("loaded_cycle_id", "count")
        .limit(1)
        .collect()
    )
    if duplicate:
        row = duplicate[0]
        raise GoldCyclePersistenceError(
            f"{label} contains duplicate loaded_cycle_id {row.loaded_cycle_id} "
            f"({row['count']} records)"
        )

    expected_cycle_id = F.sha2(
        F.concat_ws(
            "|",
            F.lit(LOADED_CYCLE_ID_VERSION),
            F.col("loaded_cycle_start_record_id"),
        ),
        256,
    )
    invalid_identity = frame.where(
        F.col("loaded_cycle_id").isNull()
        | (F.length("loaded_cycle_id") == 0)
        | F.col("loaded_cycle_start_record_id").isNull()
        | (F.length("loaded_cycle_start_record_id") == 0)
        | ~F.col("loaded_cycle_id").eqNullSafe(expected_cycle_id)
    ).limit(1)
    if invalid_identity.count():
        raise GoldCyclePersistenceError(
            f"{label} contains an invalid deterministic loaded-cycle identity"
        )

    invalid_start = frame.where(
        F.col("loaded_cycle_start_timestamp").isNull()
        | F.col("loaded_cycle_start_type").isNull()
        | ~F.col("loaded_cycle_start_type").isin("observed", "left_censored")
        | F.col("loaded_observation_count").isNull()
        | (F.col("loaded_observation_count") <= 0)
        | F.col("is_right_censored").isNull()
    ).limit(1)
    if invalid_start.count():
        raise GoldCyclePersistenceError(f"{label} contains invalid cycle start or count state")

    has_complete_stop = (
        F.col("loaded_cycle_stop_record_id").isNotNull()
        & F.col("loaded_cycle_stop_timestamp").isNotNull()
        & F.col("observed_duration_seconds").isNotNull()
    )
    has_any_stop = (
        F.col("loaded_cycle_stop_record_id").isNotNull()
        | F.col("loaded_cycle_stop_timestamp").isNotNull()
        | F.col("observed_duration_seconds").isNotNull()
    )
    expected_duration = F.timestamp_diff(
        "SECOND",
        F.col("loaded_cycle_start_timestamp"),
        F.col("loaded_cycle_stop_timestamp"),
    )
    invalid_stop = frame.where(
        (F.col("is_right_censored") & has_any_stop)
        | (~F.col("is_right_censored") & ~has_complete_stop)
        | (
            ~F.col("is_right_censored")
            & (
                (F.col("observed_duration_seconds") <= 0)
                | (F.col("observed_duration_seconds") != expected_duration)
            )
        )
    ).limit(1)
    if invalid_stop.count():
        raise GoldCyclePersistenceError(f"{label} contains inconsistent cycle stop state")
    return source_count


def _assert_compatible_schemas(source: DataFrame, target: DataFrame) -> None:
    source_types = {field.name: field.dataType for field in source.schema}
    target_types = {field.name: field.dataType for field in target.schema}
    mismatches = [
        column_name
        for column_name in GOLD_CYCLE_COLUMNS
        if source_types[column_name] != target_types[column_name]
    ]
    if mismatches:
        raise GoldCyclePersistenceError(
            "Gold cycle source and target types differ for: " + ", ".join(mismatches)
        )


def _conflicting_cycle_id(frame: DataFrame, condition: Column) -> str | None:
    row = (
        frame.where(condition)
        .select(F.col("source.loaded_cycle_id").alias("loaded_cycle_id"))
        .limit(1)
        .first()
    )
    return None if row is None else str(row.loaded_cycle_id)


def _assert_updates_are_monotonic(matched: DataFrame) -> None:
    immutable_conflict = _conflicting_cycle_id(
        matched,
        ~_all_equal(IMMUTABLE_CYCLE_COLUMNS),
    )
    if immutable_conflict:
        raise GoldCyclePersistenceError(
            f"Cycle {immutable_conflict} changes immutable start evidence"
        )

    count_regression = _conflicting_cycle_id(
        matched,
        F.col("source.loaded_observation_count") < F.col("target.loaded_observation_count"),
    )
    if count_regression:
        raise GoldCyclePersistenceError(
            f"Cycle {count_regression} regresses its loaded observation count"
        )

    settled_conflict = _conflicting_cycle_id(
        matched,
        ~F.col("target.is_right_censored") & ~_all_equal(GOLD_CYCLE_COLUMNS),
    )
    if settled_conflict:
        raise GoldCyclePersistenceError(f"Settled cycle {settled_conflict} cannot change or reopen")


def persist_loaded_cycles(
    cycles: DataFrame,
    config: RailPulseConfig,
) -> GoldCycleWriteResult:
    """Merge one cycle snapshot into ``gold.loaded_cycles`` without state regression.

    New cycle identifiers are inserted. An existing right-censored cycle may gain loaded
    observations or receive its observed stop. A closed cycle is immutable, and no cycle may lose
    observations or change its deterministic start evidence.
    """

    missing_columns = sorted(set(GOLD_CYCLE_COLUMNS) - set(cycles.columns))
    if missing_columns:
        raise GoldCyclePersistenceError(
            "Loaded cycles are missing Gold persistence columns: " + ", ".join(missing_columns)
        )

    target_path = gold_table_path(config, LOADED_CYCLES_TABLE)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path_string = str(target_path)
    source = cycles.select(*GOLD_CYCLE_COLUMNS).persist(StorageLevel.DISK_ONLY)
    matched = None
    try:
        source_count = _assert_cycle_rows_are_valid(source, label="Loaded-cycle source")
        target_exists = DeltaTable.isDeltaTable(cycles.sparkSession, target_path_string)
        before_count = 0
        inserted_count = source_count
        updated_count = 0

        if target_exists:
            target_frame = cycles.sparkSession.read.format("delta").load(target_path_string)
            before_count = _assert_cycle_rows_are_valid(target_frame, label="Gold cycle target")
            target = target_frame.select(*GOLD_CYCLE_COLUMNS).persist(StorageLevel.DISK_ONLY)
            try:
                _assert_compatible_schemas(source, target)
                matched = (
                    source.alias("source")
                    .join(
                        target.alias("target"),
                        F.col("source.loaded_cycle_id") == F.col("target.loaded_cycle_id"),
                        how="inner",
                    )
                    .persist(StorageLevel.DISK_ONLY)
                )
                _assert_updates_are_monotonic(matched)
                matched_count = matched.count()
                inserted_count = source_count - matched_count
                updated_count = matched.where(
                    F.col("target.is_right_censored") & ~_all_equal(GOLD_CYCLE_COLUMNS)
                ).count()
            finally:
                target.unpersist()

        if target_exists:
            if source_count:
                (
                    DeltaTable.forPath(cycles.sparkSession, target_path_string)
                    .alias("target")
                    .merge(
                        source.alias("source"),
                        "target.loaded_cycle_id = source.loaded_cycle_id",
                    )
                    .whenMatchedUpdateAll(
                        condition=(
                            "target.is_right_censored = true AND "
                            "(source.loaded_observation_count > "
                            "target.loaded_observation_count OR "
                            "source.is_right_censored = false)"
                        )
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
        else:
            source.write.format("delta").mode("errorifexists").save(target_path_string)

        final_target = cycles.sparkSession.read.format("delta").load(target_path_string)
        after_count = final_target.count()
        unmatched_or_different = (
            source.alias("source")
            .join(
                final_target.alias("target"),
                F.col("source.loaded_cycle_id") == F.col("target.loaded_cycle_id"),
                how="left",
            )
            .where(F.col("target.loaded_cycle_id").isNull() | ~_all_equal(GOLD_CYCLE_COLUMNS))
            .limit(1)
            .count()
        )
        if after_count != before_count + inserted_count or unmatched_or_different:
            raise GoldCyclePersistenceError(
                f"gold.{LOADED_CYCLES_TABLE} reconciliation failed: source={source_count}, "
                f"inserted={inserted_count}, before={before_count}, after={after_count}"
            )

        return GoldCycleWriteResult(
            table_name=f"{config.schemas.gold}.{LOADED_CYCLES_TABLE}",
            target_path=target_path_string,
            source_cycle_count=source_count,
            inserted_cycle_count=inserted_count,
            updated_cycle_count=updated_count,
            unchanged_cycle_count=source_count - inserted_count - updated_count,
            target_cycle_count_before=before_count,
            target_cycle_count_after=after_count,
        )
    finally:
        if matched is not None:
            matched.unpersist()
        source.unpersist()
