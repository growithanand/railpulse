"""Persist immutable failure-horizon labels to managed Gold storage."""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.catalog import CatalogNamespace
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_COLUMNS,
    FAILURE_HORIZON_VERSION,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
)

FAILURE_HORIZONS_TABLE = "failure_horizons"
FAILURE_HORIZON_KEY_COLUMN = "loaded_cycle_id"
CATALOG_FAILURE_HORIZON_CONTRACT_VERSION = "databricks-gold-failure-horizons-write-v1"
FAILURE_HORIZON_STORAGE_COLUMNS = (
    FAILURE_HORIZON_KEY_COLUMN,
    "loaded_cycle_stop_timestamp",
    *FAILURE_HORIZON_COLUMNS,
)
_SUPPORTED_STATUSES = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)
_MATCHED_FAILURE_COLUMNS = (
    "matched_failure_record_id",
    "matched_failure_source_row",
    "matched_failure_start",
    "matched_failure_end",
    "seconds_to_failure",
)


class FailureHorizonPersistenceError(RuntimeError):
    """Raised when managed failure horizons cannot be persisted safely."""


@dataclass(frozen=True)
class CatalogFailureHorizonWriteResult:
    """Count reconciliation for one immutable managed horizon merge."""

    contract_version: str
    table_name: str
    key_field: str
    source_horizon_count: int
    inserted_horizon_count: int
    unchanged_horizon_count: int
    target_horizon_count_before: int
    target_horizon_count_after: int


def catalog_failure_horizon_table(namespace: CatalogNamespace, table_name: str) -> str:
    """Return the allowlisted fully qualified managed horizon table name."""

    if table_name != FAILURE_HORIZONS_TABLE:
        raise FailureHorizonPersistenceError(
            f"Unsupported managed failure-horizon table {table_name!r}; "
            f"expected {FAILURE_HORIZONS_TABLE}"
        )
    return namespace.table("gold", table_name)


def _all_equal(columns: tuple[str, ...]) -> Column:
    condition = F.lit(True)
    for column_name in columns:
        condition = condition & F.col(f"source.{column_name}").eqNullSafe(
            F.col(f"target.{column_name}")
        )
    return condition


def _assert_compatible_schemas(source: DataFrame, target: DataFrame) -> None:
    missing_columns = sorted(set(FAILURE_HORIZON_STORAGE_COLUMNS) - set(target.columns))
    if missing_columns:
        raise FailureHorizonPersistenceError(
            "Managed failure-horizon target is missing contract columns: "
            + ", ".join(missing_columns)
        )

    source_types = {field.name: field.dataType for field in source.schema}
    target_types = {field.name: field.dataType for field in target.schema}
    mismatches = [
        column_name
        for column_name in FAILURE_HORIZON_STORAGE_COLUMNS
        if source_types[column_name] != target_types[column_name]
    ]
    if mismatches:
        raise FailureHorizonPersistenceError(
            "Managed failure-horizon source and target types differ for: " + ", ".join(mismatches)
        )


def _assert_horizon_rows_are_valid(frame: DataFrame, *, label: str) -> int:
    missing_columns = sorted(set(FAILURE_HORIZON_STORAGE_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise FailureHorizonPersistenceError(
            f"{label} is missing failure-horizon columns: " + ", ".join(missing_columns)
        )

    row_count = frame.count()
    if not row_count:
        raise FailureHorizonPersistenceError(f"{label} contains no horizon rows")

    key = F.col(FAILURE_HORIZON_KEY_COLUMN)
    if frame.where(key.isNull() | (F.length(key) == 0)).limit(1).count():
        raise FailureHorizonPersistenceError(f"{label} contains an invalid cycle key")

    duplicate = (
        frame.groupBy(FAILURE_HORIZON_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(FAILURE_HORIZON_KEY_COLUMN, "count")
        .limit(1)
        .first()
    )
    if duplicate is not None:
        raise FailureHorizonPersistenceError(
            f"{label} contains duplicate {FAILURE_HORIZON_KEY_COLUMN} {duplicate[0]} "
            f"({duplicate['count']} records)"
        )

    observation_end_count = frame.select("label_observation_end").distinct().count()
    if (
        observation_end_count != 1
        or frame.where(F.col("label_observation_end").isNull()).limit(1).count()
    ):
        raise FailureHorizonPersistenceError(
            f"{label} must contain one non-null label observation boundary"
        )

    invalid_contract = frame.where(
        ~F.col("failure_horizon_version").eqNullSafe(F.lit(FAILURE_HORIZON_VERSION))
        | ~F.col("failure_horizon_seconds").eqNullSafe(F.lit(DEFAULT_FAILURE_HORIZON_SECONDS))
        | ~F.coalesce(F.col("failure_horizon_status").isin(*_SUPPORTED_STATUSES), F.lit(False))
    ).limit(1)
    if invalid_contract.count():
        raise FailureHorizonPersistenceError(
            f"{label} contains an unsupported horizon contract or status"
        )

    prediction = F.col("prediction_timestamp")
    cycle_stop = F.col("loaded_cycle_stop_timestamp")
    horizon_end = F.col("failure_horizon_end")
    observation_end = F.col("label_observation_end")
    status = F.col("failure_horizon_status")
    target = F.col("failure_within_horizon")
    seconds_to_failure = F.col("seconds_to_failure")
    expected_horizon_end = F.timestamp_add(
        "SECOND",
        F.col("failure_horizon_seconds"),
        prediction,
    )
    expected_seconds_to_failure = F.timestamp_diff(
        "SECOND",
        prediction,
        F.col("matched_failure_start"),
    )
    has_any_match = F.lit(False)
    has_complete_match = F.lit(True)
    for column_name in _MATCHED_FAILURE_COLUMNS:
        has_any_match = has_any_match | F.col(column_name).isNotNull()
        has_complete_match = has_complete_match & F.col(column_name).isNotNull()

    positive_is_valid = (
        (status == STATUS_POSITIVE)
        & target.eqNullSafe(F.lit(True))
        & prediction.isNotNull()
        & horizon_end.isNotNull()
        & has_complete_match
        & (F.col("matched_failure_start") > prediction)
        & (F.col("matched_failure_start") <= horizon_end)
        & (F.col("matched_failure_start") <= observation_end)
        & (seconds_to_failure == expected_seconds_to_failure)
        & (seconds_to_failure > 0)
        & (seconds_to_failure <= F.col("failure_horizon_seconds"))
    )
    negative_is_valid = (
        (status == STATUS_NEGATIVE)
        & target.eqNullSafe(F.lit(False))
        & prediction.isNotNull()
        & horizon_end.isNotNull()
        & (horizon_end <= observation_end)
        & ~has_any_match
    )
    censored_is_valid = (
        (status == STATUS_HORIZON_CENSORED)
        & target.isNull()
        & prediction.isNotNull()
        & horizon_end.isNotNull()
        & (horizon_end > observation_end)
        & ~has_any_match
    )
    inside_failure_is_valid = (
        (status == STATUS_INSIDE_FAILURE)
        & target.isNull()
        & prediction.isNotNull()
        & horizon_end.isNotNull()
        & ~has_any_match
    )
    missing_prediction_is_valid = (
        (status == STATUS_MISSING_PREDICTION)
        & target.isNull()
        & prediction.isNull()
        & cycle_stop.isNull()
        & horizon_end.isNull()
        & ~has_any_match
    )
    invalid_semantics = frame.where(
        ~prediction.eqNullSafe(cycle_stop)
        | ~horizon_end.eqNullSafe(expected_horizon_end)
        | ~(
            positive_is_valid
            | negative_is_valid
            | censored_is_valid
            | inside_failure_is_valid
            | missing_prediction_is_valid
        )
    ).limit(1)
    if invalid_semantics.count():
        raise FailureHorizonPersistenceError(f"{label} contains inconsistent horizon semantics")
    return row_count


def persist_catalog_failure_horizons(
    horizons: DataFrame,
    namespace: CatalogNamespace,
) -> CatalogFailureHorizonWriteResult:
    """Insert immutable horizon rows into the allowlisted managed Gold table.

    A repeated identical snapshot is a no-op. A changed row for an existing cycle is rejected so
    that a different horizon, observation boundary, or labeling rule requires an explicit versioned
    rebuild instead of silently relabeling model targets.
    """

    missing_columns = sorted(set(FAILURE_HORIZON_STORAGE_COLUMNS) - set(horizons.columns))
    if missing_columns:
        raise FailureHorizonPersistenceError(
            "Failure horizons are missing managed Gold persistence columns: "
            + ", ".join(missing_columns)
        )

    qualified_table = catalog_failure_horizon_table(namespace, FAILURE_HORIZONS_TABLE)
    source = horizons.select(*FAILURE_HORIZON_STORAGE_COLUMNS)
    source_count = _assert_horizon_rows_are_valid(
        source,
        label="Managed failure-horizon source",
    )
    source_observation_end = source.select("label_observation_end").first()[0]
    spark = horizons.sparkSession
    target_exists = spark.catalog.tableExists(qualified_table)
    before_count = 0
    inserted_count = source_count

    if target_exists:
        target_frame = spark.table(qualified_table)
        _assert_compatible_schemas(source, target_frame)
        target = target_frame.select(*FAILURE_HORIZON_STORAGE_COLUMNS)
        before_count = _assert_horizon_rows_are_valid(
            target,
            label="Managed failure-horizon target",
        )
        target_observation_end = target.select("label_observation_end").first()[0]
        if source_observation_end != target_observation_end:
            raise FailureHorizonPersistenceError(
                "Managed failure-horizon source observation boundary differs from its "
                "immutable target snapshot"
            )
        matched = source.alias("source").join(
            target.alias("target"),
            F.col(f"source.{FAILURE_HORIZON_KEY_COLUMN}")
            == F.col(f"target.{FAILURE_HORIZON_KEY_COLUMN}"),
            how="inner",
        )
        conflict = (
            matched.where(~_all_equal(FAILURE_HORIZON_STORAGE_COLUMNS))
            .select(F.col(f"source.{FAILURE_HORIZON_KEY_COLUMN}").alias("key"))
            .limit(1)
            .first()
        )
        if conflict is not None:
            raise FailureHorizonPersistenceError(
                f"Failure horizon {conflict.key} conflicts with its immutable target row"
            )
        inserted_count = source_count - matched.count()

        (
            DeltaTable.forName(spark, qualified_table)
            .alias("target")
            .merge(
                source.alias("source"),
                f"target.{FAILURE_HORIZON_KEY_COLUMN} = source.{FAILURE_HORIZON_KEY_COLUMN}",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source.write.format("delta").mode("error").saveAsTable(qualified_table)

    final_target = spark.table(qualified_table)
    after_count = final_target.count()
    unreconciled = (
        source.alias("source")
        .join(
            final_target.alias("target"),
            F.col(f"source.{FAILURE_HORIZON_KEY_COLUMN}")
            == F.col(f"target.{FAILURE_HORIZON_KEY_COLUMN}"),
            how="left",
        )
        .where(
            F.col(f"target.{FAILURE_HORIZON_KEY_COLUMN}").isNull()
            | ~_all_equal(FAILURE_HORIZON_STORAGE_COLUMNS)
        )
        .limit(1)
        .count()
    )
    if after_count != before_count + inserted_count or unreconciled:
        raise FailureHorizonPersistenceError(
            f"{qualified_table} reconciliation failed: source={source_count}, "
            f"inserted={inserted_count}, before={before_count}, after={after_count}"
        )

    return CatalogFailureHorizonWriteResult(
        contract_version=CATALOG_FAILURE_HORIZON_CONTRACT_VERSION,
        table_name=qualified_table,
        key_field=FAILURE_HORIZON_KEY_COLUMN,
        source_horizon_count=source_count,
        inserted_horizon_count=inserted_count,
        unchanged_horizon_count=source_count - inserted_count,
        target_horizon_count_before=before_count,
        target_horizon_count_after=after_count,
    )
