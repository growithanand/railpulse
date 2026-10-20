"""Persist the immutable version 2 modelling view to managed Gold storage."""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from railpulse.catalog import CatalogNamespace
from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_VIEW_V2_COLUMNS,
    MODELING_VIEW_V2_KEY_COLUMN,
    MODELING_VIEW_V2_TABLE,
    MODELING_VIEW_V2_VERSION,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
)
from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_VERSION,
    STATUS_AVAILABLE,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_MISSING_PREVIOUS,
)
from railpulse.features.cycle_operating_context import (
    STATUS_MISSING_PREDICTION as CONTEXT_STATUS_MISSING_PREDICTION,
)
from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
)
from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
    STATUS_ELIGIBLE,
    STATUS_INELIGIBLE,
)
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_V2_VERSION,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_VERSION

CATALOG_MODELING_VIEW_V2_CONTRACT_VERSION = "databricks-gold-modeling-view-v2-write-v1"
_CONTEXT_STATUSES = (
    STATUS_AVAILABLE,
    CONTEXT_STATUS_MISSING_PREDICTION,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_MISSING_PREVIOUS,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
)
_HORIZON_STATUSES = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)
_STRING_LINEAGE_COLUMNS = (
    *FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS[1:],
    *FAILURE_LINEAGE_COLUMNS,
)
_SNAPSHOT_IDENTITY_COLUMNS = (
    *FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
    *FAILURE_LINEAGE_COLUMNS,
    "label_observation_end",
)


class ModelingViewV2PersistenceError(RuntimeError):
    """Raised when the managed modelling view cannot be persisted safely."""


@dataclass(frozen=True)
class CatalogModelingViewV2WriteResult:
    """Count reconciliation for one immutable managed modelling-view merge."""

    contract_version: str
    table_name: str
    key_field: str
    source_row_count: int
    inserted_row_count: int
    unchanged_row_count: int
    target_row_count_before: int
    target_row_count_after: int


def catalog_modeling_view_v2_table(namespace: CatalogNamespace, table_name: str) -> str:
    """Return the allowlisted fully qualified managed modelling-view table name."""

    if table_name != MODELING_VIEW_V2_TABLE:
        raise ModelingViewV2PersistenceError(
            f"Unsupported managed modelling-view v2 table {table_name!r}; "
            f"expected {MODELING_VIEW_V2_TABLE}"
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
    missing_columns = sorted(set(MODELING_VIEW_V2_COLUMNS) - set(target.columns))
    if missing_columns:
        raise ModelingViewV2PersistenceError(
            "Managed modelling-view v2 target is missing contract columns: "
            + ", ".join(missing_columns)
        )

    source_types = {field.name: field.dataType.simpleString() for field in source.schema}
    target_types = {field.name: field.dataType.simpleString() for field in target.schema}
    mismatches = [
        column_name
        for column_name in MODELING_VIEW_V2_COLUMNS
        if source_types[column_name] != target_types[column_name]
    ]
    if mismatches:
        raise ModelingViewV2PersistenceError(
            "Managed modelling-view v2 source and target types differ for: " + ", ".join(mismatches)
        )


def _snapshot_identity(frame: DataFrame, *, label: str) -> tuple[object, ...]:
    rows = frame.select(*_SNAPSHOT_IDENTITY_COLUMNS).distinct().limit(2).collect()
    if len(rows) != 1:
        raise ModelingViewV2PersistenceError(
            f"{label} must contain exactly one feature, failure, and observation lineage"
        )
    return tuple(rows[0][column] for column in _SNAPSHOT_IDENTITY_COLUMNS)


def _expected_exclusion_reasons() -> Column:
    feature_status = F.col("motor_current_eligibility_status")
    context_status = F.col("cycle_context_status")
    horizon_status = F.col("failure_horizon_status")
    return F.array_compact(
        F.array(
            F.when(feature_status == STATUS_INELIGIBLE, F.lit(REASON_FEATURE_INELIGIBLE)),
            F.when(context_status != STATUS_AVAILABLE, F.lit(REASON_CYCLE_CONTEXT_UNAVAILABLE)),
            F.when(horizon_status == STATUS_HORIZON_CENSORED, F.lit(REASON_HORIZON_CENSORED)),
            F.when(horizon_status == STATUS_INSIDE_FAILURE, F.lit(REASON_INSIDE_FAILURE)),
            F.when(horizon_status == STATUS_MISSING_PREDICTION, F.lit(REASON_MISSING_PREDICTION)),
        )
    )


def _assert_rows_are_valid(frame: DataFrame, *, label: str) -> int:
    missing_columns = sorted(set(MODELING_VIEW_V2_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ModelingViewV2PersistenceError(
            f"{label} is missing modelling-view v2 columns: " + ", ".join(missing_columns)
        )

    row_count = frame.count()
    if not row_count:
        raise ModelingViewV2PersistenceError(f"{label} contains no modelling rows")

    key = F.col(MODELING_VIEW_V2_KEY_COLUMN)
    if frame.where(key.isNull() | (F.length(key) == 0)).limit(1).count():
        raise ModelingViewV2PersistenceError(f"{label} contains an invalid cycle key")

    duplicate = (
        frame.groupBy(MODELING_VIEW_V2_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(MODELING_VIEW_V2_KEY_COLUMN, "count")
        .limit(1)
        .first()
    )
    if duplicate is not None:
        raise ModelingViewV2PersistenceError(
            f"{label} contains duplicate {MODELING_VIEW_V2_KEY_COLUMN} {duplicate[0]} "
            f"({duplicate['count']} records)"
        )

    invalid_contract = frame.where(
        ~F.col("modeling_view_version").eqNullSafe(F.lit(MODELING_VIEW_V2_VERSION))
        | ~F.col("feature_snapshot_version").eqNullSafe(F.lit(FEATURE_SNAPSHOT_V2_VERSION))
        | ~F.col("motor_current_15m_feature_version").eqNullSafe(
            F.lit(MOTOR_CURRENT_FEATURE_VERSION)
        )
        | ~F.col("motor_current_eligibility_version").eqNullSafe(
            F.lit(MOTOR_CURRENT_ELIGIBILITY_VERSION)
        )
        | ~F.col("cycle_context_feature_version").eqNullSafe(F.lit(CYCLE_OPERATING_CONTEXT_VERSION))
        | ~F.col("failure_horizon_version").eqNullSafe(F.lit(FAILURE_HORIZON_VERSION))
        | ~F.col("failure_horizon_seconds").eqNullSafe(F.lit(DEFAULT_FAILURE_HORIZON_SECONDS))
        | ~F.col("dataset_version").eqNullSafe(F.col("failure_dataset_version"))
    ).limit(1)
    if invalid_contract.count():
        raise ModelingViewV2PersistenceError(f"{label} contains unsupported contract versions")

    invalid_lineage = F.col("label_observation_end").isNull()
    for column_name in _STRING_LINEAGE_COLUMNS:
        invalid_lineage = (
            invalid_lineage
            | F.col(column_name).isNull()
            | (F.length(F.trim(F.col(column_name))) == 0)
        )
    if frame.where(invalid_lineage).limit(1).count():
        raise ModelingViewV2PersistenceError(f"{label} contains incomplete source lineage")
    _snapshot_identity(frame, label=label)

    feature_status = F.col("motor_current_eligibility_status")
    context_status = F.col("cycle_context_status")
    horizon_status = F.col("failure_horizon_status")
    target = F.col("failure_within_horizon")
    invalid_source_status = frame.where(
        ~F.coalesce(feature_status.isin(STATUS_ELIGIBLE, STATUS_INELIGIBLE), F.lit(False))
        | ~F.coalesce(context_status.isin(*_CONTEXT_STATUSES), F.lit(False))
        | ~F.coalesce(horizon_status.isin(*_HORIZON_STATUSES), F.lit(False))
        | ~(
            ((horizon_status == STATUS_POSITIVE) & target.eqNullSafe(F.lit(True)))
            | ((horizon_status == STATUS_NEGATIVE) & target.eqNullSafe(F.lit(False)))
            | (
                horizon_status.isin(
                    STATUS_HORIZON_CENSORED,
                    STATUS_INSIDE_FAILURE,
                    STATUS_MISSING_PREDICTION,
                )
                & target.isNull()
            )
        )
    ).limit(1)
    if invalid_source_status.count():
        raise ModelingViewV2PersistenceError(
            f"{label} contains unsupported feature, context, or target semantics"
        )

    is_trainable = (
        (feature_status == STATUS_ELIGIBLE)
        & (context_status == STATUS_AVAILABLE)
        & horizon_status.isin(STATUS_POSITIVE, STATUS_NEGATIVE)
    )
    expected_row_status = F.when(is_trainable, F.lit(STATUS_TRAINABLE)).otherwise(
        F.lit(STATUS_EXCLUDED)
    )
    invalid_modeling_state = frame.where(
        ~F.col("modeling_row_status").eqNullSafe(expected_row_status)
        | ~F.col("modeling_exclusion_reasons").eqNullSafe(_expected_exclusion_reasons())
    ).limit(1)
    if invalid_modeling_state.count():
        raise ModelingViewV2PersistenceError(
            f"{label} contains inconsistent modelling-row status or exclusion reasons"
        )

    expected_window_start = F.timestamp_add(
        "SECOND",
        -F.col("motor_current_15m_window_seconds"),
        F.col("prediction_timestamp"),
    )
    if (
        frame.where(~F.col("motor_current_15m_window_start").eqNullSafe(expected_window_start))
        .limit(1)
        .count()
    ):
        raise ModelingViewV2PersistenceError(
            f"{label} contains a feature window that crosses its prediction boundary"
        )
    return row_count


def persist_catalog_modeling_view_v2(
    modeling_rows: DataFrame,
    namespace: CatalogNamespace,
) -> CatalogModelingViewV2WriteResult:
    """Insert immutable modelling rows into the allowlisted managed Gold table."""

    missing_columns = sorted(set(MODELING_VIEW_V2_COLUMNS) - set(modeling_rows.columns))
    if missing_columns:
        raise ModelingViewV2PersistenceError(
            "Modelling view v2 is missing managed Gold persistence columns: "
            + ", ".join(missing_columns)
        )

    qualified_table = catalog_modeling_view_v2_table(namespace, MODELING_VIEW_V2_TABLE)
    source = modeling_rows.select(*MODELING_VIEW_V2_COLUMNS)
    source_count = _assert_rows_are_valid(source, label="Managed modelling-view v2 source")
    source_identity = _snapshot_identity(source, label="Managed modelling-view v2 source")
    spark = modeling_rows.sparkSession
    target_exists = spark.catalog.tableExists(qualified_table)
    before_count = 0
    inserted_count = source_count

    if target_exists:
        target_frame = spark.table(qualified_table)
        _assert_compatible_schemas(source, target_frame)
        target = target_frame.select(*MODELING_VIEW_V2_COLUMNS)
        before_count = _assert_rows_are_valid(target, label="Managed modelling-view v2 target")
        target_identity = _snapshot_identity(target, label="Managed modelling-view v2 target")
        if source_identity != target_identity:
            raise ModelingViewV2PersistenceError(
                "Managed modelling-view v2 source lineage differs from its immutable target"
            )

        matched = source.alias("source").join(
            target.alias("target"),
            F.col(f"source.{MODELING_VIEW_V2_KEY_COLUMN}")
            == F.col(f"target.{MODELING_VIEW_V2_KEY_COLUMN}"),
            how="inner",
        )
        conflict = (
            matched.where(~_all_equal(MODELING_VIEW_V2_COLUMNS))
            .select(F.col(f"source.{MODELING_VIEW_V2_KEY_COLUMN}").alias("key"))
            .limit(1)
            .first()
        )
        if conflict is not None:
            raise ModelingViewV2PersistenceError(
                f"Modelling row {conflict.key} conflicts with its immutable target row"
            )
        inserted_count = source_count - matched.count()

        (
            DeltaTable.forName(spark, qualified_table)
            .alias("target")
            .merge(
                source.alias("source"),
                f"target.{MODELING_VIEW_V2_KEY_COLUMN} = source.{MODELING_VIEW_V2_KEY_COLUMN}",
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
            F.col(f"source.{MODELING_VIEW_V2_KEY_COLUMN}")
            == F.col(f"target.{MODELING_VIEW_V2_KEY_COLUMN}"),
            how="left",
        )
        .where(
            F.col(f"target.{MODELING_VIEW_V2_KEY_COLUMN}").isNull()
            | ~_all_equal(MODELING_VIEW_V2_COLUMNS)
        )
        .limit(1)
        .count()
    )
    if after_count != before_count + inserted_count or unreconciled:
        raise ModelingViewV2PersistenceError(
            f"{qualified_table} reconciliation failed: source={source_count}, "
            f"inserted={inserted_count}, before={before_count}, after={after_count}"
        )

    return CatalogModelingViewV2WriteResult(
        contract_version=CATALOG_MODELING_VIEW_V2_CONTRACT_VERSION,
        table_name=qualified_table,
        key_field=MODELING_VIEW_V2_KEY_COLUMN,
        source_row_count=source_count,
        inserted_row_count=inserted_count,
        unchanged_row_count=source_count - inserted_count,
        target_row_count_before=before_count,
        target_row_count_after=after_count,
    )
