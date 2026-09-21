"""Build the leakage-safe chronological modelling view."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    MODELING_VIEW_COLUMNS,
    MODELING_VIEW_KEY_COLUMN,
    MODELING_VIEW_VERSION,
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    REASON_UNSUPPORTED_HORIZON_STATUS,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
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
from railpulse.features.feature_eligibility import (
    STATUS_ELIGIBLE as FEATURE_STATUS_ELIGIBLE,
)
from railpulse.features.feature_eligibility import (
    STATUS_INELIGIBLE as FEATURE_STATUS_INELIGIBLE,
)
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOT_COLUMNS

_HORIZON_INPUT_COLUMNS = (
    MODELING_VIEW_KEY_COLUMN,
    "loaded_cycle_stop_timestamp",
    *FAILURE_HORIZON_COLUMNS,
)
_SUPPORTED_HORIZON_STATUSES = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)


class ModelingViewError(ValueError):
    """Raised when the chronological modelling view cannot be built safely."""


def _validate_lineage(
    *,
    failure_dataset_version: str,
    failure_source_sha256: str,
    failure_ingestion_batch_id: str,
) -> None:
    lineage = {
        "failure_dataset_version": failure_dataset_version,
        "failure_source_sha256": failure_source_sha256,
        "failure_ingestion_batch_id": failure_ingestion_batch_id,
    }
    invalid = [
        name for name, value in lineage.items() if not isinstance(value, str) or not value.strip()
    ]
    if invalid:
        raise ModelingViewError(
            "Failure lineage values must be nonempty strings: " + ", ".join(invalid)
        )


def _validate_columns(snapshots: DataFrame, horizons: DataFrame) -> None:
    missing_snapshot_columns = sorted(set(FEATURE_SNAPSHOT_COLUMNS) - set(snapshots.columns))
    if missing_snapshot_columns:
        raise ModelingViewError(
            "Feature snapshots are missing modelling-view columns: "
            + ", ".join(missing_snapshot_columns)
        )
    missing_horizon_columns = sorted(set(_HORIZON_INPUT_COLUMNS) - set(horizons.columns))
    if missing_horizon_columns:
        raise ModelingViewError(
            "Failure horizons are missing modelling-view columns: "
            + ", ".join(missing_horizon_columns)
        )


def _validate_unique_keys(frame: DataFrame, *, label: str) -> None:
    invalid = frame.where(
        F.col(MODELING_VIEW_KEY_COLUMN).isNull() | (F.length(MODELING_VIEW_KEY_COLUMN) == 0)
    ).limit(1)
    if invalid.count():
        raise ModelingViewError(f"{label} contains an invalid cycle key")
    duplicate = (
        frame.groupBy(MODELING_VIEW_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(MODELING_VIEW_KEY_COLUMN)
        .limit(1)
        .first()
    )
    if duplicate is not None:
        raise ModelingViewError(f"{label} contains duplicate cycle key {duplicate[0]}")


def _validate_key_sets(snapshots: DataFrame, horizons: DataFrame) -> None:
    snapshot_keys = snapshots.select(MODELING_VIEW_KEY_COLUMN)
    horizon_keys = horizons.select(MODELING_VIEW_KEY_COLUMN)
    missing_horizon = snapshot_keys.join(horizon_keys, on=MODELING_VIEW_KEY_COLUMN, how="left_anti")
    if missing_horizon.limit(1).count():
        raise ModelingViewError("Failure horizons do not cover every feature-snapshot key")
    missing_snapshot = horizon_keys.join(
        snapshot_keys, on=MODELING_VIEW_KEY_COLUMN, how="left_anti"
    )
    if missing_snapshot.limit(1).count():
        raise ModelingViewError("Feature snapshots do not cover every failure-horizon key")


def _validate_joined_semantics(joined: DataFrame, *, failure_dataset_version: str) -> None:
    invalid_version = joined.where(
        ~F.col("snapshot.dataset_version").eqNullSafe(F.lit(failure_dataset_version))
        | ~F.col("horizon.failure_horizon_version").eqNullSafe(F.lit(FAILURE_HORIZON_VERSION))
        | ~F.col("horizon.failure_horizon_seconds").eqNullSafe(
            F.lit(DEFAULT_FAILURE_HORIZON_SECONDS)
        )
    ).limit(1)
    if invalid_version.count():
        raise ModelingViewError("Modelling-view inputs contain incompatible dataset or versions")

    feature_status = F.col("snapshot.motor_current_eligibility_status")
    horizon_status = F.col("horizon.failure_horizon_status")
    target = F.col("horizon.failure_within_horizon")
    valid_target = (
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
        | (
            ~F.coalesce(horizon_status.isin(*_SUPPORTED_HORIZON_STATUSES), F.lit(False))
            & target.isNull()
        )
    )
    invalid_status = joined.where(
        ~feature_status.isin(FEATURE_STATUS_ELIGIBLE, FEATURE_STATUS_INELIGIBLE) | ~valid_target
    ).limit(1)
    if invalid_status.count():
        raise ModelingViewError("Modelling-view inputs contain invalid eligibility or label state")

    prediction = F.col("horizon.prediction_timestamp")
    cycle_stop = F.col("horizon.loaded_cycle_stop_timestamp")
    window_start = F.col("snapshot.motor_current_15m_window_start")
    window_seconds = F.col("snapshot.motor_current_15m_window_seconds")
    expected_window_start = F.timestamp_add("SECOND", -window_seconds, prediction)
    invalid_boundary = joined.where(
        ~prediction.eqNullSafe(cycle_stop) | ~window_start.eqNullSafe(expected_window_start)
    ).limit(1)
    if invalid_boundary.count():
        raise ModelingViewError(
            "Prediction timestamp does not align with its cycle stop and feature window"
        )


def build_modeling_view(
    snapshots: DataFrame,
    horizons: DataFrame,
    *,
    failure_dataset_version: str,
    failure_source_sha256: str,
    failure_ingestion_batch_id: str,
) -> DataFrame:
    """Join point-in-time features and future labels with explicit row eligibility."""

    _validate_lineage(
        failure_dataset_version=failure_dataset_version,
        failure_source_sha256=failure_source_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
    )
    _validate_columns(snapshots, horizons)
    _validate_unique_keys(snapshots, label="Feature snapshots")
    _validate_unique_keys(horizons, label="Failure horizons")
    _validate_key_sets(snapshots, horizons)

    joined = snapshots.alias("snapshot").join(
        horizons.alias("horizon"),
        F.col(f"snapshot.{MODELING_VIEW_KEY_COLUMN}")
        == F.col(f"horizon.{MODELING_VIEW_KEY_COLUMN}"),
        how="inner",
    )
    _validate_joined_semantics(joined, failure_dataset_version=failure_dataset_version)

    feature_status = F.col("snapshot.motor_current_eligibility_status")
    horizon_status = F.col("horizon.failure_horizon_status")
    is_trainable = (feature_status == FEATURE_STATUS_ELIGIBLE) & horizon_status.isin(
        STATUS_POSITIVE,
        STATUS_NEGATIVE,
    )
    reasons = F.array_compact(
        F.array(
            F.when(
                feature_status == FEATURE_STATUS_INELIGIBLE,
                F.lit(REASON_FEATURE_INELIGIBLE),
            ),
            F.when(
                horizon_status == STATUS_HORIZON_CENSORED,
                F.lit(REASON_HORIZON_CENSORED),
            ),
            F.when(
                horizon_status == STATUS_INSIDE_FAILURE,
                F.lit(REASON_INSIDE_FAILURE),
            ),
            F.when(
                horizon_status == STATUS_MISSING_PREDICTION,
                F.lit(REASON_MISSING_PREDICTION),
            ),
            F.when(
                ~F.coalesce(horizon_status.isin(*_SUPPORTED_HORIZON_STATUSES), F.lit(False)),
                F.lit(REASON_UNSUPPORTED_HORIZON_STATUS),
            ),
        )
    )
    result = joined.select(
        *(F.col(f"snapshot.{column}").alias(column) for column in FEATURE_SNAPSHOT_COLUMNS),
        *(F.col(f"horizon.{column}").alias(column) for column in FAILURE_HORIZON_COLUMNS),
        F.lit(failure_dataset_version).alias(FAILURE_LINEAGE_COLUMNS[0]),
        F.lit(failure_source_sha256).alias(FAILURE_LINEAGE_COLUMNS[1]),
        F.lit(failure_ingestion_batch_id).alias(FAILURE_LINEAGE_COLUMNS[2]),
        F.lit(MODELING_VIEW_VERSION).alias("modeling_view_version"),
        F.when(is_trainable, F.lit(STATUS_TRAINABLE))
        .otherwise(F.lit(STATUS_EXCLUDED))
        .alias("modeling_row_status"),
        reasons.alias("modeling_exclusion_reasons"),
    )
    return result.select(*MODELING_VIEW_COLUMNS)
