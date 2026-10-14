"""Build the cycle-context chronological modelling view."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.evaluation.modeling_view import ModelingViewError, build_modeling_view
from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    REASON_UNSUPPORTED_HORIZON_STATUS,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_VIEW_V2_COLUMNS,
    MODELING_VIEW_V2_KEY_COLUMN,
    MODELING_VIEW_V2_VERSION,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
)
from railpulse.features import cycle_operating_context as cycle_context
from railpulse.features.failure_horizons import (
    FAILURE_HORIZON_COLUMNS,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
)
from railpulse.features.feature_eligibility import STATUS_INELIGIBLE as FEATURE_STATUS_INELIGIBLE
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
)
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_COLUMNS,
    FEATURE_SNAPSHOT_V2_VERSION,
)

_SUPPORTED_CONTEXT_STATUSES = (
    cycle_context.STATUS_AVAILABLE,
    cycle_context.STATUS_MISSING_PREDICTION,
    cycle_context.STATUS_LEFT_CENSORED_CURRENT,
    cycle_context.STATUS_INCOMPLETE_CURRENT,
    cycle_context.STATUS_MISSING_PREVIOUS,
    cycle_context.STATUS_INCOMPLETE_PREVIOUS,
    cycle_context.STATUS_INVALID_PREVIOUS_INTERVAL,
)
_SUPPORTED_HORIZON_STATUSES = (
    STATUS_POSITIVE,
    STATUS_NEGATIVE,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
)


class ModelingViewV2Error(ModelingViewError):
    """Raised when cycle-context snapshots cannot satisfy the version 2 view."""


def _validate_v2_snapshot(snapshots: DataFrame) -> None:
    missing_columns = sorted(set(FEATURE_SNAPSHOT_V2_COLUMNS) - set(snapshots.columns))
    if missing_columns:
        raise ModelingViewV2Error(
            "Feature snapshots v2 are missing modelling-view columns: " + ", ".join(missing_columns)
        )

    status = F.col("cycle_context_status")
    current_duration = F.col("cycle_context_current_duration_seconds")
    previous_id = F.col("cycle_context_previous_cycle_id")
    previous_duration = F.col("cycle_context_previous_duration_seconds")
    previous_idle = F.col("cycle_context_previous_idle_seconds")
    invalid = snapshots.where(
        ~F.col("feature_snapshot_version").eqNullSafe(F.lit(FEATURE_SNAPSHOT_V2_VERSION))
        | ~F.col("cycle_context_feature_version").eqNullSafe(
            F.lit(cycle_context.CYCLE_OPERATING_CONTEXT_VERSION)
        )
        | status.isNull()
        | ~status.isin(*_SUPPORTED_CONTEXT_STATUSES)
        | (current_duration.isNotNull() & (current_duration <= 0))
        | (previous_duration.isNotNull() & (previous_duration <= 0))
        | (previous_idle.isNotNull() & (previous_idle < 0))
        | (
            (status == cycle_context.STATUS_AVAILABLE)
            & (
                current_duration.isNull()
                | previous_id.isNull()
                | (F.length(previous_id) == 0)
                | previous_duration.isNull()
                | previous_idle.isNull()
            )
        )
    ).limit(1)
    if invalid.count():
        raise ModelingViewV2Error(
            "Feature snapshots v2 contain incompatible version or cycle-context values"
        )


def build_modeling_view_v2(
    snapshots: DataFrame,
    horizons: DataFrame,
    *,
    failure_dataset_version: str,
    failure_source_sha256: str,
    failure_ingestion_batch_id: str,
) -> DataFrame:
    """Join version 2 features and labels with complete-context row eligibility."""

    _validate_v2_snapshot(snapshots)
    v1_compatible_snapshots = snapshots.select(
        *(
            F.lit(FEATURE_SNAPSHOT_VERSION).alias(column)
            if column == "feature_snapshot_version"
            else F.col(column)
            for column in FEATURE_SNAPSHOT_COLUMNS
        )
    )
    base = build_modeling_view(
        v1_compatible_snapshots,
        horizons,
        failure_dataset_version=failure_dataset_version,
        failure_source_sha256=failure_source_sha256,
        failure_ingestion_batch_id=failure_ingestion_batch_id,
    )
    joined = snapshots.alias("snapshot").join(
        base.alias("base"),
        F.col(f"snapshot.{MODELING_VIEW_V2_KEY_COLUMN}")
        == F.col(f"base.{MODELING_VIEW_V2_KEY_COLUMN}"),
        how="inner",
    )

    feature_status = F.col("snapshot.motor_current_eligibility_status")
    context_status = F.col("snapshot.cycle_context_status")
    horizon_status = F.col("base.failure_horizon_status")
    is_trainable = (F.col("base.modeling_row_status") == STATUS_TRAINABLE) & (
        context_status == cycle_context.STATUS_AVAILABLE
    )
    reasons = F.array_compact(
        F.array(
            F.when(
                feature_status == FEATURE_STATUS_INELIGIBLE,
                F.lit(REASON_FEATURE_INELIGIBLE),
            ),
            F.when(
                context_status != cycle_context.STATUS_AVAILABLE,
                F.lit(REASON_CYCLE_CONTEXT_UNAVAILABLE),
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
        *(F.col(f"snapshot.{column}").alias(column) for column in FEATURE_SNAPSHOT_V2_COLUMNS),
        *(F.col(f"base.{column}").alias(column) for column in FAILURE_HORIZON_COLUMNS),
        *(F.col(f"base.{column}").alias(column) for column in FAILURE_LINEAGE_COLUMNS),
        F.lit(MODELING_VIEW_V2_VERSION).alias("modeling_view_version"),
        F.when(is_trainable, F.lit(STATUS_TRAINABLE))
        .otherwise(F.lit(STATUS_EXCLUDED))
        .alias("modeling_row_status"),
        reasons.alias("modeling_exclusion_reasons"),
    )
    return result.select(*MODELING_VIEW_V2_COLUMNS)
