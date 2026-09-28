"""Build version 2 snapshots while preserving the version 1 contract."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_COLUMNS,
    CYCLE_OPERATING_CONTEXT_VERSION,
    STATUS_AVAILABLE,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREVIOUS,
)
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.feature_snapshot import build_feature_snapshot
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_COLUMNS,
    FEATURE_SNAPSHOT_V2_KEY_COLUMN,
    FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_V2_VERSION,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS

_CONTEXT_STATUSES = (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_MISSING_PREVIOUS,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
)
_CONTEXT_REQUIRED_COLUMNS = (
    FEATURE_SNAPSHOT_V2_KEY_COLUMN,
    *CYCLE_OPERATING_CONTEXT_COLUMNS,
)


class FeatureSnapshotV2Error(ValueError):
    """Raised when a version 2 feature snapshot violates its contract."""


def _validate_cycle_context(context: DataFrame) -> int:
    missing_columns = sorted(set(_CONTEXT_REQUIRED_COLUMNS) - set(context.columns))
    if missing_columns:
        raise FeatureSnapshotV2Error(
            "Cycle operating context is missing snapshot v2 columns: " + ", ".join(missing_columns)
        )
    conflicting_columns = sorted(
        set(FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS).intersection(context.columns)
    )
    if conflicting_columns:
        raise FeatureSnapshotV2Error(
            "Cycle operating context already contains snapshot lineage columns: "
            + ", ".join(conflicting_columns)
        )

    key = F.col(FEATURE_SNAPSHOT_V2_KEY_COLUMN)
    if context.where(key.isNull() | (F.length(key) == 0)).limit(1).count():
        raise FeatureSnapshotV2Error("Cycle operating-context keys must not be null or empty")
    duplicate = (
        context.groupBy(FEATURE_SNAPSHOT_V2_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(FEATURE_SNAPSHOT_V2_KEY_COLUMN)
        .limit(1)
        .first()
    )
    if duplicate is not None:
        raise FeatureSnapshotV2Error(
            "Cycle operating-context keys must be unique; duplicate: " + str(duplicate[0])
        )

    status = F.col("cycle_context_status")
    current_duration = F.col("cycle_context_current_duration_seconds")
    previous_id = F.col("cycle_context_previous_cycle_id")
    previous_duration = F.col("cycle_context_previous_duration_seconds")
    previous_idle = F.col("cycle_context_previous_idle_seconds")
    invalid = context.where(
        ~F.col("cycle_context_feature_version").eqNullSafe(F.lit(CYCLE_OPERATING_CONTEXT_VERSION))
        | status.isNull()
        | ~status.isin(*_CONTEXT_STATUSES)
        | (current_duration.isNotNull() & (current_duration <= 0))
        | (previous_duration.isNotNull() & (previous_duration <= 0))
        | (previous_idle.isNotNull() & (previous_idle < 0))
        | (
            (status == STATUS_AVAILABLE)
            & (
                current_duration.isNull()
                | previous_id.isNull()
                | previous_duration.isNull()
                | previous_idle.isNull()
            )
        )
    ).limit(1)
    if invalid.count():
        raise FeatureSnapshotV2Error(
            "Cycle operating-context values do not satisfy the snapshot v2 contract"
        )
    return context.count()


def build_feature_snapshot_v2(
    features: DataFrame,
    cycle_context: DataFrame,
    *,
    dataset_version: str,
    telemetry_source_sha256: str,
    telemetry_ingestion_batch_id: str,
) -> DataFrame:
    """Return one label-free version 2 snapshot row per version 1 feature row."""

    base = build_feature_snapshot(
        features,
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
    )
    base_count = base.count()
    context_count = _validate_cycle_context(cycle_context)
    if context_count != base_count:
        raise FeatureSnapshotV2Error(
            "Version 1 features and cycle context must have equal populations"
        )

    context = cycle_context.select(*_CONTEXT_REQUIRED_COLUMNS)
    joined = base.alias("base").join(
        context.alias("context"),
        F.col(f"base.{FEATURE_SNAPSHOT_V2_KEY_COLUMN}")
        == F.col(f"context.{FEATURE_SNAPSHOT_V2_KEY_COLUMN}"),
        how="inner",
    )
    if joined.count() != base_count:
        raise FeatureSnapshotV2Error("Version 1 features and cycle context do not join one-to-one")

    return joined.select(
        F.col(f"base.{FEATURE_SNAPSHOT_V2_KEY_COLUMN}").alias(FEATURE_SNAPSHOT_V2_KEY_COLUMN),
        *(F.col(f"base.{column}").alias(column) for column in MOTOR_CURRENT_FEATURE_COLUMNS),
        *(F.col(f"base.{column}").alias(column) for column in MOTOR_CURRENT_ELIGIBILITY_COLUMNS),
        *(F.col(f"context.{column}").alias(column) for column in CYCLE_OPERATING_CONTEXT_COLUMNS),
        F.lit(FEATURE_SNAPSHOT_V2_VERSION).alias("feature_snapshot_version"),
        F.col("base.dataset_version").alias("dataset_version"),
        F.col("base.telemetry_source_sha256").alias("telemetry_source_sha256"),
        F.col("base.telemetry_ingestion_batch_id").alias("telemetry_ingestion_batch_id"),
    ).select(*FEATURE_SNAPSHOT_V2_COLUMNS)
