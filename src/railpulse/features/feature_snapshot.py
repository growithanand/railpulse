"""Build contract-shaped Gold feature snapshots without writing storage."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from railpulse.features.feature_eligibility import (
    MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
    MOTOR_CURRENT_ELIGIBILITY_VERSION,
)
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_KEY_COLUMN,
    FEATURE_SNAPSHOT_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
)
from railpulse.features.temporal_features import (
    MOTOR_CURRENT_FEATURE_COLUMNS,
    MOTOR_CURRENT_FEATURE_VERSION,
)

_REQUIRED_INPUT_COLUMNS = (
    FEATURE_SNAPSHOT_KEY_COLUMN,
    *MOTOR_CURRENT_FEATURE_COLUMNS,
    *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
)


class FeatureSnapshotError(ValueError):
    """Raised when a feature snapshot violates its construction contract."""


def _validate_lineage(
    *,
    dataset_version: str,
    telemetry_source_sha256: str,
    telemetry_ingestion_batch_id: str,
) -> None:
    lineage = {
        "dataset_version": dataset_version,
        "telemetry_source_sha256": telemetry_source_sha256,
        "telemetry_ingestion_batch_id": telemetry_ingestion_batch_id,
    }
    invalid = [name for name, value in lineage.items() if not isinstance(value, str) or not value]
    if invalid:
        raise FeatureSnapshotError(
            "Feature snapshot lineage values must be nonempty strings: " + ", ".join(invalid)
        )


def _validate_input(features: DataFrame) -> None:
    missing_columns = sorted(set(_REQUIRED_INPUT_COLUMNS) - set(features.columns))
    if missing_columns:
        raise FeatureSnapshotError(
            "Eligibility-enriched features are missing snapshot columns: "
            + ", ".join(missing_columns)
        )

    conflicting_columns = sorted(
        set(FEATURE_SNAPSHOT_LINEAGE_COLUMNS).intersection(features.columns)
    )
    if conflicting_columns:
        raise FeatureSnapshotError(
            "Eligibility-enriched features already contain snapshot lineage columns: "
            + ", ".join(conflicting_columns)
        )

    if features.where(F.col(FEATURE_SNAPSHOT_KEY_COLUMN).isNull()).limit(1).count():
        raise FeatureSnapshotError("Feature snapshot keys must not be null")

    duplicate_key = (
        features.groupBy(FEATURE_SNAPSHOT_KEY_COLUMN)
        .count()
        .where(F.col("count") > 1)
        .select(FEATURE_SNAPSHOT_KEY_COLUMN)
        .limit(1)
        .collect()
    )
    if duplicate_key:
        raise FeatureSnapshotError(
            f"Feature snapshot keys must be unique; duplicate: {duplicate_key[0][0]}"
        )

    invalid_version = (
        features.where(
            (F.col("motor_current_15m_feature_version") != MOTOR_CURRENT_FEATURE_VERSION)
            | F.col("motor_current_15m_feature_version").isNull()
            | (F.col("motor_current_eligibility_version") != MOTOR_CURRENT_ELIGIBILITY_VERSION)
            | F.col("motor_current_eligibility_version").isNull()
        )
        .select(FEATURE_SNAPSHOT_KEY_COLUMN)
        .limit(1)
        .collect()
    )
    if invalid_version:
        raise FeatureSnapshotError(
            "Feature snapshot component versions must match the schema contract; invalid cycle: "
            f"{invalid_version[0][0]}"
        )


def build_feature_snapshot(
    features: DataFrame,
    *,
    dataset_version: str,
    telemetry_source_sha256: str,
    telemetry_ingestion_batch_id: str,
) -> DataFrame:
    """Return one ordered, lineage-bound snapshot row per loaded cycle.

    Failure-horizon labels are intentionally excluded. They remain a separate versioned dataset
    that can be joined explicitly when a chronological modeling view is built.
    """

    _validate_lineage(
        dataset_version=dataset_version,
        telemetry_source_sha256=telemetry_source_sha256,
        telemetry_ingestion_batch_id=telemetry_ingestion_batch_id,
    )
    _validate_input(features)

    return features.select(
        FEATURE_SNAPSHOT_KEY_COLUMN,
        *MOTOR_CURRENT_FEATURE_COLUMNS,
        *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
        F.lit(FEATURE_SNAPSHOT_VERSION).alias("feature_snapshot_version"),
        F.lit(dataset_version).alias("dataset_version"),
        F.lit(telemetry_source_sha256).alias("telemetry_source_sha256"),
        F.lit(telemetry_ingestion_batch_id).alias("telemetry_ingestion_batch_id"),
    ).select(*FEATURE_SNAPSHOT_COLUMNS)
