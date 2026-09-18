"""Gold feature-snapshot schema constants and column contract."""

from __future__ import annotations

from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS

FEATURE_SNAPSHOTS_TABLE = "feature_snapshots"
FEATURE_SNAPSHOT_VERSION = "motor-current-15m-snapshot-v1"
FEATURE_SNAPSHOT_KEY_COLUMN = "loaded_cycle_id"

FEATURE_SNAPSHOT_LINEAGE_COLUMNS = (
    "feature_snapshot_version",
    "dataset_version",
    "telemetry_source_sha256",
    "telemetry_ingestion_batch_id",
)

FEATURE_SNAPSHOT_COLUMNS = (
    FEATURE_SNAPSHOT_KEY_COLUMN,
    *MOTOR_CURRENT_FEATURE_COLUMNS,
    *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
    *FEATURE_SNAPSHOT_LINEAGE_COLUMNS,
)
