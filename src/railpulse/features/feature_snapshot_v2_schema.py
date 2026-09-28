"""Version 2 Gold feature-snapshot schema and column contract."""

from __future__ import annotations

from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_COLUMNS
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOT_KEY_COLUMN
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS

FEATURE_SNAPSHOTS_V2_TABLE = "feature_snapshots_v2"
FEATURE_SNAPSHOT_V2_VERSION = "motor-current-15m-cycle-context-snapshot-v2"
FEATURE_SNAPSHOT_V2_KEY_COLUMN = FEATURE_SNAPSHOT_KEY_COLUMN

FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS = (
    "feature_snapshot_version",
    "dataset_version",
    "telemetry_source_sha256",
    "telemetry_ingestion_batch_id",
)

FEATURE_SNAPSHOT_V2_COLUMNS = (
    FEATURE_SNAPSHOT_V2_KEY_COLUMN,
    *MOTOR_CURRENT_FEATURE_COLUMNS,
    *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
    *CYCLE_OPERATING_CONTEXT_COLUMNS,
    *FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
)
