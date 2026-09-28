"""Verify version 2 feature-snapshot schema invariants."""

from __future__ import annotations

from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_COLUMNS
from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOTS_TABLE
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_COLUMNS,
    FEATURE_SNAPSHOT_V2_KEY_COLUMN,
    FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_V2_VERSION,
    FEATURE_SNAPSHOTS_V2_TABLE,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS


def test_snapshot_v2_columns_preserve_v1_features_and_add_cycle_context() -> None:
    assert FEATURE_SNAPSHOT_V2_COLUMNS[0] == FEATURE_SNAPSHOT_V2_KEY_COLUMN
    for column in (
        *MOTOR_CURRENT_FEATURE_COLUMNS,
        *MOTOR_CURRENT_ELIGIBILITY_COLUMNS,
        *CYCLE_OPERATING_CONTEXT_COLUMNS,
        *FEATURE_SNAPSHOT_V2_LINEAGE_COLUMNS,
    ):
        assert column in FEATURE_SNAPSHOT_V2_COLUMNS
    assert len(FEATURE_SNAPSHOT_V2_COLUMNS) == len(set(FEATURE_SNAPSHOT_V2_COLUMNS))


def test_snapshot_v2_has_distinct_version_and_table() -> None:
    assert FEATURE_SNAPSHOT_V2_VERSION == "motor-current-15m-cycle-context-snapshot-v2"
    assert FEATURE_SNAPSHOTS_V2_TABLE == "feature_snapshots_v2"
    assert FEATURE_SNAPSHOTS_V2_TABLE != FEATURE_SNAPSHOTS_TABLE
