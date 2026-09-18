"""Verify Gold feature-snapshot schema constants and column invariants."""

from __future__ import annotations

from railpulse.features.feature_eligibility import MOTOR_CURRENT_ELIGIBILITY_COLUMNS
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_KEY_COLUMN,
    FEATURE_SNAPSHOT_LINEAGE_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.features.temporal_features import MOTOR_CURRENT_FEATURE_COLUMNS


def test_snapshot_columns_start_with_key() -> None:
    assert FEATURE_SNAPSHOT_COLUMNS[0] == FEATURE_SNAPSHOT_KEY_COLUMN


def test_snapshot_columns_contain_all_feature_columns() -> None:
    for column in MOTOR_CURRENT_FEATURE_COLUMNS:
        assert column in FEATURE_SNAPSHOT_COLUMNS, f"missing feature column: {column}"


def test_snapshot_columns_contain_all_eligibility_columns() -> None:
    for column in MOTOR_CURRENT_ELIGIBILITY_COLUMNS:
        assert column in FEATURE_SNAPSHOT_COLUMNS, f"missing eligibility column: {column}"


def test_snapshot_columns_contain_all_lineage_columns() -> None:
    for column in FEATURE_SNAPSHOT_LINEAGE_COLUMNS:
        assert column in FEATURE_SNAPSHOT_COLUMNS, f"missing lineage column: {column}"


def test_snapshot_columns_have_no_duplicates() -> None:
    assert len(FEATURE_SNAPSHOT_COLUMNS) == len(set(FEATURE_SNAPSHOT_COLUMNS))


def test_snapshot_version_is_nonempty_string() -> None:
    assert isinstance(FEATURE_SNAPSHOT_VERSION, str)
    assert len(FEATURE_SNAPSHOT_VERSION) > 0


def test_snapshot_table_name_is_nonempty_string() -> None:
    assert isinstance(FEATURE_SNAPSHOTS_TABLE, str)
    assert len(FEATURE_SNAPSHOTS_TABLE) > 0


def test_snapshot_key_column_is_nonempty_string() -> None:
    assert isinstance(FEATURE_SNAPSHOT_KEY_COLUMN, str)
    assert len(FEATURE_SNAPSHOT_KEY_COLUMN) > 0
