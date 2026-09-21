"""Verify chronological modelling-view schema invariants."""

from __future__ import annotations

from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    MODEL_INPUT_COLUMNS,
    MODEL_TARGET_COLUMN,
    MODEL_TIME_COLUMN,
    MODELING_EXCLUSION_REASON_ORDER,
    MODELING_ROW_COLUMNS,
    MODELING_VIEW_COLUMNS,
    MODELING_VIEW_KEY_COLUMN,
    MODELING_VIEW_TABLE,
    MODELING_VIEW_VERSION,
)
from railpulse.features.failure_horizons import FAILURE_HORIZON_COLUMNS
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOT_COLUMNS


def test_modeling_view_columns_start_with_snapshot_key() -> None:
    assert MODELING_VIEW_COLUMNS[0] == MODELING_VIEW_KEY_COLUMN


def test_modeling_view_contains_snapshot_horizon_lineage_and_row_columns() -> None:
    for column in (
        *FEATURE_SNAPSHOT_COLUMNS,
        *FAILURE_HORIZON_COLUMNS,
        *FAILURE_LINEAGE_COLUMNS,
        *MODELING_ROW_COLUMNS,
    ):
        assert column in MODELING_VIEW_COLUMNS, f"missing modelling-view column: {column}"


def test_modeling_view_columns_have_no_duplicates() -> None:
    assert len(MODELING_VIEW_COLUMNS) == len(set(MODELING_VIEW_COLUMNS))


def test_model_inputs_are_snapshot_columns_and_exclude_target_and_time() -> None:
    for column in MODEL_INPUT_COLUMNS:
        assert column in FEATURE_SNAPSHOT_COLUMNS
    assert MODEL_TARGET_COLUMN not in MODEL_INPUT_COLUMNS
    assert MODEL_TIME_COLUMN not in MODEL_INPUT_COLUMNS


def test_model_target_and_time_are_horizon_columns() -> None:
    assert MODEL_TARGET_COLUMN in FAILURE_HORIZON_COLUMNS
    assert MODEL_TIME_COLUMN in FAILURE_HORIZON_COLUMNS


def test_contract_identifiers_and_reason_order_are_nonempty_and_unique() -> None:
    assert MODELING_VIEW_TABLE
    assert MODELING_VIEW_VERSION
    assert MODELING_EXCLUSION_REASON_ORDER
    assert len(MODELING_EXCLUSION_REASON_ORDER) == len(set(MODELING_EXCLUSION_REASON_ORDER))
