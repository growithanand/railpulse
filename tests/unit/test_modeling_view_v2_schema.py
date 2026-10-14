"""Verify cycle-context modelling-view schema invariants."""

from __future__ import annotations

from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    MODEL_INPUT_COLUMNS,
    MODEL_TARGET_COLUMN,
    MODEL_TIME_COLUMN,
    MODELING_EXCLUSION_REASON_ORDER,
    MODELING_ROW_COLUMNS,
    MODELING_VIEW_TABLE,
    MODELING_VIEW_VERSION,
    REASON_FEATURE_INELIGIBLE,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    CYCLE_CONTEXT_MODEL_INPUT_COLUMNS,
    MODEL_INPUT_COLUMNS_V2,
    MODELING_V2_EXCLUSION_REASON_ORDER,
    MODELING_VIEW_V2_COLUMNS,
    MODELING_VIEW_V2_KEY_COLUMN,
    MODELING_VIEW_V2_TABLE,
    MODELING_VIEW_V2_VERSION,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
)
from railpulse.features.failure_horizons import FAILURE_HORIZON_COLUMNS
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_COLUMNS


def test_modeling_view_v2_contains_snapshot_horizon_lineage_and_row_columns() -> None:
    assert MODELING_VIEW_V2_COLUMNS[0] == MODELING_VIEW_V2_KEY_COLUMN
    for column in (
        *FEATURE_SNAPSHOT_V2_COLUMNS,
        *FAILURE_HORIZON_COLUMNS,
        *FAILURE_LINEAGE_COLUMNS,
        *MODELING_ROW_COLUMNS,
    ):
        assert column in MODELING_VIEW_V2_COLUMNS, f"missing modelling-view v2 column: {column}"
    assert len(MODELING_VIEW_V2_COLUMNS) == len(set(MODELING_VIEW_V2_COLUMNS))


def test_modeling_view_v2_inputs_preserve_v1_and_add_only_numeric_cycle_context() -> None:
    assert MODEL_INPUT_COLUMNS_V2 == (
        *MODEL_INPUT_COLUMNS,
        *CYCLE_CONTEXT_MODEL_INPUT_COLUMNS,
    )
    assert CYCLE_CONTEXT_MODEL_INPUT_COLUMNS == (
        "cycle_context_current_duration_seconds",
        "cycle_context_previous_duration_seconds",
        "cycle_context_previous_idle_seconds",
    )
    assert all(column in FEATURE_SNAPSHOT_V2_COLUMNS for column in MODEL_INPUT_COLUMNS_V2)
    assert MODEL_TARGET_COLUMN not in MODEL_INPUT_COLUMNS_V2
    assert MODEL_TIME_COLUMN not in MODEL_INPUT_COLUMNS_V2
    assert "cycle_context_previous_cycle_id" not in MODEL_INPUT_COLUMNS_V2
    assert "cycle_context_status" not in MODEL_INPUT_COLUMNS_V2
    assert "cycle_context_feature_version" not in MODEL_INPUT_COLUMNS_V2


def test_modeling_view_v2_is_separate_from_v1() -> None:
    assert MODELING_VIEW_V2_TABLE == "modeling_view_v2"
    assert MODELING_VIEW_V2_VERSION == "motor-current-cycle-context-failure-modeling-view-v2"
    assert MODELING_VIEW_V2_TABLE != MODELING_VIEW_TABLE
    assert MODELING_VIEW_V2_VERSION != MODELING_VIEW_VERSION


def test_modeling_view_v2_exclusion_order_adds_context_without_reordering_v1() -> None:
    expected = (
        REASON_FEATURE_INELIGIBLE,
        REASON_CYCLE_CONTEXT_UNAVAILABLE,
        *(
            reason
            for reason in MODELING_EXCLUSION_REASON_ORDER
            if reason != REASON_FEATURE_INELIGIBLE
        ),
    )
    assert MODELING_V2_EXCLUSION_REASON_ORDER == expected
    assert len(MODELING_V2_EXCLUSION_REASON_ORDER) == len(set(MODELING_V2_EXCLUSION_REASON_ORDER))
