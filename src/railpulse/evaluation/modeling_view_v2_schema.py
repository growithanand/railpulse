"""Schema constants for the cycle-context chronological modelling view."""

from __future__ import annotations

from railpulse.evaluation.modeling_view_schema import (
    FAILURE_LINEAGE_COLUMNS,
    MODEL_INPUT_COLUMNS,
    MODELING_EXCLUSION_REASON_ORDER,
    MODELING_ROW_COLUMNS,
    REASON_FEATURE_INELIGIBLE,
)
from railpulse.features.failure_horizons import FAILURE_HORIZON_COLUMNS
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_COLUMNS,
    FEATURE_SNAPSHOT_V2_KEY_COLUMN,
)

MODELING_VIEW_V2_TABLE = "modeling_view_v2"
MODELING_VIEW_V2_VERSION = "motor-current-cycle-context-failure-modeling-view-v2"
MODELING_VIEW_V2_KEY_COLUMN = FEATURE_SNAPSHOT_V2_KEY_COLUMN

CYCLE_CONTEXT_MODEL_INPUT_COLUMNS = (
    "cycle_context_current_duration_seconds",
    "cycle_context_previous_duration_seconds",
    "cycle_context_previous_idle_seconds",
)

MODEL_INPUT_COLUMNS_V2 = (
    *MODEL_INPUT_COLUMNS,
    *CYCLE_CONTEXT_MODEL_INPUT_COLUMNS,
)

MODELING_VIEW_V2_COLUMNS = (
    *FEATURE_SNAPSHOT_V2_COLUMNS,
    *FAILURE_HORIZON_COLUMNS,
    *FAILURE_LINEAGE_COLUMNS,
    *MODELING_ROW_COLUMNS,
)

REASON_CYCLE_CONTEXT_UNAVAILABLE = "cycle_context_unavailable"

MODELING_V2_EXCLUSION_REASON_ORDER = (
    REASON_FEATURE_INELIGIBLE,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
    *(reason for reason in MODELING_EXCLUSION_REASON_ORDER if reason != REASON_FEATURE_INELIGIBLE),
)
