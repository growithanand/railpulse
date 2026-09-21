"""Schema constants for the leakage-safe chronological modelling view."""

from __future__ import annotations

from railpulse.features.failure_horizons import FAILURE_HORIZON_COLUMNS
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_KEY_COLUMN,
)

MODELING_VIEW_TABLE = "modeling_view"
MODELING_VIEW_VERSION = "motor-current-failure-modeling-view-v1"
MODELING_VIEW_KEY_COLUMN = FEATURE_SNAPSHOT_KEY_COLUMN

FAILURE_LINEAGE_COLUMNS = (
    "failure_dataset_version",
    "failure_source_sha256",
    "failure_ingestion_batch_id",
)

MODELING_ROW_COLUMNS = (
    "modeling_view_version",
    "modeling_row_status",
    "modeling_exclusion_reasons",
)

MODEL_INPUT_COLUMNS = (
    "motor_current_15m_observation_count",
    "motor_current_15m_minimum_amperes",
    "motor_current_15m_mean_amperes",
    "motor_current_15m_maximum_amperes",
    "motor_current_maximum_window_gap_seconds",
)

MODEL_TARGET_COLUMN = "failure_within_horizon"
MODEL_TIME_COLUMN = "prediction_timestamp"

MODELING_VIEW_COLUMNS = (
    *FEATURE_SNAPSHOT_COLUMNS,
    *FAILURE_HORIZON_COLUMNS,
    *FAILURE_LINEAGE_COLUMNS,
    *MODELING_ROW_COLUMNS,
)

STATUS_TRAINABLE = "trainable"
STATUS_EXCLUDED = "excluded"

REASON_FEATURE_INELIGIBLE = "feature_ineligible"
REASON_HORIZON_CENSORED = "horizon_censored"
REASON_INSIDE_FAILURE = "inside_failure_interval"
REASON_MISSING_PREDICTION = "missing_prediction_boundary"
REASON_UNSUPPORTED_HORIZON_STATUS = "unsupported_horizon_status"

MODELING_EXCLUSION_REASON_ORDER = (
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    REASON_UNSUPPORTED_HORIZON_STATUS,
)
