from __future__ import annotations

from math import nan

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.models.directional_baseline import (
    DIRECTIONAL_ALERT_COLUMN,
    DIRECTIONAL_BASELINE_VERSION,
    DIRECTIONAL_THRESHOLD_CANDIDATES,
    DirectionalBaselineError,
    DirectionalThresholdCandidate,
    apply_directional_threshold,
)


@pytest.mark.spark
def test_directional_threshold_uses_inclusive_low_current_rule_for_trainable_rows(
    spark: SparkSession,
) -> None:
    view = spark.createDataFrame(
        [
            ("below", "trainable", 0.99),
            ("boundary", "trainable", 1.0),
            ("above", "trainable", 1.01),
            ("missing", "trainable", None),
            ("nan", "trainable", nan),
            ("excluded", "excluded", 0.5),
        ],
        "loaded_cycle_id string, modeling_row_status string, motor_current_15m_mean_amperes double",
    ).withColumn("modeling_view_version", F.lit("motor-current-failure-modeling-view-v1"))
    candidate = DirectionalThresholdCandidate("fixture", 1.0)

    rows = {
        row.loaded_cycle_id: row[DIRECTIONAL_ALERT_COLUMN]
        for row in apply_directional_threshold(view, candidate).collect()
    }

    assert rows == {
        "below": True,
        "boundary": True,
        "above": False,
        "missing": None,
        "nan": None,
        "excluded": None,
    }


def test_directional_candidates_are_fixed_ordered_and_validate_thresholds() -> None:
    assert DIRECTIONAL_BASELINE_VERSION == "motor-current-low-directional-v1"
    assert [
        candidate.maximum_mean_current_amperes for candidate in DIRECTIONAL_THRESHOLD_CANDIDATES
    ] == [
        0.9,
        1.0,
        1.2,
        1.5,
        2.0,
    ]
    assert len({candidate.candidate_id for candidate in DIRECTIONAL_THRESHOLD_CANDIDATES}) == 5

    with pytest.raises(DirectionalBaselineError, match="ID must be nonempty"):
        DirectionalThresholdCandidate(" ", 1.0)
    with pytest.raises(DirectionalBaselineError, match="finite and positive"):
        DirectionalThresholdCandidate("zero", 0.0)
