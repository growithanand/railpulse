from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.cycle_operating_context_profile import (
    CYCLE_OPERATING_CONTEXT_PROFILE_VERSION,
    collect_cycle_operating_context_profile,
)
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION


def _cycles(spark: SparkSession):
    return spark.createDataFrame(
        [
            (
                "first",
                "observed",
                datetime(2020, 1, 1, 0, 0),
                datetime(2020, 1, 1, 0, 2),
                120,
                False,
            ),
            (
                "complete",
                "observed",
                datetime(2020, 1, 1, 0, 5),
                datetime(2020, 1, 1, 0, 8),
                180,
                False,
            ),
            (
                "left",
                "left_censored",
                datetime(2020, 1, 1, 0, 10),
                datetime(2020, 1, 1, 0, 12),
                120,
                False,
            ),
            (
                "after-left",
                "observed",
                datetime(2020, 1, 1, 0, 15),
                datetime(2020, 1, 1, 0, 16),
                60,
                False,
            ),
            ("open", "observed", datetime(2020, 1, 1, 0, 20), None, None, True),
        ],
        "loaded_cycle_id string, loaded_cycle_start_type string, "
        "loaded_cycle_start_timestamp timestamp_ntz, "
        "loaded_cycle_stop_timestamp timestamp_ntz, observed_duration_seconds long, "
        "is_right_censored boolean",
    )


@pytest.mark.spark
def test_cycle_operating_context_profile_reconciles_statuses_and_distributions(
    spark: SparkSession,
) -> None:
    profile = collect_cycle_operating_context_profile(
        _cycles(spark),
        dataset_version="fixture-v1",
    )
    statuses = {item.status: item.cycle_count for item in profile.status_counts}
    distributions = {item.component: item for item in profile.distributions}

    assert profile.profile_version == CYCLE_OPERATING_CONTEXT_PROFILE_VERSION
    assert profile.feature_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert profile.cycle_id_version == LOADED_CYCLE_ID_VERSION
    assert profile.dataset_version == "fixture-v1"
    assert profile.cycle_count == 5
    assert statuses == {
        "available": 1,
        "missing_prediction_boundary": 1,
        "left_censored_current_cycle": 1,
        "incomplete_current_cycle": 0,
        "missing_previous_cycle": 1,
        "incomplete_previous_cycle": 1,
        "invalid_previous_interval": 0,
    }

    current = distributions["current_duration_seconds"]
    assert current.non_null_cycle_count == 3
    assert current.minimum_seconds == 60
    assert current.median_seconds == 120
    assert current.maximum_seconds == 180
    previous = distributions["previous_duration_seconds"]
    assert previous.non_null_cycle_count == 3
    assert previous.minimum_seconds == 60
    assert previous.median_seconds == 120
    assert previous.maximum_seconds == 180
    idle = distributions["previous_idle_seconds"]
    assert idle.non_null_cycle_count == 2
    assert idle.minimum_seconds == 180
    assert idle.median_seconds == 180
    assert idle.maximum_seconds == 180
