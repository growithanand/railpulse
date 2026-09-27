from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.features.cycle_operating_context import (
    CYCLE_OPERATING_CONTEXT_COLUMNS,
    CYCLE_OPERATING_CONTEXT_VERSION,
    STATUS_AVAILABLE,
    STATUS_INCOMPLETE_CURRENT,
    STATUS_INCOMPLETE_PREVIOUS,
    STATUS_INVALID_PREVIOUS_INTERVAL,
    STATUS_LEFT_CENSORED_CURRENT,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREVIOUS,
    CycleOperatingContextError,
    add_cycle_operating_context,
)


def _cycles(spark: SparkSession, *, include_future: bool):
    rows = [
        ("first", "observed", datetime(2020, 1, 1, 0, 0), datetime(2020, 1, 1, 0, 2), 120, False),
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
    ]
    if include_future:
        rows.append(
            (
                "future",
                "observed",
                datetime(2020, 1, 1, 0, 30),
                datetime(2020, 1, 1, 0, 31),
                60,
                False,
            )
        )
    return spark.createDataFrame(
        rows,
        "loaded_cycle_id string, loaded_cycle_start_type string, "
        "loaded_cycle_start_timestamp timestamp_ntz, "
        "loaded_cycle_stop_timestamp timestamp_ntz, observed_duration_seconds long, "
        "is_right_censored boolean",
    )


@pytest.mark.spark
def test_cycle_operating_context_is_past_only_and_marks_incomplete_rows(
    spark: SparkSession,
) -> None:
    baseline = add_cycle_operating_context(_cycles(spark, include_future=False))
    with_future = add_cycle_operating_context(_cycles(spark, include_future=True))
    baseline_rows = {row.loaded_cycle_id: row for row in baseline.collect()}
    rows = {row.loaded_cycle_id: row for row in with_future.collect()}

    assert tuple(with_future.columns[-len(CYCLE_OPERATING_CONTEXT_COLUMNS) :]) == (
        CYCLE_OPERATING_CONTEXT_COLUMNS
    )
    assert rows["complete"].cycle_context_feature_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert rows["complete"].cycle_context_status == STATUS_AVAILABLE
    assert rows["complete"].cycle_context_current_duration_seconds == 180
    assert rows["complete"].cycle_context_previous_cycle_id == "first"
    assert rows["complete"].cycle_context_previous_duration_seconds == 120
    assert rows["complete"].cycle_context_previous_idle_seconds == 180

    assert rows["first"].cycle_context_status == STATUS_MISSING_PREVIOUS
    assert rows["first"].cycle_context_current_duration_seconds == 120
    assert rows["first"].cycle_context_previous_cycle_id is None
    assert rows["left"].cycle_context_status == STATUS_LEFT_CENSORED_CURRENT
    assert rows["left"].cycle_context_current_duration_seconds is None
    assert rows["after-left"].cycle_context_status == STATUS_INCOMPLETE_PREVIOUS
    assert rows["after-left"].cycle_context_previous_cycle_id == "left"
    assert rows["after-left"].cycle_context_previous_duration_seconds is None
    assert rows["after-left"].cycle_context_previous_idle_seconds == 180
    assert rows["open"].cycle_context_status == STATUS_MISSING_PREDICTION
    assert rows["open"].cycle_context_current_duration_seconds is None

    for cycle_id, baseline_row in baseline_rows.items():
        assert rows[cycle_id].asDict() == baseline_row.asDict()


@pytest.mark.spark
def test_cycle_operating_context_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    cycles = _cycles(spark, include_future=False)

    with pytest.raises(CycleOperatingContextError, match="missing operating-context"):
        add_cycle_operating_context(cycles.drop("loaded_cycle_start_type"))
    with pytest.raises(CycleOperatingContextError, match="already contain"):
        add_cycle_operating_context(cycles.withColumn("cycle_context_status", F.lit("available")))


@pytest.mark.spark
def test_cycle_operating_context_marks_inconsistent_cycle_evidence(
    spark: SparkSession,
) -> None:
    cycles = spark.createDataFrame(
        [
            (
                "previous",
                "observed",
                datetime(2020, 1, 1, 0, 0),
                datetime(2020, 1, 1, 0, 10),
                600,
                False,
            ),
            (
                "overlap",
                "observed",
                datetime(2020, 1, 1, 0, 5),
                datetime(2020, 1, 1, 0, 6),
                60,
                False,
            ),
            (
                "incomplete",
                "observed",
                datetime(2020, 1, 1, 0, 20),
                datetime(2020, 1, 1, 0, 21),
                None,
                False,
            ),
        ],
        "loaded_cycle_id string, loaded_cycle_start_type string, "
        "loaded_cycle_start_timestamp timestamp_ntz, "
        "loaded_cycle_stop_timestamp timestamp_ntz, observed_duration_seconds long, "
        "is_right_censored boolean",
    )
    rows = {row.loaded_cycle_id: row for row in add_cycle_operating_context(cycles).collect()}

    assert rows["overlap"].cycle_context_status == STATUS_INVALID_PREVIOUS_INTERVAL
    assert rows["overlap"].cycle_context_previous_idle_seconds is None
    assert rows["incomplete"].cycle_context_status == STATUS_INCOMPLETE_CURRENT
    assert rows["incomplete"].cycle_context_current_duration_seconds is None
