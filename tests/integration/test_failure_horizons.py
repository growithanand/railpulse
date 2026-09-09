from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.features.failure_horizons import (
    DEFAULT_FAILURE_HORIZON_SECONDS,
    FAILURE_HORIZON_VERSION,
    STATUS_HORIZON_CENSORED,
    STATUS_INSIDE_FAILURE,
    STATUS_MISSING_PREDICTION,
    STATUS_NEGATIVE,
    STATUS_POSITIVE,
    FailureHorizonError,
    assign_cycle_failure_horizons,
)


def _cycle_frame(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("loaded_cycle_stop_timestamp", TimestampNTZType(), nullable=True),
            StructField("is_right_censored", BooleanType(), nullable=False),
        ]
    )
    rows = [
        ("just-before-failure", datetime(2020, 1, 1, 9, 59, 59), False),
        ("at-failure-start", datetime(2020, 1, 1, 10, 0), False),
        ("at-upper-bound", datetime(2020, 1, 2, 10, 0), False),
        ("after-upper-bound", datetime(2020, 1, 3, 10, 0), False),
        ("positive-before-observation-end", datetime(2020, 1, 9, 23, 0), False),
        ("incomplete-horizon", datetime(2020, 1, 10, 0, 20), False),
        ("open-cycle", None, True),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _failure_frame(spark: SparkSession):
    schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_row", LongType(), nullable=False),
            StructField("failure_start", TimestampNTZType(), nullable=False),
            StructField("failure_end", TimestampNTZType(), nullable=False),
        ]
    )
    rows = [
        ("failure-tie-second", 2, datetime(2020, 1, 1, 10, 0), datetime(2020, 1, 1, 11, 0)),
        ("failure-tie-first", 1, datetime(2020, 1, 1, 10, 0), datetime(2020, 1, 1, 11, 0)),
        ("failure-at-upper", 3, datetime(2020, 1, 2, 12, 0), datetime(2020, 1, 2, 12, 30)),
        (
            "failure-after-upper",
            4,
            datetime(2020, 1, 3, 12, 0, 1),
            datetime(2020, 1, 3, 12, 30),
        ),
        (
            "failure-before-observation-end",
            5,
            datetime(2020, 1, 10, 0, 15),
            datetime(2020, 1, 10, 0, 19),
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_failure_horizon_contract_handles_temporal_boundaries_and_censoring(
    spark: SparkSession,
) -> None:
    result = assign_cycle_failure_horizons(
        _cycle_frame(spark),
        _failure_frame(spark),
        horizon_seconds=DEFAULT_FAILURE_HORIZON_SECONDS,
        observation_end=datetime(2020, 1, 10, 0, 30),
    )
    rows = {row.loaded_cycle_id: row for row in result.collect()}

    just_before = rows["just-before-failure"]
    assert just_before.failure_horizon_version == FAILURE_HORIZON_VERSION
    assert just_before.failure_horizon_seconds == 7_200
    assert just_before.failure_horizon_status == STATUS_POSITIVE
    assert just_before.failure_within_horizon is True
    assert just_before.matched_failure_record_id == "failure-tie-first"
    assert just_before.matched_failure_source_row == 1
    assert just_before.seconds_to_failure == 1

    at_start = rows["at-failure-start"]
    assert at_start.failure_horizon_status == STATUS_INSIDE_FAILURE
    assert at_start.failure_within_horizon is None
    assert at_start.matched_failure_record_id is None
    assert at_start.seconds_to_failure is None

    upper = rows["at-upper-bound"]
    assert upper.failure_horizon_end == datetime(2020, 1, 2, 12, 0)
    assert upper.failure_horizon_status == STATUS_POSITIVE
    assert upper.matched_failure_record_id == "failure-at-upper"
    assert upper.seconds_to_failure == 7_200

    after_upper = rows["after-upper-bound"]
    assert after_upper.failure_horizon_status == STATUS_NEGATIVE
    assert after_upper.failure_within_horizon is False
    assert after_upper.matched_failure_record_id is None

    partial_positive = rows["positive-before-observation-end"]
    assert partial_positive.label_observation_end == datetime(2020, 1, 10, 0, 30)
    assert partial_positive.failure_horizon_end > partial_positive.label_observation_end
    assert partial_positive.failure_horizon_status == STATUS_POSITIVE
    assert partial_positive.matched_failure_record_id == "failure-before-observation-end"

    incomplete = rows["incomplete-horizon"]
    assert incomplete.failure_horizon_status == STATUS_HORIZON_CENSORED
    assert incomplete.failure_within_horizon is None

    open_cycle = rows["open-cycle"]
    assert open_cycle.failure_horizon_status == STATUS_MISSING_PREDICTION
    assert open_cycle.prediction_timestamp is None
    assert open_cycle.failure_horizon_end is None
    assert open_cycle.failure_within_horizon is None
    assert len(rows) == 7


@pytest.mark.spark
def test_failure_horizon_contract_rejects_invalid_inputs(spark: SparkSession) -> None:
    cycles = _cycle_frame(spark)
    failures = _failure_frame(spark)

    with pytest.raises(FailureHorizonError, match="Gold cycles are missing"):
        assign_cycle_failure_horizons(
            cycles.drop("loaded_cycle_id"),
            failures,
            horizon_seconds=7_200,
            observation_end=datetime(2020, 1, 10),
        )
    with pytest.raises(FailureHorizonError, match="Accepted failure events are missing"):
        assign_cycle_failure_horizons(
            cycles,
            failures.drop("failure_end"),
            horizon_seconds=7_200,
            observation_end=datetime(2020, 1, 10),
        )
    with pytest.raises(FailureHorizonError, match="already contain"):
        assign_cycle_failure_horizons(
            cycles.withColumn("prediction_timestamp", F.lit(None)),
            failures,
            horizon_seconds=7_200,
            observation_end=datetime(2020, 1, 10),
        )
    with pytest.raises(FailureHorizonError, match="must be positive"):
        assign_cycle_failure_horizons(
            cycles,
            failures,
            horizon_seconds=0,
            observation_end=datetime(2020, 1, 10),
        )
    with pytest.raises(FailureHorizonError, match="timezone-free"):
        assign_cycle_failure_horizons(
            cycles,
            failures,
            horizon_seconds=7_200,
            observation_end=datetime(2020, 1, 10, tzinfo=UTC),
        )
