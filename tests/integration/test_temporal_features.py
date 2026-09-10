from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from railpulse.features.temporal_features import (
    MOTOR_CURRENT_FEATURE_COLUMNS,
    MOTOR_CURRENT_FEATURE_VERSION,
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
    TemporalFeatureError,
    add_motor_current_15m_features,
)


def _cycles(spark: SparkSession):
    schema = StructType(
        [
            StructField("loaded_cycle_id", StringType(), nullable=False),
            StructField("prediction_timestamp", TimestampNTZType(), nullable=True),
        ]
    )
    return spark.createDataFrame(
        [
            ("observed", datetime(2020, 1, 1, 10, 0)),
            ("unmatched", datetime(2020, 1, 1, 11, 0)),
            ("open", None),
        ],
        schema=schema,
    )


def _telemetry(spark: SparkSession, *, include_future: bool):
    schema = StructType(
        [
            StructField("event_timestamp", TimestampNTZType(), nullable=False),
            StructField("motor_current", DoubleType(), nullable=False),
        ]
    )
    rows = [
        (datetime(2020, 1, 1, 9, 44, 59), 8.0),
        (datetime(2020, 1, 1, 9, 45, 0), 9.0),
        (datetime(2020, 1, 1, 9, 45, 1), 2.0),
        (datetime(2020, 1, 1, 9, 59, 59), 4.0),
        (datetime(2020, 1, 1, 10, 0), 6.0),
    ]
    if include_future:
        rows.append((datetime(2020, 1, 1, 10, 0, 1), 9.2))
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_motor_current_window_is_past_only_and_respects_exact_boundaries(
    spark: SparkSession,
) -> None:
    baseline = add_motor_current_15m_features(
        _cycles(spark),
        _telemetry(spark, include_future=False),
    )
    with_future = add_motor_current_15m_features(
        _cycles(spark),
        _telemetry(spark, include_future=True),
    )
    baseline_row = baseline.where(F.col("loaded_cycle_id") == "observed").first()
    rows = {row.loaded_cycle_id: row for row in with_future.collect()}
    observed = rows["observed"]

    assert tuple(with_future.columns[-len(MOTOR_CURRENT_FEATURE_COLUMNS) :]) == (
        MOTOR_CURRENT_FEATURE_COLUMNS
    )
    assert observed.motor_current_15m_feature_version == MOTOR_CURRENT_FEATURE_VERSION
    assert observed.motor_current_15m_window_seconds == 900
    assert observed.motor_current_15m_window_start == datetime(2020, 1, 1, 9, 45)
    assert observed.motor_current_15m_status == STATUS_AVAILABLE
    assert observed.motor_current_15m_observation_count == 3
    assert observed.motor_current_15m_first_observation_timestamp == datetime(2020, 1, 1, 9, 45, 1)
    assert observed.motor_current_15m_last_observation_timestamp == datetime(2020, 1, 1, 10, 0)
    assert observed.motor_current_15m_minimum_amperes == 2.0
    assert observed.motor_current_15m_mean_amperes == 4.0
    assert observed.motor_current_15m_maximum_amperes == 6.0
    assert observed.asDict() == baseline_row.asDict()

    unmatched = rows["unmatched"]
    assert unmatched.motor_current_15m_status == STATUS_MISSING_PREDICTION_OBSERVATION
    assert unmatched.motor_current_15m_observation_count == 0
    assert unmatched.motor_current_15m_mean_amperes is None

    open_cycle = rows["open"]
    assert open_cycle.motor_current_15m_status == STATUS_MISSING_PREDICTION
    assert open_cycle.motor_current_15m_window_start is None
    assert open_cycle.motor_current_15m_observation_count == 0


@pytest.mark.spark
def test_motor_current_window_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    cycles = _cycles(spark)
    telemetry = _telemetry(spark, include_future=False)

    with pytest.raises(TemporalFeatureError, match="Cycle horizons are missing"):
        add_motor_current_15m_features(cycles.drop("prediction_timestamp"), telemetry)
    with pytest.raises(TemporalFeatureError, match="Accepted telemetry is missing"):
        add_motor_current_15m_features(cycles, telemetry.drop("motor_current"))
    with pytest.raises(TemporalFeatureError, match="already contain"):
        add_motor_current_15m_features(
            cycles.withColumn("motor_current_15m_mean_amperes", F.lit(4.0)),
            telemetry,
        )
