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

from railpulse.features.pressure_context import (
    PRESSURE_CONTEXT_FEATURE_COLUMNS,
    PRESSURE_CONTEXT_FEATURE_VERSION,
    PressureContextError,
    add_pressure_context_15m_features,
)
from railpulse.features.temporal_features import (
    STATUS_AVAILABLE,
    STATUS_MISSING_PREDICTION,
    STATUS_MISSING_PREDICTION_OBSERVATION,
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
            StructField("tp3", DoubleType(), nullable=False),
            StructField("reservoirs", DoubleType(), nullable=False),
        ]
    )
    rows = [
        (datetime(2020, 1, 1, 9, 45), 10.0, 1.0),
        (datetime(2020, 1, 1, 9, 45, 1), 9.0, 8.5),
        (datetime(2020, 1, 1, 9, 59, 59), 8.0, 7.0),
        (datetime(2020, 1, 1, 10, 0), 7.0, 6.8),
    ]
    if include_future:
        rows.append((datetime(2020, 1, 1, 10, 0, 1), 10.0, 1.0))
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.spark
def test_pressure_context_is_past_only_and_respects_exact_boundaries(
    spark: SparkSession,
) -> None:
    baseline = add_pressure_context_15m_features(
        _cycles(spark), _telemetry(spark, include_future=False)
    )
    with_future = add_pressure_context_15m_features(
        _cycles(spark), _telemetry(spark, include_future=True)
    )
    baseline_row = baseline.where(F.col("loaded_cycle_id") == "observed").first()
    rows = {row.loaded_cycle_id: row for row in with_future.collect()}
    observed = rows["observed"]

    assert tuple(with_future.columns[-len(PRESSURE_CONTEXT_FEATURE_COLUMNS) :]) == (
        PRESSURE_CONTEXT_FEATURE_COLUMNS
    )
    assert observed.pressure_context_15m_feature_version == PRESSURE_CONTEXT_FEATURE_VERSION
    assert observed.pressure_context_15m_window_seconds == 900
    assert observed.pressure_context_15m_window_start == datetime(2020, 1, 1, 9, 45)
    assert observed.pressure_context_15m_status == STATUS_AVAILABLE
    assert observed.pressure_context_15m_observation_count == 3
    assert observed.pressure_context_15m_first_observation_timestamp == datetime(
        2020, 1, 1, 9, 45, 1
    )
    assert observed.pressure_context_15m_last_observation_timestamp == datetime(2020, 1, 1, 10, 0)
    assert observed.pressure_context_15m_mean_difference_bar == pytest.approx(1.7 / 3)
    assert observed.pressure_context_15m_mean_absolute_difference_bar == pytest.approx(1.7 / 3)
    assert observed.pressure_context_15m_maximum_absolute_difference_bar == 1.0
    assert observed.asDict() == baseline_row.asDict()

    unmatched = rows["unmatched"]
    assert unmatched.pressure_context_15m_status == STATUS_MISSING_PREDICTION_OBSERVATION
    assert unmatched.pressure_context_15m_observation_count == 0
    assert unmatched.pressure_context_15m_mean_difference_bar is None

    open_cycle = rows["open"]
    assert open_cycle.pressure_context_15m_status == STATUS_MISSING_PREDICTION
    assert open_cycle.pressure_context_15m_window_start is None
    assert open_cycle.pressure_context_15m_observation_count == 0


@pytest.mark.spark
def test_pressure_context_rejects_missing_or_conflicting_columns(spark: SparkSession) -> None:
    cycles = _cycles(spark)
    telemetry = _telemetry(spark, include_future=False)

    with pytest.raises(PressureContextError, match="Cycle horizons are missing"):
        add_pressure_context_15m_features(cycles.drop("prediction_timestamp"), telemetry)
    with pytest.raises(PressureContextError, match="Accepted telemetry is missing"):
        add_pressure_context_15m_features(cycles, telemetry.drop("reservoirs"))
    with pytest.raises(PressureContextError, match="already contain"):
        add_pressure_context_15m_features(
            cycles.withColumn("pressure_context_15m_mean_difference_bar", F.lit(0.1)),
            telemetry,
        )
