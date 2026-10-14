from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType

from railpulse.evaluation.modeling_view import ModelingViewError
from railpulse.evaluation.modeling_view_schema import (
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
from railpulse.evaluation.modeling_view_v2 import (
    ModelingViewV2Error,
    build_modeling_view_v2,
)
from railpulse.evaluation.modeling_view_v2_schema import (
    MODELING_VIEW_V2_COLUMNS,
    MODELING_VIEW_V2_VERSION,
    REASON_CYCLE_CONTEXT_UNAVAILABLE,
)
from railpulse.features.failure_horizons import assign_cycle_failure_horizons
from railpulse.features.feature_snapshot_v2 import build_feature_snapshot_v2
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOT_V2_VERSION

_STOPS = {
    "negative": datetime(2020, 1, 1, 7, 0),
    "context-unavailable": datetime(2020, 1, 1, 8, 0),
    "positive": datetime(2020, 1, 1, 9, 59),
    "inside": datetime(2020, 1, 1, 10, 30),
    "censored": datetime(2020, 1, 1, 11, 0),
    "missing": None,
}


def _horizons(spark: SparkSession):
    cycles = spark.createDataFrame(
        [(cycle_id, stop, stop is None) for cycle_id, stop in _STOPS.items()],
        "loaded_cycle_id string, loaded_cycle_stop_timestamp timestamp_ntz, "
        "is_right_censored boolean",
    )
    failure_schema = StructType(
        [
            StructField("record_id", StringType(), nullable=False),
            StructField("source_row", LongType(), nullable=False),
            StructField("failure_start", TimestampNTZType(), nullable=False),
            StructField("failure_end", TimestampNTZType(), nullable=False),
        ]
    )
    failures = spark.createDataFrame(
        [("failure-1", 1, datetime(2020, 1, 1, 10, 15), datetime(2020, 1, 1, 10, 45))],
        schema=failure_schema,
    )
    return assign_cycle_failure_horizons(
        cycles,
        failures,
        horizon_seconds=7_200,
        observation_end=datetime(2020, 1, 1, 12, 0),
    )


def _features(spark: SparkSession):
    rows = []
    for cycle_id, stop in _STOPS.items():
        window_start = None if stop is None else stop - timedelta(minutes=15)
        eligible = cycle_id != "missing"
        rows.append(
            (
                cycle_id,
                "motor-current-15m-v1",
                900,
                window_start,
                "available" if stop is not None else "missing_prediction_boundary",
                91 if stop is not None else 0,
                window_start,
                stop,
                3.0 if stop is not None else None,
                4.0 if stop is not None else None,
                5.0 if stop is not None else None,
                "motor-current-15m-eligibility-v1",
                20,
                10 if stop is not None else None,
                "eligible" if eligible else "ineligible",
                [] if eligible else ["feature_missing_prediction_boundary"],
            )
        )
    return spark.createDataFrame(
        rows,
        "loaded_cycle_id string, motor_current_15m_feature_version string, "
        "motor_current_15m_window_seconds long, motor_current_15m_window_start timestamp_ntz, "
        "motor_current_15m_status string, motor_current_15m_observation_count long, "
        "motor_current_15m_first_observation_timestamp timestamp_ntz, "
        "motor_current_15m_last_observation_timestamp timestamp_ntz, "
        "motor_current_15m_minimum_amperes double, motor_current_15m_mean_amperes double, "
        "motor_current_15m_maximum_amperes double, motor_current_eligibility_version string, "
        "motor_current_eligibility_minimum_excluded_gap_seconds long, "
        "motor_current_maximum_window_gap_seconds long, motor_current_eligibility_status string, "
        "motor_current_eligibility_reasons array<string>",
    )


def _context(spark: SparkSession):
    return spark.createDataFrame(
        [
            (
                cycle_id,
                "loaded-cycle-operating-context-v1",
                (
                    "missing_prediction_boundary"
                    if cycle_id == "missing"
                    else "incomplete_previous_cycle"
                    if cycle_id == "context-unavailable"
                    else "available"
                ),
                None if cycle_id == "missing" else 120,
                None if cycle_id == "missing" else f"previous-{cycle_id}",
                None if cycle_id in {"missing", "context-unavailable"} else 110,
                None if cycle_id in {"missing", "context-unavailable"} else 800,
            )
            for cycle_id in _STOPS
        ],
        "loaded_cycle_id string, cycle_context_feature_version string, "
        "cycle_context_status string, cycle_context_current_duration_seconds long, "
        "cycle_context_previous_cycle_id string, cycle_context_previous_duration_seconds long, "
        "cycle_context_previous_idle_seconds long",
    )


def _snapshots(spark: SparkSession):
    return build_feature_snapshot_v2(
        _features(spark),
        _context(spark),
        dataset_version="fixture-v1",
        telemetry_source_sha256="telemetry-sha",
        telemetry_ingestion_batch_id="telemetry-batch",
    )


def _build(snapshots, horizons):
    return build_modeling_view_v2(
        snapshots,
        horizons,
        failure_dataset_version="fixture-v1",
        failure_source_sha256="failure-sha",
        failure_ingestion_batch_id="failure-batch",
    )


@pytest.mark.spark
def test_modeling_view_v2_assigns_context_eligibility_and_ordered_reasons(
    spark: SparkSession,
) -> None:
    snapshots = _snapshots(spark)
    result = _build(snapshots, _horizons(spark))
    rows = {row.loaded_cycle_id: row for row in result.collect()}

    assert result.columns == list(MODELING_VIEW_V2_COLUMNS)
    assert {rows[key].modeling_row_status for key in ("positive", "negative")} == {STATUS_TRAINABLE}
    assert rows["context-unavailable"].modeling_row_status == STATUS_EXCLUDED
    assert rows["context-unavailable"].modeling_exclusion_reasons == [
        REASON_CYCLE_CONTEXT_UNAVAILABLE
    ]
    assert rows["inside"].modeling_exclusion_reasons == [REASON_INSIDE_FAILURE]
    assert rows["censored"].modeling_exclusion_reasons == [REASON_HORIZON_CENSORED]
    assert rows["missing"].modeling_exclusion_reasons == [
        REASON_FEATURE_INELIGIBLE,
        REASON_CYCLE_CONTEXT_UNAVAILABLE,
        REASON_MISSING_PREDICTION,
    ]
    assert {row.modeling_view_version for row in rows.values()} == {MODELING_VIEW_V2_VERSION}
    assert {row.feature_snapshot_version for row in rows.values()} == {FEATURE_SNAPSHOT_V2_VERSION}
    assert rows["positive"].cycle_context_previous_idle_seconds == 800
    assert snapshots.columns != result.columns


@pytest.mark.spark
def test_modeling_view_v2_rejects_invalid_context_and_inherits_v1_guards(
    spark: SparkSession,
) -> None:
    snapshots = _snapshots(spark)
    horizons = _horizons(spark)

    with pytest.raises(ModelingViewV2Error, match="missing modelling-view columns"):
        _build(snapshots.drop("cycle_context_status"), horizons)

    unsupported_snapshot = snapshots.withColumn("feature_snapshot_version", F.lit("unsupported"))
    with pytest.raises(ModelingViewV2Error, match="incompatible version or cycle-context"):
        _build(unsupported_snapshot, horizons)

    invalid_context = snapshots.withColumn(
        "cycle_context_previous_idle_seconds",
        F.when(F.col("loaded_cycle_id") == "positive", F.lit(None).cast("long")).otherwise(
            F.col("cycle_context_previous_idle_seconds")
        ),
    )
    with pytest.raises(ModelingViewV2Error, match="incompatible version or cycle-context"):
        _build(invalid_context, horizons)

    with pytest.raises(ModelingViewError, match="do not cover every feature-snapshot key"):
        _build(snapshots, horizons.where(F.col("loaded_cycle_id") != "negative"))
