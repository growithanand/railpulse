from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType

from railpulse.evaluation.modeling_view import ModelingViewError, build_modeling_view
from railpulse.evaluation.modeling_view_schema import (
    MODELING_VIEW_COLUMNS,
    MODELING_VIEW_VERSION,
    REASON_FEATURE_INELIGIBLE,
    REASON_HORIZON_CENSORED,
    REASON_INSIDE_FAILURE,
    REASON_MISSING_PREDICTION,
    REASON_UNSUPPORTED_HORIZON_STATUS,
    STATUS_EXCLUDED,
    STATUS_TRAINABLE,
)
from railpulse.features.failure_horizons import assign_cycle_failure_horizons
from railpulse.features.feature_snapshot import build_feature_snapshot


def _horizons(spark: SparkSession):
    cycles = spark.createDataFrame(
        [
            ("negative", datetime(2020, 1, 1, 7, 0), False),
            ("positive", datetime(2020, 1, 1, 9, 59), False),
            ("inside", datetime(2020, 1, 1, 10, 30), False),
            ("censored", datetime(2020, 1, 1, 11, 0), False),
            ("missing", None, True),
        ],
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


def _snapshots(spark: SparkSession):
    stops = {
        "negative": datetime(2020, 1, 1, 7, 0),
        "positive": datetime(2020, 1, 1, 9, 59),
        "inside": datetime(2020, 1, 1, 10, 30),
        "censored": datetime(2020, 1, 1, 11, 0),
        "missing": None,
    }
    rows = []
    for cycle_id, stop in stops.items():
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
    features = spark.createDataFrame(
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
    return build_feature_snapshot(
        features,
        dataset_version="fixture-v1",
        telemetry_source_sha256="telemetry-sha",
        telemetry_ingestion_batch_id="telemetry-batch",
    )


def _build(snapshots, horizons):
    return build_modeling_view(
        snapshots,
        horizons,
        failure_dataset_version="fixture-v1",
        failure_source_sha256="failure-sha",
        failure_ingestion_batch_id="failure-batch",
    )


@pytest.mark.spark
def test_modeling_view_assigns_trainable_and_ordered_exclusion_reasons(
    spark: SparkSession,
) -> None:
    result = _build(_snapshots(spark), _horizons(spark))
    rows = {row.loaded_cycle_id: row for row in result.collect()}

    assert result.columns == list(MODELING_VIEW_COLUMNS)
    assert {rows[key].modeling_row_status for key in ("positive", "negative")} == {STATUS_TRAINABLE}
    assert rows["positive"].failure_within_horizon is True
    assert rows["negative"].failure_within_horizon is False
    assert rows["inside"].modeling_exclusion_reasons == [REASON_INSIDE_FAILURE]
    assert rows["censored"].modeling_exclusion_reasons == [REASON_HORIZON_CENSORED]
    assert rows["missing"].modeling_exclusion_reasons == [
        REASON_FEATURE_INELIGIBLE,
        REASON_MISSING_PREDICTION,
    ]
    assert {rows[key].modeling_row_status for key in ("inside", "censored", "missing")} == {
        STATUS_EXCLUDED
    }
    assert {row.modeling_view_version for row in rows.values()} == {MODELING_VIEW_VERSION}
    assert {row.failure_source_sha256 for row in rows.values()} == {"failure-sha"}


@pytest.mark.spark
def test_modeling_view_rejects_key_boundary_version_and_lineage_mismatches(
    spark: SparkSession,
) -> None:
    snapshots = _snapshots(spark)
    horizons = _horizons(spark)

    with pytest.raises(ModelingViewError, match="do not cover every feature-snapshot key"):
        _build(snapshots, horizons.where(F.col("loaded_cycle_id") != "negative"))

    shifted = horizons.withColumn(
        "prediction_timestamp",
        F.when(
            F.col("loaded_cycle_id") == "positive",
            F.timestamp_add("SECOND", F.lit(1), F.col("prediction_timestamp")),
        ).otherwise(F.col("prediction_timestamp")),
    )
    with pytest.raises(ModelingViewError, match="does not align"):
        _build(snapshots, shifted)

    unsupported = horizons.withColumn("failure_horizon_version", F.lit("unsupported"))
    with pytest.raises(ModelingViewError, match="incompatible dataset or versions"):
        _build(snapshots, unsupported)

    unknown_status = horizons.withColumn(
        "failure_horizon_status",
        F.when(F.col("loaded_cycle_id") == "censored", F.lit("future_status")).otherwise(
            F.col("failure_horizon_status")
        ),
    )
    unknown_row = (
        _build(snapshots, unknown_status).where(F.col("loaded_cycle_id") == "censored").first()
    )
    assert unknown_row.modeling_row_status == STATUS_EXCLUDED
    assert unknown_row.modeling_exclusion_reasons == [REASON_UNSUPPORTED_HORIZON_STATUS]

    with pytest.raises(ModelingViewError, match="nonempty strings"):
        build_modeling_view(
            snapshots,
            horizons,
            failure_dataset_version="fixture-v1",
            failure_source_sha256="",
            failure_ingestion_batch_id="failure-batch",
        )
