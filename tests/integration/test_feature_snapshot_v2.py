from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.features.feature_snapshot import build_feature_snapshot
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
)
from railpulse.features.feature_snapshot_v2 import (
    FeatureSnapshotV2Error,
    build_feature_snapshot_v2,
)
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_COLUMNS,
    FEATURE_SNAPSHOT_V2_VERSION,
)


def _features(spark: SparkSession):
    timestamp = datetime(2020, 4, 1, 12, 0)
    return spark.createDataFrame(
        [
            (
                "cycle-1",
                "motor-current-15m-v1",
                900,
                timestamp,
                "available",
                91,
                timestamp,
                timestamp,
                3.0,
                4.0,
                5.0,
                "motor-current-15m-eligibility-v1",
                20,
                12,
                "eligible",
                [],
                "positive",
            ),
            (
                "cycle-2",
                "motor-current-15m-v1",
                900,
                timestamp,
                "available",
                75,
                timestamp,
                timestamp,
                2.0,
                3.0,
                4.0,
                "motor-current-15m-eligibility-v1",
                20,
                120,
                "ineligible",
                ["material_window_gap"],
                "negative",
            ),
        ],
        [
            "loaded_cycle_id",
            "motor_current_15m_feature_version",
            "motor_current_15m_window_seconds",
            "motor_current_15m_window_start",
            "motor_current_15m_status",
            "motor_current_15m_observation_count",
            "motor_current_15m_first_observation_timestamp",
            "motor_current_15m_last_observation_timestamp",
            "motor_current_15m_minimum_amperes",
            "motor_current_15m_mean_amperes",
            "motor_current_15m_maximum_amperes",
            "motor_current_eligibility_version",
            "motor_current_eligibility_minimum_excluded_gap_seconds",
            "motor_current_maximum_window_gap_seconds",
            "motor_current_eligibility_status",
            "motor_current_eligibility_reasons",
            "failure_horizon_status",
        ],
    )


def _context(spark: SparkSession):
    return spark.createDataFrame(
        [
            (
                "cycle-1",
                "loaded-cycle-operating-context-v1",
                "available",
                120,
                "previous-cycle",
                110,
                800,
                "positive",
            ),
            (
                "cycle-2",
                "loaded-cycle-operating-context-v1",
                "left_censored_current_cycle",
                None,
                "cycle-1",
                120,
                None,
                "negative",
            ),
        ],
        "loaded_cycle_id string, cycle_context_feature_version string, "
        "cycle_context_status string, cycle_context_current_duration_seconds long, "
        "cycle_context_previous_cycle_id string, "
        "cycle_context_previous_duration_seconds long, "
        "cycle_context_previous_idle_seconds long, failure_horizon_status string",
    )


def _arguments() -> dict[str, str]:
    return {
        "dataset_version": "fixture-v1",
        "telemetry_source_sha256": "source-sha",
        "telemetry_ingestion_batch_id": "batch-id",
    }


@pytest.mark.spark
def test_build_snapshot_v2_adds_context_without_labels_or_changing_v1(
    spark: SparkSession,
) -> None:
    snapshot = build_feature_snapshot_v2(_features(spark), _context(spark), **_arguments())
    rows = {row.loaded_cycle_id: row for row in snapshot.collect()}

    assert snapshot.columns == list(FEATURE_SNAPSHOT_V2_COLUMNS)
    assert "failure_horizon_status" not in snapshot.columns
    assert rows["cycle-1"].cycle_context_previous_idle_seconds == 800
    assert rows["cycle-2"].cycle_context_current_duration_seconds is None
    assert {row.feature_snapshot_version for row in rows.values()} == {FEATURE_SNAPSHOT_V2_VERSION}

    version_one = build_feature_snapshot(_features(spark), **_arguments())
    assert version_one.columns == list(FEATURE_SNAPSHOT_COLUMNS)
    assert {row.feature_snapshot_version for row in version_one.collect()} == {
        FEATURE_SNAPSHOT_VERSION
    }


@pytest.mark.spark
def test_build_snapshot_v2_rejects_invalid_or_unmatched_context(
    spark: SparkSession,
) -> None:
    features = _features(spark)
    context = _context(spark)

    with pytest.raises(FeatureSnapshotV2Error, match="missing snapshot v2 columns"):
        build_feature_snapshot_v2(
            features,
            context.drop("cycle_context_status"),
            **_arguments(),
        )
    duplicate = context.unionByName(context.limit(1))
    with pytest.raises(FeatureSnapshotV2Error, match="duplicate:"):
        build_feature_snapshot_v2(features, duplicate, **_arguments())
    invalid_version = context.withColumn("cycle_context_feature_version", F.lit("unsupported"))
    with pytest.raises(FeatureSnapshotV2Error, match="do not satisfy"):
        build_feature_snapshot_v2(features, invalid_version, **_arguments())
    unmatched = context.where(F.col("loaded_cycle_id") == "cycle-1").unionByName(
        context.where(F.col("loaded_cycle_id") == "cycle-2").withColumn(
            "loaded_cycle_id", F.lit("cycle-other")
        )
    )
    with pytest.raises(FeatureSnapshotV2Error, match="do not join one-to-one"):
        build_feature_snapshot_v2(features, unmatched, **_arguments())
