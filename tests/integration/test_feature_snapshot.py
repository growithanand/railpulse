from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.features.feature_snapshot import FeatureSnapshotError, build_feature_snapshot
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
)


def _features(spark: SparkSession):
    timestamp = datetime(2020, 4, 1, 12, 0)
    rows = [
        (
            "cycle-eligible",
            "motor-current-15m-v1",
            900,
            timestamp,
            "available",
            91,
            timestamp,
            timestamp,
            3.1,
            4.2,
            5.3,
            "motor-current-15m-eligibility-v1",
            20,
            12,
            "eligible",
            [],
            "positive",
        ),
        (
            "cycle-ineligible",
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
    ]
    columns = [
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
    ]
    return spark.createDataFrame(rows, columns)


@pytest.mark.spark
def test_build_feature_snapshot_selects_contract_columns_and_lineage(
    spark: SparkSession,
) -> None:
    snapshot = build_feature_snapshot(
        _features(spark),
        dataset_version="fixture-v1",
        telemetry_source_sha256="source-sha",
        telemetry_ingestion_batch_id="batch-id",
    )
    rows = {row.loaded_cycle_id: row for row in snapshot.collect()}

    assert snapshot.columns == list(FEATURE_SNAPSHOT_COLUMNS)
    assert "failure_horizon_status" not in snapshot.columns
    assert set(rows) == {"cycle-eligible", "cycle-ineligible"}
    assert rows["cycle-eligible"].motor_current_eligibility_status == "eligible"
    assert rows["cycle-ineligible"].motor_current_eligibility_reasons == ["material_window_gap"]
    assert {row.feature_snapshot_version for row in rows.values()} == {FEATURE_SNAPSHOT_VERSION}
    assert {row.dataset_version for row in rows.values()} == {"fixture-v1"}
    assert {row.telemetry_source_sha256 for row in rows.values()} == {"source-sha"}
    assert {row.telemetry_ingestion_batch_id for row in rows.values()} == {"batch-id"}


@pytest.mark.spark
def test_build_feature_snapshot_rejects_invalid_shape_keys_versions_and_lineage(
    spark: SparkSession,
) -> None:
    features = _features(spark)
    arguments = {
        "dataset_version": "fixture-v1",
        "telemetry_source_sha256": "source-sha",
        "telemetry_ingestion_batch_id": "batch-id",
    }

    with pytest.raises(FeatureSnapshotError, match="missing snapshot columns"):
        build_feature_snapshot(features.drop("motor_current_15m_mean_amperes"), **arguments)

    with pytest.raises(FeatureSnapshotError, match="already contain snapshot lineage"):
        build_feature_snapshot(features.withColumn("dataset_version", F.lit("old")), **arguments)

    duplicate = features.unionByName(features.where(F.col("loaded_cycle_id") == "cycle-eligible"))
    with pytest.raises(FeatureSnapshotError, match="duplicate: cycle-eligible"):
        build_feature_snapshot(duplicate, **arguments)

    null_key = features.unionByName(
        features.limit(1).withColumn("loaded_cycle_id", F.lit(None).cast("string"))
    )
    with pytest.raises(FeatureSnapshotError, match="keys must not be null"):
        build_feature_snapshot(null_key, **arguments)

    invalid_version = features.withColumn("motor_current_eligibility_version", F.lit("unsupported"))
    with pytest.raises(FeatureSnapshotError, match="component versions"):
        build_feature_snapshot(invalid_version, **arguments)

    with pytest.raises(FeatureSnapshotError, match="nonempty strings"):
        build_feature_snapshot(features, **{**arguments, "telemetry_source_sha256": ""})
