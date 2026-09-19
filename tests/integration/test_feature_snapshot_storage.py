from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_storage import gold_table_path
from railpulse.features.feature_snapshot import build_feature_snapshot
from railpulse.features.feature_snapshot_schema import FEATURE_SNAPSHOTS_TABLE
from railpulse.features.feature_snapshot_storage import (
    FeatureSnapshotPersistenceError,
    persist_feature_snapshots,
)


def _test_config(tmp_path: Path) -> RailPulseConfig:
    return RailPulseConfig(
        name="railpulse-test",
        environment="test",
        dataset_version="fixture-v1",
        project_root=tmp_path,
        paths=StoragePaths(
            raw_data=tmp_path / "raw",
            processed_data=tmp_path / "processed",
            delta=tmp_path / "delta",
            checkpoints=tmp_path / "checkpoints",
            artifacts=tmp_path / "artifacts",
        ),
        schemas=SchemaNames(
            catalog="railpulse_test", bronze="bronze", silver="silver", gold="gold"
        ),
    )


def _snapshots(spark: SparkSession):
    timestamp = datetime(2020, 4, 1, 12, 0)
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
    ]
    features = spark.createDataFrame(
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
            ),
        ],
        columns,
    )
    return build_feature_snapshot(
        features,
        dataset_version="fixture-v1",
        telemetry_source_sha256="source-sha",
        telemetry_ingestion_batch_id="batch-id",
    )


@pytest.mark.spark
def test_feature_snapshot_merge_is_insert_only_and_idempotent(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)

    first = persist_feature_snapshots(snapshots, config)
    repeated = persist_feature_snapshots(snapshots, config)

    target = spark.read.format("delta").load(str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE)))
    assert first.table_name == "gold.feature_snapshots"
    assert (
        first.source_snapshot_count,
        first.inserted_snapshot_count,
        first.unchanged_snapshot_count,
        first.target_snapshot_count_before,
        first.target_snapshot_count_after,
    ) == (2, 2, 0, 0, 2)
    assert (
        repeated.source_snapshot_count,
        repeated.inserted_snapshot_count,
        repeated.unchanged_snapshot_count,
        repeated.target_snapshot_count_before,
        repeated.target_snapshot_count_after,
    ) == (2, 0, 2, 2, 2)
    assert {row.loaded_cycle_id for row in target.collect()} == {"cycle-1", "cycle-2"}


@pytest.mark.spark
def test_feature_snapshot_merge_rejects_conflicting_immutable_row(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)
    persist_feature_snapshots(snapshots, config)
    changed = snapshots.where(F.col("loaded_cycle_id") == "cycle-1").withColumn(
        "motor_current_15m_mean_amperes", F.lit(999.0)
    )

    with pytest.raises(FeatureSnapshotPersistenceError, match="conflicts with its immutable"):
        persist_feature_snapshots(changed, config)

    target = spark.read.format("delta").load(str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE)))
    assert (
        target.where(F.col("loaded_cycle_id") == "cycle-1").first()[
            "motor_current_15m_mean_amperes"
        ]
        == 4.0
    )


@pytest.mark.spark
def test_feature_snapshot_merge_rejects_duplicate_source_and_incompatible_target(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)
    duplicate = snapshots.unionByName(snapshots.limit(1))
    with pytest.raises(FeatureSnapshotPersistenceError, match="contains duplicate"):
        persist_feature_snapshots(duplicate, config)

    target_path = str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE))
    target = snapshots.withColumn(
        "motor_current_15m_observation_count",
        F.col("motor_current_15m_observation_count").cast("string"),
    )
    target.write.format("delta").mode("overwrite").save(target_path)
    with pytest.raises(FeatureSnapshotPersistenceError, match="types differ for"):
        persist_feature_snapshots(snapshots, config)
