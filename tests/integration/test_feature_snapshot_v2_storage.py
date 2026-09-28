from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_storage import gold_table_path
from railpulse.features.feature_snapshot_v2 import build_feature_snapshot_v2
from railpulse.features.feature_snapshot_v2_schema import FEATURE_SNAPSHOTS_V2_TABLE
from railpulse.features.feature_snapshot_v2_storage import (
    FeatureSnapshotV2PersistenceError,
    persist_feature_snapshots_v2,
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
        ],
    )
    context = spark.createDataFrame(
        [
            (
                "cycle-1",
                "loaded-cycle-operating-context-v1",
                "available",
                120,
                "previous-cycle",
                110,
                800,
            ),
            (
                "cycle-2",
                "loaded-cycle-operating-context-v1",
                "left_censored_current_cycle",
                None,
                "cycle-1",
                120,
                None,
            ),
        ],
        "loaded_cycle_id string, cycle_context_feature_version string, "
        "cycle_context_status string, cycle_context_current_duration_seconds long, "
        "cycle_context_previous_cycle_id string, "
        "cycle_context_previous_duration_seconds long, "
        "cycle_context_previous_idle_seconds long",
    )
    return build_feature_snapshot_v2(
        features,
        context,
        dataset_version="fixture-v1",
        telemetry_source_sha256="source-sha",
        telemetry_ingestion_batch_id="batch-id",
    )


@pytest.mark.spark
def test_feature_snapshot_v2_merge_is_insert_only_and_idempotent(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)

    first = persist_feature_snapshots_v2(snapshots, config)
    repeated = persist_feature_snapshots_v2(snapshots, config)

    target = spark.read.format("delta").load(
        str(gold_table_path(config, FEATURE_SNAPSHOTS_V2_TABLE))
    )
    assert first.table_name == "gold.feature_snapshots_v2"
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
def test_feature_snapshot_v2_merge_rejects_conflicting_immutable_row(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)
    persist_feature_snapshots_v2(snapshots, config)
    changed = snapshots.where(F.col("loaded_cycle_id") == "cycle-1").withColumn(
        "cycle_context_previous_idle_seconds", F.lit(999).cast("long")
    )

    with pytest.raises(FeatureSnapshotV2PersistenceError, match="conflicts with its immutable"):
        persist_feature_snapshots_v2(changed, config)

    target = spark.read.format("delta").load(
        str(gold_table_path(config, FEATURE_SNAPSHOTS_V2_TABLE))
    )
    assert (
        target.where(F.col("loaded_cycle_id") == "cycle-1").first()[
            "cycle_context_previous_idle_seconds"
        ]
        == 800
    )


@pytest.mark.spark
def test_feature_snapshot_v2_merge_rejects_invalid_source_and_incompatible_target(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    snapshots = _snapshots(spark)

    unsupported = snapshots.withColumn("cycle_context_feature_version", F.lit("unsupported"))
    with pytest.raises(FeatureSnapshotV2PersistenceError, match="unsupported contract versions"):
        persist_feature_snapshots_v2(unsupported, config)

    duplicate = snapshots.unionByName(snapshots.limit(1))
    with pytest.raises(FeatureSnapshotV2PersistenceError, match="contains duplicate"):
        persist_feature_snapshots_v2(duplicate, config)

    target_path = str(gold_table_path(config, FEATURE_SNAPSHOTS_V2_TABLE))
    target = snapshots.withColumn(
        "cycle_context_current_duration_seconds",
        F.col("cycle_context_current_duration_seconds").cast("string"),
    )
    target.write.format("delta").mode("overwrite").save(target_path)
    with pytest.raises(FeatureSnapshotV2PersistenceError, match="types differ for"):
        persist_feature_snapshots_v2(snapshots, config)
