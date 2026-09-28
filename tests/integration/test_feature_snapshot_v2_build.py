from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_operating_context import CYCLE_OPERATING_CONTEXT_VERSION
from railpulse.features.cycle_storage import gold_table_path, persist_loaded_cycles
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.features.feature_snapshot import build_feature_snapshot
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_COLUMNS,
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.features.feature_snapshot_storage import persist_feature_snapshots
from railpulse.features.feature_snapshot_v2_build import (
    FEATURE_SNAPSHOT_V2_BUILD_VERSION,
    build_full_source_feature_snapshots_v2,
)
from railpulse.features.feature_snapshot_v2_schema import (
    FEATURE_SNAPSHOT_V2_VERSION,
    FEATURE_SNAPSHOTS_V2_TABLE,
)

SOURCE_SHA256 = "source-sha"
INGESTION_BATCH_ID = "batch-id"


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
            catalog="railpulse_test",
            bronze="bronze",
            silver="silver",
            gold="gold",
        ),
    )


def _cycle_id(start_record_id: str) -> str:
    value = f"{LOADED_CYCLE_ID_VERSION}|{start_record_id}".encode()
    return hashlib.sha256(value).hexdigest()


def _cycles(spark: SparkSession):
    start = datetime(2020, 4, 1, 12, 0)
    return spark.createDataFrame(
        [
            (
                _cycle_id("start-1"),
                "start-1",
                "observed",
                start,
                "stop-1",
                start + timedelta(seconds=120),
                12,
                False,
                120,
            ),
            (
                _cycle_id("start-2"),
                "start-2",
                "observed",
                start + timedelta(seconds=920),
                "stop-2",
                start + timedelta(seconds=1030),
                11,
                False,
                110,
            ),
        ],
        "loaded_cycle_id string, loaded_cycle_start_record_id string, "
        "loaded_cycle_start_type string, loaded_cycle_start_timestamp timestamp, "
        "loaded_cycle_stop_record_id string, loaded_cycle_stop_timestamp timestamp, "
        "loaded_observation_count long, is_right_censored boolean, "
        "observed_duration_seconds long",
    )


def _source_snapshots(spark: SparkSession, config: RailPulseConfig):
    timestamp = datetime(2020, 4, 1, 12, 0)
    features = spark.createDataFrame(
        [
            (
                _cycle_id("start-1"),
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
                _cycle_id("start-2"),
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
    return build_feature_snapshot(
        features,
        dataset_version=config.dataset_version,
        telemetry_source_sha256=SOURCE_SHA256,
        telemetry_ingestion_batch_id=INGESTION_BATCH_ID,
    )


@pytest.mark.spark
def test_full_source_snapshot_v2_build_persists_rerun_and_preserves_v1(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    persist_loaded_cycles(_cycles(spark), config)
    persist_feature_snapshots(_source_snapshots(spark, config), config)
    source_path = str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE))
    source_before = {
        row.loaded_cycle_id: row.asDict(recursive=True)
        for row in spark.read.format("delta")
        .load(source_path)
        .select(*FEATURE_SNAPSHOT_COLUMNS)
        .collect()
    }

    first = build_full_source_feature_snapshots_v2(spark, config)
    second = build_full_source_feature_snapshots_v2(spark, config)

    target = spark.read.format("delta").load(
        str(gold_table_path(config, FEATURE_SNAPSHOTS_V2_TABLE))
    )
    source_after = {
        row.loaded_cycle_id: row.asDict(recursive=True)
        for row in spark.read.format("delta")
        .load(source_path)
        .select(*FEATURE_SNAPSHOT_COLUMNS)
        .collect()
    }
    row = target.first()

    assert first.build_version == second.build_version == FEATURE_SNAPSHOT_V2_BUILD_VERSION
    assert first.source_snapshot_version == FEATURE_SNAPSHOT_VERSION
    assert first.snapshot_version == FEATURE_SNAPSHOT_V2_VERSION
    assert first.dataset_version == config.dataset_version
    assert first.telemetry_source_sha256 == SOURCE_SHA256
    assert first.context_profile.feature_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert first.context_profile.cycle_count == 2
    assert first.source_snapshot_count == first.source_snapshot_count_after == 2
    assert (
        first.write.source_snapshot_count,
        first.write.inserted_snapshot_count,
        first.write.unchanged_snapshot_count,
    ) == (2, 2, 0)
    assert (
        second.write.source_snapshot_count,
        second.write.inserted_snapshot_count,
        second.write.unchanged_snapshot_count,
    ) == (2, 0, 2)
    assert second.write.target_snapshot_count_before == 2
    assert second.write.target_snapshot_count_after == 2
    assert target.count() == 2
    assert row.feature_snapshot_version == FEATURE_SNAPSHOT_V2_VERSION
    assert row.cycle_context_feature_version == CYCLE_OPERATING_CONTEXT_VERSION
    assert source_before == source_after
