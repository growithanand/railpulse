from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_build import build_full_source_cycles
from railpulse.features.cycle_storage import gold_table_path
from railpulse.features.feature_snapshot_build import (
    FEATURE_SNAPSHOT_BUILD_VERSION,
    build_full_source_feature_snapshots,
)
from railpulse.features.feature_snapshot_schema import (
    FEATURE_SNAPSHOT_VERSION,
    FEATURE_SNAPSHOTS_TABLE,
)
from railpulse.ingestion.bronze import (
    TELEMETRY_TABLE,
    bronze_table_path,
    file_sha256,
    merge_bronze_records,
    read_telemetry_bronze,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TELEMETRY_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "metropt3_telemetry_sample.csv"
INGESTED_AT = datetime(2026, 9, 2, 8, 30, tzinfo=UTC)


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


@pytest.mark.spark
def test_full_source_snapshot_build_persists_and_reconciles_rerun(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    source_sha256 = file_sha256(TELEMETRY_FIXTURE)
    bronze = read_telemetry_bronze(
        spark,
        TELEMETRY_FIXTURE,
        source_sha256=source_sha256,
        dataset_version=config.dataset_version,
        ingested_at=INGESTED_AT,
    )
    merge_bronze_records(
        bronze,
        bronze_table_path(config, TELEMETRY_TABLE),
        table_name=TELEMETRY_TABLE,
        source_sha256=source_sha256,
        dataset_version=config.dataset_version,
    )
    build_full_source_cycles(spark, config)

    first = build_full_source_feature_snapshots(spark, config)
    second = build_full_source_feature_snapshots(spark, config)
    target = spark.read.format("delta").load(str(gold_table_path(config, FEATURE_SNAPSHOTS_TABLE)))
    row = target.first()

    assert first.build_version == second.build_version == FEATURE_SNAPSHOT_BUILD_VERSION
    assert first.snapshot_version == FEATURE_SNAPSHOT_VERSION
    assert first.profile.dataset_version == config.dataset_version
    assert first.profile.telemetry_source_sha256 == source_sha256
    assert first.profile.accepted_telemetry_record_count == 3
    assert first.profile.cycle_count == 1
    assert (
        first.write.source_snapshot_count,
        first.write.inserted_snapshot_count,
        first.write.unchanged_snapshot_count,
    ) == (1, 1, 0)
    assert (
        second.write.source_snapshot_count,
        second.write.inserted_snapshot_count,
        second.write.unchanged_snapshot_count,
    ) == (1, 0, 1)
    assert second.write.target_snapshot_count_before == 1
    assert second.write.target_snapshot_count_after == 1
    assert target.count() == 1
    assert row.feature_snapshot_version == FEATURE_SNAPSHOT_VERSION
    assert row.telemetry_source_sha256 == source_sha256
