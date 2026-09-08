from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from railpulse.config import RailPulseConfig, SchemaNames, StoragePaths
from railpulse.features.cycle_build import CYCLE_BUILD_VERSION, build_full_source_cycles
from railpulse.features.cycle_profile import CYCLE_PROFILE_VERSION
from railpulse.features.cycle_storage import LOADED_CYCLES_TABLE, gold_table_path
from railpulse.features.cycles import LOADED_CYCLE_ID_VERSION
from railpulse.ingestion.bronze import (
    TELEMETRY_TABLE,
    bronze_table_path,
    file_sha256,
    merge_bronze_records,
    read_telemetry_bronze,
)
from railpulse.validation.silver_storage import TELEMETRY_VALIDATION_VERSION

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
def test_full_source_cycle_build_profiles_persists_and_reconciles_rerun(
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

    first = build_full_source_cycles(spark, config)
    second = build_full_source_cycles(spark, config)
    target = spark.read.format("delta").load(str(gold_table_path(config, LOADED_CYCLES_TABLE)))
    cycle = target.first()

    assert first.build_version == second.build_version == CYCLE_BUILD_VERSION
    assert first.profile.profile_version == CYCLE_PROFILE_VERSION
    assert first.profile.cycle_id_version == LOADED_CYCLE_ID_VERSION
    assert first.profile.telemetry_validation_version == TELEMETRY_VALIDATION_VERSION
    assert first.profile.dataset_version == config.dataset_version
    assert first.profile.source_sha256 == source_sha256
    assert first.profile.ingestion_batch_id
    assert first.profile.telemetry_quality.total_record_count == 3
    assert first.profile.telemetry_quality.accepted_record_count == 3
    assert first.profile.telemetry_quality.quarantined_record_count == 0
    assert first.profile.loaded_cycles.cycle_count == 1
    assert first.profile.loaded_cycles.right_censored_cycle_count == 1
    assert (
        first.write.source_cycle_count,
        first.write.inserted_cycle_count,
        first.write.updated_cycle_count,
        first.write.unchanged_cycle_count,
    ) == (1, 1, 0, 0)
    assert (
        second.write.source_cycle_count,
        second.write.inserted_cycle_count,
        second.write.updated_cycle_count,
        second.write.unchanged_cycle_count,
    ) == (1, 0, 0, 1)
    assert second.write.target_cycle_count_before == 1
    assert second.write.target_cycle_count_after == 1
    assert target.count() == 1
    assert cycle.loaded_cycle_start_record_id
    assert cycle.loaded_observation_count == 2
    assert cycle.is_right_censored is True
