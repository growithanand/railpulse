from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from railpulse.config import load_config
from railpulse.ingestion.bronze import file_sha256, read_telemetry_bronze
from railpulse.validation.silver_storage import (
    TELEMETRY_ACCEPTED_TABLE,
    TELEMETRY_QUARANTINE_TABLE,
    SilverTelemetryPersistenceError,
    persist_telemetry_quality_split,
    silver_table_path,
)
from railpulse.validation.silver_telemetry import (
    TelemetryQualitySplit,
    split_telemetry_by_quality,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TELEMETRY_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "metropt3_telemetry_sample.csv"
INGESTED_AT = datetime(2026, 9, 2, 8, 30, tzinfo=UTC)


def _test_config(tmp_path: Path):
    config_path = tmp_path / "test.toml"
    delta_path = (tmp_path / "delta").as_posix()
    config_path.write_text(
        f"""
[project]
name = "railpulse-test"
environment = "test"
dataset_version = "fixture-v1"

[paths]
raw_data = "raw"
processed_data = "processed"
delta = "{delta_path}"
checkpoints = "checkpoints"
artifacts = "artifacts"

[schemas]
catalog = "railpulse_test"
bronze = "bronze"
silver = "silver"
gold = "gold"
""".strip(),
        encoding="utf-8",
    )
    return load_config(config_path, project_root=tmp_path)


def _bronze_fixture(spark: SparkSession):
    return read_telemetry_bronze(
        spark,
        TELEMETRY_FIXTURE,
        source_sha256=file_sha256(TELEMETRY_FIXTURE),
        dataset_version="fixture-v1",
        ingested_at=INGESTED_AT,
    )


def _validated_fixture(spark: SparkSession):
    bronze = _bronze_fixture(spark)
    with_outlier = bronze.withColumn(
        "tp2_raw",
        F.when(F.col("source_index_raw") == "10", F.lit("99.000")).otherwise(F.col("tp2_raw")),
    )
    return split_telemetry_by_quality(with_outlier)


@pytest.mark.spark
def test_telemetry_silver_tables_are_separate_reconciled_and_idempotent(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    split = _validated_fixture(spark)

    first = persist_telemetry_quality_split(split, config)
    second = persist_telemetry_quality_split(split, config)

    assert first.total_record_count == second.total_record_count == 3
    assert first.accepted.table_name == "silver.telemetry_accepted"
    assert first.quarantined.table_name == "silver.telemetry_quarantine"
    assert (first.accepted.source_record_count, first.accepted.inserted_record_count) == (2, 2)
    assert (first.quarantined.source_record_count, first.quarantined.inserted_record_count) == (
        1,
        1,
    )
    assert (second.accepted.inserted_record_count, second.accepted.target_record_count_after) == (
        0,
        2,
    )
    assert (
        second.quarantined.inserted_record_count,
        second.quarantined.target_record_count_after,
    ) == (0, 1)

    accepted = spark.read.format("delta").load(
        str(silver_table_path(config, TELEMETRY_ACCEPTED_TABLE))
    )
    quarantined = spark.read.format("delta").load(
        str(silver_table_path(config, TELEMETRY_QUARANTINE_TABLE))
    )
    accepted_rows = accepted.orderBy("source_index").collect()
    quarantined_rows = quarantined.collect()

    assert [row.source_index for row in accepted_rows] == [0, 20]
    assert all(row.rejection_reasons == [] for row in accepted_rows)
    assert len(quarantined_rows) == 1
    assert quarantined_rows[0].source_index == 10
    assert quarantined_rows[0].tp2_raw == "99.000"
    assert quarantined_rows[0].tp2 == 99.0
    assert quarantined_rows[0].rejection_reasons == ["out_of_range_tp2"]
    assert accepted.select("record_id").intersect(quarantined.select("record_id")).count() == 0


@pytest.mark.spark
def test_telemetry_silver_persistence_creates_an_empty_quarantine_table(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    split = split_telemetry_by_quality(_bronze_fixture(spark))

    result = persist_telemetry_quality_split(split, config)
    quarantine = spark.read.format("delta").load(
        str(silver_table_path(config, TELEMETRY_QUARANTINE_TABLE))
    )

    assert result.total_record_count == 3
    assert (result.accepted.source_record_count, result.accepted.inserted_record_count) == (3, 3)
    assert (
        result.quarantined.source_record_count,
        result.quarantined.inserted_record_count,
        result.quarantined.target_record_count_after,
    ) == (0, 0, 0)
    assert quarantine.count() == 0
    assert quarantine.schema.fieldNames() == split.all_records.schema.fieldNames()
    assert [field.dataType for field in quarantine.schema] == [
        field.dataType for field in split.all_records.schema
    ]


@pytest.mark.spark
def test_telemetry_silver_persistence_rejects_duplicate_record_ids_before_writes(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    split = _validated_fixture(spark)
    duplicated = split.all_records.unionByName(split.all_records.limit(1))
    invalid_split = TelemetryQualitySplit(
        all_records=duplicated,
        accepted=split.accepted,
        quarantined=split.quarantined,
    )

    with pytest.raises(SilverTelemetryPersistenceError, match="duplicate record_id"):
        persist_telemetry_quality_split(invalid_split, config)

    assert not silver_table_path(config, TELEMETRY_ACCEPTED_TABLE).exists()
    assert not silver_table_path(config, TELEMETRY_QUARANTINE_TABLE).exists()
