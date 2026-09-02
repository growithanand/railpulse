from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from railpulse.config import load_config
from railpulse.ingestion.bronze import (
    FAILURE_TABLE,
    TELEMETRY_TABLE,
    BronzeIngestionError,
    bronze_table_path,
    file_sha256,
    ingest_failure_reports,
    ingest_telemetry,
    read_telemetry_bronze,
)
from railpulse.ingestion.schemas import TELEMETRY_RAW_FIELDS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"
INGESTED_AT = datetime(2026, 9, 2, 8, 30, tzinfo=UTC)


def _test_config(tmp_path: Path):
    config_path = tmp_path / "test.toml"
    config_path.write_text(
        f"""
[project]
name = "railpulse-test"
environment = "test"
dataset_version = "fixture-v1"

[paths]
raw_data = "raw"
processed_data = "processed"
delta = "{(tmp_path / "delta").as_posix()}"
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


@pytest.mark.spark
def test_telemetry_ingestion_preserves_raw_values_and_is_idempotent(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    source = FIXTURES / "metropt3_telemetry_sample.csv"
    checksum = file_sha256(source)

    first = ingest_telemetry(
        spark,
        config,
        source,
        expected_sha256=checksum,
        ingested_at=INGESTED_AT,
    )
    second = ingest_telemetry(
        spark,
        config,
        source,
        expected_sha256=checksum,
        ingested_at=datetime(2026, 9, 2, 9, 30, tzinfo=UTC),
    )

    assert (first.source_row_count, first.inserted_row_count) == (3, 3)
    assert (first.target_row_count_before, first.target_row_count_after) == (0, 3)
    assert (second.source_row_count, second.inserted_row_count) == (3, 0)
    assert (second.target_row_count_before, second.target_row_count_after) == (3, 3)
    assert first.ingestion_batch_id == second.ingestion_batch_id

    target_path = bronze_table_path(config, TELEMETRY_TABLE)
    target = spark.read.format("delta").load(str(target_path))
    rows = target.orderBy("source_index_raw").collect()

    assert target.count() == target.select("record_id").distinct().count() == 3
    assert rows[0].tp2_raw == "1.000"
    assert rows[0].event_timestamp_raw == "2020-02-01 00:00:00"
    assert rows[0].source_filename == source.name
    assert rows[0].source_sha256 == checksum
    assert rows[0].dataset_version == "fixture-v1"
    ingestion_time_text = (
        target.select(F.date_format("ingested_at", "yyyy-MM-dd HH:mm:ss").alias("value"))
        .first()
        .value
    )
    assert ingestion_time_text == "2026-09-02 08:30:00"
    assert all(row.corrupt_record is None for row in rows)
    assert all(
        isinstance(target.schema[name].dataType, StringType) for name in TELEMETRY_RAW_FIELDS
    )

    history = DeltaTable.forPath(spark, str(target_path)).history().collect()
    assert [row.operation for row in history] == ["WRITE"]


@pytest.mark.spark
def test_failure_ingestion_is_separate_traceable_and_idempotent(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    source = FIXTURES / "metropt3_failure_sample.csv"
    checksum = file_sha256(source)
    document_checksum = "b" * 64

    first = ingest_failure_reports(
        spark,
        config,
        source,
        expected_sha256=checksum,
        source_document_sha256=document_checksum,
        ingested_at=INGESTED_AT,
    )
    second = ingest_failure_reports(
        spark,
        config,
        source,
        expected_sha256=checksum,
        source_document_sha256=document_checksum,
        ingested_at=INGESTED_AT,
    )

    assert (first.source_row_count, first.inserted_row_count) == (2, 2)
    assert (second.source_row_count, second.inserted_row_count) == (2, 0)
    assert second.target_row_count_after == 2

    target = spark.read.format("delta").load(str(bronze_table_path(config, FAILURE_TABLE)))
    rows = target.orderBy("source_row_raw").collect()
    assert [row.source_report_number_raw for row in rows] == ["#1", "#1"]
    assert rows[1].report_raw == "Maintenance on 30Apr at 12:00"
    assert {row.source_document_sha256 for row in rows} == {document_checksum}


@pytest.mark.spark
def test_malformed_telemetry_row_is_retained_as_corrupt(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source = tmp_path / "malformed.csv"
    source.write_text(
        (FIXTURES / "metropt3_telemetry_sample.csv").read_text(encoding="utf-8").splitlines()[0]
        + "\n30,2020-02-01 00:00:30,1.0\n",
        encoding="utf-8",
    )

    frame = read_telemetry_bronze(
        spark,
        source,
        source_sha256=file_sha256(source),
        dataset_version="fixture-v1",
        ingested_at=INGESTED_AT,
    )
    row = frame.collect()[0]

    assert row.source_index_raw == "30"
    assert row.tp2_raw == "1.0"
    assert row.caudal_impulses_raw is None
    assert row.corrupt_record == "30,2020-02-01 00:00:30,1.0"


@pytest.mark.spark
def test_duplicate_source_keys_fail_without_writing_a_table(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    config = _test_config(tmp_path)
    fixture_lines = (
        (FIXTURES / "metropt3_telemetry_sample.csv").read_text(encoding="utf-8").splitlines()
    )
    source = tmp_path / "duplicate.csv"
    source.write_text("\n".join([fixture_lines[0], fixture_lines[1], fixture_lines[1]]) + "\n")

    with pytest.raises(BronzeIngestionError, match="duplicate source key"):
        ingest_telemetry(
            spark,
            config,
            source,
            expected_sha256=file_sha256(source),
            ingested_at=INGESTED_AT,
        )

    assert not bronze_table_path(config, TELEMETRY_TABLE).exists()
