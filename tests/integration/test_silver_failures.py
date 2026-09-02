from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType

from railpulse.ingestion.bronze import (
    file_sha256,
    load_dataset_artifacts,
    read_failure_reports_bronze,
)
from railpulse.ingestion.schemas import CORRUPT_RECORD_FIELD, FAILURE_RAW_FIELDS
from railpulse.validation.silver_failures import (
    SilverFailureError,
    split_failure_events_by_quality,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FAILURE_REFERENCE = PROJECT_ROOT / "data" / "reference" / "metropt3_failure_events.csv"
DATASET_MANIFEST = PROJECT_ROOT / "docs" / "dataset_manifest.json"


def _failure_frame(spark: SparkSession, **overrides: Any) -> DataFrame:
    values = {
        "source_row_raw": "1",
        "source_report_number_raw": "#1",
        "start_time_raw": "4/18/2020 0:00",
        "end_time_raw": "4/18/2020 23:59",
        "failure_raw": "Air leak",
        "severity_raw": "High stress",
        "report_raw": None,
        "source_note_raw": "As published",
        "record_id": "failure-1",
        CORRUPT_RECORD_FIELD: None,
    }
    values.update(overrides)
    schema = StructType(
        [
            StructField(name, StringType(), nullable=True)
            for name in (*FAILURE_RAW_FIELDS, "record_id", CORRUPT_RECORD_FIELD)
        ]
    )
    return spark.createDataFrame([values], schema=schema)


@pytest.mark.spark
def test_versioned_failure_reference_reconciles_through_bronze_and_silver(
    spark: SparkSession,
) -> None:
    artifacts = load_dataset_artifacts(DATASET_MANIFEST)
    source_sha256 = file_sha256(FAILURE_REFERENCE)
    assert source_sha256 == artifacts.failure_transcription_sha256

    bronze = read_failure_reports_bronze(
        spark,
        FAILURE_REFERENCE,
        source_sha256=source_sha256,
        source_document_sha256=artifacts.failure_source_document_sha256,
        dataset_version=artifacts.dataset_version,
        ingested_at=datetime(2026, 9, 2, 8, 30, tzinfo=UTC),
    )
    split = split_failure_events_by_quality(bronze)
    rows = split.accepted.orderBy("source_row").collect()

    assert split.all_records.count() == 4
    assert len(rows) == 4
    assert split.quarantined.count() == 0
    assert [row.source_row for row in rows] == [1, 2, 3, 4]
    assert [row.source_report_number_raw for row in rows] == ["#1", "#1", "#3", "#4"]
    assert [(row.failure_start, row.failure_end) for row in rows] == [
        (datetime(2020, 4, 18, 0, 0), datetime(2020, 4, 18, 23, 59)),
        (datetime(2020, 5, 29, 23, 30), datetime(2020, 5, 30, 6, 0)),
        (datetime(2020, 6, 5, 10, 0), datetime(2020, 6, 7, 14, 30)),
        (datetime(2020, 7, 15, 14, 30), datetime(2020, 7, 15, 19, 0)),
    ]
    assert rows[1].report_raw == "Maintenance on 30Apr at 12:00"
    assert "unresolved source ambiguity" in rows[1].source_note_raw
    assert {row.source_sha256 for row in rows} == {source_sha256}
    assert {row.source_document_sha256 for row in rows} == {
        artifacts.failure_source_document_sha256
    }
    assert {row.dataset_version for row in rows} == {artifacts.dataset_version}


@pytest.mark.spark
def test_failure_typing_preserves_duplicate_report_labels_and_ambiguous_text(
    spark: SparkSession,
) -> None:
    first = _failure_frame(spark)
    ambiguous = _failure_frame(
        spark,
        source_row_raw="2",
        record_id="failure-2",
        start_time_raw="5/29/2020 23:30",
        end_time_raw="5/30/2020 6:00",
        report_raw="Maintenance on 30Apr at 12:00",
        source_note_raw="Duplicate report number and unresolved source ambiguity",
    )

    split = split_failure_events_by_quality(first.unionByName(ambiguous))
    rows = {row.record_id: row for row in split.accepted.collect()}

    assert set(rows) == {"failure-1", "failure-2"}
    assert split.quarantined.count() == 0
    assert rows["failure-1"].source_row == 1
    assert rows["failure-1"].failure_start == datetime(2020, 4, 18, 0, 0)
    assert rows["failure-1"].failure_end == datetime(2020, 4, 18, 23, 59)
    assert rows["failure-2"].source_report_number_raw == "#1"
    assert rows["failure-2"].report_raw == "Maintenance on 30Apr at 12:00"
    assert "unresolved source ambiguity" in rows["failure-2"].source_note_raw
    assert isinstance(split.accepted.schema["source_row"].dataType, LongType)
    assert isinstance(split.accepted.schema["failure_start"].dataType, TimestampNTZType)
    assert isinstance(split.accepted.schema["failure_end"].dataType, TimestampNTZType)


@pytest.mark.spark
def test_failure_validation_emits_deterministic_structural_reasons(
    spark: SparkSession,
) -> None:
    invalid = _failure_frame(
        spark,
        record_id="invalid",
        source_row_raw="invalid",
        source_report_number_raw=" ",
        start_time_raw="invalid",
        end_time_raw="invalid",
        failure_raw="",
        severity_raw=None,
    )
    reversed_interval = _failure_frame(
        spark,
        source_row_raw="2",
        record_id="reversed",
        start_time_raw="6/7/2020 14:30",
        end_time_raw="6/5/2020 10:00",
    )
    malformed = _failure_frame(
        spark,
        source_row_raw="3",
        record_id="malformed",
        corrupt_record="source,row,with,wrong,shape",
    )

    split = split_failure_events_by_quality(
        invalid.unionByName(reversed_interval).unionByName(malformed)
    )
    rows = {row.record_id: row.rejection_reasons for row in split.quarantined.collect()}

    assert split.accepted.count() == 0
    assert rows["invalid"] == [
        "invalid_source_row",
        "missing_report_number",
        "invalid_failure_start",
        "invalid_failure_end",
        "missing_failure_type",
        "missing_severity",
    ]
    assert rows["reversed"] == ["reversed_failure_interval"]
    assert rows["malformed"] == ["malformed_csv"]


@pytest.mark.spark
def test_failure_validation_marks_duplicate_source_rows_but_not_null_ids(
    spark: SparkSession,
) -> None:
    first_duplicate = _failure_frame(spark, record_id="duplicate-a")
    second_duplicate = _failure_frame(
        spark,
        record_id="duplicate-b",
        source_report_number_raw="#2",
        start_time_raw="5/1/2020 0:00",
        end_time_raw="5/1/2020 1:00",
    )
    invalid_a = _failure_frame(spark, record_id="invalid-a", source_row_raw="invalid-a")
    invalid_b = _failure_frame(spark, record_id="invalid-b", source_row_raw="invalid-b")

    source = (
        first_duplicate.unionByName(second_duplicate).unionByName(invalid_a).unionByName(invalid_b)
    )
    split = split_failure_events_by_quality(source)
    rows = {row.record_id: row.rejection_reasons for row in split.quarantined.collect()}

    assert split.accepted.count() == 0
    assert rows["duplicate-a"] == ["duplicate_source_row"]
    assert rows["duplicate-b"] == ["duplicate_source_row"]
    assert rows["invalid-a"] == ["invalid_source_row"]
    assert rows["invalid-b"] == ["invalid_source_row"]


@pytest.mark.spark
def test_failure_validation_rejects_missing_or_conflicting_columns(
    spark: SparkSession,
) -> None:
    incomplete = spark.createDataFrame([("1",)], ["source_row_raw"])
    with pytest.raises(SilverFailureError, match="missing required columns"):
        split_failure_events_by_quality(incomplete)

    conflicting = _failure_frame(spark).selectExpr("*", "1 AS source_row")
    with pytest.raises(SilverFailureError, match="already contain Silver columns"):
        split_failure_events_by_quality(conflicting)
